from peewee import SQL

from .sql import QueryParser
from .mapper import RemoteField, RemoteMap
from .sql_executor_base import SQLExecutorBase


class SQLExecutor(SQLExecutorBase):
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',
        'where_specific_branch': 'branch = %s AND ',
        'where_branch': '(branch in (%s, \'master\')) AND '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def execute_data_query_by_id(self, schema, _id):
        model = self._get_model_from_schema(schema)
        return self._execute_query(self._get_query_by_id(model, _id))

    def execute_data_query(self, schema, filter_name, params):
        branch, page, page_size = self._get_default_params(params)
        model = self._get_model_from_schema(schema)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        query = self._get_query(model, user_query_filter, branch)
        if page and page_size:
            query = query.paginate(int(page), int(page_size))
        return self._execute_query(query)

    def execute_data_query_disabled_branch(self, schema, filter_name, params):
        branch, page, page_size = self._get_default_params(params)

        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)

        model = self._get_model_from_schema(schema)
        model_build = model.build(self.db)

        data_disabled = '2020-04-20 20:01:02'

        query_master = model_build.select()
        where_statement = self._get_where_statement(model, user_query_filter)
        where_params = self._get_where_params(branch, user_query_filter)
        query_master = query_master.where(SQL('((modified is null and date_created <= %s) or modified <= %s)',
                                              (data_disabled, data_disabled,))) \
            .where(SQL(where_statement, where_params))

        model_history = self._get_model_from_schema(schema, True)
        model_history_build = model_history.build(self.db)
        query_master_history = model_history_build.select()
        where_statement = self._get_where_statement(model_history, user_query_filter)
        where_params = self._get_where_params(branch, user_query_filter)
        query_master_history = query_master_history.where(
            SQL('(%s between COALESCE(modified, date_created) and modified_until)',
                (data_disabled,))) \
            .where(SQL(where_statement, where_params))

        query = query_master.union(query_master_history).order_by(model_build.modified_at)

        if page and page_size:
            query = query.paginate(int(page), int(page_size))

        return self._execute_query(query)

    def _get_default_params(self, params):
        page = params.get('page')
        branch = params.get('branch')
        page_size = params.get('page_size', 20)
        return branch, page, page_size

    def execute_history_data_query(self, schema, _id):
        model = self._get_model_from_schema(schema, True)
        return self._execute_query(self._get_query_by_id(model, _id))

    def execute_count_query(self, schema, filter_name, params):
        branch = params.get('branch')
        model = self._get_model_from_schema(schema)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        return self._execute_query(self._get_count_query(model, user_query_filter, branch))

    def _get_model_from_schema(self, schema, history=False):
        return self._get_model(schema['model'], schema['fields'] + schema['metadata'], history)

    def _get_user_query_filter(self, filters, filter_name, params):
        sql_filter = self._get_sql_filter(filter_name, filters)
        params = {k: v for k, v in params.items() if k and v}
        return self._get_sql_query(sql_filter, params)

    def _get_query_by_id(self, model, _id):
        query = model.build(self.db).select()
        return query.where(SQL('id = %s', (_id,)))

    def _get_count_query(self, model, user_query_parameters, branch):
        return self._get_query(model, user_query_parameters, branch).count()

    def _get_query(self, model, user_query_filter, branch, specific_branch=False):
        query = model.build(self.db).select()
        where_statement = self._get_where_statement(model, user_query_filter, specific_branch)
        where_params = self._get_where_params(branch, user_query_filter, specific_branch)
        return query.where(SQL(where_statement, where_params))

    def _get_where_params(self, branch, user_query_filter, specific_branch=False):
        branch_param = branch or 'master'
        where_params = (branch_param, branch_param,)
        if specific_branch:
            where_params = (branch_param,)

        if self._user_has_params(user_query_filter):
            where_params += self._get_user_query_parameters(user_query_filter)

        return where_params

    def _get_where_statement(self, model, user_query_filter, specific_branch=False):
        user_has_query = self._user_has_query(user_query_filter)
        where_statement = ''
        where_statement += self._apply_branch_query(model, specific_branch)
        where_statement += self._apply_deleted_query(user_has_query)
        where_statement += self._apply_user_query(user_query_filter)
        return where_statement

    def _apply_deleted_query(self, user_has_query):
        query = self.QUERIES['not_deleted']
        if user_has_query:
            query += ' AND '
        return query

    def _apply_branch_query(self, model, specific_branch=False):
        if specific_branch:
            return self.QUERIES['where_specific_branch'].format(table=model.table)
        return self.QUERIES['where_branch'].format(table=model.table)

    @staticmethod
    def _apply_user_query(user_query_filter):
        if user_query_filter and user_query_filter['sql_query']:
            return user_query_filter['sql_query']
        return ' '

    @staticmethod
    def _get_user_query_parameters(user_query_filter):
        if user_query_filter and user_query_filter['sql_query']:
            return user_query_filter['query_params']

    @staticmethod
    def _user_has_query(user_query_filter):
        return user_query_filter and user_query_filter['sql_query']

    @staticmethod
    def _user_has_params(user_query_filter):
        return user_query_filter and user_query_filter['query_params']

    @staticmethod
    def _get_sql_query(sql_filter, params):
        if sql_filter and params:
            parser = QueryParser(sql_filter)
            query, params = parser.parse(params)
            return {
                'sql_query': query,
                'query_params': params
            }

    @staticmethod
    def _get_fields(fields):
        return [RemoteField(
            f['alias'], f['field_type'], f['column_name']) for f in fields]

    def _get_model(self, model, fields, history=False):
        return RemoteMap(model['name'], model['table'], self._get_fields(fields), self.orm, history)

    @staticmethod
    def _get_sql_filter(filter_name, filters):
        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)
