from peewee import SQL

from .sql import QueryParser
from .mapper import RemoteField, RemoteMap
from .sql_executor_base import SQLExecutorBase


class SQLExecutor(SQLExecutorBase):
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',
        'where_branch': '(branch in (%s, \'master\')) AND '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def execute_data_query_by_id(self, schema, _id):
        model = self._get_model_from_schema(schema)
        return self._execute_query(self._get_query_by_id(model, _id))

    def execute_data_query(self, schema, filter_name, params):
        page = params.get('page')
        branch = params.get('branch')
        page_size = params.get('page_size')
        model = self._get_model_from_schema(schema)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        return self._execute_query(self._get_data_query(model, user_query_filter, branch, page, page_size))

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

    def _get_data_query(self, model, user_query_filter, branch, page, page_size = 20):
        base_query = self._get_query(model, user_query_filter, branch)
        if page and page_size:
            base_query = base_query.paginate(int(page), int(page_size))
        return base_query

    def _get_query(self, model, user_query_filter, branch):
        where_statement = ''
        branch_param = branch or 'master'
        where_params = (branch_param, branch_param,)

        query = model.build(self.db).select()
        user_has_query = self._user_has_query(user_query_filter)
        user_has_params = self._user_has_params(user_query_filter)

        where_statement += self._apply_branch_query(model)
        where_statement += self._apply_deleted_query(user_has_query)
        where_statement += self._apply_user_query(user_query_filter)

        if user_has_params:
            where_params += self._get_user_query_parameters(user_query_filter)

        return query.where(SQL(where_statement, where_params))

    def _apply_deleted_query(self, user_has_query):
        query = self.QUERIES['not_deleted']
        if user_has_query:
            query += ' AND '
        return query

    def _apply_branch_query(self, model):
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
