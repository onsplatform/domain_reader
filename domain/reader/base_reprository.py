import pytz
import datetime

from peewee import SQL

from domain.reader.orms.query_parser import QueryParser
from domain.reader.orms.mapper import RemoteField, RemoteMap


class RepositoryBase:
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',
        'where_specific_branch': 'branch = %s AND ',
        'where_branch': '(branch in (%s, \'master\')) AND '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings):
        self.orm = orm
        self.db_settings = db_settings
        self._create_db(orm, db_settings)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def _execute_query(self, query):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                return query
        except Exception as e:
            self._trace_local('##### _execute_query ##### ERROR ', e)
        finally:
            self.db.close()

    def execute_data_query_by_id(self, schema, params):
        model = self._get_model_from_schema(schema)
        return self._execute_query(self._get_query_by_id(model, params['id']))

    def _get_query_at_time(self, date_validity, filter_name, params, schema):
        date_validity = datetime.datetime.strptime(date_validity, '%Y-%m-%dT%H:%M:%SZ')
        pst = pytz.timezone('Etc/GMT-3')
        date_validity = pst.localize(date_validity)
        branch, page, page_size = self._get_default_params(params)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        model = self._get_model_from_schema(schema)
        model_build = model.build(self.db)
        query_master = model_build.select().where(SQL('reproduction_id is null'))
        where_statement = self._get_where_statement(model, user_query_filter)
        where_params = self._get_where_params(branch, user_query_filter)
        query_master = query_master.where(
            SQL('((modified is null and date_created <= %s) or modified <= %s)', (date_validity, date_validity,))
        ).where(SQL(where_statement, where_params))
        model_history = self._get_model_from_schema(schema, True)
        model_history_build = model_history.build(self.db)
        query_master_history = model_history_build.select().where(SQL('reproduction_id is null'))
        where_statement = self._get_where_statement(model_history, user_query_filter)
        where_params = self._get_where_params(branch, user_query_filter)
        query_master_history = query_master_history.where(
            SQL('(%s between COALESCE(modified, date_created) and modified_until)', (date_validity,))
        ).where(SQL(where_statement, where_params))
        query = query_master.union(query_master_history).order_by(model_build.modified_at)
        return query

    def _get_default_params(self, params):
        page = params.get('page')
        branch = params.get('branch')
        page_size = params.get('page_size', 20)
        return branch, page, page_size

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
        return query.where(SQL(where_statement, where_params)).where(SQL('reproduction_id is null'))

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
