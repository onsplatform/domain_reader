from peewee import SQL
from pydash import objects

from .sql import QueryParser
from .mapper import RemoteField, RemoteMap


class SQLExecutor:
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',
        'where_branch': '(branch in (%s, \'master\')) and '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings):
        self.orm = orm
        self.db_settings = db_settings
        self._create_db(orm, db_settings)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def execute_data_query_by_id(self, schema, _id):
        model = self._get_model_from_schema(schema)
        return self._execute_query(self._get_query_by_id(model, _id))

    def execute_data_query(self, schema, filter_name, params):
        page = params.get('page')
        branch = params.get('branch')
        page_size = params.get('page_size')
        model = self._get_model_from_schema(schema)
        sql_properties = self._get_sql_properties(schema['filters'], filter_name, params)
        return self._execute_query(self._get_data_query(model, sql_properties, branch, page, page_size))

    def execute_history_data_query(self, schema, _id):
        model = self._get_model_from_schema(schema)
        return self._execute_query(self._get_query_by_id(model, _id))

    def execute_count_query(self, schema, user_query_parameters, branch):
        model = self._get_model_from_schema(schema)
        query = self._get_count_query(model, user_query_parameters, branch).count()
        return self._execute_query(query)

    def _get_model_from_schema(self, schema):
        return self._get_model(schema['model'], schema['fields'] + schema['metadata'])

    def _get_sql_properties(self, filters, filter_name, params, history=False):
        sql_filter = self._get_sql_filter(filter_name, filters, history)
        params = {k: v for k, v in params.items() if k and v}
        sql_query = self._get_sql_query(sql_filter, params)
        return sql_query

    def _get_query_by_id(self, model, _id):
        query = model.build(self.db).select()
        return query.where(SQL('id = %id', _id))

    def _get_count_query(self, model, user_query_parameters, branch):
        return self._get_base_query(model, user_query_parameters, branch).count()

    def _get_data_query(self, model, user_query_parameters, branch, page, page_size):
        base_query = self._get_base_query(model, user_query_parameters, branch)
        if page and page_size:
            base_query = base_query.paginate(int(page), int(page_size))
        return base_query

    def _get_base_query(self, model, user_query_parameters, branch):
        user_sql = ''
        query_params = ()
        query_not_deleted = self.QUERIES['not_deleted']

        import pdb;pdb.set_trace()
        query_branch = self.QUERIES['where_branch'].format(table=model.table)
        query = model.build(self.db).select()

        if user_query_parameters and user_query_parameters['sql_query']:
            user_sql = user_query_parameters['sql_query']
            query_params = user_query_parameters['query_params']
        query_params = (branch, branch,) + query_params
        if user_sql:
            query_not_deleted += ' AND '
        query = query.where(SQL(
            query_branch + query_not_deleted + user_sql,
            query_params
        ))
        return query

    def _execute_query(self, query):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                return query
        except Exception as e:
            self._trace_local('##### _execute_query ##### ERROR ', e)
        finally:
            self.db.close()

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
    def _get_sql_filter(filter_name, filters, history):
        if history:
            return 'id = :id'
        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)
