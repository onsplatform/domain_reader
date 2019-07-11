from peewee import SQL
import autologging

from platform_sdk.domain.schema.api import SchemaApi

from .mapper import RemoteField, RemoteMap
from .sql import QueryParser


@autologging.traced("get_data", exclude=True)
@autologging.logged
class DomainReader:
    def __init__(self, orm, db_settings, schema_settings):
        self.orm = orm
        self.db = orm.db_factory('postgres', **db_settings)()
        self.schema_api = SchemaApi(schema_settings)

        # wrapping local tracer
        self._trace_local = lambda v, m:\
            self._DomainReader__log.log(
                msg=f'{v}:{m}', level=autologging.TRACE)

    def get_data(self, _map, _type, filter_name, params, history=False):
        self._trace_local('###### get_data ######', _map)
        params = {k: v for k, v in params.items() if k and v and v != 'null'}
        self._trace_local('params', params)
        import ipdb; ipdb.set_trace()
        api_response = self.schema_api.get_schema(_map, _type)

        if api_response:
            self._trace_local('api_response', api_response.get('id'))

            model = self._get_model(
                api_response['model'], api_response['fields'], history)
            self._trace_local('model', model)

            sql_filter = self._get_sql_filter(
                filter_name, api_response['filters'])
            self._trace_local('sql_filter', sql_filter)

            sql_query = self._get_sql_query(sql_filter, params)
            self._trace_local('sql_query', sql_query)

            data = self._execute_query(model, sql_query)
            self._trace_local('data size', len(data or []))

            return self._get_response_data(data, api_response['fields'])

    def _execute_query(self, model, sql_query):  # pragma: no cover
        proxy_model = model.build(self.db)
        try:
            query = proxy_model.select()
            if (sql_query and sql_query['sql_query']):
                sql = SQL(sql_query['sql_query'], sql_query['query_params'])
                query = query.where(sql)
        except Exception as e:
            self._trace_local('_execute_query ERROR: ', e)
        return query

    def _get_sql_query(self, sql_filter, params):
        if sql_filter:
            parser = QueryParser(sql_filter)
            query, params = parser.parse(params)

            return {
                'sql_query': query,
                'query_params': params
            }

    def _get_response_data(self, entities, fields):
        if entities:
            return [{f['alias']: getattr(e, f['alias']) for f in fields}
                    for e in entities]

    def _get_fields(self, fields):
        return [RemoteField(
            f['alias'], f['field_type'], f['column_name']) for f in fields]

    def _get_model(self, model, fields, history=False):
        return RemoteMap(
            model['name'], model['table'], self._get_fields(fields), self.orm, history)

    def _get_sql_filter(self, filter_name, filters):
        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)
