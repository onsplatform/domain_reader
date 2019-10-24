import autologging

from platform_sdk.domain.schema.api import SchemaApi
from .sql_executor import SQLExecutor


@autologging.logged
class DomainReader(SQLExecutor):

    def __init__(self, orm, db_settings, schema_settings):
        super().__init__(orm, db_settings)

        self.schema_api = SchemaApi(schema_settings)
        self._trace_local = lambda v, m: self._DomainReader__log.log(msg=f'{v}:{m}', level=autologging.TRACE)

    def get_data(self, _map, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _type)
        return self._get_data(schema, filter_name, params)

    def get_history_data(self, _map, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _type)
        ret = self._get_history_data(schema, filter_name, params)
        if ret:
            return ret
        return self._get_data_by_id(schema, filter_name, params)

    def get_data_count(self, _map, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _type)
        if schema:
            sql_properties = self._get_sql_properties(schema, filter_name, params)
            return self.execute_count_query(**sql_properties)

    def _get_data(self, schema, filter_name, params):
        if schema:
            ret = self.execute_data_query(schema, filter_name, params)
            return list(self._get_response_data(ret, schema['fields'], schema['metadata']))

    def _get_history_data(self, schema, filter_name, params):
        if schema:
            sql_properties = self._get_sql_properties(schema, filter_name, params)
            ret = self.execute_data_query(**sql_properties)
            return list(self._get_response_data(ret, schema['fields'], schema['metadata']))

    def _get_data_by_id(self, schema, filter_name, params):
        if schema:
            sql_properties = self._get_sql_properties(schema, filter_name, params)
            ret = self.execute_data_query_by_id(**sql_properties)
            return list(self._get_response_data(ret, schema['fields'], schema['metadata']))

    @staticmethod
    def _get_response_data(entities, fields, metadata):
        if entities:
            for entity in entities:
                dic = {field['alias']: getattr(entity, field['alias']) for field in fields}
                dic['_metadata'] = {meta['alias']: getattr(entity, meta['alias']) for meta in metadata}
                yield dic
