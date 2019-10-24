import autologging

from platform_sdk.domain.schema.api import SchemaApi
from .sql_executor import SQLExecutor


@autologging.logged
class DomainReader(SQLExecutor):

    def __init__(self, orm, db_settings, schema_settings):
        super().__init__(orm, db_settings)

        self.schema_api = SchemaApi(schema_settings)
        self._trace_local = lambda v, m: self._DomainReader__log.log(msg=f'{v}:{m}', level=autologging.TRACE)

    def get_data(self, _map, _type, filter_name, params, history=False):
        schema = self.schema_api.get_schema(_map, _type)

        return self._get_data(schema, filter_name, params, history)

    def get_history_data(self, _map, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _type)
        ret = self._get_data(schema, filter_name, params, history=True, get_by_id=True)

        return ret if ret else self._get_data(
            schema, filter_name, params, history=False, count=False, get_by_id=True
        )

    def get_data_count(self, _map, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _type)

        return self._get_data(schema, filter_name, params, count=True)

    def _get_data(self, schema, filter_name, params, history=False, count=False, get_by_id=False):
        if schema:
            sql_properties = self._get_sql_properties(schema, filter_name, params, history, count, get_by_id)
            ret = self._execute_query(**sql_properties)

            return ret if count else list(self._get_response_data(ret, schema['fields'], schema['metadata']))

    @staticmethod
    def _get_response_data(entities, fields, metadata):
        if entities:
            for entity in entities:
                dic = {field['alias']: getattr(entity, field['alias']) for field in fields}
                dic['_metadata'] = {meta['alias']: getattr(entity, meta['alias']) for meta in metadata}

                yield dic
