import autologging

from platform_sdk.domain.schema.api import SchemaApi
from .regular_repository import RegularRepository
from .reproduction_repository import ReproductionRepository
from .disabled_branch_repository import DisabledBranchRepository


@autologging.logged
class DomainReader:

    def __init__(self, orm, db_settings, schema_settings):
        self.schema_api = SchemaApi(schema_settings)
        self._trace_local = lambda v, m: self._DomainReader__log.log(msg=f'{v}:{m}', level=autologging.TRACE)
        self.regular_repository = RegularRepository(orm, db_settings)
        self.reproduction_repository = ReproductionRepository(orm, db_settings)
        self.disabled_branch_repository = DisabledBranchRepository(orm, db_settings)

    def get_data(self, _map, _version, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _version, _type)
        if schema:
            solution = self.schema_api.get_solution(params.get("solution_id"))
            branch = self.schema_api.get_branch(params.get('branch'), solution['name'])
            if 'reproduction_id' in params:
                ret = self.reproduction_repository.get_data(schema, filter_name, params)
            elif branch['disabled']:
                ret = self.disabled_branch_repository.get_data(schema, filter_name, params, branch['disabled'])
            else:
                ret = self.regular_repository.get_data(schema, filter_name, params)

            if filter_name.lower() == 'byid':
                return self._get_single_response_entity(ret, schema)

            return list(self._get_response_data(ret, schema))

    def get_data_count(self, _map, _version, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _version, _type)
        if schema:
            solution = self.schema_api.get_solution(params.get("solution_id"))
            branch = self.schema_api.get_branch(params.get('branch'), solution['name'])
            if 'reproduction_id' in params:
                ret = self.reproduction_repository.get_count(schema, filter_name, params)
            elif branch['disabled']:
                ret = self.disabled_branch_repository.get_count(schema, filter_name, params,
                                                                          branch['disabled'])
            else:
                ret = self.regular_repository.get_count(schema, filter_name, params)

            return ret

    def get_history_data(self, _map, _version, _type, _id):
        schema = self.schema_api.get_schema(_map, _version, _type)
        if schema:
            history_data = self.regular_repository.get_history_data(schema, _id)
            if history_data:
                return list(self._get_response_data(history_data, schema))

            data = self.regular_repository.execute_data_query_by_id(schema, {'id': _id})
            return self._get_response_data(data, schema)

    def get_data_from_table(self, _map, _version, _type, filter_name, params):
        schema = self.schema_api.get_schema(_map, _version, _type)
        if schema:
            ret = self.regular_repository.get_data(schema, filter_name, params)
            return {'table': schema['model']['table'], 'data': list(self._get_response_data(ret, schema))}

    @staticmethod
    def _get_response_data(entities, schema):
        if entities:
            for entity in entities:
                yield from DomainReader._get_response_entity(entity, schema)

    @staticmethod
    def _get_response_entity(entity, schema):
        yield DomainReader._apply_data_modification(entity, schema)

    @staticmethod
    def _get_single_response_entity(entity, schema):
        return DomainReader._apply_data_modification(entity, schema)

    @staticmethod
    def _apply_data_modification(entity, schema):
        if entity:
            fields = schema['fields']
            metadata = schema['metadata']
            dic = {field['alias']: getattr(entity, field['alias']) for field in fields}
            dic['_metadata'] = {meta['alias']: getattr(entity, meta['alias']) for meta in metadata}
            dic['_metadata']['type'] = schema['name']
            dic['_metadata']['table'] = schema['model']['table']
            dic['id'] = dic['_metadata']['reproduction_from_id'] if dic['_metadata']['reproduction_from_id'] else \
                dic['id']
            return dic
