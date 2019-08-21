from peewee import SQL
import autologging

from platform_sdk.domain.schema.api import SchemaApi

from .mapper import RemoteField, RemoteMap
from .sql import QueryParser


@autologging.logged
class DomainReader:
    def __init__(self, orm, db_settings, schema_settings):
        self.orm = orm
        self.db_settings = db_settings
        self.schema_api = SchemaApi(schema_settings)
        self._create_db(orm, db_settings)

        # wrapping local tracer
        self._trace_local = lambda v, m: \
            self._DomainReader__log.log(
                msg=f'{v}:{m}', level=autologging.TRACE)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def get_data(self, _map, _type, filter_name, params, history=False):
        self._trace_local('###### get_data ######', _map)

        api_response = self.schema_api.get_schema(_map, _type)
        self._trace_local('schema map id', api_response.get('id'))

        if api_response:
            model = self._get_model(api_response['model'], api_response['fields'], history)
            self._trace_local('model', model)

            sql_filter = self._get_sql_filter(filter_name, api_response['filters'], history)
            self._trace_local('sql_filter', sql_filter)

            page = params.get('page')
            page_size = params.get('page_size')
            params = {k: v for k, v in params.items() if k and v}
            self._trace_local('params', params)

            sql_query = self._get_sql_query(sql_filter, params)
            self._trace_local('sql_query', sql_query)

            data = self._execute_query(model, sql_query, page, page_size)

            return self._get_response_data(data, api_response['fields'])

    def _execute_query(self, model, sql_query, page, page_size=20):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                proxy_model = model.build(self.db)
                query = proxy_model.select()

                if sql_query and sql_query['sql_query']:
                    sql_statement = SQL('(deleted is null or not deleted) and ' + sql_query['sql_query'], sql_query['query_params'])
                    query = query.where(sql_statement)

                if page and page_size:
                    query = query.paginate(int(page), int(page_size))

                self._trace_local('data size', len(query or []))
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
    def _get_response_data(entities, fields):
        if entities:
            ret = []
            for entity in entities:
                dic = {}
                meta = {}
                for field in fields:
                    if field['alias'].startswith('_metadata.'):
                        meta[field['alias'][10:]] = getattr(entity, field['alias'])
                    else:
                        dic[field['alias']] = getattr(entity, field['alias'])
                dic['_metadata'] = meta
                ret.append(dic)
            return ret

    @staticmethod
    def _get_fields(fields):
        return [RemoteField(
            f['alias'], f['field_type'], f['column_name']) for f in fields]

    def _get_model(self, model, fields, history=False):
        """
        TODO: move to domain_schema
        """
        fields.append({'field_type': 'boolean', 'column_name': 'deleted', 'alias': '_metadata.deleted'})
        fields.append({'field_type': 'varchar', 'column_name': 'meta_instance_id', 'alias': '_metadata.instance_id'})
        fields.append({'field_type': 'timestamp', 'column_name': 'modified', 'alias': '_metadata.modified_at'})
        fields.append({'field_type': 'varchar', 'column_name': 'from_id', 'alias': '_metadata.from_id'})
        fields.append({'field_type': 'varchar', 'column_name': 'branch', 'alias': '_metadata.branch'})

        return RemoteMap(model['name'], model['table'], self._get_fields(fields), self.orm, history)

    @staticmethod
    def _get_sql_filter(filter_name, filters, history):
        if history:
            return 'id = :id'

        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)
