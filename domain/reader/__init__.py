import re

from peewee import SQL

from .mapper import RemoteField, RemoteMap

from platform_sdk.domain.schema.api import SchemaApi


class DomainReader:
    def __init__(self, orm, db_settings, schema_settings):
        self.orm = orm
        self.db = orm.db_factory('postgres', **db_settings)()
        self.schema_api = SchemaApi(schema_settings)

    def get_data(self, _map, _type, filter_name, params, history=False):
        api_response = self.schema_api.get_schema(_map, _type)

        if api_response:
            model = self._get_model(
                api_response['model'], api_response['fields'], history)
            sql_filter = self._get_sql_filter(
                filter_name, api_response['filters'])
            sql_query = self._get_sql_query(sql_filter, params)
            data = self._execute_query(model, sql_query)
            return self._get_response_data(data, api_response['fields'])

    def _execute_query(self, model, sql_query):  # pragma: no cover
        proxy_model = model.build(self.db)
        query = proxy_model.select()
        if (sql_query):
            query = query.where(
                SQL(sql_query['sql_query'], sql_query['query_params']))
        return list([d for d in query])

    def _get_sql_query(self, sql_filter, params):
        if sql_filter:
            query_params = ()
            matches = re.finditer(r"([:,\$]\w+)", sql_filter, re.MULTILINE)
            for arg in matches:
                # named parameters, eg: $name :name
                arg = arg.group()
                # get named value from params
                val = params.get(arg[1:])
                # if parameter is list or begins with $, make tuple
                if (isinstance(val, list)):
                    val = tuple(val,)
                elif (arg[0:1] == '$'):
                    val = (val,)
                # make a tuple
                query_params = (*query_params, val)
                # replace argument with %s
                sql_filter = sql_filter.replace(arg, '%s')

            return {
                'sql_query': sql_filter,
                'query_params': query_params
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
        if filters and filter_name and filter_name != '':
            return next(f['expression'] for f in filters if f['name'] == filter_name)
