import requests

from api.schema.schema import *
from api.schema.schema_query import *


class SchemaApi:
    # TODO: move to sdk.
    url_schema_api = "http://localhost/schema/{0}/{1}/{2}"

    def get_schema(self, solution, app, _map):
        api_response = self.get_schema_response(solution, app, _map)
        model = api_response['model']
        fields = self.get_fields(api_response['fields'])
        filters = self.get_filter(api_response['filter'])
        return SchemaQuery(model, fields, filters)

    def _get_schema_response(self, solution, app, _map):
        response = requests.get(self._get_schema_api_url(solution, app, _map))
        if response.ok:
            return response

    def _get_schema_api_url(self, solution, app, _map):
        return self.url_schema_api.format(solution, app, _map)

    def _get_filter(self, _filter):
        return Filter(_filter['name'], _filter['expression'])

    def _get_fields(self, fields):
        return [Field(f['name'], f['alias']) for f in fields]
