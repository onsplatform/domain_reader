import requests

from api.schema.schema import *
from api.schema.schema_query import *


class SchemaApi:

    url_schema_api = "http://localhost/schema/{0}/{1}/{2}"

    def get_schema(self, solution, app, map):
        api_response = self.get_schema_response(solution, app, map)
        model = api_response['model']
        fields = self.get_fields(api_response['fields'])
        filters = self.get_filter(api_response['filter'])
        return SchemaQuery(model, fields, filters)

    def get_schema_response(self, solution, app, map):
        response = requests.get(self.get_schema_api_url(solution, app, map))
        if response.ok:
            return response
        return None

    def get_schema_api_url(self, solution, app, map):
        return self.url_schema_api.format(solution, app, map)

    def get_filter(self, filter):
        return Filter(filter['name'], filter['expression'])

    def get_fields(self, fields):
        ret = []
        for field in fields:
            ret.append(Field(field['name'], field['alias']))
        return ret
