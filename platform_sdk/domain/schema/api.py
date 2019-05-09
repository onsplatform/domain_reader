import requests

class SchemaApi:
    url_schema_api = "http://localhost/schema/{0}/{1}/{2}"

    def get_schema(self, solution, app, _map):
        api_response = self._get_schema_response(solution, app, _map)
        if api_response:
            return api_response.json()

    def _get_schema_response(self, solution, app, _map):
        response = requests.get(self._get_schema_api_url(solution, app, _map))
        if response.ok:
            return response

    def _get_schema_api_url(self, solution, app, _map):
        return self.url_schema_api.format(solution, app, _map)
