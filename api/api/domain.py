import falcon
import json


class DomainResource():

    def __init__(self, schema_api):
        self.schema_api = schema_api

    def on_get(self, req, resp, solution, app, map, filter, query):

        if solution == '' or app == '' or map == '':
            resp.status = falcon.HTTP_400
            return
            
        data = self.schema_api.get_schema(solution, app, map)

        if data is None:
            resp.status = falcon.HTTP_400
        else:
            resp.body = json.dumps(data, ensure_ascii=False)
            resp.status = falcon.HTTP_200
