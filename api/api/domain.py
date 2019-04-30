import falcon
import json


class DomainResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_get(self, req, resp, solution, app, map, filter, query):

        if solution == '' or app == '' or map == '':
            resp.status = falcon.HTTP_400
            return

        data = self.domain_reader.get_data(solution, app, map)

        if data is None:
            resp.status = falcon.HTTP_400
        else:
            resp.body = json.dumps(json.dumps(data), ensure_ascii=False)
            resp.status = falcon.HTTP_200
