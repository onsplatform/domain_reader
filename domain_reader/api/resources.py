import falcon
import json


class DomainResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_get(self, req, resp, solution, app, map, filter, query):

        if solution == '' or app == '' or map == '':
            resp.status = falcon.HTTP_400
            return
        
        data = self.domain_reader.get_data(solution, app, map, filter, req.params)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200
