import falcon
import json


class DomainResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_get(self, req, resp, _map, _filter):
        if _map == '':
            resp.status = falcon.HTTP_400
            return
        
        data = self.domain_reader.get_data(_map, _filter, req.params)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200

class DomainHistoryResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_get(self, req, resp, _map, id):
        if _map == '':
            resp.status = falcon.HTTP_400
            return
        
        data = self.domain_reader.get_data(_map, None, req.params, True)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200