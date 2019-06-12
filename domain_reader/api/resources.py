import falcon
import json

class DomainWriterResource():

    def __init__(self, domain_writer):
        self.domain_writer = domain_writer

    def on_post(self, req, resp, map):
        process_memory_id = req.get_header('Instance-Id')
        if process_memory_id == '':
            resp.status = falcon.HTTP_400
            return

        ret = self.domain_writer.save_data(process_memory_id)

        if not ret:
            resp.status = falcon.HTTP_500
        else:
            resp.status = falcon.HTTP_200

class DomainResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_post(self, req, resp, _map, type, _filter):
        if _map == '':
            resp.status = falcon.HTTP_400
            return

        body = req.stream.read().decode('utf-8')
        _params = json.loads(body)
        data = self.domain_reader.get_data(_map, _filter, _params)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200

    def on_get(self, req, resp, _map, type, _filter):
        if _map == '':
            resp.status = falcon.HTTP_400
            return

        __import__('ipdb').set_trace()
        data = self.domain_reader.get_data(_map, _filter, req.params)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200

class DomainHistoryResource():

    def __init__(self, domain_reader):
        self.domain_reader = domain_reader

    def on_get(self, req, resp, _map, type, id):
        if _map == '':
            resp.status = falcon.HTTP_400
            return

        data = self.domain_reader.get_data(_map, None, req.params, True)

        if data is None:
            resp.status = falcon.HTTP_404
        else:
            resp.body = json.dumps(data)
            resp.status = falcon.HTTP_200
