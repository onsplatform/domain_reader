import falcon
import json


class BaseResource:
    def __init__(self, controller):
        self.controller = controller

    def bad_request(self, resp):
        resp.status = falcon.HTTP_400

    def internal_error(self, resp):
        resp.status = falcon.HTTP_500

    def json(self, data, resp, status_code=falcon.HTTP_200):
        if not data:
            return self.not_found(resp)

        resp.status = status_code
        resp.body = json.dumps(data)

    def accepted(self, resp, status_code=falcon.HTTP_200):
        resp.status = status_code

    def not_found(self, resp):
        resp.status = falcon.HTTP_404


class DomainWriterResource(BaseResource):
    def on_post(self, req, resp, _map):
        process_memory_id = req.get_header('Instance-Id')

        if not process_memory_id:
            return self.bad_request(resp)

        if not self.controller.save_data(process_memory_id):
            return self.internal_error(resp)

        return self.accepted(resp)


class DomainReaderResource(BaseResource):
    def on_post(self, req, resp, _map, _type, _filter):
        if _map:
            body = req.stream.read().decode('utf-8')
            params = json.loads(body)
            data = self.domain_reader.get_data(_map, _filter, params)
            return self.json(data, resp)

        return self.bad_request(resp)

    def on_get(self, req, resp, _map, type, _filter):
        if  _map:
            data = self.controller.get_data(_map, _filter, req.params)
            return self.json(data, resp)

        return self.bad_request(resp)


class DomainHistoryResource(BaseResource):
    def on_get(self, req, resp, _map, type, id):
        if _map:
            data = self.domain_reader.get_data(_map, None, req.params, True)
            return json(data, resp)

        return self.bad_request(resp)
