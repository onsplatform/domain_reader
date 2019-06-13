import json


class BaseResource:
    def __init__(self, controller):
        self.controller = controller


class DomainWriterResource(BaseResource):
    def on_post(self, req, resp, _map):
        process_memory_id = req.get_header('Instance-Id')

        if not process_memory_id:
            return resp.bad_request()

        if not self.controller.save_data(process_memory_id):
            return resp.internal_error()

        return resp.accepted()


class DomainReaderResource(BaseResource):
    def on_post(self, req, resp, _map, _type, _filter):
        if _map:
            body = req.stream.read().decode('utf-8')
            params = json.loads(body)
            data = self.controller.get_data(_map, _filter, params)
            return resp.json(data)

        return resp.bad_request()

    def on_get(self, req, resp, _map, type, _filter):
        if  _map:
            data = self.controller.get_data(_map, _filter, req.params)
            return resp.json(data)

        return resp.bad_request()


class DomainHistoryResource(BaseResource):
    def on_get(self, req, resp, _map, type, id):
        if _map:
            data = self.controller.get_data(_map, None, req.params, True)
            return resp.json(data)

        return resp.bad_request()
