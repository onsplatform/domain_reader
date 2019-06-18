import json


class BaseResource:
    """
    """
    def __init__(self, controller):
        self.controller = controller


class DomainWriterResource(BaseResource):
    """
    """
    def on_post(self, req, resp, _map):
        if not req.instance_id:
            return resp.bad_request()

        if not self.controller.save_data(req.instance_id):
            return resp.internal_error()

        return resp.accepted()


class DomainReaderResource(BaseResource):
    """
    """
    def on_post(self, req, resp, _map, _type, _filter):
        if _map:
            data = self.controller.get_data(_map, _filter, req.json())
            return resp.json(data)

        return resp.bad_request()

    def on_get(self, req, resp, _map, type, _filter):
        __import__('ipdb').set_trace()
        if  _map:
            data = self.controller.get_data(_map, _filter, req.params)
            return resp.json(data)

        return resp.bad_request()


class DomainHistoryResource(BaseResource):
    """
    """
    def on_get(self, req, resp, _map, type, id):
        if _map:
            data = self.controller.get_data(_map, None, req.params, True)
            return resp.json(data)

        return resp.bad_request()
