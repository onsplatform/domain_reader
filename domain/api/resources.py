import json


class BaseResource:
    """
    """

    def __init__(self, controller):
        self.controller = controller


class DomainBatchWriterResource(BaseResource):
    """
    """

    def on_post(self, req, resp):
        if not self.controller.import_data(req.json()):
            return resp.bad_request()

        return resp.accepted()


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
            data = self.controller.get_data(_map, _type, _filter, req.json())
            return resp.json(data)

        return resp.bad_request()

    def on_get(self, req, resp, _map, _type, _filter):
        print(f'DomainReaderResource::on_get::request uri {req.uri}')
        print(f'DomainReaderResource::on_get::arguments::map::{_map} type::{_type} filter::{_filter}')
        if _map:
            try:
                data = self.controller.get_data(_map, _type, _filter, req.params)
                print(f'DomainReaderResource::on_get::data:: {len(data)}')
                return resp.json(data)
            except Exception as e:
                print(f'DomainReaderResource::on_get::internal error::{e}')
                return resp.internal_error('error to be handled.')

        return resp.bad_request()


class DomainReaderNoFilterResource(DomainReaderResource):
    """
    """

    def on_post(self, req, resp, _map, _type):
        return super().on_post(req, resp, _map, _type, None)

    def on_get(self, req, resp, _map, _type):
        return super().on_get(req, resp, _map, _type, None)


class DomainHistoryResource(BaseResource):
    """
    """

    def on_get(self, req, resp, _map, type, id):
        if _map:
            data = self.controller.get_data(_map, None, req.params, True)
            return resp.json(data)

        return resp.bad_request()
