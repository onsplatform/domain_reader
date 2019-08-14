import json
import autologging
from domain.writer import import_task


class BaseResource:
    """
    """

    def __init__(self, controller):
        self.controller = controller
        # wrapping local tracer
        self._trace_local = lambda v, m: \
            self._DomainReaderResource__log.log(
                msg=f'{v}:{m}', level=autologging.TRACE)


@autologging.traced
@autologging.logged
class DomainBatchWriterResource(BaseResource):
    """
    """

    def on_post(self, req, resp):
        import_task.import_data.delay(req.json())

        return resp.accepted()


@autologging.traced
@autologging.logged
class DomainWriterResource(BaseResource):
    """
    """

    def on_post(self, req, resp, _map):
        if not req.instance_id:
            return resp.bad_request()

        if not self.controller.save_data(req.instance_id):
            return resp.internal_error()

        return resp.accepted()


@autologging.traced
@autologging.logged
class DomainReaderResource(BaseResource):
    """
    """

    def on_post(self, req, resp, _map, _type, _filter):
        if _map:
            data = self.controller.get_data(_map, _type, _filter, req.json())
            return resp.json(data)

        return resp.bad_request()

    def on_get(self, req, resp, _map, _type, _filter):
        if _map:
            try:
                data = self.controller.get_data(
                    _map, _type, _filter, req.params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()


@autologging.traced
@autologging.logged
class DomainReaderNoFilterResource(DomainReaderResource):
    """
    """

    def on_post(self, req, resp, _map, _type):
        return super().on_post(req, resp, _map, _type, None)

    def on_get(self, req, resp, _map, _type):
        return super().on_get(req, resp, _map, _type, None)


@autologging.traced
@autologging.logged
class DomainHistoryResource(BaseResource):
    """
    """

    def on_get(self, req, resp, _map, _type, id):
        if _map:
            params = ({'id': id})
            data = self.controller.get_data(_map, _type, 'byId', params, True)
            return resp.json(data)

        return resp.bad_request()
