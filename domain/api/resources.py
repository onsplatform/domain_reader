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

    @staticmethod
    def add_branch_filter(req, params):
        if req.get_header('Branch'):
            params['branch'] = req.get_header('Branch')
        return params


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

    def on_post(self, req, resp, _instance_id):
        if not _instance_id:
            return resp.bad_request()

        if not self.controller.save_data(_instance_id):
            return resp.internal_error()

        return resp.accepted()


@autologging.traced
@autologging.logged
class DomainReaderResource(BaseResource):
    """
    """

    def on_post(self, req, resp, _map, _type, _filter):
        if _map:
            try:
                params = self.add_branch_filter(req, req.json())
                data = self.controller.get_data(_map, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_get(self, req, resp, _map, _type, _filter):
        if _map:
            try:
                params = self.add_branch_filter(req, req.params)
                data = self.controller.get_data(_map, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_post_count(self, req, resp, _map, _type, _filter):
        if _map:
            try:
                params = self.add_branch_filter(req, req.json())
                data = self.controller.get_data_count(_map, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_get_count(self, req, resp, _map, _type, _filter):
        if _map:
            try:
                params = self.add_branch_filter(req, req.params)
                data = self.controller.get_data_count(_map, _type, _filter, params)
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
            params = self.add_branch_filter(req, req.params)
            params['id'] = id
            data = self.controller.get_history_data(_map, _type, 'byId', params)
            return resp.json(data)

        return resp.bad_request()
