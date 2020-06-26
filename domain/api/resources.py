import json
import autologging
from domain.writer import import_task


class BaseResource:
    def __init__(self, controller):
        self.controller = controller
        self._trace_local = lambda v, m: \
            self._DomainReaderResource__log.log(
                msg=f'{v}:{m}', level=autologging.TRACE)

    @staticmethod
    def add_branch_filter(req, params):
        if req.get_header('Branch'):
            params['branch'] = req.get_header('Branch')
        return params

    @staticmethod
    def add_reproduction_id(req, params):
        if req.get_header('ReproductionId'):
            params['reproduction_id'] = req.get_header('ReproductionId')
            params['reproduction_date'] = req.get_header('ReproductionDate')
        return params

    @staticmethod
    def add_solution_id(req, params):
        if req.get_header('SolutionId'):
            params['solution_id'] = req.get_header('SolutionId')
        return params

    def _read_params(self, req, params):
        self.add_branch_filter(req, params)
        self.add_reproduction_id(req, params)
        self.add_solution_id(req, params)
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

        params = self._read_params(req, req.params)
        if not self.controller.save_data(_instance_id, params):
            return resp.internal_error()

        return resp.accepted()


@autologging.traced
@autologging.logged
class DomainReaderResource(BaseResource):
    """
    """

    def on_post(self, req, resp, _map, _version, _type, _filter):
        if _map:
            try:
                params = self._read_params(req, req.json())
                data = self.controller.get_data(_map, _version, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_get(self, req, resp, _map, _version, _type, _filter):
        if _map:
            try:
                params = self.read_params(req, req.params)
                data = self.controller.get_data(_map, _version, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_post_count(self, req, resp, _map, _version, _type, _filter):
        if _map:
            try:
                params = self.read_params(req, req.json())
                data = self.controller.get_data_count(_map, _version, _type, _filter, params)
                return resp.json(data)
            except Exception as e:
                self._trace_local('###### ERROR ######', e)
                return resp.internal_error("error, see stack")

        return resp.bad_request()

    def on_get_count(self, req, resp, _map, _version, _type, _filter):
        if _map:
            try:
                params = self.read_params(req, req.params)
                data = self.controller.get_data_count(_map, _version, _type, _filter, params)
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

    def on_post(self, req, resp, _map, _version, _type):
        return super().on_post(req, resp, _map, _version, _type, None)

    def on_get(self, req, resp, _map, _version, _type):
        return super().on_get(req, resp, _map, _version, _type, None)


@autologging.traced
@autologging.logged
class DomainHistoryResource(BaseResource):
    """
    """

    def on_get(self, req, resp, _map, _version, _type, id):
        if _map:
            data = self.controller.get_history_data(_map, _version, _type, id)
            return resp.json(data)

        return resp.bad_request()


@autologging.traced
@autologging.logged
class DomainReaderInstanceFilterResource(DomainReaderResource):
    """
    """

    def get_entities_from_table(self, entities, table):
        return [entity for entity in entities if entity['_metadata']['table'] == table]

    def on_post(self, req, resp):
        try:
            request = req.json()
            filters = request['filters']
            entities = request['entities']

            result = set()
            for filter in filters:
                data = self.controller.get_data_from_table(filter['app'],
                                                           filter['version'],
                                                           filter['type'],
                                                           filter['filter_name'],
                                                           filter['params'])

                if data['data']:
                    entities_from_table = self.get_entities_from_table(entities, data['table'])
                    for entity in entities_from_table:
                        if str(entity['id']) in [str(data_item['id']) for data_item in data['data']]:
                            result.add(filter['instance_id'])

            return resp.json(list(result))
        except Exception as e:
            return resp.internal_error("error, see stack")
