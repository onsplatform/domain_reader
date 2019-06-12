import falcon

from platform_sdk.domain.reader import DomainReader
from platform_sdk.domain.writer import DomainWriter

from .api import resources, settings


from functools import partial


class GlobalMiddleware:
    def process_resource(self, req, resp, resource, params):
        resource.bad_request = partial(resource.bad_request, resp=resp)
        resource.internal_error = partial(resource.internal_error, resp=resp)
        resource.accepted = partial(resource.accepted, resp=resp)
        resource.json = partial(resource.json, resp=resp)
        resource.not_found = partial(resource.not_found, resp=resp)


api = falcon.API(middleware=[GlobalMiddleware()])
api_version = 1

domain_reader = DomainReader(settings.ORM, settings.DATABASE, settings.SCHEMA)
domain_writer = DomainWriter(
    settings.ORM, settings.DATABASE, settings.PROCESS_MEMORY)

domain_resource = resources.DomainReaderResource(domain_reader)
domain_history_resource = resources.DomainHistoryResource(domain_reader)
domain_writer_resource = resources.DomainWriterResource(domain_writer)

api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{type}/{_filter}/', domain_resource)
api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{type}/history/{id}', domain_history_resource)
api.add_route(
    settings.BASE_URI_WRITER[api_version] + '{map}/persist', domain_writer_resource)
