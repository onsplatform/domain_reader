import falcon

from platform_sdk.domain.reader import DomainReader
from platform_sdk.domain.writer import DomainWriter

from .api import resources, settings


api = falcon.API()
api_version = 1

domain_reader = DomainReader(settings.ORM, settings.DATABASE, settings.SCHEMA)
domain_writer = DomainWriter(settings.ORM, settings.DATABASE, settings.PROCESS_MEMORY)

domain_reader_resource = resources.DomainReaderResource(domain_reader)
domain_history_resource = resources.DomainHistoryResource(domain_reader)
domain_writer_resource = resources.DomainWriterResource(domain_writer)

api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{type}/{_filter}', domain_reader_resource)
api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{type}/history/{id}', domain_history_resource)
api.add_route(
    settings.BASE_URI_WRITER[api_version] + '{map}/persist', domain_writer_resource)
