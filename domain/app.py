import sys
import logging

import autologging
import falcon

from .reader import DomainReader
from .writer import DomainWriter

from .api import resources, settings
from .api import utils

# initializing logger
logging.basicConfig(
    level=autologging.TRACE,
    stream=sys.stdout,
    format="%(levelname)s:%(name)s:%(funcName)s:%(message)s")

api = falcon.API(response_type=utils.APIResponse, request_type=utils.APIRequest)
api_version = 1

domain_reader = DomainReader(settings.ORM, settings.DATABASE, settings.SCHEMA)
domain_writer = DomainWriter(settings.ORM, settings.DATABASE, settings.PROCESS_MEMORY, settings.SCHEMA)

domain_reader_resource = resources.DomainReaderResource(domain_reader)
domain_reader_instance_filter_resource = resources.DomainReaderInstanceFilterResource(domain_reader)
domain_reader_nofilter_resource = resources.DomainReaderNoFilterResource(domain_reader)
domain_history_resource = resources.DomainHistoryResource(domain_reader)
domain_writer_resource = resources.DomainWriterResource(domain_writer)
domain_batch_writer_resource = resources.DomainBatchWriterResource(domain_writer)

api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{_version}/{_type}', domain_reader_nofilter_resource)
api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{_version}/{_type}/{_filter}', domain_reader_resource)
api.add_route(
    settings.BASE_URI[api_version] + 'count/{_map}/{_version}/{_type}/{_filter}', domain_reader_resource, suffix='count')
api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{_version}/{_type}/history/{id}', domain_history_resource)
api.add_route(
    settings.BASE_URI[api_version] + 'persist/{_instance_id}', domain_writer_resource)
api.add_route(
    settings.BASE_URI_WRITER[api_version] + 'batch/persist', domain_batch_writer_resource)
api.add_route(
    settings.BASE_URI[api_version] + 'instanceswhichquerieswouldreturnentity', domain_reader_instance_filter_resource)

