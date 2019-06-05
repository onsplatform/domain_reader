import falcon

from platform_sdk.domain.reader import DomainReader

from .api import resources, settings


api = falcon.API()
api_version = 1

domain_reader = DomainReader(settings.ORM, settings.DATABASE, settings.SCHEMA)
domain_resource = resources.DomainResource(domain_reader)
domain_history_resource = resources.DomainHistoryResource(domain_reader)

api.add_route(
    settings.BASE_URI[api_version] + '{_map}/{_filter}/', domain_resource)
api.add_route(
    settings.BASE_URI[api_version] + '{_map}/history/{id}', domain_history_resource)