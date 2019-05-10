import falcon

from platform_sdk.domain.reader import DomainReader

from domain_reader.api import resources, settings


api = falcon.API()
api_version = 1

domain_reader = DomainReader(settings.DB)
domain_resource = resources.DomainResource(domain_reader)

api.add_route(
    settings.BASE_URI[api_version] + '{solution}/{app}/{map}/{filter}/{query}', domain_resource)
