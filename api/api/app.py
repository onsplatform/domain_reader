import falcon

from .domain import DomainResource
api = application = falcon.API()

base_uri_v1 = '/reader/api/v1/'

domain = DomainResource()
api.add_route(base_uri_v1 + '{project}/{solution}/{map}/{filter}/', domain)
