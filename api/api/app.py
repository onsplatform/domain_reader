import falcon

from .dbconfig import db
from .domain import DomainResource
from platform_sdk.reader.domain_reader import DomainReader

api = application = falcon.API()

base_uri_v1 = '/reader/api/v1/'

domain_reader = DomainReader(db)

domain = DomainResource(domain_reader)
api.add_route(base_uri_v1 + '{solution}/{app}/{map}/{filter}/{query}', domain)