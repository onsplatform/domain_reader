import falcon

from .dbconfig import db
from .domain import DomainResource
from ...sdk.schema.schema_api import SchemaApi

api = application = falcon.API()

base_uri_v1 = '/reader/api/v1/'

schema_api = SchemaApi(db)

domain = DomainResource(schema_api)
api.add_route(base_uri_v1 + '{solution}/{app}/{map}/{filter}/{query}', domain)