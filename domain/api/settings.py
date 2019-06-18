import os

from platform_sdk.domain.reader.orms.peewee import Peewee


DATABASE = {
    'database': os.environ.get('POSTGRES_DB', 'platform_domain_schema'),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    'host': os.environ.get('POSTGRES_HOST', 'postgres'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
}

SCHEMA = {
    'uri': os.environ.get('SCHEMA_URI', 'http://domain_schema/schema/'),
}

PROCESS_MEMORY = {
    'api_url': os.environ.get('SCHEMA_URL_API', 'http://10.24.1.91/process_memory/'),
}

ORM = Peewee

BASE_URI = {
    1: '/reader/api/v1/'
}

BASE_URI_WRITER = {
    1: '/writer/api/v1/'
}