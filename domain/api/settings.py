import os

from domain.reader.orms.peewee import Peewee

CELERY = { 
    'broker': os.environ.get('CELERY_BROKER', 'pyamqp://guest@localhost//'),
    'name': 'task'
}


DATABASE = {
    'database': os.environ.get('POSTGRES_DB', 'platform_domain_schema'),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    'host': os.environ.get('POSTGRES_HOST', '10.24.1.251'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
}

SCHEMA = {
    'uri': os.environ.get('SCHEMA_URI', 'http://schema/api/v1/entitymap/'),
}

PROCESS_MEMORY = {
    'api_url': os.environ.get('PROCESS_MEMORY_URI', 'http://process_memory/'),
}

ORM = Peewee

BASE_URI = {
    1: '/reader/api/v1/'
}

BASE_URI_WRITER = {
    1: '/writer/api/v1/'
}