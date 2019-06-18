import os
import pytest

from ..orms.peewee import Peewee


@pytest.fixture(scope='class')
def db(request):
    return Peewee

@pytest.fixture(scope='class')
def db_settings():
    return {
        'database': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': os.environ.get('POSTGRES_HOST'),
        'port': os.environ.get('POSTGRES_PORT', 5432),
    }

@pytest.fixture(scope='class')
def schema_settings():
    return {
        'uri': 'http://localhost:3000/schema/',
    }
