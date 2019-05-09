import pytest


from reader.orms.peewee import Peewee


@pytest.fixture(scope='class')
def db(request):
    create_db = Peewee.db_factory('sqlite', path='wee.db')
    return create_db()
