from platform_sdk.domain.reader.orms.peewee import Peewee


DB = Peewee.db_factory('sqlite', path='database.sqlite3')

BASE_URI = {
    1: '/reader/api/v1/'
}
