import peewee

from playhouse.pool import PooledPostgresqlExtDatabase

class PeeweeSqliteDbFactory:
    def __init__(self, path):
        self.path = path

    def __call__(self):
        return peewee.SqliteDatabase(self.path)


class PostgresDbFactory:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def __call__(self):
        return PooledPostgresqlExtDatabase(**self.kwargs, max_connections=30, stale_timeout=300)


class Peewee:
    BASE_CLASSES = (peewee.Model, )

    FIELD_TYPES = {
        'str': peewee.CharField,
        'varchar': peewee.CharField,
        'text': peewee.TextField,
        'char': peewee.CharField,
        'bool': peewee.BooleanField,
        'float': peewee.FloatField,
        'int': peewee.IntegerField,
        'timestamp': peewee.DateTimeField,
        'uuid': peewee.UUIDField,
        'dec': peewee.DecimalField,
        'boolean': peewee.BooleanField
    }

    FACTORIES = {
        'sqlite': PeeweeSqliteDbFactory,
        'postgres': PostgresDbFactory
    }

    @classmethod
    def build_field(cls, field):
        wrapper = cls.FIELD_TYPES.get(field.field_type.lower())

        if not wrapper:
            raise NotImplementedError(f"field type {field.field_type} is not supported.")

        return wrapper(null=True, column_name=field.column_name)

    @classmethod
    def build_class(cls, dyn_type, _map, database, schema='public'):
        dyn_type._meta.table_name = _map.table
        dyn_type._meta.database = database
        dyn_type._meta.schema = schema

    @classmethod
    def db_factory(cls, db, *args, **kwargs):
        factory_cls = cls.FACTORIES[db]
        return factory_cls(*args, **kwargs)


