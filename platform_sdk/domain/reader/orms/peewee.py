import peewee


class PeeweeSqliteDbFactory:
    def __init__(self, path):
        self.path = path

    def __call__(self):
        return peewee.SqliteDatabase(self.path)


class Peewee:
    BASE_CLASSES = (peewee.Model, )

    FIELD_TYPES = {
        'str': peewee.CharField,
        'bool': peewee.BooleanField,
        'float': peewee.FloatField,
        'int': peewee.IntegerField,
    }

    FACTORIES = {
        'sqlite': PeeweeSqliteDbFactory
    }

    @classmethod
    def build_field(cls, field):
        wrapper = cls.FIELD_TYPES.get(field.field_type)
        return wrapper(null=True, column_name=field.column_name)

    @classmethod
    def build_class(cls, dyn_type, _map, database):
        dyn_type._meta.table_name = _map.table
        dyn_type._meta.database = database

    @classmethod
    def db_factory(cls, db, *args, **kwargs):
        factory_cls = cls.FACTORIES[db]
        return factory_cls(*args, **kwargs)


