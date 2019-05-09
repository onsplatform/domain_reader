class RemoteField:
    def __init__(self, name, field_type, column_name, required=True):
        self.name = name
        self.field_type = field_type
        self.column_name = column_name
        self.required = required


class RemoteMap:
    def __init__(self, name, table, fields, orm):
        self.name = name
        self.table = table
        self.fields = fields
        self.orm = orm

    def build(self, database):
        fields = { f.name: self.orm.build_field(f) for f in self.fields }
        dyn_type = type(self.name, self.orm.BASE_CLASSES, fields)
        self.orm.build_class(dyn_type, self, database)
        return dyn_type

