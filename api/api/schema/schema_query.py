from api.schema.schema_query import *


class SchemaQuery:
    str_select  = 'select '
    str_from    = ' from '
    str_where   = ' where '

    def __init__(self, model, fields, filters):
        self.model = model
        self.fields = fields
        self.filter = filters

    def get_from(self):
        if not self.model:
            raise ValueError('Model is required.')

        return self.str_from + self.model

    def get_fields(self):
        if not self.fields:
            raise ValueError('At least one field is required.')

        return str.join(', ', [str(f) for f in self.fields])

    def get_filter(self):
        if not self.filter:
            return ''

        return self.str_where + self.filter.get_expression()

    def __repr__(self):
        fields = self.get_fields()
        str_from = self.get_from()
        str_filter = self.get_filter()
        return self.str_select + fields + str_from + str_filter
