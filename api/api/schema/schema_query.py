from api.schema.schema_query import *


class SchemaQuery:
    str_select  = 'select '
    str_from    = ' from '
    str_where   = ' where '
    str_comma   = ', '

    def __init__(self, model, fields, filters):
        self.model = model
        self.fields = fields
        self.filter = filters

    def get_from(self):
        if self.model is None:
            raise ValueError('Model is none')

        return self.str_from + self.model

    def get_fields(self):
        if self.fields is None:
            raise ValueError('Fields is none')

        return self.str_comma.join(map(str, self.fields)) 

    def get_filter(self):
        if self.filter is None or self.filter == '':
            return ''

        return self.str_where + self.filter.get_expression()

    def __repr__(self):
        fields = self.get_fields()
        str_from = self.get_from()
        str_filter = self.get_filter()
        return self.str_select + str(fields) + str(str_from) + str_filter
