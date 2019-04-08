class SchemaQuery:
    def __init__(self, model, fields, filters):
        self.model = model
        self.fields = fields
        self.filter = filters

    def get_from(self):
        if self.model is None:
            raise ValueError('Model is none')

        return ' from ' + self.model

    def get_fields(self):
        if self.fields is None:
            raise ValueError('Fields is none')

        expression = ', '.join(map(str, self.fields))

        return expression

    def get_filter(self):
        if self.filter is None or self.filter == '':
            return ''

        expression = ' where ' + self.filter.get_expression()

        return expression

    def __repr__(self):
        fields = self.get_fields()
        str_from = self.get_from()
        str_filter = self.get_filter()

        return 'select ' + str(fields) + str(str_from) + str_filter


class Field:
    def __init__(self, name, alias: str = None):
        self.name = name

        if (alias is not None):
            self.alias = alias
        else:
            self.alias = name

    def __repr__(self):
        return self.name + ' as ' + self.alias


class Filter:
    def __init__(self, name, expression):
        self.name = name
        self.expression = expression

    def get_expression(self):
        return self.expression
