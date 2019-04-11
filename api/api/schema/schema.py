

class Field:
    def __init__(self, name, alias=None):
        self.name = name
        self.alias = alias or name

    def __repr__(self):
        return f'{self.name} as {self.alias}'


class Filter:
    def __init__(self, name, expression):
        self.name = name
        self.expression = expression

    def get_expression(self):
        return self.expression
