

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
