
class SchemaApi:

    def get_schema(self, solution, app, map):
        '''
        Call API: 
        '''
        api_response = {
            'fields': [
                {
                    'name': 'nome_longo',
                    'alias': 'nome'
                }
            ],
            'filter': [
                {
                    'name': 'byName',
                    'expression': 'nome = :nome'
                }
            ],
            'args': [
                {
                    'name': ':nome'
                }
            ]
        }

        schema_type = api_response['type']
        fields = self.get_fields(api_response['fields'])
        filters = self.get_filter(api_response['filter'])
        args = self.get_args(api_response['args'])
        return api_response

    def get_args(sefl, args):
        if args == null:
            return null

        ret = []
        for arg in args:
            ret.append(Argument(arg.name))

    def get_filter(self, filters):
        if filters == null:
            return null

        ret = []
        for filter in filters:
            ret.append(Filter(filter.name, filter.expression))

    def get_fields(self, fields):
        if fields == null:
            return null

        ret = []
        for field in fields:
            ret.append(Field(field.name, field.alias))

        return ret
