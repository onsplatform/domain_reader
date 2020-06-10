import re


class QueryParser:
    def __init__(self, query):
        self.query = query
        self.regex = re.compile(r"\[.*?([:\$].*?)]|([:\$]\w*)")
        self.regex_cleaner = re.compile(r"[^a-zA-Z0-9]")

    def parse_param(self, place_holder, parameters):
        # TODO: if the parameter is enclosed between parenthesis, the regex
        # includes the closing one in the match.
        # We need a fix for this.
        # Manual replacements were executed as a temporary solution.
        opt_param, req_param = place_holder.groups()
        holder = None
        value = None

        if opt_param:
            # this line can be removed once we fix the regex.
            opt_param = opt_param.replace(')', '')
            value = parameters.get(opt_param[1:])
            holder = opt_param if value else place_holder.group()
        else:
            # this line can be removed once we fix the regex.
            req_param = req_param.replace(')', '')
            value = parameters[req_param[1:]]
            holder = req_param

        if value and holder.startswith('$'):
            
            if isinstance(value, str):
                value = tuple(value.split(';'),)
            else:
                value = (*value, )

        return holder, value

    def make_translator(self, parameters):
        translator = {}
        values = []

        for place_holder in re.finditer(self.regex, self.query):
            holder, value = self.parse_param(place_holder, parameters)
            translator[holder] = '%s' if value else ''
            if value:
                values.append(value)

        translator['['] = ''
        translator[']'] = ''
        return translator, values

    def parse(self, parameters):
        translator, values = self.make_translator(parameters)
        pattern = re.compile("|".join([re.escape(k)
                                       for k in translator.keys()]))
        query = pattern.sub(lambda m: translator[m.group(0)], self.query)
        return query.strip(), (*values,)
