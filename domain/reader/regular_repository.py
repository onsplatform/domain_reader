from .base_reprository import RepositoryBase


class RegularRepository(RepositoryBase):
    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def get_data(self, schema, filter_name, params):
        return self._execute_regular_data_query(filter_name, params, schema)

    def get_history_data(self, schema, _id):
        model = self._get_model_from_schema(schema, True)
        return self._execute_query(self._get_query_by_id(model, _id))

    def get_count(self, schema, filter_name, params):
        branch = params.get('branch')
        model = self._get_model_from_schema(schema)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        return self._execute_query(self._get_count_query(model, user_query_filter, branch))

    def _execute_regular_data_query(self, filter_name, params, schema):
        branch, page, page_size = self._get_default_params(params)
        model = self._get_model_from_schema(schema)
        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        query = self._get_query(model, user_query_filter, branch)
        if page and page_size:
            query = query.paginate(int(page), int(page_size))
        return self._execute_query(query)