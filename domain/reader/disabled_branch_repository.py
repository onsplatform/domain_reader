from .base_reprository import RepositoryBase


class DisabledBranchRepository(RepositoryBase):
    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def get_data(self, schema, filter_name, params, disabled_branch):
        query = self._get_query_at_time(disabled_branch, filter_name, params, schema)
        branch, page, page_size = self._get_default_params(params)
        query = query
        if page and page_size:
            query = query.paginate(int(page), int(page_size))
        return self._execute_query(query)

    def get_count(self, schema, filter_name, params, disabled_branch):
        return 0
