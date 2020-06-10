from peewee import SQL

from .base_reprository import RepositoryBase


class ReproductionRepository(RepositoryBase):

    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def get_data(self, schema, filter_name, params):
        branch, page, page_size = self._get_default_params(params)
        if filter_name.lower() == 'byid':
            ret = self._get_reproduction_data_by_id(schema, filter_name, params)
        else:
            ret = self._get_reproduction_data(schema, filter_name, params)
        if page and page_size:
            offset = (page - 1) * page_size
            ret = ret[offset:page_size]
        return ret

    def get_count(self, schema, filter_name, params):
        ret = self._get_reproduction_data(schema, filter_name, params)
        return len(ret)

    def _get_reproduction_data(self, schema, filter_name, params):
        branch, page, page_size = self._get_default_params(params)
        query_at_time = self._get_query_at_time(params['reproduction_date'], filter_name, params, schema)
        data_at_time = self._execute_query(query_at_time)

        user_query_filter = self._get_user_query_filter(schema['filters'], filter_name, params)
        model = self._get_model_from_schema(schema)
        model_build = model.build(self.db)
        reproduction_data_query = model_build.select().where(SQL('reproduction_id = %s', (params['reproduction_id'],)))
        where_statement = self._get_where_statement(model, user_query_filter)
        where_params = self._get_where_params(branch, user_query_filter)
        reproduction_data_query = reproduction_data_query.where(SQL(where_statement, where_params)) \
            .order_by(model_build.modified_at)
        reproduction_data = self._execute_query(reproduction_data_query)

        reproduction_data_from_ids = [item.reproduction_from_id for item in list(reproduction_data) if
                                      item.reproduction_from_id]

        not_override = [data for data in list(data_at_time) if data.id not in reproduction_data_from_ids] + \
                       list(reproduction_data)

        return not_override

    def _get_reproduction_data_by_id(self, schema, filter_name, params):
        branch, page, page_size = self._get_default_params(params)
        model = self._get_model_from_schema(schema)
        model_build = model.build(self.db)
        reproduction_data_query = model_build.select().where(
            SQL('reproduction_id = %s and reproduction_from_id = %s', (params['reproduction_id'], params['id'],)))
        where_statement = self._get_where_statement(model, None)
        where_params = self._get_where_params(branch, None)
        reproduction_data_query = reproduction_data_query.where(SQL(where_statement, where_params)) \
            .order_by(model_build.modified_at)
        reproduction_data_by_id = self._execute_query(reproduction_data_query)

        if reproduction_data_by_id:
            reproduction_data_by_id = reproduction_data_by_id[0]
            if reproduction_data_by_id.deleted:
                return None
            else:
                reproduction_data_by_id.id = reproduction_data_by_id.reproduction_from_id
                return reproduction_data_by_id
        else:
            query_at_time = self._get_query_at_time(params['reproduction_date'], filter_name, params, schema)
            data_at_time = self._execute_query(query_at_time)
            return data_at_time
