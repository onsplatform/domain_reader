from peewee import SQL
from pydash import objects

from .sql import QueryParser
from .mapper import RemoteField, RemoteMap


class SQLExecutor:
    QUERIES = {
        'not_deleted': '(deleted is null or not deleted)',
        'where_branch': '(branch in (%s, \'master\')) and '
                        'id not in (select from_id from entities.{table} where from_id is not null and branch=%s) AND '
    }

    def __init__(self, orm, db_settings):
        self.orm = orm
        self.db_settings = db_settings
        self._create_db(orm, db_settings)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def _execute_query(self, **options):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                query = self._get_query(**options)
                return query.count() if options.get('count') else query
        except Exception as e:
            self._trace_local('##### _execute_query ##### ERROR ', e)
        finally:
            self.db.close()

    def _get_sql_properties(self, schema, filter_name, params, history, count=False, get_by_id=False):
        model = self._get_model(schema['model'], schema['fields'] + schema['metadata'], history)
        sql_filter = self._get_sql_filter(filter_name, schema['filters'], history, get_by_id)

        page = params.get('page')
        branch = params.get('branch')
        page_size = params.get('page_size')
        table = objects.get(schema, 'model.table')
        sql_query = self._get_sql_query(sql_filter, {k: v for k, v in params.items() if k and v})

        return dict(model=model, table=table, branch=branch,
                    sql_query=sql_query, page=page, page_size=page_size,
                    count=count, get_by_id=get_by_id)

    def _get_query(self, **options):
        user_sql = ''
        user_sql_query = options.get('sql_query')
        query_not_deleted = self.QUERIES['not_deleted']
        query = options.get('model').build(self.db).select()

        if user_sql_query and user_sql_query['sql_query']:
            user_sql = user_sql_query['sql_query']
            query_params = user_sql_query['query_params']

        query_branch = self.QUERIES['where_branch'].format(table=options.get('table'))
        query_params = (options.get('branch'), options.get('branch'),) + query_params

        if user_sql:
            query_not_deleted += ' AND '

        if options.get('get_by_id'):
            query_not_deleted = query_branch = ''
            query_params = user_sql_query['query_params']

        query = query.where(SQL(
            query_branch + query_not_deleted + user_sql,
            query_params
        ))

        if options.get('page') and options.get('page_size') and not options.get('count'):
            query = query.paginate(int(options.get('page')), int(options.get('page_size')))

        return query

    @staticmethod
    def _get_sql_query(sql_filter, params):
        if sql_filter and params:
            parser = QueryParser(sql_filter)
            query, params = parser.parse(params)
            return dict(sql_query=query, query_params=params)

    @staticmethod
    def _get_fields(fields):
        return [RemoteField(
            f['alias'], f['field_type'], f['column_name']) for f in fields]

    def _get_model(self, model, fields, history=False):
        return RemoteMap(model['name'], model['table'], self._get_fields(fields), self.orm, history)

    @staticmethod
    def _get_sql_filter(filter_name, filters, history, get_by_id=False):
        if history or get_by_id:
            return 'id = :id'
        if filters and filter_name:
            return next(f['expression'] for f in filters if f['name'] == filter_name)