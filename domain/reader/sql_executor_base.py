
class SQLExecutorBase:
    def __init__(self, orm, db_settings):
        self.orm = orm
        self.db_settings = db_settings
        self._create_db(orm, db_settings)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def _execute_query(self, query):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                return query
        except Exception as e:
            self._trace_local('##### _execute_query ##### ERROR ', e)
        finally:
            self.db.close()
