import traceback


class SqlExecutorBase:

    def __init__(self, orm, db_settings):
        self.orm = orm
        self.db_settings = db_settings
        self._create_db(orm, db_settings)

    def _create_db(self, orm, db_settings):
        self.db = orm.db_factory('postgres', **db_settings)()

    def _execute_query(self, bulk_sql):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                for sql in bulk_sql:
                    try:
                        self.db.execute_sql(sql['query'], sql['params'])
                        # print(sql)
                    except Exception as e:
                        print('##### ERROR execute_sql: #####')
                        traceback.print_exc()
                        raise e
        except Exception as e:
            print('##### ERROR _execute_query #####')
            traceback.print_exc()
            raise e
        finally:
            self.db.close()

    def _execute_scalar_query(self, sql, params):  # pragma: no cover
        row = self.db.execute_sql(sql, params).fetchone()
        if row:
            return row[0]