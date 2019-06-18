from platform_sdk.process_memory import ProcessMemoryApi


class DomainWriter:
    def __init__(self, orm, db_settings, process_memory_settings):
        self.orm = orm
        self.db = orm.db_factory('postgres', **db_settings)()
        self.process_memory_api = ProcessMemoryApi(process_memory_settings)

    def save_data(self, process_memory_id):
        data = self.process_memory_api.get_process_memory_data(
            process_memory_id)
        if (data and data['map'] and data['map']['content'] and data['dataset']['entities']):
            bulk_sql = self._get_sql(
                data['dataset']['entities'],
                self._get_schema(
                    data['map']['content']
                )
            )

        self._execute_query(bulk_sql)
        return True

    def _execute_query(self, bulk_sql):  # pragma: no cover
        with self.db.atomic():
            for sql in bulk_sql:
                # print(sql)
                self.db.execute_sql(sql)

    def _get_sql(self, entites, schema):
        bulk_sql = []
        for key in entites.keys():
            for entity in entites[key]:
                table = schema[key]['table']
                fields = schema[key]['fields']
                instance_id = entity['_metadata']['instance_id']
                if (instance_id != None):
                    bulk_sql.append(self._get_update_sql(
                        entity['id'],
                        table,
                        entity,
                        fields
                    ))
                else:
                    bulk_sql.append(
                        self._get_insert_sql(table, entity, fields)
                    )
        return bulk_sql

    def _get_update_sql(self, instance_id, table, entity, fields):
        values = ''
        for schema_field in fields:
            column = schema_field['column']
            value = entity[schema_field['name']]
            values += '%s=\'%s\',' % (column, value)
        update_sql = 'update %s set %s where id=\'%s\';'
        return update_sql % (table, values[:-1], instance_id)

    def _get_insert_sql(self, table, entity, fields):
        values = ''
        columns = (field['column'] for field in fields)
        for schema_field in fields:
            schema_field_name = schema_field['name']
            values += '\'%s\',' % entity[schema_field_name]
        insert_sql = 'insert into %s (%s) values (%s);'
        return insert_sql % (table, str.join(',', columns), (values[:-1]))

    def _get_schema(self, content):
        schema = {}
        for key in content.keys():
            fields = content[key]['fields']
            schema[key] = {
                'table': content[key]['model'],
                'fields':  [{'name': k, 'column': v['column']} for k, v in fields.items()]
            }
        return schema
