from pydash import objects

from platform_sdk.process_memory import ProcessMemoryApi


class DomainWriter:
    QUERIES = {
        'insert': 'INSERT INTO entities.{table} ({columns}) VALUES ({values});',
        'update': 'UPDATE entities."{table}" SET {values} WHERE id="{pk}";'
    }

    HOLDERS = {
        "'": '"',
        "%": '%%'
    }

    def __init__(self, orm, db_settings, process_memory_settings):
        self.orm = orm
        self.db = orm.db_factory('postgres', **db_settings)()
        self.process_memory_api = ProcessMemoryApi(process_memory_settings)
        self.query_translator = str.maketrans(self.HOLDERS)

    def import_data(self, entities):
        bulk_sql = self._convert_imported_entity_to_sql(entities)
        self._execute_query(bulk_sql)
        return True

    def _convert_imported_entity_to_sql(self, entities):
        for entity in entities:
            table = entity.pop('_metadata')['type']
            columns = ','.join(entity)
            values = ','.join(self._mogrify(p) for p in entity.values())
            yield self.QUERIES['insert'].format(table=table, columns=columns, values=values)

    def _mogrify(self, value):
        if value is None:
            return 'null'

        translated = value.translate(
            self.query_translator) if isinstance(value, str) else str(value)

        return f"'{translated}'"

    def save_data(self, process_memory_id):
        data = self.process_memory_api.get_process_memory_data(process_memory_id)
        content = objects.get(data, 'map.content')
        entities = objects.get(data, 'dataset.entities')

        if content and entities:
            bulk_sql = self._get_sql(entities, content)
            self._execute_query(bulk_sql)
            return True

    def _execute_query(self, bulk_sql):  # pragma: no cover
        with self.db.atomic():
            for sql in bulk_sql:
                try:
                    self.db.execute_sql(sql)
                except Exception as e:
                    print("sql error: " + str(e))

    def _get_sql(self, data, schemas):
        for _type, entities in data.items():
            schema = schemas[_type]
            table = schema['table']
            fields = schema['fields']
            for entity in entities:
                instance_id = objects.get(entity, '_metadata.instance_id')
                if (instance_id):
                    yield self._get_update_sql(entity['id'], table, entity, fields)
                    continue

                yield self._get_insert_sql(table, entity, fields)

    def _get_update_sql(self, instance_id, table, entity, fields):
        values = [f"{f['column']}='{entity[f['name']]}'" for f in fields]

        return self.QUERIES['update'].format(
            table=table,
            values=str.join(',', values),
            pk=instance_id)

    def _get_insert_sql(self, table, entity, fields):
        columns = [field['column'] for field in fields]
        values = [self._mogrify(entity[f['name']]) for f in fields]
        return self.QUERIES['insert'].format(
            table=table,
            columns=str.join(',', columns),
            values=str.join(',', values),
        )

    def _get_schema(self, content):
        schema = {}
        for key in content.keys():
            fields = content[key]['fields']
            schema[key] = {
                'table': content[key]['model'],
                'fields':  [{'name': k, 'column': v['column']} for k, v in fields.items()]
            }
        return schema
