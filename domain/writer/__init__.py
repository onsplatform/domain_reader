import traceback

from pydash import objects
from datetime import datetime

from platform_sdk.process_memory import ProcessMemoryApi

from autologging import logged


@logged
class DomainWriter:
    QUERIES = {
        'insert': 'INSERT INTO entities.{table} ({columns}) VALUES ({values});',
        'update': 'UPDATE entities.{table} SET {values} WHERE id=%s;',
        'count_entity': 'SELECT count(1) FROM entities.{table} WHERE id=%s;'
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
            branch = objects.get(entity, '_metadata.branch')
            table = entity.pop('_metadata')['type']
            columns = ','.join(entity)
            values = ','.join(self._mogrify(p) for p in entity.values())

            yield self.QUERIES['insert'].format(table=table, columns=columns, values=values, branch=branch)

    def _mogrify(self, value):
        if value is None:
            return 'null'

        translated = value.translate(
            self.query_translator) if isinstance(value, str) else str(value)

        return f"{translated}"

    def save_data(self, process_memory_id):
        data = self.process_memory_api.get_process_memory_data(process_memory_id)
        content = objects.get(data, 'map.content')
        entities = objects.get(data, 'dataset.entities')
        instance_id = objects.get(data, 'instanceId')

        if content and entities:
            bulk_sql = self._get_sql(entities, content, instance_id)
            self._execute_query(bulk_sql)
            return True

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

    def _get_sql(self, data, schemas, instance_id):
        for _type, entities in data.items():
            if entities:
                schema = self._get_schema(schemas)[_type]
                table = schema['table']
                fields = schema['fields']
                for entity in entities:
                    entity['meta_instance_id'] = instance_id
                    entity['modified'] = datetime.now()
                    branch = objects.get(entity, '_metadata.branch')
                    change_track = objects.get(entity, '_metadata.changeTrack')
                    from_id = objects.get(entity, '_metadata.from_id')

                    if change_track:
                        if change_track in {'update', 'destroy'} and instance_id and branch == 'master':
                            yield self._get_update_sql(entity['id'], table, entity, fields, change_track)

                        if change_track in {'update', 'destroy'} and instance_id and branch != 'master':
                            count_entity = self._execute_scalar_query(
                                self.QUERIES['count_entity'].format(table=table), (entity['id'],)
                            )
                            if count_entity > 0:
                                yield self._get_update_sql(entity['id'], table, entity, fields, change_track)
                            else:
                                yield self._get_insert_sql(table, entity, fields, from_id)

                        if change_track == 'create':
                            yield self._get_insert_sql(table, entity, fields, from_id)

    def _get_update_sql(self, entity_id, table, entity, fields, change_track):
        entity['deleted'] = change_track == 'destroy'
        values = [f"{field['column']}=%s" for field in fields if field['name'] in entity]
        params = \
            tuple(entity[field['name']] for field in fields if field['name'] in entity) + (entity_id, )
        return {
            'query': self.QUERIES['update'].format(
                table=table,
                values=str.join(',', values)),
            'params': params
        }

    def _get_insert_sql(self, table, entity, fields, from_id):
        if from_id:
            entity['from_id'] = from_id
        entity['branch'] = objects.get(entity, '_metadata.branch')
        columns = [field['column'] for field in fields if field['name'] in entity]
        values = ['%s' for field in fields if field['name'] in entity]
        params = tuple(entity[field['name']] for field in fields if field['name'] in entity)
        return {
            'query': self.QUERIES['insert'].format(
                table=table,
                values=str.join(',', values),
                columns=str.join(',', columns)
            ),
            'params': params
        }

    def _get_schema(self, content):
        schema = {}
        for key in content.keys():
            fields = content[key]['fields']
            fields['deleted'] = {'name': 'deleted', 'column': 'deleted'}
            fields['meta_instance_id'] = {'name': 'meta_instance_id', 'column': 'meta_instance_id'}
            fields['modified'] = {'name': 'modified', 'column': 'modified'}
            fields['from_id'] = {'name': 'from_id', 'column': 'from_id'}
            fields['branch'] = {'name': 'branch', 'column': 'branch'}

            schema[key] = {
                'table': content[key]['model'],
                'fields': [{'name': k, 'column': v['column']} for k, v in fields.items()]
            }
        return schema
