from pydash import objects
from datetime import datetime

from platform_sdk.process_memory import ProcessMemoryApi

from autologging import logged


@logged
class DomainWriter:
    QUERIES = {
        'insert':        'INSERT INTO entities.{table} ({columns}, branch) VALUES ({values},\'{branch}\');',
        'update':        'UPDATE entities.{table} SET {values}, branch=\'{branch}\' WHERE id=\'{pk}\';',
        'update_branch': 'UPDATE entities.{table} SET {values} WHERE id=\'{pk}\' and branch=\'{branch}\';',
        'count_entity':  'SELECT count(1) from entities.{table} WHERE id=\'{pk}\' and branch=\'{branch}\';'
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

    def import_data(self, entities, solution_id):
        bulk_sql = self._convert_imported_entity_to_sql(entities, solution_id)
        self._execute_query(bulk_sql)
        return True

    def _convert_imported_entity_to_sql(self, entities, solution_id):
        for entity in entities:
            branch = objects.get(entity, '_metadata.branch')
            table = entity.pop('_metadata')['type']
            columns = ','.join(entity)
            values = ','.join(self._mogrify(p) for p in entity.values())

            yield self.QUERIES['insert'].format(table=table, columns=columns, values=values, branch=branch,
                                                solution_id=solution_id)

    def _mogrify(self, value):
        if value is None:
            return 'null'

        translated = value.translate(
            self.query_translator) if isinstance(value, str) else str(value)

        return f"'{translated}'"

    def save_data(self, process_memory_id, solution_id):
        data = self.process_memory_api.get_process_memory_data(process_memory_id)
        content = objects.get(data, 'map.content')
        entities = objects.get(data, 'dataset.entities')
        instance_id = objects.get(data, 'instanceId')
        fork = objects.get(data, 'fork')

        if content and entities:
            bulk_sql = self._get_sql(entities, content, instance_id, solution_id, fork)
            self._execute_query(bulk_sql)
            return True

    def _execute_query(self, bulk_sql):  # pragma: no cover
        try:
            self.db.connect(reuse_if_open=True)
            with self.db.atomic():
                for sql in bulk_sql:
                    try:
                        self.db.execute_sql(sql)
                        #print(sql)
                    except Exception as e:
                        print("sql error: " + str(e))
                        raise e
        except Exception as e:
            print('##### _execute_query ##### ERROR ')
        finally:
            self.db.close()

    def _execute_scalar_query(self, sql):  # pragma: no cover
        row = self.db.execute_sql(sql).fetchone()
        if row:
            return row[0]

    def _get_sql(self, data, schemas, instance_id, solution_id, fork):
        for _type, entities in data.items():
            if entities:
                schema = self._get_schema(schemas)[_type]
                table = schema['table']
                fields = schema['fields']
                for entity in entities:
                    now = datetime.now()
                    entity['meta_instance_id'] = instance_id
                    entity['modified'] = now
                    branch = objects.get(entity, '_metadata.branch')
                    change_track = objects.get(entity, '_metadata.changeTrack')

                    if change_track:
                        if change_track in {'update', 'destroy'} and instance_id and branch == 'master':
                            entity['deleted'] = change_track == 'destroy'
                            yield self._get_update_sql(entity['id'], table, entity, fields, branch, solution_id)

                        if change_track in {'update', 'destroy'} and instance_id and branch != 'master':
                            count_entity = self._execute_scalar_query(
                                self.QUERIES['count_entity'].format(table=table,pk=entity['id'],solution_id=solution_id,branch=branch)
                            )
                            if count_entity > 0:
                                entity['deleted'] = change_track == 'destroy'
                                yield self._get_update_sql(entity['id'], table, entity, fields, branch, solution_id, True)
                            else:
                                entity['from_id'] = entity['id']
                                yield self._get_insert_sql(table, entity, fields, branch, solution_id)

                        if change_track == 'create':
                            yield self._get_insert_sql(table, entity, fields, branch, solution_id)

    def _get_update_sql(self, instance_id, table, entity, fields, branch_name, solution_id, branch = False):
        values = [f"{field['column']}='{entity[field['name']]}'" for field in fields if field['name'] in entity]
        update_branch = ''
        if branch:
            update_branch = "_branch"
        return self.QUERIES['update' + update_branch].format(
            table=table,
            values=str.join(',', values),
            pk=instance_id,
            branch=branch_name,
            solution_id=solution_id
        )

    def _get_insert_sql(self, table, entity, fields, branch, solution_id):
        columns = [field['column'] for field in fields if field['name'] in entity]
        values = [self._mogrify(entity[field['name']]) for field in fields if field['name'] in entity]
        return self.QUERIES['insert'].format(
            table=table,
            columns=str.join(',', columns),
            values=str.join(',', values),
            branch=branch,
            solution_id=solution_id
        )

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
