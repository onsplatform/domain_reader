from pydash import objects

from . import DomainWriter


class DomainImportData(DomainWriter):

    def __init__(self, orm, db_settings):
        super().__init__(orm, db_settings)

    def import_data(self, entities):
        bulk_sql = self._convert_imported_entity_to_sql(entities)
        self._execute_query(bulk_sql)
        return True

    def _convert_imported_entity_to_sql(self, entities):
        for entity in entities:
            branch = objects.get(entity, '_metadata.branch')
            table = entity.pop('_metadata')['type']
            entity['branch'] = branch
            columns = ','.join(entity)
            values = ['%s' for p in entity.values()]
            params = tuple(value for value in entity.values())

            yield {
                'query': self.QUERIES['insert'].format(
                    table=table,
                    values=str.join(',', values),
                    columns=columns
                ),
                'params': params
            }
