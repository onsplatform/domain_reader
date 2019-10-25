from .import_data import DomainImportData
from domain.api import settings
from celery import Celery

app = Celery(settings.CELERY['name'], broker=settings.CELERY['broker'])
app.conf.task_default_queue = 'import_data'

@app.task
def import_data(entities):
    domain_writer = DomainImportData(settings.ORM, settings.DATABASE)
    domain_writer.import_data(entities)
