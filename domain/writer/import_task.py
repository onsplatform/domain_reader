from . import DomainWriter
from domain.api import settings
from celery import Celery

app = Celery(settings.CELERY['name'], broker=settings.CELERY['broker'])
app.conf.task_default_queue = 'import_data'

@app.task
def import_data(entities, _solution_id):
    domain_writer = DomainWriter(settings.ORM, settings.DATABASE, settings.PROCESS_MEMORY)
    domain_writer.import_data(entities, _solution_id)
