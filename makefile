debug:
	@gunicorn domain.app:api --workers 1 --bind=0.0.0.0:8002 --log-level=debug --timeout=30000

run:
	@gunicorn domain.app:api --bind 0.0.0.0:8002 --timeout 300

test:
	@pytest -s .

migrate:
	@python manage.py makemigrations
	@python manage.py migrate

clean:
	@find . -name *.pyc -delete


