run:
	@gunicorn domain_reader.app:api --bind 0.0.0.0:8002

test:
	@pytest -s .

migrate:
	@python manage.py makemigrations
	@python manage.py migrate

clean:
	@find . -name *.pyc -delete
	

