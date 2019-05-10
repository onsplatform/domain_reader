run:
	@python manage.py runserver

test:
	@pytest -s .

migrate:
	@python manage.py makemigrations
	@python manage.py migrate

clean:
	@find . -name *.pyc -delete
	

