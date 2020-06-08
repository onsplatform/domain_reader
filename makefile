run:
	@gunicorn domain.app:api --log-level=debug --bind 0.0.0.0:8001 --timeout 3000

test:
	@pytest -s .


