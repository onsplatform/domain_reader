debug:
	@gunicorn domain.app:api --workers 1 --bind=0.0.0.0:8002 --log-level=debug --timeout=30000

run:
	@gunicorn domain.app:api --log-level=debug --bind 0.0.0.0:8001 --timeout 3000

test:
	@pytest -s .