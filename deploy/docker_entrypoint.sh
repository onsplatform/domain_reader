#!/usr/bin/env bash

service nginx start

gunicorn domain.app:api --bind 0.0.0.0:80 --name Reader --worker 3 --bind=unix:/var/www/schema/gunicorn.sock --log-level=debug --log-file=-
