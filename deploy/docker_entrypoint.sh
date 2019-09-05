#!/usr/bin/env bash

service nginx start

gunicorn domain.app:api --bind 0.0.0.0:80 --name Reader --workers 3 --bind=unix:/var/www/reader/gunicorn.sock --log-level=debug --log-file=-
