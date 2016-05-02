#!/bin/sh

celery -A task worker --loglevel=info 2>&1 &
gunicorn --bind 0.0.0.0:8000 --bind [::1]:8000 --log-level info app 2>&1 &
