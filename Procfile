release: python manage.py migrate
web: gunicorn trwwapi.wsgi --log-file=-
worker: python manage.py rqworker default