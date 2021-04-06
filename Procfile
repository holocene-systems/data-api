release: python manage.py migrate
web: gunicorn trwwapi.wsgi --max-requests 2000 --max-requests-jitter 3000
worker: python manage.py rqworker default