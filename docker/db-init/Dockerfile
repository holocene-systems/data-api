FROM python:3.8

RUN apt-get update && \
    apt-get -y autoremove && \
    apt-get install -y \
    postgresql-client \
	libgeos-dev \
	binutils \
	libproj-dev \
	gdal-bin \
	git

RUN pip install pipenv
# COPY requirements.txt /code/
COPY Pipfile /code/
COPY Pipfile.lock /code/
WORKDIR /code/
RUN pipenv lock -r > requirements.txt
RUN pip install -r requirements.txt
# RUN pipenv install --deploy

COPY . /code/

CMD [ "./docker/db-init/wait-for-postgres.sh", "db", "./docker/db-init/startup.sh" ]
# CMD [ "./docker/db-init/wait-for-postgres.sh", "db", "python3", "manage.py", "migrate", "--no-input"]
# CMD [ "./docker/db-init/wait-for-postgres.sh", "db", "startup.sh" ]
