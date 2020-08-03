FROM python:3

RUN apt-get update && \
    apt-get -y autoremove && \
    apt-get install -y \
    postgresql-client \
	libgeos-dev \
	binutils \
	libproj-dev \
	gdal-bin \
	git

COPY requirements.txt /code/
WORKDIR /code/
RUN pip install -r requirements.txt

COPY . /code/

CMD [ "./docker/db-init/wait-for-postgres.sh", "db", "./docker/db-init/startup.sh" ]
