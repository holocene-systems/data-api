FROM python:3.8

RUN apt-get update
RUN apt-get -y autoremove && apt-get install -y \
	libgeos-dev \
	binutils \
	libproj-dev \
	gdal-bin \
	git

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
WORKDIR /code/
# RUN pip install
RUN pip install pipenv==2018.11.26
# COPY requirements.txt /code/requirements.txt
COPY Pipfile /code/Pipfile
COPY Pipfile.lock /code/Pipfile.lock
RUN pipenv lock -r > /code/requirements.txt
RUN pip install -r /code/requirements.txt
# RUN pipenv install --deploy

COPY . /code/

CMD [ "./docker/app/startup.sh" ]