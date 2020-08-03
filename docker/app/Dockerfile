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
COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY . /code/

CMD [ "./docker/app/startup.sh" ]