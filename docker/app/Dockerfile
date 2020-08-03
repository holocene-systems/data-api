FROM python:3

RUN apt-get update
RUN apt-get -y autoremove && apt-get install -y \
	libgeos-dev \
	binutils \
	libproj-dev \
	gdal-bin \
	git

ENV PYTHONBUFFERED 1

RUN mkdir /code
COPY requirements.txt /code/
WORKDIR /code/
RUN pip install -r requirements.txt

COPY . /code/

CMD [ "./docker/app/startup.sh" ]