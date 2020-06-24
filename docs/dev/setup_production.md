# Cloud setup w/ Heroku

This approach uses the Heroku CLI to set up the application in Heroku with the required buildpacks and database service, and links the repo via Git.

## Create the application

```sh
heroku apps:create -a trwwapi
```

## Add the buildpacks

```sh
heroku buildpacks:set heroku/nodejs
heroku buildpacks:set https://github.com/heroku/heroku-geo-buildpack.git
heroku buildpacks:set heroku/python
```

The order matters here. The `heroku-geo-buildpack` includes GDAL, GEOS, and PROJ software; it needs to be in place before `heroku/python` because of GeoDjango. `heroku/nodejs` is run before `heroku/python` so that the React build is done before the Python buildpack runs Django's `collectstatic` command.

## Setup the remote database

```sh
heroku pg:psql
=> CREATE EXTENSION postgis;
```

## Configure Git

```sh
heroku git:remote -a trwwapi
```
