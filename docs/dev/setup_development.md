# Local Setup

These instructions assume use of `pipenv` for Python virtual environment and depenedency mangement, `pnpm` for NodeJS dependency mangement, and Docker for local development

## Create the Python virtual environment + install dependencies

In the project root folder:

```sh
pipenv install
```

## Setup the database

With PostgreSQL:

```sql
CREATE DATABASE trwwapidb;
CREATE EXTENSION postgis;
```

`psql` or database client can be used for executing the database commands

## Setup environment variables

Copy the `.env.example` file, rename to `.env`, and replace the parameters as appropriate.

## Install GDAL, GEOS, and PROJ

This software is needed for GeoDjango. Depending on which OS you're using, how you install it all will vary. On Windows, use OSGeo4W.
