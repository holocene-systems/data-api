# Deployment on Heroku

## Before pushing to Heroku

Run tests, fix failing tests, and then commit changes with a useful commit message.

### Run Tests

#### Backend

To run tests on the Django app, at the root run: `pytest`

The backend Django app uses `pytest` instead of Django's built in testing suite.

#### Frontend

(TBC)

### Commit changes

The `master` branch is to be used for deployments: `git checkout master`

Merge changes and resolve any conflicts, re-running tests if needed.

Then, follow the the usual git commit routine:

```sh
git add .
git commit -m "message here"
```

This can also be done through the a git client (GitHub Desktop, SourceTree, Visual Studio Code, etc.)

> TODO: relate to a branching/merging approach for the project

## Push to Heroku

Once changes are commited locally, you push them to the master branch on Heroku as follows:

### Maintenance mode On (optional)

You can hide the site from the WWW temporarily with `heroku maintenance:on`.

### Code Push

With the `master` branch checked-out, run `git push heroku master` will push the locally commited changes to Heroku, and build the application.

### Run Migrations on Heroku (as needed)

If the updates included changes to any database models, you'll also need to run the database migrations on Heroku. If you have to run migrations, it's recommended you also have the maintainence mode on via `heroku maintenance:on`.

Run `heroku run python manage.py makemigrations` if migrations were not committed locally (but they should have been included).

Then run `heroku run python manage.py migrate` to migrate.

### Maintenance Mode Off (optional)

Turn maintenance mode off with: `heroku maintenance:off`