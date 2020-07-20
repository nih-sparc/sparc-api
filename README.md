# Overview
This is the API service which is build as a Flask Application. It runs independent of the Nuxt.js web-application.
# Requirements

## Python 3
Make sure you have python 3 installed `python3 --version`

## Running the app
```
python3 -m venv ./venv
. ./venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
gunicorn main:app
```

# Testing

If you do not have the NIH SPARC portal user environment variables setup already:

1. Create a .env file with the configuration variables of the NIH SPARC portal user or add them to your bash profile.
2. If you created a separate file, run source {fileName}.env.

After the previous steps or if you already have those environment variables setup, run:

```
pip install -r requirements-dev.txt
pytest
```
