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
