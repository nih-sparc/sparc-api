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
gunicorn main:app
```

# Testing

If you do not have the NIH SPARC portal user environment variables setup already:

1. Create a .env file with the configuration variables of the NIH SPARC portal user or add them to your bash profile.
2. If you created a separate file, run source {fileName}.env.

After the previous steps or if you already have those environment variables setup, run:

```
export PYTHONPATH=`pwd`
pip install -r requirements-dev.txt
pytest
```

## External requirements of sparc-api

#### Pennsieve python client
**Source:** https://github.com/Pennsieve/pennsieve-python \
**Summary:** Python client for the pennsieve data management platform \
**Used on sparc-api for:** Getting emails for pennsieve users \
**Critical:** no

#### Pennsieve Discover API
**Source:** https://developer.pennsieve.io/api/index.html \
**Summary:** API endpoint for the Pennsieve Discovaer data management platform \
**Used on sparc-api for:** 
 - Retrieving dataset awards
 - Retrieving dataset metadata and sending it for the simcore endpoint 
**Critical:** no

#### Amazon Simple Email Service
**Source:** https://docs.aws.amazon.com/ses/index.html \
**Summary:** Sends emails \
**Used on sparc-api for:** Sending emails \
**Critical:** no

#### SPARC Portal amazon s3
**Source:** https://aws.amazon.com/s3/ \
**Summary:** Cloud file storage \
**Used on sparc-api for:** accessing pennsieve files for the portal (ie. displaying a scaffold) \
**Critical:** yes

#### Biolucida 
**Source:** https://www.mbfbioscience.com/biolucida \
**Summary:** Image viewer and image repository \
**Used on sparc-api for:** Retrieving images and image metadata. (These are displayed on sparc-app) \
**Critical:** Critical for image display \

#### Scicrunch 
**Source:** https://scicrunch.org/ \
**Summary:** Processes SPARC datasets, pulling out metadata from them to make them searchable \
**Used on sparc-api for:** 
 - Searching across datasets
 - Filtering on datasets
 - Providing the contents of a dataset (ie. does it have a scaffold? Where is the scaffold file located?)
**Critical:** Critical for much of the /maps page and some functionality of the /data page

#### Wrike
**Source:** https://www.wrike.com/ \
**Summary:** Project management software \
**Used on sparc-api for:** Creates tickets from user feedback to be managed \
**Critical:** no

#### Mail Chimp
**Source:** https://mailchimp.com/ \
**Summary:**  Email service \
**Used on sparc-api for:** Email subscriptions from sparc-app \
**Critical:** no

#### oSparc
**Source:** https://osparc.io/ \
**Summary:**  Biomedeical Modelling and simulation software \
**Used on sparc-api for:** Running simulations from sparc-app \
**Critical:** critical for running simulations from sparc-app

#### Nuerolucidia 
**Source:** https://www.mbfbioscience.com/neurolucida \
**Summary:** 3d Imaging software \
**Used on sparc-api for:** Providing 3d images and metadata? \
**Critical:** critical for the 3d images?

#### SCI_CRUNCH_INTERLEX
**Source:** unknown \
**Summary:** Provides translation of medical terms \
**Used on sparc-api for:** Converting terms between forms and finding similar terms \
**Critical:** not sure

#### SendGrid
**Source:** https://sendgrid.com/ \
**Summary:** Sends emails \
**Used on sparc-api for:** Sending an email to a user after they give feedback \
**Critical:** no
