#!/usr/bin/env python3

from config import Config

from blackfynn import Blackfynn
from pymongo import MongoClient

from collections import Counter

bf = None
embargoed = None

def connect_blackfynn():
    'Connect to Blackfynn'
    print('Connecting to Blackfynn ...', end=' ')
    bf = Blackfynn(
        api_token=Config.BLACKFYNN_API_TOKEN,
        api_secret=Config.BLACKFYNN_API_SECRET,
        env_override=False,
        host=Config.BLACKFYNN_API_HOST,
    )
    print('done')
    return bf

def connect_mongo():
    ### 'Connect to MongoDB'
    print('Connecting to MongoDB ...', end=' ')
    client = MongoClient(Config.MONGODB_URI)
    db = client[Config.MONGODB_NAME]
    collection = db[Config.MONGODB_COLLECTION]
    print('done')
    return collection

def transform(ds):
    'Convert dataset JSON from Blackfynn to a database entry'
    content = ds['content']
    # models = bf.get_dataset(content['id']).models()
    doc = {
        '_id': content['intId'],
        'name': content['name'],
        'description': content.get('description'),
        'createdAt': content['createdAt'],
        'updatedAt': content['updatedAt'],
        'tags': content['tags'],
        'contributors': content['contributors'],
        'organization': ds['organization'],
        'userId': ds['owner'],
        'size': ds['storage'],
        'organization': org['organization']['name'],
        'organizationId': org['organization']['intId'],
        # 'modelCount': {name: m.count for name,m in models.items()},
        # 'fileCount': Counter(),
        #'recordCount': sum(m.count for m in models.values()),
        'banner': api._get(api._uri('/datasets/{dsid}/banner', dsid=content['id'])).get('banner', None)
    }

    # Get file counts:
    #cursor = ''
    #while True:
    #    resp = api._get(api._uri('/datasets/{dsid}/packages?cursor={cursor}', dsid=content['id'], cursor=cursor))
    #    for p in resp['packages']:
    #        ptype = p['content']['packageType']
    #        doc['fileCount'][ptype] += 1
    #    cursor = resp.get('cursor')
    #    if cursor is None:
    #        break
    
    return doc

if __name__ == '__main__':
    print('Starting embargoed data sync')
    bf = connect_blackfynn()
    api = bf._api.core
    embargoed = connect_mongo()

    org = api._get(api._uri('/organizations/{orgid}', orgid=bf.context.id))
    all_datasets = api._get('/datasets') # assuming all of these are part of SPARC Consortium org.
    publishedIds = [x['sourceDatasetId'] for x in api._get('/datasets/published')]

    print(embargoed)
    ### Delete all existing entries:
    embargoed.drop()

    entries = []
    for ds in all_datasets:
        print(ds)
        # skip if dataset is published or isn't part of Embargoed Data Team:
        print( api._get(api._uri('/datasets/{dsid}/collaborators/teams', dsid=ds['content']['id'])) )
        if ds['content']['intId'] in publishedIds or not any(t['id'] == Config.BLACKFYNN_EMBARGO_TEAM_ID for t in \
            api._get(api._uri('/datasets/{dsid}/collaborators/teams', dsid=ds['content']['id']))):
            print('skipping')
            continue
        entry = transform(ds)
        entries.append(entry)
        print('Found:', entry['_id'], entry['name'], sep='\t')
    if entries:
        embargoed.insert_many(entries)
    print('Sync finished.')
