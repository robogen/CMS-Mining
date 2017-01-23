#!/usr/bin/env python

import os, sys, time

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection,  exceptions as es_exceptions, helpers

es = Elasticsearch([{'host':'10.71.102.210', 'port':9200}], timeout=600)
ce = Elasticsearch([{'host':'atlas-kibana.mwt2.org', 'port':9200}], timeout=600)

for day in range(15,25):
    index = 'network_weather_2-2016.7.' + str(day)
    print (index)
    try:
        res = helpers.reindex(ce, index, index, target_client=es, chunk_size=10000)
    except:
        print('issue')

print('All done.')
