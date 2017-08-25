#!/usr/bin/env python

import os, sys, time, pprint

from datetime import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions as es_exceptions, helpers
from elasticsearch.client import IndicesClient

es = Elasticsearch([{'host':'10.71.102.210', 'port':9200}], timeout=600)
#ce = Elasticsearch([{'host':'hcc-metrics.unl.edu', 'port':9200}], timeout=600)
ce = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}], timeout=600)

esCon = IndicesClient(es)

pp = pprint.PrettyPrinter(indent=4)

props = {"mappings":
             {"throughput":
                 {"properties":
                     {"MA":{"type":"keyword"},
                      "dest":{"type":"keyword"},
                      "destProduction":{"type":"boolean"},
                      "destSite":{"type":"keyword"},
                      "destVO":{"type":"keyword"},
                      "src":{"type":"keyword"},
                      "srcProduction":{"type":"boolean"},
                      "srcSite":{"type":"keyword"},
                      "srcVO":{"type":"keyword"},
                      "throughput":{"type":"long"},
                      "timestamp":
                          {"type":"date",
                           "format":"basic_date_time_no_millis||epoch_millis"}
                      }},
              "latency":
                  {"properties":
                      {"MA":{"type":"keyword"},
                       "delay_mean":{"type":"float"},
                       "delay_median":{"type":"float"},
                       "delay_sd":{"type":"float"},
                       "dest":{"type":"keyword"},
                       "destProduction":{"type":"boolean"},
                       "destSite":{"type":"keyword"},
                       "destVO":{"type":"keyword"},
                       "src":{"type":"keyword"},
                       "srcProduction":{"type":"boolean"},
                       "srcSite":{"type":"keyword"},
                       "srcVO":{"type":"keyword"},
                       "timestamp":
                           {"type":"date",
                            "format":"basic_date_time_no_millis||epoch_millis"}
                      }},
              "traceroute":
                  {"properties":
                      {"MA":{"type":"keyword"},
                       "dest":{"type":"keyword"},
                       "destProduction":{"type":"boolean"},
                       "destSite":{"type":"keyword"},
                       "destVO":{"type":"keyword"},
                       "hash":{"type":"long"},
                       "hops":{"type":"keyword"},
                       "rtts":{"type":"float"},
                       "src":{"type":"keyword"},
                       "srcProduction":{"type":"boolean"},
                       "srcSite":{"type":"keyword"},
                       "srcVO":{"type":"keyword"},
                       "timestamp":
                           {"type":"date",
                            "format":"basic_date_time_no_millis||epoch_millis"},
                       "ttls":{"type":"integer"}
                      }},
              "packet_loss_rate":
                  {"properties":
                      {"MA":{"type":"keyword"},
                       "dest":{"type":"keyword"},
                       "destProduction":{"type":"boolean"},
                       "destSite":{"type":"keyword"},
                       "destVO":{"type":"keyword"},
                       "packet_loss":{"type":"float"},
                       "src":{"type":"keyword"},
                       "srcProduction":{"type":"boolean"},
                       "srcSite":{"type":"keyword"},
                       "srcVO":{"type":"keyword"},
                       "timestamp":
                           {"type":"date",
                            "format":"basic_date_time_no_millis||epoch_millis"}
                      }}}}


for day in range(1,32):
    indice = 'network_weather-2017.7.' + str(day)
    print (indice)
    try:
        if esCon.exists(index=indice):
            pp.pprint(esCon.delete(index=indice))

        tempExec = esCon.create(index=indice,
                               body=props)
        pp.pprint(tempExec)

        res = helpers.reindex(ce,
                              indice,
                              indice,
                              target_client=es,
                              chunk_size=10000,
                              scroll=u'2m')
        pp.pprint(res)
    except:
        print('Unexpected error:', sys.exc_info())

print('All done.')
