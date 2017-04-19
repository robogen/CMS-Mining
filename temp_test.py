from elasticsearch import Elasticsearch
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json
import pprint

pretty = pprint.PrettyPrinter(indent=4)

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    if type(time) is str:
        return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()
    elif type(time) is int:
        return time

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

esAtlas = Elasticsearch([{
    'host': contents[4], 'port': contents[3]
}], timeout=50)

esHCC = Elasticsearch([{
    'host': contents[0], 'port': contents[1]
}], timeout=50)

esCon = Elasticsearch([{
    'host': contents[4], 'port': contents[5]
}], timeout=50)

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-25T00:00:00"
tenMin = np.multiply(10,60)
stampStart = conAtlasTime(startDate) - tenMin
stampEnd = conAtlasTime(endDate)
tasktype = ["DIGIRECO", "RECO", "DIGI", "DataProcessing"]
#tasktype = ["gensim"]

loc = {}
loc["location"] = np.array([])

def esConAgg(field, task):
    queryBody={"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"CMS_JobType" : "Processing"}
                      },
                      #{"range" :
                      #   {"EventRate" : {"gte" : "0"}}
                      #},
                      {"exists" : 
                         { "field" : "ChirpCMSSWEventRate" }
                      },
                      {"exists" :
                         { "field" : "InputGB" }
                      },
                      {"match" : 
                         {"TaskType" : task}
                      },
                      {"range" : {
                         "JobFinishedHookDone" : {
                             "gt" : int(conAtlasTime(startDate)),
                             "lt" : int(conAtlasTime(endDate))
                         }
                      }},
                      {"match" :
                         {"DataLocationsCount" : 1}
                      },
                      {"match" :
                         {"InputData" : "Offsite"}
                      }
                  ]#,
                #"filter" : {
                #    "term" : { "TaskType" : task }
                #}
              }
          },
          "aggs": {
            "dev": {
              "terms": {"field":field}
            }
          }
      }

    scannerCon = esCon.search(index="cms-*",
                              body=queryBody,
                              search_type="query_then_fetch",
                              scroll=scrollPreserve)
    scrollIdCon = scannerCon['aggregations']['dev']
    conTotalRec = scrollIdCon['buckets']
    arrRet = np.array([])

    pretty.pprint(conTotalRec)
    pretty.pprint(scrollIdCon)

    if conTotalRec == 0:
        return None
    else:
        for hit in conTotalRec:
            arrRet = np.append(arrRet, hit['key'])
        return arrRet

returnedVal = esConAgg("Workflow", "DataProcessing")
pretty.pprint(returnedVal)
