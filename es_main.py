from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import datetime as dt
from datetime import timedelta
from datetime import timezone
import math
import json

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

esAtlas = Elasticsearch([{
    'host': contents[2], 'port': contents[3]
}], timeout=30)

esHCC = Elasticsearch([{
    'host': contents[0], 'port': contents[1]
}], timeout=30)

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-23T00:00:00"

def conAtlasTime(time):
    return dt.datetime.strptime(time, '%Y-%m-%dT%X')

def atlasLatency(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" : 
                         {"_type" : "latency" }
                     },
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = {}
    arrRet["dates"] = np.zeros(0)
    arrRet["datesF"] = np.zeros(0)
    arrRet["delay_mean"] = np.zeros(0)
    arrRet["delay_median"] = np.zeros(0)
    arrRet["delay_sd"] = np.zeros(0)

    while atlasTotalRec > 0:
        responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                       scroll=scrollPreserve)
        for hit in responseAtlas["hits"]["hits"]:
            arrRet["dates"] = np.append(arrRet["dates"], 
                              conAtlasTime(hit["_source"]["timestamp"]))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"])
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["delay_mean"] = np.append(arrRet["delay_mean"], float(hit["_source"]["delay_mean"]))
            arrRet["delay_median"] = np.append(arrRet["delay_median"], float(hit["_source"]["delay_median"]))
            arrRet["delay_sd"] = np.append(arrRet["delay_sd"], float(hit["_source"]["delay_sd"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
    es.clear_scroll(scroll_id=scrollIdAtlas)
    return arrRet


def atlasPacketLoss(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" : 
                         {"_type" : "packet_loss_rate"}
                     },
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = {}
    arrRet["dates"] = np.zeros(0)
    arrRet["datesF"] = np.zeros(0)
    arrRet["packet_loss"] = np.zeros(0)

    while atlasTotalRec > 0:
        responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                       scroll=scrollPreserve)
        for hit in responseAtlas["hits"]["hits"]:
            arrRet["dates"] = np.append(arrRet["dates"], 
                              conAtlasTime(hit["_source"]["timestamp"]))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"])
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["packet_loss"] = np.append(arrRet["packet_loss"], float(hit["_source"]["packet_loss"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
    es.clear_scroll(scroll_id=scrollIdAtlas)
    return arrRet

def atlasThroughput(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" : 
                         {"_type" : "throughput"}
                     },
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = {}
    arrRet["dates"] = np.zeros(0)
    arrRet["datesF"] = np.zeros(0)
    arrRet["throughput"] = np.zeros(0)

    while atlasTotalRec > 0:
        responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                       scroll=scrollPreserve)
        for hit in responseAtlas["hits"]["hits"]:
            arrRet["dates"] = np.append(arrRet["dates"], 
                              conAtlasTime(hit["_source"]["timestamp"]))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"])
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["throughput"] = np.append(arrRet["throughput"], float(hit["_source"]["throughput"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
    es.clear_scroll(scroll_id=scrollIdAtlas)
    return arrRet

def hccQuery(site):
    queryHCC={"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"Type" : "production"}
                      },
                      {"range" :
                         {"EventRate" : {"gte" : "0"}}
                      },
                      {"range" : {
                         "CompletionDate" : {
                             "gt" : conAtlasTime(startDate),
                             "lt" : conAtlasTime(endDate)
                         }
                      }},
                      {"match" :
                         {"DataLocationsCount" : 1}
                      },
                      {"match" : 
                         {"Site" : site }
                      },
                      {"match" :
                         {"InputData" : "Offsite"}
                      }
                  ]
              }
          }
      }

    scannerHCC = es.search(index="cms-*", 
                           doc_type="job", 
                           body=queryHCC, 
                           search_type="scan", 
                           scroll=scrollPreserve)
    scrollIdHCC = scannerHCC['_scroll_id']
    countHitsHCC = scannerHCC["hits"]["total"]
    arrRet = {}

    while countHitsHCC > 0:
        responseHCC = es.scroll(scroll_id=scrollIdHCC, 
                                scroll=scrollPreserve)
        for hit in responseHCC["hits"]["hits"]:
            print("placeholder")
        countHitsHCC -= len(responseHCC['hits']['hits'])
    es.clear_scroll(scroll_id=scrollIdHCC)
    return arrRet

print(conAtlasTime(startDate).replace(tzinfo=timezone.utc).timestamp())

#arrayAtlas = atlasPacketLoss(queryAtlas)
#fig = plt.figure(figsize=(12, 6))

#hput = fig.add_subplot(111)
#hput.plot(arrayAtlas["dates"], arrayAtlas["packetLoss"], 'bs')
#hput.hlines(arrayAtlas["packetLoss"], arrayAtlas["datesF"], arrayAtlas["dates"])
#hput.set_xlabel("Time Period")
#hput.set_title("Packet Loss")
#fig.autofmt_xdate()
#plt.show()

#with PdfPages('CMS_Plots.pdf') as pp:
#    d = pp.infodict()
#    d['Title'] = 'CMS Grid Plots'
#    d['Author'] = u'Jerrod T. Dixon\xe4nen'
#    d['Subject'] = 'Plot of network affects on grid jobs'
#    d['Keywords'] = 'PdfPages matplotlib CMS grid'
#    d['CreationDate'] = dt.datetime.today()
#    d['ModDate'] = dt.datetime.today()

#    pp.savefig(fig)
