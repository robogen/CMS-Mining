from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import datetime as dt
from datetime import timedelta
import math
import json

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

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

def conAtlasTime(time, switch):
    if switch:
        return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()
    else:
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
                              conAtlasTime(hit["_source"]["timestamp"], False))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"], False)
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["delay_mean"] = np.append(arrRet["delay_mean"], float(hit["_source"]["delay_mean"]))
            arrRet["delay_median"] = np.append(arrRet["delay_median"], float(hit["_source"]["delay_median"]))
            arrRet["delay_sd"] = np.append(arrRet["delay_sd"], float(hit["_source"]["delay_sd"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
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
                              conAtlasTime(hit["_source"]["timestamp"], False))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"], False)
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["packet_loss"] = np.append(arrRet["packet_loss"], float(hit["_source"]["packet_loss"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
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
                              conAtlasTime(hit["_source"]["timestamp"], False))
            arrRet["datesF"] = np.append(arrRet["datesF"], 
                               conAtlasTime(hit["_source"]["timestamp"], False)
                                 - timedelta(seconds=14400))
        #throughPut = np.append(throughPut, float(hit["_source"]["throughput"]) / math.pow(1000,3))
            arrRet["throughput"] = np.append(arrRet["throughput"], float(hit["_source"]["throughput"]))

        atlasTotalRec -= len(responseAtlas['hits']['hits'])
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
                             "gt" : int(conAtlasTime(startDate, True)),
                             "lt" : int(conAtlasTime(endDate, True))
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

    scannerHCC = esHCC.search(index="cms-*", 
                              doc_type="job", 
                              body=queryHCC, 
                              search_type="scan", 
                              scroll=scrollPreserve)
    scrollIdHCC = scannerHCC['_scroll_id']
    countHitsHCC = scannerHCC["hits"]["total"]
    arrRet = {}
    arrRet["location"] = np.zeros(0)
    while countHitsHCC > 0:
        responseHCC = esHCC.scroll(scroll_id=scrollIdHCC, 
                                   scroll=scrollPreserve)
        for hit in responseHCC["hits"]["hits"]:
            location = hit["_source"]["DataLocations"]
            if not str(location[0]) in arrRet["location"]:
                arrRet["location"] = np.append(arrRet["location"], 
                                               str(location[0]))
                arrRet[str(location[0])] = {}
                arrRet[str(location[0])]["CpuEff"] = np.zeros(0)
                arrRet[str(location[0])]["EventRate"] = np.zeros(0)
                arrRet[str(location[0])]["startDate"] = np.zeros(0)
                arrRet[str(location[0])]["endDate"] = np.zeros(0)
            arrRet[str(location[0])]["CpuEff"] = \
                  np.append(arrRet[str(location[0])]["CpuEff"], 
                  float(hit["_source"]["CpuEff"]))
            arrRet[str(location[0])]["EventRate"] = \
                  np.append(arrRet[str(location[0])]["EventRate"], 
                  float(hit["_source"]["EventRate"]))
            arrRet[str(location[0])]["startDate"] = \
                  np.append(arrRet[str(location[0])]["startDate"], 
                  dt.datetime.fromtimestamp(hit["_source"]["JobCurrentStartDate"], 
                        dt.timezone.utc))
            arrRet[str(location[0])]["endDate"] = \
                  np.append(arrRet[str(location[0])]["endDate"], 
                  dt.datetime.fromtimestamp(hit["_source"]["JobFinishedHookDone"], 
                        dt.timezone.utc))

            
        countHitsHCC -= len(responseHCC['hits']['hits'])
    return arrRet

with PdfPages('CMS_Plots.pdf') as pp:
    d = pp.infodict()
    d['Title'] = 'CMS Grid Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs'
    d['Keywords'] = 'PdfPages matplotlib CMS grid'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()

    for hit in cmsLocate["locations"]:
        hccResult = hccQuery(hit)
        for note in hccResult["location"]:
            fig = plt.figure(figsize=(12, 6))
            fig.autofmt_xdate()
            hcpu = fig.add_subplot(111)
            hcpu.plot(hccResult[note]["startDate"], hccResult[note]["CpuEff"], 'bs')
            hcpu.hlines(hccResult[note]["CpuEff"], 
                        hccResult[note]["startDate"], 
                        hccResult[note]["endDate"])
            hcpu.set_xlabel("Date of job period")
            hcpu.set_ylabel("CpuEff")
            hcpu.set_title(str("From " + hit + " To " + note + " Cpu Efficiency"))

            figu = plt.figure(figsize=(12,6))
            figu.autofmt_xdate()        
            hevent = figu.add_subplot(111)
            hevent.plot(hccResult[note]["startDate"], hccResult[note]["EventRate"], 'bs')
            hevent.hlines(hccResult[note]["EventRate"], 
                          hccResult[note]["startDate"], 
                          hccResult[note]["endDate"])
            hevent.set_xlabel("Date of job period")
            hevent.set_ylabel("EventRate")
            hevent.set_title(str("From " + hit + " To " + note + " Event Rate"))

            pp.savefig(fig)
            pp.savefig(figu)
            plt.close(figu)
            plt.close(fig)
