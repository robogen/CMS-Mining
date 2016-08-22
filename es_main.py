from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
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
            if str(location[0]).lower() in cmsLocate["locations"]:
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
            atlasT = atlasThroughput(sitesArray[hit], sitesArray[note.lower()])
            atlasP = atlasPacketLoss(sitesArray[hit], sitesArray[note.lower()])
            atlasL = atlasLatency(sitesArray[hit], sitesArray[note.lower()])
            figH, axH = plt.subplots(2, sharex=True)
            figA, axA = plt.subplots(3, sharex=True)

            axH[1].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
                                                              defaultfmt="%m-%d %H:%M"))
            figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            axH[0].plot(hccResult[note]["startDate"], hccResult[note]["CpuEff"], 'bs')
            axH[0].hlines(hccResult[note]["CpuEff"], 
                        hccResult[note]["startDate"], 
                        hccResult[note]["endDate"])
            axH[0].set_ylabel("CpuEff")
            axH[0].set_title(str("2016 From " + hit + " To " + note))

            axH[1].plot(hccResult[note]["startDate"], hccResult[note]["EventRate"], 'bs')
            axH[1].hlines(hccResult[note]["EventRate"], 
                         hccResult[note]["startDate"], 
                         hccResult[note]["endDate"])
            axH[1].set_ylabel("EventRate")

            #axA[2].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
            #                                                   defaultfmt="%m-%d %H:%M"))
            axA[0].set_title(str("2016 From " + \
                                 hit + " (" + \
                                 sitesArray[hit] + \
                                 ")" + " To " + \
                                 note + " (" + sitesArray[note.lower()] + ")"))
            figA.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            axA[0].plot(atlasP["dates"], atlasP["packet_loss"], 'bs')
            axA[0].set_ylabel("Packet Loss")
            axA[0].hlines(atlasP["packet_loss"],
                          atlasP["datesF"],
                          atlasP["dates"])
            axA[1].set_ylabel("Latency")
            axA[1].plot(atlasL["dates"], atlasL["delay_mean"], 'bs', label="delay_mean")
            axA[1].hlines(atlasL["delay_mean"],
                          atlasL["datesF"],
                          atlasL["dates"])
            axA[1].plot(atlasL["dates"], atlasL["delay_median"], 'rs', label="delay_median")
            axA[1].hlines(atlasL["delay_median"],
                          atlasL["datesF"],
                          atlasL["dates"])
            axA[1].plot(atlasL["dates"], atlasL["delay_sd"], 'g^', label="delay_sd")
            axA[1].hlines(atlasL["delay_sd"],
                          atlasL["datesF"],
                          atlasL["dates"])
            axA[2].set_ylabel("Throughput")
            axA[2].plot(atlasT["dates"], atlasT["throughput"], 'bs')
            axA[2].hlines(atlasT["throughput"],
                          atlasT["datesF"],
                          atlasT["dates"])

            pp.savefig(figH)
            pp.savefig(figA)
            plt.close(figH)
            plt.close(figA)
