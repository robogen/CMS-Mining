from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json
import time

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    if type(time) == type("s"):
        return (dt.datetime.strptime(time, '%Y-%m-%dT%X')) \
                 .replace(tzinfo=dt.timezone.utc).timestamp()
    else:
        return time

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

esHCC = Elasticsearch([{
    'host': contents[0], 'port': contents[1]
}], timeout=600)

esCon = Elasticsearch([{
    'host': contents[4], 'port': contents[5]
}], timeout=600)

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-25T00:00:00"
tenMin = np.multiply(10,60)
stampStart = conAtlasTime(startDate) - tenMin
stampEnd = conAtlasTime(endDate)

loc = {}
loc["location"] = np.array([])

def atlasLatency(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             #"gt" : int(conAtlasTime(startDate)),
                             #"lt" : int(conAtlasTime(endDate))
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    responseAtlas = esCon.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan",
                                  size=500,
                                  doc_type="latency", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = responseAtlas['_scroll_id']
    atlasTotalRec = responseAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esCon.scroll(scroll_id=scrollIdAtlas, 
                                       scroll=scrollPreserve)
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None # Initialize
                if hit["_source"]["src"] == hit["_source"]["MA"]: # means MA is the src site
                    tempRay = np.array([#hit["_source"]["timestamp"],
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)),
                                        float(hit["_source"]["delay_mean"]),
                                        float(hit["_source"]["delay_median"]),
                                        float(hit["_source"]["delay_sd"]),
                                        float(0.0)])
                elif hit["_source"]["dest"] == hit["_source"]["MA"]: # means MA is the dest site
                    tempRay = np.array([#hit["_source"]["timestamp"],
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)),
                                        float(hit["_source"]["delay_mean"]),
                                        float(hit["_source"]["delay_median"]),
                                        float(hit["_source"]["delay_sd"]),
                                        float(1.0)])
                else:
                    raise NameError('MA is not the src or dest')
                if arrRet.size == 0:
                    arrRet = np.reshape(tempRay, (1,6))
                else:
                    arrRet = np.vstack((arrRet, tempRay))
            scrollIdAtlas = responseAtlas['_scroll_id']
            atlasTotalRec = len(responseAtlas['hits']['hits'])
        arrRet.view('f8,f8,f8,f8,f8,f8').sort(order=['f0'], axis=0)
        return arrRet

def atlasPacketLoss(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             #"gt" : int(conAtlasTime(startDate)),
                             #"lt" : int(conAtlasTime(endDate))
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    responseAtlas = esCon.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  doc_type="packet_loss_rate",
                                  search_type="scan",
                                  size=500, 
                                  scroll=scrollPreserve)
    scrollIdAtlas = responseAtlas['_scroll_id']
    atlasTotalRec = responseAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esCon.scroll(scroll_id=scrollIdAtlas, 
                                           scroll=scrollPreserve)
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None # Initialize
                if hit["_source"]["src"] == hit["_source"]["MA"]: # means MA is the src site
                    tempRay = np.array([#hit["_source"]["timestamp"],
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)),
                                        float(hit["_source"]["packet_loss"]),
                                        float(0.0)])
                elif hit["_source"]["dest"] == hit["_source"]["MA"]: # means MA is the dest site
                    tempRay = np.array([#hit["_source"]["timestamp"],
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)),
                                        float(hit["_source"]["packet_loss"]),
                                        float(1.0)])
                else:
                    raise NameError('MA is not src or dest')
                if arrRet.size == 0:
                    arrRet = np.reshape(tempRay, (1, 4))
                else:
                    arrRet = np.vstack((arrRet, tempRay))
            scrollIdAtlas = responseAtlas['_scroll_id']
            atlasTotalRec = len(responseAtlas['hits']['hits'])
        arrRet.view('f8,f8,f8,f8').sort(order=['f0'], axis=0)
        return arrRet

def atlasThroughput(srcSite, destSite):
    queryAtlas={"query" :
                {"bool": {
                   "must": [
                     {"match" :
                         {"srcSite" : srcSite }
                     },
                     {"match" :
                         {"destSite" : destSite }
                     },
                     {"range" : {
                         "timestamp" : {
                             #"gt" : int(conAtlasTime(startDate)),
                             #"lt" : int(conAtlasTime(endDate))
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               }
    responseAtlas = esCon.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  doc_type="throughput",
                                  size=500,
                                  scroll=scrollPreserve)
    scrollIdAtlas = responseAtlas['_scroll_id']
    atlasTotalRec = responseAtlas["hits"]["total"]
    arrRet = np.array([])
    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esCon.scroll(scroll_id=scrollIdAtlas, 
                                           scroll=scrollPreserve)
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None #Initialize in local context
                if hit["_source"]["src"] == hit["_source"]["MA"]: # Means MA is the src site
                    tempRay = np.array([#hit["_source"]["timestamp"], 
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)), 
                                        float(hit["_source"]["throughput"]),
                                        float(0.0)])
                elif hit["_source"]["dest"] == hit["_source"]["MA"]: #Means MA is the dest site
                    tempRay = np.array([#hit["_source"]["timestamp"],
                                        #hit["_source"]["timestamp"]
                                        conAtlasTime(hit["_source"]["timestamp"]),
                                        conAtlasTime(hit["_source"]["timestamp"])
                                          - np.multiply(4, np.multiply(60, 60)),
                                        float(hit["_source"]["throughput"]),
                                        float(1.0)])
                else:
                    raise NameError('MA is not src or dest')
                if arrRet.size == 0:
                    arrRet = np.reshape(tempRay, (1, 4))
                else:
                    arrRet = np.vstack((arrRet, tempRay))
            scrollIdAtlas = responseAtlas['_scroll_id']
            atlasTotalRec = len(responseAtlas['hits']['hits'])
        arrRet.view('f8,f8,f8,f8').sort(order=['f0'], axis=0)
        return arrRet

def hccQuery(site):
    queryHCC={"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"CMS_JobType" : "Processing"}
                      },
                      {"range" :
                         {"EventRate" : {"gte" : "0"}}
                      },
                      #{"match" : 
                      #   {"TaskType" : "Production"}
                      #},
                      {"range" : {
                         "CompletionDate" : {
                             "gt" : int(conAtlasTime(startDate)),
                             "lt" : int(conAtlasTime(endDate))
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
    if countHitsHCC == 0:
        return None
    else:
        while countHitsHCC > 0:
            responseHCC = esHCC.scroll(scroll_id=scrollIdHCC, 
                                       scroll=scrollPreserve)
            for hit in responseHCC["hits"]["hits"]:
                location = hit["_source"]["DataLocations"]
                if str(location[0]).lower() in cmsLocate["locations"]:
                    tempHolder = np.array([hit["_source"]["CpuEff"],
                                           #hit["_source"]["EventRate"],
                                           hit["_source"]["ChirpCMSSWEventRate"],
                                           hit["_source"]["JobCurrentStartDate"],
                                           hit["_source"]["JobFinishedHookDone"],
                                           hit["_source"]["CpuTimeHr"],
                                           hit["_source"]["WallClockHr"],
                                           hit["_source"]["RequestCpus"],
                                           hit["_source"]["MemoryMB"],
                                           hit["_source"]["QueueHrs"],
                                           hit["_source"]["RequestMemory"],
                                           hit["_source"]["CoreHr"],
                                           hit["_source"]["CpuBadput"],
                                           hit["_source"]["KEvents"]])
                    if not str(location[0]) in loc["location"]:
                        loc["location"] = np.append(loc["location"], 
                                                    str(location[0]))
                        arrRet[str(location[0])] = np.reshape(tempHolder, (1,13))
                    else:
                        arrRet[str(location[0])] = np.vstack((arrRet[str(location[0])],tempHolder))
            scrollIdHCC = responseHCC['_scroll_id']
            countHitsHCC = len(responseHCC['hits']['hits'])
        for hit in arrRet:
            #print(arrRet)
            #tempRay = arrRet[str(hit)]
            #arrRet[str(hit)] = tempRay[tempRay[:,2].argsort()]
            #arrRet[str(hit)] = sorted(arrRet[str(hit)], key=lambda x : x[2])
            arrRet[str(hit)].view('f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8').sort(order=['f2'], axis=0)
        
        return arrRet
print("Start")
start_time = time.time()
with PdfPages('CMS_Plots_Tester.pdf') as pp:
    d = pp.infodict()
    d['Title'] = 'CMS Grid Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs'
    d['Keywords'] = 'PdfPages matplotlib CMS grid'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()

    for hit in cmsLocate["locations"]:
        loc["location"] = np.array([])
        hccResult = hccQuery(hit)
        for note in loc["location"]:
            print("step 1")
            atlasT = atlasThroughput(sitesArray[hit], sitesArray[note.lower()])
            print("step 2")
            atlasP = atlasPacketLoss(sitesArray[hit], sitesArray[note.lower()])
            atlasL = atlasLatency(sitesArray[hit], sitesArray[note.lower()])
            print(hit + " To " + note)
            tempArr = hccResult[note]
            arrCpu = np.array([]);
            arrEvent = np.array([]);
            arrStart = np.array([]);
            arrEnd = np.array([]);
            for tpl in tempArr:
                arrCpu = np.append(arrCpu, tpl[0]);
                arrEvent = np.append(arrEvent, tpl[1]);
                arrStart = np.append(arrStart, utcDate(tpl[2]));
                arrEnd = np.append(arrEnd, utcDate(tpl[3]));

            figH, axH = plt.subplots(2, sharex=True)
            axH[1].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
                                                              defaultfmt="%m-%d %H:%M"))
            figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
            axH[0].plot(arrStart, arrCpu, 'bs')
            axH[0].hlines(arrCpu, 
                          arrStart, 
                          arrEnd)
            axH[0].set_ylabel("CpuEff")
            axH[0].set_title(str("2016 From " + hit + " To " + note))

            axH[1].plot(arrStart, arrEvent, 'bs')
            axH[1].hlines(arrEvent, 
                          arrStart, 
                          arrEnd)
            axH[1].set_ylabel("EventRate")
            pp.savefig(figH)
            plt.close(figH)
            #axA[2].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
            #                                                   defaultfmt="%m-%d %H:%M"))
            if not type(atlasT) == type(None) and not type(atlasP) == type(None):
                tDate = np.array([])
                tDatef = np.array([])
                tPut = np.array([])
                pDate = np.array([])
                pDatef = np.array([])
                pLoss = np.array([])
                for tpl in atlasT:
                    tDate = np.append(tDate, tpl[0])
                    tDatef = np.append(tDatef, tpl[1])
                    tPut = np.append(tPut, tpl[2])
                for tpl in atlasP:
                    pDate = np.append(pDate, tpl[0])
                    pDatef = np.append(pDatef, tpl[1])
                    pLoss = np.append(pLoss, tpl[2])
                figA, axA = plt.subplots(2, sharex=True)
                axA[0].set_title(str("2016 From " + \
                                     hit + " (" + \
                                     sitesArray[hit] + \
                                     ")" + " To " + \
                                     note + " (" + sitesArray[note.lower()] + ")"))
                figA.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                axA[0].plot(pDate, pLoss, 'bs')
                axA[0].set_ylabel("Packet Loss")
                axA[0].hlines(pLoss,
                              pDatef,
                              pDate)

                axA[1].set_ylabel("Throughput")
                axA[1].plot(tDate, tPut, 'bs')
                axA[1].hlines(tPut,
                              tDatef,
                              tDate)
                pp.savefig(figA)
                plt.close(figA)

            if not type(atlasL) == type(None):
                lDate = np.array([])
                lDatef = np.array([])
                lMean = np.array([])
                lMedian = np.array([])
                lStd = np.array([])
                for tpl in atlasL:
                    lDate = np.append(lDate, tpl[0])
                    lDatef = np.append(lDatef, tpl[1])
                    lMean = np.append(lMean, tpl[2])
                    lMedian = np.append(lMedian, tpl[3])
                    lStd = np.append(lStd, tpl[4])
                figL, axL = plt.subplots(3, sharex=True)
                axL[0].set_title(str("2016 Latency From " + \
                                     hit + " (" + \
                                     sitesArray[hit] + \
                                     ")" + " To " + \
                                     note + " (" + sitesArray[note.lower()] + ")"))
                figL.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                axL[0].set_ylabel("Mean")
                axL[0].plot(lDate, lMean, 'bs', label="delay_mean")
                axL[0].hlines(lMean,
                              lDatef,
                              lDate)
                axL[1].set_ylabel("Median")
                axL[1].plot(lDate, lMedian, 'rs', label="delay_median")
                axL[1].hlines(lMedian,
                              lDatef,
                              lDate)
                axL[2].set_ylabel("Std. Dev")
                axL[2].plot(lDate, lStd, 'g^', label="delay_sd")
                axL[2].hlines(lStd,
                              lDatef,
                              lDate)
                pp.savefig(figL)
                plt.close(figL)
print("finished")
elapsed_time = time.time() - start_time
print("Elaspsed time is %f" % elapsed_time)
