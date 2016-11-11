from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

esAtlas = Elasticsearch([{
    'host': contents[2], 'port': contents[3]
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

loc = {}
loc["location"] = np.array([])

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
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
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
            atlasTotalRec -= len(responseAtlas['hits']['hits'])
        arrRet.view('f8,f8,f8,f8,f8,f8').sort(order=['f0'], axis=0)
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
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
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

            atlasTotalRec -= len(responseAtlas['hits']['hits'])
        arrRet.view('f8,f8,f8,f8').sort(order=['f0'], axis=0)
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
    scannerAtlas = esAtlas.search(index="network_weather_2-*", 
                                  body=queryAtlas, 
                                  search_type="scan", 
                                  scroll=scrollPreserve)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])
    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
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
            atlasTotalRec -= len(responseAtlas['hits']['hits'])
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

            countHitsHCC -= len(responseHCC['hits']['hits'])
        for hit in arrRet:
            #print(arrRet)
            #tempRay = arrRet[str(hit)]
            #arrRet[str(hit)] = tempRay[tempRay[:,2].argsort()]
            #arrRet[str(hit)] = sorted(arrRet[str(hit)], key=lambda x : x[2])
            arrRet[str(hit)].view('f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8').sort(order=['f2'], axis=0)

        return arrRet

def main():
    for hit in cmsLocate["locations"]:
        loc["location"] = np.array([])
        hccResult = hccQuery(hit)
        for note in loc["location"]:
            atlasT = atlasThroughput(sitesArray[hit], sitesArray[note.lower()])
            atlasP = atlasPacketLoss(sitesArray[hit], sitesArray[note.lower()])
            atlasL = atlasLatency(sitesArray[hit], sitesArray[note.lower()])

            stampStart = conAtlasTime(startDate) - tenMin
            while stampStart <= stampEnd:
                tplCpu = np.array([])
                tplRate = np.array([])
                tplCpuTimeHr = np.array([])
                tplWallClockHr = np.array([])
                tplRequestCpus = np.array([])
                tplMemoryMB = np.array([])
                tplQueueHrs = np.array([])
                tplRequestMemory = np.array([])
                tplCoreHr = np.array([])
                tplCpuBadput = np.array([])
                tplKEvents = np.array([])
                #print(hccResult[note])
                for tpl in hccResult[note]:
                    if tpl[2] <= int(stampStart) and tpl[3] >= (int(stampStart) + tenMin):
                        tplCpu = np.append(tplCpu, tpl[0])
                        tplRate = np.append(tplRate, tpl[1])
                        tplCpuTimeHr = np.append(tplCpuTimeHr, tpl[4])
                        tplWallClockHr = np.append(tplWallClockHr, tpl[5])
                        tplRequestCpus = np.append(tplRequestCpus, tpl[6])
                        tplMemoryMB = np.append(tplMemoryMB, tpl[7])
                        tplQueueHrs = np.append(tplQueueHrs, tpl[8])
                        tplRequestMemory = np.append(tplRequestMemory, tpl[9])
                        tplCoreHr = np.append(tplCoreHr, tpl[10])
                        tplCpuBadput = np.append(tplCpuBadput, tpl[11])
                        tplKEvents = np.append(tplKEvents, tpl[12])
                if not tplCpu.size > 0:
                    stampStart = stampStart + tenMin
                elif type(atlasT) == type(None) and type(atlasP) == type(None) and type(atlasL) == type(None):
                    stampStart = stampStart + tenMin
                else:
                    srcThrough = np.array([])
                    destThrough = np.array([])
                    if not type(atlasT) == type(None):
                        for tpl in atlasT:
                            if tpl[1] <= stampStart and tpl[0] >= (stampStart + tenMin):
                                if tpl[3] == float(0.0):
                                    srcThrough = np.append(srcThrough, tpl[2])
                                else:
                                    destThrough = np.append(destThrough, tpl[2])
                    srcPacket = np.array([])
                    destPacket = np.array([])
                    if not type(atlasP) == type(None):
                        for tpl in atlasP:
                            if tpl[1] <= stampStart and tpl[0] >= (stampStart + tenMin):
                                if tpl[3] == float(0.0):
                                    srcPacket = np.append(srcPacket, tpl[2])
                                else:
                                    destPacket = np.append(destPacket, tpl[2])
                    srcLatency = np.array([])
                    destLatency = np.array([])
                    if not type(atlasL) == type(None):
                        for tpl in atlasL:
                            if tpl[1] <= stampStart and tpl[0] >= (stampStart + tenMin):
                                if tpl[5] == float(0.0):
                                    srcLatency = np.append(srcLatency, tpl[2])
                                else:
                                    destLatency = np.append(destLatency, tpl[2])
                    qBody={
                              "src": hit,
                              "dest": note,
                              "CpuEff": np.mean(tplCpu),
                              "EventRate": np.mean(tplRate),
                              "CpuTimeHr": np.mean(tplCpuTimeHr),
                              "WallClockHr": np.mean(tplWallClockHr),
                              "RequestCpus": np.mean(tplRequestCpus),
                              "MemoryMB": np.mean(tplMemoryMB),
                              "QueueHrs": np.mean(tplQueueHrs),
                              "RequestMemory": np.mean(tplRequestMemory),
                              "CoreHr": np.mean(tplCoreHr),
                              "CpuBadput": np.mean(tplCpuBadput),
                              "KEvents": np.mean(tplKEvents),
                              "beginDate": int(stampStart),
                              "endDate": int(stampStart + tenMin)
                          }
                    if srcThrough.size > 0:
                        qBody['srcThroughput'] = np.mean(srcThrough)
                    if destThrough.size > 0:
                        qBody['destThroughput'] = np.mean(destThrough)
                    if srcPacket.size > 0:
                        qBody['srcPacket'] = np.mean(srcPacket)
                    if destPacket.size > 0:
                        qBody['destPacket'] = np.mean(destPacket)
                    if srcLatency.size > 0:
                        qBody['srcLatency'] = np.mean(srcLatency)
                    if destLatency.size > 0:
                        qBody['destLatency'] = np.mean(destLatency)
                    print("about to post")
                    esCon.index(index='net-health', doc_type='dev', body=qBody)
                    stampStart = stampStart + tenMin

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
