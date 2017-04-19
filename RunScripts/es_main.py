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
        con = time.split('+', 1)[0]
        fom = ""
        if "-" not in con:
            fom = "%Y%m%dT%H%M%S"
        else:
            fom = "%Y-%m-%dT%X"
        return (dt.datetime.strptime(con, fom)).replace(tzinfo=dt.timezone.utc).timestamp()
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
startDate = "2017-02-01T00:00:00"
endDate = "2017-02-28T23:59:59"
tenMin = np.multiply(10,60)
stampStart = conAtlasTime(startDate) - tenMin
stampEnd = conAtlasTime(endDate)
querySize = 10000
tasktype = ["DIGIRECO", "RECO", "DIGI", "DataProcessing"]
#tasktype = ["gensim"]

loc = {}
loc["location"] = np.array([])
loc["Workflow"] = np.array([])
loc["LastRemoteHost"] = np.array([])

def esConAgg(field, task, site):
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
                      {"match" :
                         {"Site" : site }
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

    if conTotalRec == 0:
        return None
    else:
        for hit in conTotalRec:
            arrRet = np.append(arrRet, hit['key'])
        pretty.pprint(arrRet)
        return arrRet

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
                             "gt" : int(conAtlasTime(startDate) * 1000),
                             "lt" : int(conAtlasTime(endDate) * 1000)
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather-*", 
                                  body=queryAtlas, 
                                  search_type="query_then_fetch", 
                                  scroll=scrollPreserve,
                                  size=querySize)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                       scroll=scrollPreserve)
            print("latency: " + str(atlasTotalRec))
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None # Initialize
                if "srcSite" in hit["_source"].keys() and "destSite" in hit["_source"].keys():
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
            atlasTotalRec -= querySize
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
                             "gt" : int(conAtlasTime(startDate) * 1000),
                             "lt" : int(conAtlasTime(endDate) * 1000)
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather-*", 
                                  body=queryAtlas, 
                                  search_type="query_then_fetch", 
                                  scroll=scrollPreserve,
                                  size=querySize)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                           scroll=scrollPreserve)
            print("packet: " + str(atlasTotalRec))
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None # Initialize
                if "srcSite" in hit["_source"].keys() and "destSite" in hit["_source"].keys():
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

            atlasTotalRec -= querySize
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
                             "gt" : int(conAtlasTime(startDate) * 1000),
                             "lt" : int(conAtlasTime(endDate) * 1000)
                         }
                     }}
                   ]
                 }
                }
               }
    scannerAtlas = esAtlas.search(index="network_weather-*", 
                                  body=queryAtlas, 
                                  search_type="query_then_fetch", 
                                  scroll=scrollPreserve,
                                  size=querySize)
    scrollIdAtlas = scannerAtlas['_scroll_id']
    atlasTotalRec = scannerAtlas["hits"]["total"]
    arrRet = np.array([])
    if atlasTotalRec == 0:
        return None
    else:
        while atlasTotalRec > 0:
            responseAtlas = esAtlas.scroll(scroll_id=scrollIdAtlas, 
                                           scroll=scrollPreserve)
            print("throughput: " + str(atlasTotalRec))
            for hit in responseAtlas["hits"]["hits"]:
                tempRay = None #Initialize in local context
                if "srcSite" in hit["_source"].keys() and "destSite" in hit["_source"].keys():
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
            atlasTotalRec -= querySize
        arrRet.view('f8,f8,f8,f8').sort(order=['f0'], axis=0)
        return arrRet

def hccQuery(site, task):
    queryHCC={"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"CMS_JobType" : "Processing"}
                      },
                      #{"range" :
                      #   {"EventRate" : {"gte" : "0"}}
                      #},
                      {"match" :
                         { "Status" : "Completed" }
                      },
                      {"exists" :
                         { "field" : "CMSSWKLumis" }
                      },
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
                         {"Site" : site }
                      },
                      {"match" :
                         {"InputData" : "Offsite"}
                      }
                  ]#,
                #"filter" : {
                #    "term" : { "TaskType" : task }
                #}
              }
          }
      }

    scannerHCC = esHCC.search(index="cms-*", 
                              doc_type="job", 
                              body=queryHCC, 
                              search_type="scan", 
                              scroll=scrollPreserve,
                              size=querySize)
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
                #pretty.pprint(hit)
                #print(hit["_source"]["TaskType"])
                if str(location[0]).lower() in cmsLocate["locations"]:
                    if hit["_source"]["KEvents"] > 0:
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
                                               hit["_source"]["CoreHr"],
                                               hit["_source"]["CpuBadput"],
                                               hit["_source"]["KEvents"],
                                               hit["_source"]["InputGB"],
                                               np.divide(hit["_source"]["WallClockHr"],
                                                         hit["_source"]["KEvents"]),
                                               hit["_source"]["CMSSWKLumis"],
                                               hit["_source"]["ReadTimeMins"]])
                    else:
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
                                               hit["_source"]["CoreHr"],
                                               hit["_source"]["CpuBadput"],
                                               hit["_source"]["KEvents"],
                                               hit["_source"]["InputGB"],
                                               float(0.0),
                                               hit["_source"]["CMSSWKLumis"],
                                               hit["_source"]["ReadTimeMins"]])
                    prevHost = str("")
                    if not "LastRemoteHost" in hit["_source"]:
                        prevHost = "Unknown"
                    else:
                        prevHost = str(hit["_source"]["LastRemoteHost"])

                    if not str(str(location[0]) + str(hit["_source"]["Workflow"]) + prevHost) in arrRet:
                        if not str(location[0]) in loc["location"]:
                            loc["location"] = np.append(loc["location"], 
                                                    str(location[0]))
                        if not str(hit["_source"]["Workflow"]) in loc["Workflow"]:
                            loc["Workflow"] = np.append(loc["Workflow"],
                                                    str(hit["_source"]["Workflow"]))
                        if not prevHost in loc["LastRemoteHost"]:
                            loc["LastRemoteHost"] = np.append(loc["LastRemoteHost"],
                                                              prevHost)
                        arrRet[str(location[0]) + str(hit["_source"]["Workflow"]) + prevHost] = np.reshape(tempHolder, (1,16))
                    else:
                        arrRet[str(location[0]) + str(hit["_source"]["Workflow"]) + prevHost] = np.vstack((arrRet[str(location[0]) + str(hit["_source"]["Workflow"]) + prevHost],tempHolder))

            countHitsHCC -= len(responseHCC['hits']['hits'])
        for hit in arrRet:
            #print(arrRet)
            #tempRay = arrRet[str(hit)]
            #arrRet[str(hit)] = tempRay[tempRay[:,2].argsort()]
            #arrRet[str(hit)] = sorted(arrRet[str(hit)], key=lambda x : x[2])
            arrRet[str(hit)].view('f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8,f8').sort(order=['f2'], axis=0)

        return arrRet

def main():
    with PdfPages('PDFOut/CMS_Plots.pdf') as pp:
        d = pp.infodict()
        d['Title'] = 'CMS Grid Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()

        for hit in cmsLocate["locations"]:
            loc["location"] = np.array([])
            loc["Workflow"] = np.array([])
            loc["LastRemoteHost"] = np.array([])
            for task in tasktype:
                hccResult = hccQuery(hit, task)
                if not type(hccResult) is type(None):
                    for note in loc["location"]:
                        atlasT = atlasThroughput(sitesArray[hit], sitesArray[note.lower()])
                        atlasP = atlasPacketLoss(sitesArray[hit], sitesArray[note.lower()])
                        atlasL = atlasLatency(sitesArray[hit], sitesArray[note.lower()])
                        print("atlas stage")
                        pretty.pprint(atlasP)
                        for workFlow in loc["Workflow"]:
                            for lastHost in loc["LastRemoteHost"]:
                                if not type(note) is type(None) and not type(workFlow) is type(None) and not type(lastHost) is type(None):
                                    if str(note + workFlow + lastHost) in hccResult:
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
                                            tplInputGB = np.array([])
                                            tplLumos = np.array([])
                                            tplLumi = np.array([])
                                            tplRead = np.array([])
                                            #print(hccResult[note])
                                            for tpl in hccResult[note + workFlow + lastHost]:
                                                if tpl[2] <= int(stampStart) and tpl[3] >= (int(stampStart) + tenMin):
                                                    tplCpu = np.append(tplCpu, tpl[0])
                                                    tplRate = np.append(tplRate, tpl[1])
                                                    tplCpuTimeHr = np.append(tplCpuTimeHr, tpl[4])
                                                    tplWallClockHr = np.append(tplWallClockHr, tpl[5])
                                                    tplRequestCpus = np.append(tplRequestCpus, tpl[6])
                                                    tplMemoryMB = np.append(tplMemoryMB, tpl[7])
                                                    tplQueueHrs = np.append(tplQueueHrs, tpl[8])
                                                    tplCoreHr = np.append(tplCoreHr, tpl[9])
                                                    tplCpuBadput = np.append(tplCpuBadput, tpl[10])
                                                    tplKEvents = np.append(tplKEvents, tpl[11])
                                                    tplInputGB = np.append(tplInputGB, tpl[12])
                                                    tplLumos = np.append(tplLumos, tpl[13])
                                                    tplLumi = np.append(tplLumi, tpl[14])
                                                    tplRead = np.append(tplRead, tpl[15])
                                            if not tplCpu.size > 0:
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
            
                                                          "minCpuEff": np.min(tplCpu),
                                                          "meanCpuEff": np.mean(tplCpu),
                                                          "maxCpuEff": np.max(tplCpu),
                                                          "medianCpuEff": np.median(tplCpu),
                                                          "stdCpuEff": np.std(tplCpu),
            
                                                          "minEventRate": np.min(tplRate),
                                                          "meanEventRate": np.mean(tplRate),
                                                          "maxEventRate": np.max(tplRate),
                                                          "medianEventRate": np.median(tplRate),
                                                          "stdEventRate": np.std(tplRate),
            
                                                          "meanCpuTimeHr": np.mean(tplCpuTimeHr),
                                                          "minCpuTimeHr": np.min(tplCpuTimeHr),
                                                          "maxCpuTimeHr": np.max(tplCpuTimeHr),
                                                          "medianCpuTimeHr": np.median(tplCpuTimeHr),
                                                          "stdCpuTimeHr": np.std(tplCpuTimeHr),
            
                                                          "meanWallClockHr": np.mean(tplWallClockHr),
                                                          "medianWallClockHr":np.median(tplWallClockHr),
                                                          "maxWallClockHr":np.max(tplWallClockHr),
                                                          "minWallClockHr": np.min(tplWallClockHr),
                                                          "stdWallClockHr": np.std(tplWallClockHr),
            
                                                          "meanRequestCpus": np.mean(tplRequestCpus),
                                                          "medianRequestCpus": np.median(tplRequestCpus),
                                                          "minRequestCpus": np.min(tplRequestCpus),
                                                          "maxRequestCpus": np.max(tplRequestCpus),
                                                          "stdRequestCpus": np.std(tplRequestCpus),
              
                                                          "meanMemoryMB": np.mean(tplMemoryMB),
                                                          "medianMemoryMB": np.median(tplMemoryMB),
                                                          "minMemoryMB": np.min(tplMemoryMB),
                                                          "maxMemoryMB": np.max(tplMemoryMB),
                                                          "stdMemoryMB": np.std(tplMemoryMB),
            
                                                          "meanQueueHrs": np.mean(tplQueueHrs),
                                                          "medianQueueHrs": np.median(tplQueueHrs),
                                                          "minQueueHrs": np.min(tplQueueHrs),
                                                          "maxQueueHrs": np.max(tplQueueHrs),
                                                          "stdQueueHrs": np.std(tplQueueHrs),
            
                                                          "meanCoreHr": np.mean(tplCoreHr),
                                                          "medianCoreHr": np.median(tplCoreHr),
                                                          "minCoreHr": np.min(tplCoreHr),
                                                          "maxCoreHr": np.max(tplCoreHr),
                                                          "stdCoreHr": np.std(tplCoreHr),
            
                                                          "meanCpuBadput": np.mean(tplCpuBadput),
                                                          "medianCpuBadput": np.median(tplCpuBadput),
                                                          "stdCpuBadput": np.std(tplCpuBadput),
                                                          "maxCpuBadput": np.max(tplCpuBadput),
                                                          "minCpuBadput": np.min(tplCpuBadput),
            
                                                          "stdInputGB" : np.std(tplInputGB),
                                                          "meanInputGB" : np.mean(tplInputGB),
                                                          "medianInputGB" : np.median(tplInputGB),
                                                          "maxInputGB" : np.max(tplInputGB),
                                                          "minInputGB" : np.min(tplInputGB),
            
                                                          "maxLumosity" : np.max(tplLumos),
                                                          "minLumosity" : np.min(tplLumos),
                                                          "stdLumosity" : np.std(tplLumos),
                                                          "medianLumosity" : np.median(tplLumos),
                                                          "meanLumosity" : np.mean(tplLumos),
            
                                                          "stdKEvents": np.std(tplKEvents),
                                                          "meanKEvents": np.mean(tplKEvents),
                                                          "medianKEvents": np.median(tplKEvents),
                                                          "maxKEvents": np.max(tplKEvents),
                                                          "minKEvents": np.min(tplKEvents),
            
                                                          "stdCMSSWKLumis": np.std(tplLumi),
                                                          "meanCMSSWKLumis": np.mean(tplLumi),
                                                          "medianCMSSWKLumis": np.median(tplLumi),
                                                          "maxCMSSWKLumis": np.max(tplLumi),
                                                          "minCMSSWKLumis": np.min(tplLumi),
            
                                                          "stdReadTimeMins": np.std(tplRead),
                                                          "meanReadTimeMins": np.mean(tplRead),
                                                          "medianReadTimeMins": np.mean(tplRead),
                                                          "maxReadTimeMins": np.max(tplRead),
                                                          "minReadTimeMins": np.min(tplRead),
            
                                                          "beginDate": int(stampStart),
                                                          "Workflow": str(workFlow),
                                                          "LastRemoteHost": str(lastHost),
                                                          "endDate": int(stampStart + tenMin)
                                                         }
            
                                                if srcThrough.size > 0:
                                                    qBody['mediansrcThroughput'] = np.median(srcThrough)
                                                    qBody['stdsrcThroughput'] = np.std(srcThrough)
                                                    qBody['maxsrcThroughput'] = np.max(srcThrough)
                                                    qBody['minsrcThroughput'] = np.min(srcThrough)
                                                    qBody['meansrcThroughput'] = np.mean(srcThrough)
        
                                                if destThrough.size > 0:
                                                    qBody['stddestThroughput'] = np.std(destThrough)
                                                    qBody['maxdestThroughput'] = np.max(destThrough)
                                                    qBody['mindestThroughput'] = np.min(destThrough)
                                                    qBody['mediandestThroughput'] = np.median(destThrough)
                                                    qBody['meandestThroughput'] = np.mean(destThrough)
            
                                                if srcPacket.size > 0:
                                                    qBody['stdsrcPacket'] = np.std(srcPacket)
                                                    qBody['maxsrcPacket'] = np.max(srcPacket)
                                                    qBody['minsrcPacket'] = np.min(srcPacket)
                                                    qBody['mediansrcPacket'] = np.median(srcPacket)
                                                    qBody['meansrcPacket'] = np.mean(srcPacket)
            
                                                if destPacket.size > 0:
                                                    qBody['stddestPacket'] = np.std(destPacket)
                                                    qBody['maxdestPacket'] = np.max(destPacket)
                                                    qBody['mindestPacket'] = np.min(destPacket)
                                                    qBody['mediandestPacket'] = np.median(destPacket)
                                                    qBody['meandestPacket'] = np.mean(destPacket)
            
                                                if srcLatency.size > 0:
                                                    qBody['stdsrcLatency'] = np.std(srcLatency)
                                                    qBody['maxsrcLatency'] = np.max(srcLatency)
                                                    qBody['minsrcLatency'] = np.min(srcLatency)
                                                    qBody['mediansrcLatency'] = np.median(srcLatency)
                                                    qBody['meansrcLatency'] = np.mean(srcLatency)
            
                                                if destLatency.size > 0:
                                                    qBody['meandestLatency'] = np.mean(destLatency)
                                                    qBody['mediandestLatency'] = np.median(destLatency)
                                                    qBody['mindestLatency'] = np.min(destLatency)
                                                    qBody['maxdestLatency'] = np.max(destLatency)
                                                    qBody['stddestLatency'] = np.std(destLatency)
            
                                                print("posting")
                                                esCon.index(index='net-health', doc_type=task, body=qBody)
                                                stampStart = stampStart + tenMin
                                                print(stampStart)
        
#                                arrCpu = np.array([]);
#                                arrEvent = np.array([]);
#                                arrStart = np.array([]);
#                                arrEnd = np.array([]);
#                                for tpl in hccResult[note + workFlow]:
#                                    arrCpu = np.append(arrCpu, tpl[0]);
#                                    arrEvent = np.append(arrEvent, tpl[1]);
#                                    arrStart = np.append(arrStart, utcDate(tpl[2]));
#                                    arrEnd = np.append(arrEnd, utcDate(tpl[3]));

#                                figH, axH = plt.subplots(2, sharex=True)
#                                axH[1].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
#                                                                                   defaultfmt="%m-%d %H:%M"))
#                                figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
#                                axH[0].plot(arrStart, arrCpu, 'bs')
#                                axH[0].hlines(arrCpu, 
#                                              arrStart, 
#                                              arrEnd)
#                                axH[0].set_ylabel("CpuEff")
#                                axH[0].set_title(str("2016 From " + hit + " To " + note + "in Task " + task))
        
#                                axH[1].plot(arrStart, arrEvent, 'bs')
#                                axH[1].hlines(arrEvent, 
#                                              arrStart, 
#                                              arrEnd)
#                                axH[1].set_ylabel("EventRate")
#                                pp.savefig(figH)
#                                plt.close(figH)
#                                if not type(atlasT) == type(None) and not type(atlasP) == type(None):
#                                    tDate = np.array([])
#                                    tDatef = np.array([])
#                                    tPut = np.array([])
#                                    pDate = np.array([])
#                                    pDatef = np.array([])
#                                    pLoss = np.array([])
#                                    for tpl in atlasT:
#                                        tDate = np.append(tDate, tpl[0])
#                                        tDatef = np.append(tDatef, tpl[1])
#                                        tPut = np.append(tPut, tpl[2])
#                                    for tpl in atlasP:
#                                        pDate = np.append(pDate, tpl[0])
#                                        pDatef = np.append(pDatef, tpl[1])
#                                        pLoss = np.append(pLoss, tpl[2])
#                                    figA, axA = plt.subplots(2, sharex=True)
#                                    axA[0].set_title(str("2016 From " + \
#                                                         hit + " (" + \
#                                                         sitesArray[hit] + \
#                                                         ")" + " To " + \
#                                                         note + " (" + sitesArray[note.lower()] + ")"))
#                                    figA.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
#                                    axA[0].plot(pDate, pLoss, 'bs')
#                                    axA[0].set_ylabel("Packet Loss")
#                                    axA[0].hlines(pLoss,
#                                                  pDatef,
#                                                  pDate)
    
#                                    axA[1].set_ylabel("Throughput")
#                                    axA[1].plot(tDate, tPut, 'bs')
#                                    axA[1].hlines(tPut,
#                                                  tDatef,
#                                                  tDate)
#                                    pp.savefig(figA)
#                                    plt.close(figA)

#                                if not type(atlasL) == type(None):
#                                    lDate = np.array([])
#                                    lDatef = np.array([])
#                                    lMean = np.array([])
#                                    lMedian = np.array([])
#                                    lStd = np.array([])
#                                    for tpl in atlasL:
#                                        lDate = np.append(lDate, tpl[0])
#                                        lDatef = np.append(lDatef, tpl[1])
#                                        lMean = np.append(lMean, tpl[2])
#                                        lMedian = np.append(lMedian, tpl[3])
#                                        lStd = np.append(lStd, tpl[4])
#                                    figL, axL = plt.subplots(3, sharex=True)
#                                    axL[0].set_title(str("2016 Latency " + task + " From " + \
#                                                         hit + " (" + \
#                                                         sitesArray[hit] + \
#                                                         ")" + " To " + \
#                                                         note + " (" + sitesArray[note.lower()] + ")"))
#                                    figL.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
#                                    axL[0].set_ylabel("Mean")
#                                    axL[0].plot(lDate, lMean, 'bs', label="delay_mean")
#                                    axL[0].hlines(lMean,
#                                                  lDatef,
#                                                  lDate)
#                                    axL[1].set_ylabel("Median")
#                                    axL[1].plot(lDate, lMedian, 'rs', label="delay_median")
#                                    axL[1].hlines(lMedian,
#                                                  lDatef,
#                                                  lDate)
#                                    axL[2].set_ylabel("Std. Dev")
#                                    axL[2].plot(lDate, lStd, 'g^', label="delay_sd")
#                                    axL[2].hlines(lStd,
#                                                  lDatef,
#                                                  lDate)
#                                    pp.savefig(figL)
#                                    plt.close(figL)


print("start")
main()
print("finish")
