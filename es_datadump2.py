from elasticsearch import Elasticsearch
import numpy as np
import datetime as dt
import math
import json
import pprint

pp = pprint.PrettyPrinter(indent=4)

with open('cms-limit.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc).strftime('%Y-%m-%dT%X')

def utcDateIndex(time):
    year = dt.datetime.fromtimestamp(time, dt.timezone.utc).strftime('%Y')
    month = dt.datetime.fromtimestamp(time, dt.timezone.utc).strftime('%m').lstrip("0")
    day = dt.datetime.fromtimestamp(time, dt.timezone.utc).strftime('%d')
    returnStr = year + "." + month + "." + day
    return returnStr

esAtlas = Elasticsearch([{
    'host': contents[2], 'port': contents[3]
}], timeout=50)

esHCC = Elasticsearch([{
    'host': contents[0], 'port': contents[1]
}], timeout=50)

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-25T00:00:00"
stampStart = conAtlasTime(startDate)
stampEnd = conAtlasTime(endDate)
oneDay = np.multiply(24,np.multiply(60,60))

def atlasLatency(query, dex, name):
    responseAtlas = esAtlas.search(index=dex, 
                                   body=query, 
                                   size=500,
                                   doc_type="latency",
                                   search_type="query_then_fetch", 
                                   scroll=scrollPreserve)
    atlasTotalRec = responseAtlas["hits"]["total"]
    scrollID = responseAtlas['_scroll_id']
    scrollSize = len(responseAtlas["hits"]["hits"])
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        filename = 'data/atlas-latency.json' #% name
        with open(filename, 'a') as outfile:
            while scrollSize > 0:
                responseAtlas = esAtlas.scroll(scroll_id=scrollID, scroll=scrollPreserve)
                for hit in responseAtlas["hits"]["hits"]:
                    json.dump(hit, outfile)
                    outfile.write('\n')
                #scrollID = responseAtlas['_scroll_id']
                scrollSize = len(responseAtlas["hits"]["hits"])
        return arrRet

def atlasPacketLoss(query, dex, name):
    responseAtlas = esAtlas.search(index=dex, 
                                   body=query, 
                                   size=500,
                                   doc_type="packet_loss_rate",
                                   search_type="query_then_fetch", 
                                   scroll=scrollPreserve)
    atlasTotalRec = responseAtlas["hits"]["total"]
    scrollID = responseAtlas['_scroll_id']
    scrollSize = len(responseAtlas["hits"]["hits"])
    arrRet = np.array([])

    if atlasTotalRec == 0:
        return None
    else:
        filename = 'data/atlas-packet.json' #% name
        with open(filename, 'a') as outfile:
            while scrollSize > 0:
                responseAtlas = esAtlas.scroll(scroll_id=scrollID, scroll=scrollPreserve)
                for hit in responseAtlas["hits"]["hits"]:
                    json.dump(hit, outfile)
                    outfile.write('\n')
                #scrollID = responseAtlas['_scroll_id']
                scrollSize = len(responseAtlas["hits"]["hits"])
        return arrRet

def atlasThroughput(query, dex, name):
    responseAtlas = esAtlas.search(index=dex, 
                                   body=query, 
                                   size=500,
                                   doc_type="throughput",
                                   search_type="query_then_fetch",
                                   scroll=scrollPreserve)
    atlasTotalRec = responseAtlas["hits"]["total"]
    scrollID = responseAtlas['_scroll_id']
    scrollSize = len(responseAtlas["hits"]["hits"])
    arrRet = np.array([])
    if atlasTotalRec == 0:
        return None
    else:
        filename = 'data/atlas-through.json' #% name
        with open(filename, 'a') as outfile:
            while scrollSize > 0:
                responseAtlas = esAtlas.scroll(scroll_id=scrollID, scroll=scrollPreserve)
                for hit in responseAtlas["hits"]["hits"]:
                    json.dump(hit, outfile)
                    outfile.write('\n')
                #scrollID = responseAtlas['_scroll_id']
                scrollSize = len(responseAtlas["hits"]["hits"])
        return arrRet

def hccQuery():
    queryHCC={"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"CMS_JobType" : "Processing"}
                      },
                      {"range" :
                         {"EventRate" : {"gte" : "0"}}
                      },
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
        with open('data/hcc.json', 'w') as outfile:
            while countHitsHCC > 0:
                responseHCC = esHCC.scroll(scroll_id=scrollIdHCC, 
                                           scroll=scrollPreserve)
                for hit in responseHCC["hits"]["hits"]:
                        json.dump(hit, outfile)
                        outfile.write('\n')
                countHitsHCC -= len(responseHCC['hits']['hits'])
        return arrRet

def main():
    #hccResult = hccQuery()
    print("hccResult finished")

    starter = stampStart 
    ender = stampEnd
    indexTimer = "network_weather_2-%s"
    while starter <= ender:
        query={
          "query" : {
            "bool" : {
              "must" : {
                "range" : {
                  "timestamp" : {
                    "gt" : utcDate(starter),
                    "lt" : utcDate(starter + oneDay)
                  }
                }
              },
              "filter" : {
                "terms" : {
                  "srcSite" : cmsLocate["locations"]
                },
                "terms" : {
                  "destSite" : cmsLocate["locations"]
                }
              }
            }
          }
        }
        timer = indexTimer % utcDateIndex(starter)
        print(timer)
        print(utcDate(starter))
        atlasT = atlasThroughput(query, timer, utcDateIndex(starter))
        print("atlasThroughput finished")
        atlasP = atlasPacketLoss(query, timer, utcDateIndex(starter))
        print("atlasPacketLoss finished")
        atlasL = atlasLatency(query, timer, utcDateIndex(starter))
        print("atlasLatency finished")
        starter = starter + oneDay

print("started")
main()
print("finished")
