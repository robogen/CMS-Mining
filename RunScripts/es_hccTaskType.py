from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json
import pprint

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

with open("cms.json", "r+") as txt:
    cmsLocate = json.load(txt)

esCon = Elasticsearch([{
    'host': contents[0], 'port': contents[5]
}], timeout=30)

pp = pprint.PrettyPrinter(indent=4)

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

def utcStamp(time):
    return (dt.datetime.strptime(time,'%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

scrollPreserve="3m"
#startDate = "2016-07-17T00:00:00"
#endDate = "2016-07-25T00:00:00"
startDate = "2017-02-01T00:00:00"
endDate = "2017-02-09T00:00:00"
utcStart = utcStamp(startDate)
utcEnd = utcStamp(endDate)
oneDay = np.multiply(24,np.multiply(60,60))
tasktype = ["RECO", "DIGIRECO"]

def esConAgg(field):
    queryBody={"aggs": {
                 "dev": {
                   "terms": {"field":field}
                 }
               }
              }
    scannerCon = esCon.search(index="net-health",
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
        return arrRet

def esConQuery(task, site):
    queryBody={"query" :
                {"bool": {
                  "must": [
                    {"match" :
                      {"TaskType" : task}
                    },
                    #{"match" :
                    #  {"DataLocationsCount": 1}
                    #},
                    {"match" :
                      {"InputData" : "Offsite"}
                    },
                    {"exists":
                      {"field": "ChirpCMSSWEventRate"}
                    },
                    {"match" :
                      {"Site" : site}
                    },
                    {"range" : {
                       "JobFinishedHookDone" : {
                         "gt" : int(utcStart),
                         "lt" : int((utcStart + oneDay))
                       }
                     }
                    }
                  ]
                 }
                }, "sort": {"JobFinishedHookDone": {"order": "desc"}}
              }
    scannerCon = esCon.search(index="cms-*",
                              body=queryBody,
                              search_type="scan",
                              scroll=scrollPreserve)
    scrollIdCon = scannerCon['_scroll_id']
    conTotalRec = scannerCon["hits"]["total"]
    arrRet = {}
    arrRet['return'] = np.array([])

    if conTotalRec == 0:
        return None
    else:
        while conTotalRec > 0:
            responseCon = esCon.scroll(scroll_id=scrollIdCon,
                                       scroll=scrollPreserve)
            for hit in responseCon["hits"]["hits"]:
                if not arrRet['return'].size > 0:
                    arrRet['return'] = np.reshape(np.array([hit["_source"]["CpuEff"],
                                                            hit["_source"]["ChirpCMSSWEventRate"]]), (1,2))
                else:
                    arrRet['return'] = np.vstack((arrRet['return'],
                                                  np.array([hit["_source"]["CpuEff"],
                                                            hit["_source"]["ChirpCMSSWEventRate"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        return arrRet

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main(utcStart):
    with PdfPages('CMS_HCCTaskType.pdf') as pc:
        d = pc.infodict()
        d['Title'] = 'CMS Scatter Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()
        #qResults = esConQuery('t1_de_kit','T1_ES_PIC')
        while utcStart <= utcEnd:
            workDate = utcDate(utcStart)
            for hit in cmsLocate["locations"]:
                for task in tasktype:
                    qResults = esConQuery(task, hit)
                    if not type(qResults) == type(None):
                        valRet = qResults['return']
                        figsT, axsT = plt.subplots(1)
                        axsT.scatter(valRet[:,0],valRet[:,1])
                        axsT.set_ylabel("EventRate")
                        axsT.set_xlabel("CpuEff")
                        axsT.set_title(str(task + " on " + workDate.strftime('%d-%B-%Y') + " at " + hit))
                        pc.savefig(figsT)
                        plt.close(figsT)
            utcStart = utcStart + oneDay
            print(workDate.strftime('%d-%B-%Y'))
                
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("CpuEff")

# Run Main code
print("start")
main(utcStart)
print("finished")
