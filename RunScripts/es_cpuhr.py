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

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

esCon = Elasticsearch([{
    'host': contents[4], 'port': contents[5]
}], timeout=30)

pp = pprint.PrettyPrinter(indent=4)

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

def utcStamp(time):
    return (dt.datetime.strptime(time,'%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-25T00:00:00"
utcStart = utcStamp(startDate)
utcEnd = utcStamp(endDate)
oneDay = np.multiply(24,np.multiply(60,60))

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

def esConQuery(src, dest):
    queryBody={"query" :
                {"bool": {
                  "must": [
                    {"match" :
                      {"src" : src}
                    },
                    {"match" :
                      {"dest" : dest}
                    },
                    {"range" : {
                       "beginDate" : {
                         "gt" : int(utcStart),
                         "lt" : int((utcStart + oneDay))
                       }
                     }
                    }
                  ]
                 }
                }, "sort": {"beginDate": {"order": "desc"}}
              }
    scannerCon = esCon.search(index="net-health",
                              body=queryBody,
                              search_type="scan",
                              scroll=scrollPreserve)
    scrollIdCon = scannerCon['_scroll_id']
    conTotalRec = scannerCon["hits"]["total"]
    arrRet = {}
    arrRet['disk'] = np.array([])

    if conTotalRec == 0:
        return None
    else:
        while conTotalRec > 0:
            responseCon = esCon.scroll(scroll_id=scrollIdCon,
                                       scroll=scrollPreserve)
            for hit in responseCon["hits"]["hits"]:
                if not arrRet['disk'].size > 0:
                    arrRet['disk'] = np.reshape(np.array([hit["_source"]["CpuTimeHr"],
                                                                   hit["_source"]["CpuEff"],
                                                                   hit["_source"]["EventRate"]]), (1,3))
                else:
                    arrRet['disk'] = np.vstack((arrRet['disk'],
                                                         np.array([hit["_source"]["CpuTimeHr"],
                                                                   hit["_source"]["CpuEff"],
                                                                   hit["_source"]["EventRate"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        return arrRet

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main(utcStart):
    with PdfPages('CMS_CpuHr.pdf') as pc:
        d = pc.infodict()
        d['Title'] = 'CMS Scatter Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()
        #qResults = esConQuery('t1_de_kit','T1_ES_PIC')
        while utcStart <= utcEnd:
            srcSites = esConAgg("src")
            destSites = esConAgg("dest")
            workDate = utcDate(utcStart)
            for ping in srcSites:
                for pong in destSites:
                    qResults = esConQuery(ping, pong)
                    if not type(qResults) == type(None):
                        diskUsage = qResults['disk']
                        figsT, axsT = plt.subplots(2, sharex=True)
                        axsT[0].scatter(diskUsage[:,0],diskUsage[:,1])
                        axsT[1].scatter(diskUsage[:,0],diskUsage[:,2])
                        axsT[0].set_ylabel("CpuEff")
                        axsT[1].set_ylabel("EventRate")
                        axsT[1].set_xlabel("CpuTimeHr")
                        axsT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                        pc.savefig(figsT)
                        plt.close(figsT)
            utcStart = utcStart + oneDay
                
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("CpuEff")

# Run Main code
print("start")
main(utcStart)
print("finish")
