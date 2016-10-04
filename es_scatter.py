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

esCon = Elasticsearch([{
    'host': contents[4], 'port': contents[5]
}], timeout=30)

scrollPreserve="3m"
startDate = "2016-07-21T00:00:00"
endDate = "2016-07-25T00:00:00"

pp = pprint.PrettyPrinter(indent=4)

def utcStamp(time):
    return (dt.datetime.strptime(time,'%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

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
                         "gt" : int(utcStamp(startDate)),
                         "lt" : int(utcStamp(endDate))
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
    arrRet['src'] = np.array([])
    arrRet['dest'] = np.array([])

    if conTotalRec == 0:
        return None
    else:
        while conTotalRec > 0:
            responseCon = esCon.scroll(scroll_id=scrollIdCon,
                                       scroll=scrollPreserve)
            for hit in responseCon["hits"]["hits"]:
                if 'srcPacket' in hit["_source"]:
                    if not arrRet['src'].size > 0:
                        arrRet['src'] = np.reshape(np.array([hit["_source"]["srcLatency"],
                                                             hit["_source"]["CpuEff"],
                                                             hit["_source"]["EventRate"]]), (1,3))
                    else:
                        arrRet['src'] = np.vstack((arrRet['src'], np.array([hit["_source"]["srcLatency"],
                                                                            hit["_source"]["CpuEff"],
                                                                            hit["_source"]["EventRate"]])))
                if 'destPacket' in hit["_source"]:
                    if not arrRet['dest'].size > 0:
                        arrRet['dest'] = np.reshape(np.array([hit["_source"]["destLatency"],
                                                              hit["_source"]["CpuEff"],
                                                              hit["_source"]["EventRate"]]), (1,3))
                    else:
                        arrRet['dest'] = np.vstack((arrRet['dest'], np.array([hit["_source"]["destLatency"],
                                                                              hit["_source"]["CpuEff"],
                                                                              hit["_source"]["EventRate"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        return arrRet

print(esConAgg("src"))
print(esConAgg("dest"))

with PdfPages('CMS_Scatter.pdf') as pc:
    d = pc.infodict()
    d['Title'] = 'CMS Scatter Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs'
    d['Keywords'] = 'PdfPages matplotlib CMS grid'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()
    #qResults = esConQuery('t1_de_kit','T1_ES_PIC')
    srcSites = esConAgg("src")
    destSites = esConAgg("dest")
    for ping in srcSites:
        for pong in destSites:
            qResults = esConQuery(ping, pong)
            if not type(qResults) == type(None):
                srcRes = qResults['src']
                destRes = qResults['dest']
                figL, axL = plt.subplots(1, sharex=True)
                axL.scatter(srcRes[:,0],srcRes[:,1])
                axL.set_ylabel("CpuEff")
                axL.set_xlabel("Latency")
                axL.set_title(str(ping + " to " + pong))
                pc.savefig(figL)
                plt.close(figL)
                figP, axP = plt.subplots(1, sharex=True)
                axP.scatter(srcRes[:,0],srcRes[:,2])
                axP.set_ylabel("EventRate")
                axP.set_xlabel("Latency")
                axP.set_title(str(ping + " to " + pong))
                pc.savefig(figP)
                plt.close(figP)
                
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("CpuEff")
