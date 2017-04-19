from elasticsearch import Elasticsearch
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
from scipy import stats
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

loc = {}
loc["Workflow"] = np.array([])

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

    if conTotalRec == 0:
        return None
    else:
        while conTotalRec > 0:
            responseCon = esCon.scroll(scroll_id=scrollIdCon,
                                       scroll=scrollPreserve)
            for hit in responseCon["hits"]["hits"]:
                if not str(hit["_source"]["Workflow"]) in arrRet:
                    loc["Workflow"] = np.append(loc["Workflow"],
                                                str(hit["_source"]["Workflow"]))
                    arrRet[str(hit["_source"]["Workflow"])] = np.reshape(np.array([hit["_source"]["meanKEvents"],
                                                                   hit["_source"]["meanCpuEff"],
                                                                   hit["_source"]["meanWallClockHr"]]), (1,3))
                else:
                    arrRet[str(hit["_source"]["Workflow"])] = np.vstack((arrRet[str(hit["_source"]["Workflow"])],
                                                         np.array([hit["_source"]["meanKEvents"],
                                                                   hit["_source"]["meanCpuEff"],
                                                                   hit["_source"]["meanWallClockHr"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        return arrRet

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main(utcStart):
    with PdfPages('PDFOut/CMS_Lumosity.pdf') as pc:
        d = pc.infodict()
        d['Title'] = 'CMS Scatter Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()
        with open("WorkOut/Lumosity.out", "w") as ww:
            while utcStart <= utcEnd:
                srcSites = esConAgg("src")
                destSites = esConAgg("dest")
                workDate = utcDate(utcStart)
                for ping in srcSites:
                    for pong in destSites:
                        qResults = esConQuery(ping, pong)
                        if not type(qResults) == type(None):
                            ww.write(str(workDate.strftime('%d-%B-%Y') + "\n\n"))
                            for hit in qResults:
                                diskUsage = qResults[str(hit)]
                                figsT, axsT = plt.subplots(1, sharex=True)
                                axsT.scatter(diskUsage[:,0],diskUsage[:,1])
                                axsT.set_ylabel("meanWallClockHr")
                                axsT.set_xlabel(str("meanKEvents (" + hit + ")"))
                                axsT.set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                pc.savefig(figsT)
                                plt.close(figsT)
                                print("diskUsage[:,0]")
                                print(diskUsage[:,0])
                                print("diskUsage[:,1]")
                                print(diskUsage[:,1])
                                ww.write(str("Workflow: " + hit + "\n"))
                                ww.write(str("Work site: " + ping + "\n"))
                                ww.write(str("Data site: " + pong + "\n"))
                                cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(diskUsage[:,0],diskUsage[:,1])
                                ww.write(str("X: CpuEff  Y: InputGB\n"))
                                ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                ww.write(str("\n\n"))
 
                utcStart = utcStart + oneDay
                
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("meanCpuEff")

# Run Main code
print("start")
main(utcStart)
print("finish")
