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

def estest():
    queryBody={"query": {
                 "bool": {
                   "must": [
                       {"range": {
                         "beginDate" : {
                           "gt" : int(utcStart),
                           "lt" : int((utcStart + oneDay))
                         }
                        }
                       }
                 ]}
               }
              }
    scannerCon = esCon.search(index="net-health",
                              body=queryBody,
                              search_type="query_then_fetch",
                              scroll=scrollPreserve)
    #scrollIdCon = scannerCon['aggregations']['dev']
    #conTotalRec = scrollIdCon['buckets']
    arrRet = np.array([])
    pp.pprint(scannerCon)
    if conTotalRec == 0:
        return None
    else:
        #for hit in conTotalRec:
        #    arrRet = np.append(arrRet, hit['key'])
        return arrRet

def main(utcStart):
    with PdfPages('CMS_Scatter.pdf') as pc:
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
                        srcLatency = qResults['srcLatency']
                        destLatency = qResults['destLatency']
                        srcPacket = qResults['srcPacket']
                        destPacket = qResults['destPacket']
                        srcThrough = qResults['srcThroughput']
                        destThrough = qResults['destThroughput']
                        if srcThrough.size > 0:
                            figsT, axsT = plt.subplots(2, sharex=True)
                            axsT[0].scatter(srcThrough[:,0],srcThrough[:,1])
                            axsT[1].scatter(srcThrough[:,0],srcThrough[:,2])
                            axsT[0].set_ylabel("CpuEff")
                            axsT[1].set_ylabel("EventRate")
                            axsT[1].set_xlabel("Source Throughput")
                            axsT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figsT)
                            plt.close(figsT)
                        if destThrough.size > 0:
                            figdT, axdT = plt.subplots(2, sharex=True)
                            axdT[0].scatter(destThrough[:,0],destThrough[:,1])
                            axdT[1].scatter(destThrough[:,0],destThrough[:,2])
                            axdT[0].set_ylabel("CpuEff")
                            axdT[1].set_ylabel("EventRate")
                            axdT[1].set_xlabel("Destination Throughput")
                            axdT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figdT)
                            plt.close(figdT)
                        if srcPacket.size > 0:
                            figsP, axsP = plt.subplots(2, sharex=True)
                            axsP[0].scatter(srcPacket[:,0],srcPacket[:,1])
                            axsP[1].scatter(srcPacket[:,0],srcPacket[:,2])
                            axsP[0].set_ylabel("CpuEff")
                            axsP[1].set_ylabel("EventRate")
                            axsP[1].set_xlabel("Source Packet Loss")
                            axsP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figsP)
                            plt.close(figsP)
                        if destPacket.size > 0:
                            figdP, axdP = plt.subplots(2, sharex=True)
                            axdP[0].scatter(destPacket[:,0],destPacket[:,1])
                            axdP[1].scatter(destPacket[:,0],destPacket[:,2])
                            axdP[0].set_ylabel("CpuEff")
                            axdP[1].set_ylabel("EventRate")
                            axdP[1].set_xlabel("Destination Packet Loss")
                            axdP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figdP)
                            plt.close(figdP)
                        if srcLatency.size > 0:
                            figL, axL = plt.subplots(2, sharex=True)
                            axL[0].scatter(srcLatency[:,0],srcLatency[:,1])
                            axL[1].scatter(srcLatency[:,0],srcLatency[:,2])
                            axL[0].set_ylabel("CpuEff")
                            axL[1].set_ylabel("EventRate")
                            axL[1].set_xlabel("Source Latency")
                            axL[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figL)
                            plt.close(figL)
                        if destLatency.size > 0:
                            figP, axP = plt.subplots(2, sharex=True)
                            axP[1].scatter(destLatency[:,0],destLatency[:,2])
                            axP[0].scatter(destLatency[:,0],destLatency[:,1])
                            axP[0].set_ylabel("CpuEff")
                            axP[1].set_ylabel("EventRate")
                            axP[1].set_xlabel("Destination Latency")
                            axP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                            pc.savefig(figP)
                            plt.close(figP)
            utcStart = utcStart + oneDay
                
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("CpuEff")

# Run Main code
# main(utcStart)
estest()
