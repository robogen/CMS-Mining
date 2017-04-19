from elasticsearch import Elasticsearch
import matplotlib
matplotlib.use("Agg")
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
}], timeout=10000)

pp = pprint.PrettyPrinter(indent=4)

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

def utcStamp(time):
    return (dt.datetime.strptime(time,'%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

scrollPreserve="3m"
startDate = "2017-02-07T00:00:00"
endDate = "2017-02-14T00:00:00"
utcStart = utcStamp(startDate)
utcEnd = utcStamp(endDate)
oneDay = np.multiply(24,np.multiply(60,60))
querySize = 10000

def esConAgg(field):
    queryBody={"aggs": {
                 "dev": {
                   "terms": {"field":field}
                 }
               }
              }
    scannerCon = esCon.search(index="net-health",
                              body=queryBody,
                              doc_type="DIGIRECO",
                              size=querySize,
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

def esConQuery(src, dest, slot):
    queryBody={"query" :
                {"bool": {
                  "must": [
                    {"match" :
                      {"src" : src}
                    },
                    {"match" :
                      {"dest" : dest}
                    },
                    {"match" :
                      {"LastRemoteHost" : slot}
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
                              doc_type="DIGIRECO",
                              size=querySize,
                              search_type="query_then_fetch",
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
                workflow = str(hit["_source"]["Workflow"])
                if not workflow in arrRet:
                    arrRet[workflow] = {}

                if 'meansrcThroughput' in hit["_source"]:
                    #if float(hit["_source"]["meansrcThroughput"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meansrcThroughput' in arrRet[workflow]:
                        arrRet[workflow]['meansrcThroughput'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meanCpuEff"],
                                                                       hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meansrcThroughput'] = np.vstack((arrRet[workflow]['meansrcThroughput'],
                                                             np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meanCpuEff"],
                                                                       hit["_source"]["meanEventRate"]])))
                if 'meandestThroughput' in hit["_source"]:
                    #if float(hit["_source"]["meandestThroughput"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meandestThroughput' in arrRet[workflow]:
                        arrRet[workflow]['meandestThroughput'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meanCpuEff"],
                                                                        hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meandestThroughput'] = np.vstack((arrRet[workflow]['meandestThroughput'],
                                                              np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meanCpuEff"],
                                                                        hit["_source"]["meanEventRate"]])))
                if 'meansrcPacket' in hit["_source"]:
                    #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meansrcPacket' in arrRet[workflow]:
                        arrRet[workflow]['meansrcPacket'] = np.reshape(np.array([hit["_source"]["meansrcPacket"],
                                                                   hit["_source"]["meanCpuEff"],
                                                                   hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meansrcPacket'] = np.vstack((arrRet[workflow]['meansrcPacket'],
                                                         np.array([hit["_source"]["meansrcPacket"],
                                                                   hit["_source"]["meanCpuEff"],
                                                                   hit["_source"]["meanEventRate"]])))
                if 'meandestPacket' in hit["_source"]:
                    #if float(hit["_source"]["meandestPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meandestPacket' in arrRet[workflow]:
                        arrRet[workflow]['meandestPacket'] = np.reshape(np.array([hit["_source"]["meandestPacket"],
                                                                    hit["_source"]["meanCpuEff"],
                                                                    hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meandestPacket'] = np.vstack((arrRet[workflow]['meandestPacket'],
                                                          np.array([hit["_source"]["meandestPacket"],
                                                                    hit["_source"]["meanCpuEff"],
                                                                    hit["_source"]["meanEventRate"]])))
                if 'meansrcLatency' in hit["_source"]:
                    #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meansrcLatency' in arrRet[workflow]:
                        arrRet[workflow]['meansrcLatency'] = np.reshape(np.array([hit["_source"]["meansrcLatency"],
                                                                    hit["_source"]["meanCpuEff"],
                                                                    hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meansrcLatency'] = np.vstack((arrRet[workflow]['meansrcLatency'], 
                                                          np.array([hit["_source"]["meansrcLatency"],
                                                                    hit["_source"]["meanCpuEff"],
                                                                    hit["_source"]["meanEventRate"]])))
                if 'meandestLatency' in hit["_source"]:
                    #if float(hit["_source"]["meandestLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if not 'meandestLatency' in arrRet[workflow]:
                        arrRet[workflow]['meandestLatency'] = np.reshape(np.array([hit["_source"]["meandestLatency"],
                                                                 hit["_source"]["meanCpuEff"],
                                                                 hit["_source"]["meanEventRate"]]), (1,3))
                    else:
                        arrRet[workflow]['meandestLatency'] = np.vstack((arrRet[workflow]['meandestLatency'], 
                                                           np.array([hit["_source"]["meandestLatency"],
                                                                     hit["_source"]["meanCpuEff"],
                                                                     hit["_source"]["meanEventRate"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        return arrRet

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main(utcStart):
    with PdfPages('PDFOut/CMS_Scatter.pdf') as pc:
        d = pc.infodict()
        d['Title'] = 'CMS Scatter Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()

        countBit = {}
        countBit["total"] = 0

        with open("WorkOut/Scatter.out", "w") as ww:
            while utcStart <= utcEnd:
                srcSites = esConAgg("src")
                destSites = esConAgg("dest")
                prevSites = esConAgg("LastRemoteHost")
                workDate = utcDate(utcStart)
                for ping in srcSites:
                    for pong in destSites:
                        for slot in prevSites:
                            qResults = esConQuery(ping, pong, slot)
                            if not type(qResults) == type(None):
                                ww.write(str(workDate.strftime('%d-%B-%Y') + "\n"))
                                for hit in qResults:
                                    if str(hit) not in countBit:
                                        countBit[str(hit)] = 0
                                    if str(slot) not in countBit:
                                        countBit[str(slot)] = 0
                                    if 'meansrcThroughput' in qResults[hit]:
                                        srcThrough = qResults[hit]['meansrcThroughput']
                                        print("meansrcThroughput")
                                        pp.pprint(srcThrough)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(srcThrough[:,0],srcThrough[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(srcThrough[:,0],srcThrough[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:

                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1

                                            figsT, axsT = plt.subplots(2, sharex=True)
                                            axsT[0].scatter(srcThrough[:,0],srcThrough[:,1])
                                            axsT[1].scatter(srcThrough[:,0],srcThrough[:,2])
                                            axsT[0].set_ylabel("meanCpuEff")
                                            axsT[1].set_ylabel("meanEventRate")
                                            axsT[1].set_xlabel("Source Throughput (" + hit + ")")
                                            axsT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figsT)
                                            plt.close(figsT)
        
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Throughput value measured at work site\n"))
                                            ww.write(str("X: Source Throughput  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Source Throughput  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                                    if 'meandestThroughput' in qResults[hit]:
                                        destThrough = qResults[hit]['meandestThroughput']
                                        print("meandestThroughput")
                                        pp.pprint(destThrough)
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(destThrough[:,0],destThrough[:,2])
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(destThrough[:,0],destThrough[:,1])
                                        if ep_value < 0.05 and cp_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figdT, axdT = plt.subplots(2, sharex=True)
                                            axdT[0].scatter(destThrough[:,0],destThrough[:,1])
                                            axdT[1].scatter(destThrough[:,0],destThrough[:,2])
                                            axdT[0].set_ylabel("meanCpuEff")
                                            axdT[1].set_ylabel("meanEventRate")
                                            axdT[1].set_xlabel("Destination Throughput (" + hit + ")")
                                            axdT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figdT)
                                            plt.close(figdT)
    
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Throughput value measured at data site\n"))
                                            ww.write(str("X: Destination Throughput  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Throughput  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                                    if 'meansrcPacket' in qResults[hit]:
                                        meansrcPacket = qResults[hit]['meansrcPacket']
                                        print("meansrcPacket")
                                        pp.pprint(meansrcPacket)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(meansrcPacket[:,0],meansrcPacket[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(meansrcPacket[:,0],meansrcPacket[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figsP, axsP = plt.subplots(2, sharex=True)
                                            axsP[0].scatter(meansrcPacket[:,0],meansrcPacket[:,1])
                                            axsP[1].scatter(meansrcPacket[:,0],meansrcPacket[:,2])
                                            axsP[0].set_ylabel("meanCpuEff")
                                            axsP[1].set_ylabel("meanEventRate")
                                            axsP[1].set_xlabel("Source Packet Loss (" + hit + ")")
                                            axsP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figsP)
                                            plt.close(figsP)
        
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Packet loss value measured at work site\n"))
                                            ww.write(str("X: Source Packet Loss  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Source Packet Loss  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                                    if 'meandestPacket' in qResults[hit]:
                                        meandestPacket = qResults[hit]['meandestPacket']
                                        print("meandestPacket")
                                        pp.pprint(meandestPacket)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(meandestPacket[:,0],meandestPacket[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(meandestPacket[:,0],meandestPacket[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figdP, axdP = plt.subplots(2, sharex=True)
                                            axdP[0].scatter(meandestPacket[:,0],meandestPacket[:,1])
                                            axdP[1].scatter(meandestPacket[:,0],meandestPacket[:,2])
                                            axdP[0].set_ylabel("meanCpuEff")
                                            axdP[1].set_ylabel("meanEventRate")
                                            axdP[1].set_xlabel("Destination Packet Loss (" + hit + ")")
                                            axdP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figdP)
                                            plt.close(figdP)
    
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Packet loss value measured at data site\n"))
                                            ww.write(str("X: Destination Packet Loss  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Packet Loss  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                                    if 'meansrcLatency' in qResults[hit]:
                                        meansrcLatency = qResults[hit]['meansrcLatency']
                                        print("meansrcLatency")
                                        pp.pprint(meansrcLatency)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(meansrcLatency[:,0],meansrcLatency[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(meansrcLatency[:,0],meansrcLatency[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figL, axL = plt.subplots(2, sharex=True)
                                            axL[0].scatter(meansrcLatency[:,0],meansrcLatency[:,1])
                                            axL[1].scatter(meansrcLatency[:,0],meansrcLatency[:,2])
                                            axL[0].set_ylabel("meanCpuEff")
                                            axL[1].set_ylabel("meanEventRate")
                                            axL[1].set_xlabel("Source Latency (" + hit + ")")
                                            axL[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figL)
                                            plt.close(figL)
    
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Latency value measured at work site\n"))
                                            ww.write(str("X: Source Latency  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Source Latency  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                                    if 'meandestLatency' in qResults[hit]:
                                        meandestLatency = qResults[hit]['meandestLatency']
                                        print("meandestLatency")
                                        pp.pprint(meandestLatency)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(meandestLatency[:,0],meandestLatency[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(meandestLatency[:,0],meandestLatency[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figP, axP = plt.subplots(2, sharex=True)
                                            axP[1].scatter(meandestLatency[:,0],meandestLatency[:,2])
                                            axP[0].scatter(meandestLatency[:,0],meandestLatency[:,1])
                                            axP[0].set_ylabel("meanCpuEff")
                                            axP[1].set_ylabel("meanEventRate")
                                            axP[1].set_xlabel("Destination Latency (" + hit + ")")
                                            axP[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figP)
                                            plt.close(figP)
    
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Latency value measured at data site\n"))
                                            ww.write(str("X: Destination Latency  Y: meanCpuEff\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Latency  Y: Event Rate\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))
    
                utcStart = utcStart + oneDay

            ww.write("\n\n\n")
            counter = 0
            for hit in countBit:
                if hit != "total":
                    counter += countBit[str(hit)]
                    ww.write(str(hit + " occurs " + str(countBit[str(hit)]/countBit["total"]) + "\n"))
            ww.write("\n")
            ww.write("Total occurs " + str(counter/countBit["total"]))
    #axC[1].scatter(destRes[:,0],destRes[:,1])
    #axC[1].set_ylabel("meanCpuEff")

# Run Main code
print("start")
main(utcStart)
print("finish")
