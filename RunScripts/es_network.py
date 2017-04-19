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
startDate = "2017-02-14T00:00:00"
endDate = "2017-02-21T00:00:00"
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

def esClear(ids):
    scannerCon = esCon.clear_scroll(scroll_id=ids)
    return scannerCon

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
                 }},    
                "sort": {"beginDate": {"order": "desc"}}
              }
    scannerCon = esCon.search(index="net-health",
                              doc_type="DIGIRECO",
                              body=queryBody,
                              size=querySize,
                              search_type="query_then_fetch",
                              scroll=scrollPreserve)
    scrollIdCon = scannerCon['_scroll_id']
    conTotalRec = scannerCon["hits"]["total"]
    idList = []
    arrRet = {}

    if conTotalRec == 0:
        return None
    else:
        while conTotalRec > 0:
            idList.append(str(scrollIdCon))
            responseCon = esCon.scroll(scroll_id=scrollIdCon,
                                       scroll=scrollPreserve)
            for hit in responseCon["hits"]["hits"]:
                workflow = str(hit["_source"]["Workflow"])
                if not workflow in arrRet:
                    arrRet[workflow] = {}

                if 'meansrcThroughput' in hit["_source"]:
                    #if float(hit["_source"]["meansrcThroughput"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if 'meansrcPacket' in hit["_source"] and not 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       float(0.0)]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       float(0.0)])))
                    elif not 'meansrcPacket' in hit["_source"] and 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                       float(0.0),
                                                                       hit["_source"]["meandestPacket"]]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meansrcThroughput"],
                                                                       float(0.0),
                                                                       hit["_source"]["meandestPacket"]])))
                    elif 'meansrcPacket' in hit["_source"] and 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       hit["_source"]["meandestPacket"]]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meansrcThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       hit["_source"]["meandestPacket"]])))
                    if 'meansrcLatency' in hit["_source"] and not 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        float(0.0)]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meansrcThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        float(0.0)])))
                    elif not 'meansrcLatency' in hit["_source"] and 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                        float(0.0),
                                                                        hit["_source"]["meandestLatency"]]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meansrcThroughput"],
                                                                        float(0.0),
                                                                        hit["_source"]["meandestLatency"]])))
                    elif 'meansrcLatency' in hit["_source"] and 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meansrcThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        hit["_source"]["meandestLatency"]]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meansrcThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        hit["_source"]["meandestLatency"]])))
                if 'meandestThroughput' in hit["_source"]:
                    #if float(hit["_source"]["meansrcThroughput"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                    if 'meansrcPacket' in hit["_source"] and not 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       float(0.0)]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meandestThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       float(0.0)])))
                    elif not 'meansrcPacket' in hit["_source"] and 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                       float(0.0),
                                                                       hit["_source"]["meandestPacket"]]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meandestThroughput"],
                                                                       float(0.0),
                                                                       hit["_source"]["meandestPacket"]])))
                    elif 'meansrcPacket' in hit["_source"] and 'meandestPacket' in hit["_source"]:
                        #if float(hit["_source"]["meansrcPacket"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcPacket' in arrRet[workflow]:
                            arrRet[workflow]['srcPacket'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       hit["_source"]["meandestPacket"]]), (1,3))
                        else:
                            arrRet[workflow]['srcPacket'] = np.vstack((arrRet[workflow]['srcPacket'],
                                                             np.array([hit["_source"]["meandestThroughput"],
                                                                       hit["_source"]["meansrcPacket"],
                                                                       hit["_source"]["meandestPacket"]])))
                    if 'meansrcLatency' in hit["_source"] and not 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        float(0.0)]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        float(0.0)])))
                    elif not 'meansrcLatency' in hit["_source"] and 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                        float(0.0),
                                                                        hit["_source"]["meandestLatency"]]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meandestThroughput"],
                                                                        float(0.0),
                                                                        hit["_source"]["meandestLatency"]])))
                    elif 'meansrcLatency' in hit["_source"] and 'meandestLatency' in hit["_source"]:
                        #if float(hit["_source"]["meansrcLatency"]) > 0 and float(hit["_source"]["meanCpuEff"]) > 0 and float(hit["_source"]["meanEventRate"]) > 0:
                        if not 'srcLatency' in arrRet[workflow]:
                            arrRet[workflow]['srcLatency'] = np.reshape(np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        hit["_source"]["meandestLatency"]]), (1,3))
                        else:
                            arrRet[workflow]['srcLatency'] = np.vstack((arrRet[workflow]['srcLatency'], 
                                                              np.array([hit["_source"]["meandestThroughput"],
                                                                        hit["_source"]["meansrcLatency"],
                                                                        hit["_source"]["meandestLatency"]])))
            conTotalRec -= len(responseCon['hits']['hits'])
        esClear(idList)
        return arrRet

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main(utcStart):
    with PdfPages('PDFOut/CMS_Network.pdf') as pc:
        d = pc.infodict()
        d['Title'] = 'CMS Scatter Plots'
        d['Author'] = u'Jerrod T. Dixon\xe4nen'
        d['Subject'] = 'Plot of network affects on grid jobs'
        d['Keywords'] = 'PdfPages matplotlib CMS grid'
        d['CreationDate'] = dt.datetime.today()
        d['ModDate'] = dt.datetime.today()

        countBit = {}
        countBit["total"] = 0

        with open("WorkOut/Network.out", "w") as ww:
            while utcStart <= utcEnd:
                print("Agg Query")
                srcSites = esConAgg("src")
                destSites = esConAgg("dest")
                prevSites = esConAgg("LastRemoteHost")
                workDate = utcDate(utcStart)
                for ping in srcSites:
                    for pong in destSites:
                        for slot in prevSites:
                            print("Main Query")
                            qResults = esConQuery(ping, pong, slot)
                            if not type(qResults) == type(None):
                                ww.write(str(workDate.strftime('%d-%B-%Y') + "\n"))
                                for hit in qResults:
                                    if str(hit) not in countBit:
                                        countBit[str(hit)] = 0
                                    if str(slot) not in countBit:
                                        countBit[str(slot)] = 0
                                    if 'srcPacket' in qResults[hit]:
                                        srcPacket = qResults[hit]['srcPacket']
                                        print("srcPacket")
                                        pp.pprint(srcPacket)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(srcPacket[:,0],srcPacket[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(srcPacket[:,0],srcPacket[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figsT, axsT = plt.subplots(2, sharex=True)
                                            axsT[0].scatter(srcPacket[:,0],srcPacket[:,1])
                                            axsT[1].scatter(srcPacket[:,0],srcPacket[:,2])
                                            axsT[0].set_ylabel("meansrcPacket")
                                            axsT[1].set_ylabel("meandestPacket")
                                            axsT[1].set_xlabel("Source Throughput (" + hit + ")")
                                            axsT[0].set_title(str(ping + " to " + pong + " at " + slot + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figsT)
                                            plt.close(figsT)
        
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Slot: " + slot + "\n"))
                                            ww.write(str("Throughput value measured at work site\n"))
                                            ww.write(str("X: Source Throughput  Y: meansrcPacket\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Source Throughput  Y: meansrcPacket\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))

                                    if 'srcLatency' in qResults[hit]:
                                        srcLatency = qResults[hit]['srcLatency']
                                        print("srcLatency")
                                        pp.pprint(srcLatency)
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(srcLatency[:,0],srcLatency[:,1])
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(srcLatency[:,0],srcLatency[:,2])
                                        if ep_value < 0.05 and cp_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figdT, axdT = plt.subplots(2, sharex=True)
                                            axdT[0].scatter(srcLatency[:,0],srcLatency[:,1])
                                            axdT[1].scatter(srcLatency[:,0],srcLatency[:,2])
                                            axdT[0].set_ylabel("meansrcLatency")
                                            axdT[1].set_ylabel("meandestLatency")
                                            axdT[1].set_xlabel("Destination Throughput (" + hit + ")")
                                            axdT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figdT)
                                            plt.close(figdT)

                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Slot: " + slot + "\n"))
                                            ww.write(str("Throughput value measured at data site\n"))
                                            ww.write(str("X: Destination Throughput  Y: meansrcLatency\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Throughput  Y: meandestLatency\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))

                                    if 'destPacket' in qResults[hit]:
                                        destPacket = qResults[hit]['destPacket']
                                        print("destPacket")
                                        pp.pprint(destPacket)
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(destPacket[:,0],destPacket[:,1])
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(destPacket[:,0],destPacket[:,2])
                                        if cp_value < 0.05 and ep_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figsT, axsT = plt.subplots(2, sharex=True)
                                            axsT[0].scatter(destPacket[:,0],destPacket[:,1])
                                            axsT[1].scatter(destPacket[:,0],destPacket[:,2])
                                            axsT[0].set_ylabel("meansrcPacket")
                                            axsT[1].set_ylabel("meandestPacket")
                                            axsT[1].set_xlabel("Destination Throughput (" + hit + ")")
                                            axsT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figsT)
                                            plt.close(figsT)
    
                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Slot: " + slot + "\n"))
                                            ww.write(str("Throughput value measured at work site\n"))
                                            ww.write(str("X: Destination Throughput  Y: meansrcPacket\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Throughput  Y: meansrcPacket\n"))
                                            ww.write(str("e_Slope: " + str(eslope) + "\n"))
                                            ww.write(str("e_Intercept: " + str(eintercept) + "\n"))
                                            ww.write(str("e_R Value: " + str(er_value) + "\n"))
                                            ww.write(str("e_P Value: " + str(ep_value) + "\n"))
                                            ww.write(str("e_std err: " + str(estd_err) + "\n"))
                                            ww.write(str("\n\n"))

                                    if 'destLatency' in qResults[hit]:
                                        destLatency = qResults[hit]['destLatency']
                                        print("destLatency")
                                        pp.pprint(destLatency)
                                        eslope, eintercept, er_value, ep_value, estd_err = stats.linregress(destLatency[:,0],destLatency[:,1])
                                        cslope, cintercept, cr_value, cp_value, cstd_err = stats.linregress(destLatency[:,0],destLatency[:,2])
                                        if ep_value < 0.05 and cp_value < 0.05:
    
                                            if (cslope > 0 and eslope < 0) or (cslope < 0 and eslope > 0):
                                                countBit[str(hit)] += 1
                                                countBit[str(slot)] += 1
                                            countBit["total"] += 1
    
                                            figdT, axdT = plt.subplots(2, sharex=True)
                                            axdT[0].scatter(destLatency[:,0],destLatency[:,1])
                                            axdT[1].scatter(destLatency[:,0],destLatency[:,2])
                                            axdT[0].set_ylabel("meansrcLatency")
                                            axdT[1].set_ylabel("meandestLatency")
                                            axdT[1].set_xlabel("Destination Throughput (" + hit + ")")
                                            axdT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                                            pc.savefig(figdT)
                                            plt.close(figdT)

                                            ww.write(str("Workflow: " + hit + "\n"))
                                            ww.write(str("Work site: " + ping + "\n"))
                                            ww.write(str("Data site: " + pong + "\n"))
                                            ww.write(str("Slot: " + slot + "\n"))
                                            ww.write(str("Throughput value measured at data site\n"))
                                            ww.write(str("X: Destination Throughput  Y: meansrcLatency\n"))
                                            ww.write(str("c_Slope: " + str(cslope) + "\n"))
                                            ww.write(str("c_Intercept: " + str(cintercept) + "\n"))
                                            ww.write(str("c_R Value: " + str(cr_value) + "\n"))
                                            ww.write(str("c_P Value: " + str(cp_value) + "\n"))
                                            ww.write(str("c_std err: " + str(cstd_err) + "\n"))
                                            ww.write(str("X: Destination Throughput  Y: meandestLatency\n"))
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
