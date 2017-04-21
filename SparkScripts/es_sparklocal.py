from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json
import pprint
import sys

pp = pprint.PrettyPrinter(indent=4)

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    if type(time) == type("s"):
        return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()
    else:
        return time

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
sparkConf = SparkConf().setAppName("SparkTest")
sc = SparkContext(conf=sparkConf)

hcc_rdd = sc.textFile("data/hcc.json").map(json.loads) \
                                      .map(lambda x: x["_source"]) \
                                      .cache()

atlas_through = sc.textFile("data/atlas-through.json") \
                                      .map(json.loads) \
                                      .map(lambda x: x["_source"]) \
                                      .filter(lambda x: "srcSite" in x.keys()) \
                                      .filter(lambda x: "destSite" in x.keys()) \
                                      .cache()

atlas_packet = sc.textFile("data/atlas-packet.json") \
                                      .map(json.loads) \
                                      .map(lambda x: x["_source"]) \
                                      .filter(lambda x: "srcSite" in x.keys()) \
                                      .filter(lambda x: "destSite" in x.keys()) \
                                      .cache()

atlas_latency = sc.textFile("data/atlas-latency.json") \
                                      .map(json.loads) \
                                      .map(lambda x: x["_source"]) \
                                      .filter(lambda x: "srcSite" in x.keys()) \
                                      .filter(lambda x: "destSite" in x.keys()) \
                                      .cache()

def hccSite(x):
    return str(x["Site"]).lower() == hit

def hccSrcT(accum, x):
    temp={'srcThroughput' : np.append(accum["srcThroughput"], x["srcThroughput"]),
          'CpuEff' : np.append(accum["CpuEff"], x["CpuEff"]),
          'EventRate' : np.append(accum["EventRate"], x["EventRate"])}
    return temp
def hccDestT(accum, x):
    temp={'destThroughput' : np.append(accum["destThroughput"], x["destThroughput"]),
          'CpuEff' : np.append(accum["CpuEff"], x["CpuEff"]),
          'EventRate' : np.append(accum["EventRate"], x["EventRate"])}
    return temp

def atlasRedT(accum, x):
    if not "timestampF" in accum.keys():
        temp={'throughput' : np.append(accum["throughput"], 
                                       x["throughput"]),
              'timestamp' : np.append(conAtlasTime(accum["timestamp"]), 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(conAtlasTime(accum["timestamp"]), 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    elif type(x["timestamp"]) == type(np.array([])):
        temp={'throughput' : np.append(accum["throughput"], x["throughput"]),
              'timestamp' : np.append(accum["timestamp"], x["timestamp"]),
              'timestampF' : np.append(accum["timestampF"], x["timestampF"])} 
    else:
        temp={'throughput' : np.append(accum["throughput"], 
                                       x["throughput"]),
              'timestamp' : np.append(accum["timestamp"], 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(accum["timestampF"], 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    return temp
def atlasRedP(accum, x):
    if not "timestampF" in accum.keys():
        temp={'packet_loss' : np.append(accum["packet_loss"], 
                                        x["packet_loss"]),
              'timestamp' : np.append(conAtlasTime(accum["timestamp"]), 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(conAtlasTime(accum["timestamp"]), 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    elif type(x["timestamp"]) == type(np.array([])):
        temp={'packet_loss' : np.append(accum["packet_loss"], x["packet_loss"]),
              'timestamp' : np.append(accum["timestamp"], x["timestamp"]),
              'timestampF' : np.append(accum["timestampF"], x["timestampF"])}
    else:
        temp={'packet_loss' : np.append(accum["packet_loss"], 
                                        x["packet_loss"]),
              'timestamp' : np.append(accum["timestamp"], 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(accum["timestampF"], 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    return temp
def atlasRedL(accum, x):
    if not "timestampF" in accum.keys():
        temp={'delay_median' : np.append(accum["delay_median"], 
                                         x["delay_median"]),
              'delay_sd' : np.append(accum["delay_sd"], 
                                     x["delay_sd"]),
              'delay_mean' : np.append(accum["delay_mean"], 
                                       x["delay_mean"]),
              'timestamp' : np.append(conAtlasTime(accum["timestamp"]), 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(conAtlasTime(accum["timestamp"]), 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    elif type(x["timestamp"]) == type(np.array([])):
        temp={'delay_median' : np.append(accum["delay_median"], x["delay_median"]),
              'delay_sd' : np.append(accum["delay_sd"], x["delay_sd"]),
              'delay_mean' : np.append(accum["delay_mean"], x["delay_mean"]),
              'timestamp' : np.append(accum["timestamp"], x["timestamp"]),
              'timestampF' : np.append(accum["timestampF"], x["timestampF"])}
    else:
        temp={'delay_median' : np.append(accum["delay_median"], 
                                         x["delay_median"]),
              'delay_sd' : np.append(accum["delay_sd"], 
                                     x["delay_sd"]),
              'delay_mean' : np.append(accum["delay_mean"], 
                                       x["delay_mean"]),
              'timestamp' : np.append(accum["timestamp"], 
                                      conAtlasTime(x["timestamp"])),
              'timestampF' : np.append(accum["timestampF"], 
                                       conAtlasTime(x["timestamp"])
                                                        - np.multiply(4,np.multiply(60,60)))}
    return temp

def hccReduce(accum, x):
    temp={'CpuEff' : np.append(accum["CpuEff"], \
                               x["CpuEff"]),
          'ChirpCMSSWEventRate' : np.append(accum["ChirpCMSSWEventRate"], \
                                            x["ChirpCMSSWEventRate"]),
          'JobCurrentStartDate' : np.append(accum["JobCurrentStartDate"], \
                                            x["JobCurrentStartDate"]),
          'JobFinishedHookDone' : np.append(accum["JobFinishedHookDone"], \
                                            x["JobFinishedHookDone"])}
    return temp

def main(ps):
    for ping in cmsLocate["locations"]:
        hcc_single = hcc_rdd.filter(lambda x: str(x["Site"]).lower() == ping)
        if not hcc_single.isEmpty():
            loc = hcc_single.flatMap(lambda x: x["DataLocations"]).collect()
            loc = list(set(loc))
            for pong in loc:
                hcc_pong = hcc_single.filter(lambda x: x["DataLocations"][0] == pong)
                if not hcc_pong.isEmpty(): 
                    hcc_result = hcc_pong.reduce(hccReduce)

                    figH, axH = plt.subplots(2, sharex=True)
                    figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                    axH[0].plot(hcc_result["JobCurrentStartDate"], \
                                hcc_result["CpuEff"], 'bs')
                    axH[0].hlines(hcc_result["CpuEff"],
                                  hcc_result["JobCurrentStartDate"],
                                  hcc_result["JobFinishedHookDone"])
                    axH[0].set_ylabel("CpuEff")
                    axH[0].set_title(str("2016 From " + ping + " To " + pong))
    
                    axH[1].plot(hcc_result["JobCurrentStartDate"], 
                                hcc_result["ChirpCMSSWEventRate"], 'bs')
                    axH[1].hlines(hcc_result["ChirpCMSSWEventRate"],
                                  hcc_result["JobCurrentStartDate"],
                                  hcc_result["JobFinishedHookDone"])
                    axH[1].set_ylabel("EventRate")
                    ps.savefig(figH)
                    plt.close(figH)
                    if pong.lower() in sitesArray.keys():
                        atlasT = atlas_through \
                          .filter(lambda x : x["srcSite"] == sitesArray[ping]) \
                          .filter(lambda x: x["destSite"] == sitesArray[pong.lower()])
                        atlasP = atlas_packet \
                          .filter(lambda x : x["srcSite"] == sitesArray[ping]) \
                          .filter(lambda x: x["destSite"] == sitesArray[pong.lower()])
                        atlasL = atlas_latency \
                          .filter(lambda x : x["srcSite"] == sitesArray[ping]) \
                          .filter(lambda x: x["destSite"] == sitesArray[pong.lower()])
                        if not atlasT.isEmpty() and not atlasP.isEmpty():
                            atlasT_result = atlasT.reduce(atlasRedT)
                            atlasP_result = atlasP.reduce(atlasRedP)
                            figA, axA = plt.subplots(2, sharex=True)
                            axA[0].set_title(str("2016 From " + \
                                             ping + " (" + \
                                             sitesArray[ping] + \
                                             ")" + " To " + \
                                             pong + " (" + sitesArray[pong.lower()] + ")"))
                            figA.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                            axA[0].plot(atlasP_result["timestamp"],
                                        atlasP_result["packet_loss"], 'bs')
                            axA[0].set_ylabel("Packet Loss")
                            axA[0].hlines(atlasP_result["packet_loss"],
                                          atlasP_result["timestampF"],
                                          atlasP_result["timestamp"])

                            axA[1].set_ylabel("Throughput")
                            axA[1].plot(atlasT_result["timestamp"],
                                        atlasT_result["throughput"], 'bs')
                            axA[1].hlines(atlasT_result["throughput"],
                                          atlasT_result["timestampF"],
                                          atlasT_result["timestamp"])
                            ps.savefig(figA)
                            plt.close(figA)
                        if not atlasL.isEmpty():
                            atlasL_result = atlasL.reduce(atlasRedL)
                            figL, axL = plt.subplots(3, sharex=True)
                            axL[0].set_title(str("2016 Latency From " + \
                                             ping + " (" + \
                                             sitesArray[ping] + \
                                             ")" + " To " + \
                                             pong + " (" + sitesArray[pong.lower()] + ")"))
                            figL.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                            axL[0].set_ylabel("Mean")
                            axL[0].plot(atlasL_result["timestamp"], 
                                        atlasL_result["delay_mean"], 
                                        'bs', label="delay_mean")
                            axL[0].hlines(atlasL_result["delay_mean"],
                                          atlasL_result["timestampF"],
                                          atlasL_result["timestamp"])
                            axL[1].set_ylabel("Median")
                            axL[1].plot(atlasL_result["timestamp"], 
                                        atlasL_result["delay_median"], 
                                        'rs', label="delay_median")
                            axL[1].hlines(atlasL_result["delay_median"],
                                          atlasL_result["timestampF"],
                                          atlasL_result["timestamp"])
                            axL[2].set_ylabel("Std. Dev")
                            axL[2].plot(atlasL_result["timestamp"], 
                                        atlasL_result["delay_sd"],
                                        'g^', label="delay_sd")
                            axL[2].hlines(atlasL_result["delay_sd"],
                                          atlasL_result["timestampF"],
                                          atlasL_result["timestamp"])
                            ps.savefig(figL)
                            plt.close(figL)


print("Started")
with PdfPages('CMS_SparkScatter_Locale_%s.pdf' % str(sys.argv[1])) as ps:
    d = ps.infodict()
    d['Title'] = 'CMS Scatter Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs generated by Spark'
    d['Keywords'] = 'PdfPages matplotlib CMS grid spark'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()
    main(ps)
print("Finished")
