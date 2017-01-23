from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.dates import AutoDateLocator, AutoDateFormatter
import numpy as np
import datetime as dt
import math
import json

with open('sites.json', 'r+') as txt:
    sitesArray = json.load(txt)

with open('cms.json', 'r+') as txt:
    cmsLocate = json.load(txt)

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

def conAtlasTime(time):
    return (dt.datetime.strptime(time, '%Y-%m-%dT%X')).replace(tzinfo=dt.timezone.utc).timestamp()

def utcDate(time):
    return dt.datetime.fromtimestamp(time, dt.timezone.utc)

scrollPreserve="3m"
startDate = "2016-07-17T00:00:00"
endDate = "2016-07-25T00:00:00"
tenMin = np.multiply(10,60)
stampStart = conAtlasTime(startDate) - tenMin
stampEnd = conAtlasTime(endDate)
sparkConf = SparkConf().setAppName("SparkTest")
sc = SparkContext(conf=sparkConf)
atlas_query=json.dumps({"query" :
                {"bool": {
                   "must": [
#                     {"match" : 
#                         {"_type" : "throughput"}
#                     },
                     {"range" : {
                         "timestamp" : {
                             #"gt" : int(conAtlasTime(startDate)),
                             #"lt" : int(conAtlasTime(endDate))
                             "gt" : startDate,
                             "lt" : endDate
                         }
                     }}
                   ]
                 }
                }
               })
hcc_query=json.dumps(
           {"query" : 
              {"bool": {
                  "must": [
                      {"match" : 
                         {"CMS_JobType" : "Processing"}
                      },
                      {"range" :
                         {"EventRate" : {"gte" : 0}}
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
)

hcc_conf= {
        "es.resource" : "cms-*/job",
        "es.nodes" : contents[0],
        "es.port" : contents[1],
        "es.nodes.wan.only" : "true",
        "es.nodes.discovery": "false",
        "es.nodes.data.only": "false",
        "es.query" : hcc_query,
        "es.http.timeout" : "20m",
        "es.http.retries" : "10",
        "es.scroll.keepalive" : "10m"
      }
atlas_through_conf= {
        "es.resource" : "network_weather_2-*/throughput",
        "es.nodes" : contents[2],
        "es.port" : contents[3],
        "es.nodes.wan.only" : "true",
        "es.nodes.discovery": "false",
        "es.nodes.data.only": "false",
        "es.query" : atlas_query,
        "es.http.timeout" : "20m",
        "es.http.retries" : "10",
        "es.scroll.keepalive" : "10m"
      }
atlas_packet_conf= {
        "es.resource" : "network_weather_2-*/packet_loss_rate",
        "es.nodes" : contents[2],
        "es.port" : contents[3],
        "es.nodes.wan.only" : "true",
        "es.nodes.discovery": "false",
        "es.nodes.data.only": "false",
        "es.query" : atlas_query,
        "es.http.timeout" : "10m",
        "es.http.retries" : "5",
        "es.scroll.keepalive" : "10m"
      }
atlas_latency_conf= {
        "es.resource" : "network_weather_2-*/latency",
        "es.nodes" : contents[2],
        "es.port" : contents[3],
        "es.nodes.wan.only" : "true",
        "es.nodes.discovery": "false",
        "es.nodes.data.only": "false",
        "es.query" : atlas_query,
        "es.http.timeout" : "10m",
        "es.http.retries" : "5",
        "es.scroll.keepalive" : "10m"
      }

hcc_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
                             keyClass="org.apache.hadoop.io.NullWritable",\
                             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
                             conf=hcc_conf).map(lambda x: x[1]) \
                             .filter(lambda x: str(x["Site"]).lower() in cmsLocate["locations"]) \
                             .filter(lambda x: str(x["DataLocations"][0]).lower() in cmsLocate["locations"]) \
                             .cache()
#through_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
#                                 keyClass="org.apache.hadoop.io.NullWritable",\
#                                 valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
#                                 conf=atlas_through_conf).map(lambda x: x[1]).cache()
#packet_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
#                                keyClass="org.apache.hadoop.io.NullWritable",\
#                                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
#                                conf=atlas_packet_conf).map(lambda x: x[1]).cache()
latency_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
                                 keyClass="org.apache.hadoop.io.NullWritable",\
                                 valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
                                 conf=atlas_latency_conf).map(lambda x: x[1]).cache()

def hccSite(x):
    return str(x["Site"]).lower() == hit

def hccCpu(accum, x):
    temp={'CpuEff' : np.append(accum["CpuEff"], x["CpuEff"]),
          'ChirpCMSSWEventRate' : np.append(accum["ChirpCMSSWEventRate"], x["ChirpCMSSWEventRate"]),
          'JobCurrentStartDate' : np.append(accum["JobCurrentStartDate"], x["JobCurrentStartDate"]),
          'JobFinishedHookDone' : np.append(accum["JobFinishedHookDone"], x["JobFinishedHookDone"])}
    return temp

def throughRed(accum, x):
    temp={'date' : np.append(accum["date"], x["timestamp"]),
          'datef' : np.append(accum["datef"], x["timestamp"] - np.multiply(4, np.multiply(60,60))),
          'throughput' : np.append(accum["throughput"], x["throughput"])}
    return temp

def packetRed(accum, x):
    temp={'date' : np.append(accum["date"], x["timestamp"]),
          'datef' : np.append(accum["datef"], x["timestamp"] - np.multiply(4, np.multiply(60,60))),
          'packet_loss' : np.append(accum["packet_loss"], x["packet_loss"])}
    return temp

def latencyRed(accum, x):
    temp={'date' : np.append(accum["date"], x["timestamp"]),
          'datef' : np.append(accum["datef"], x["timestamp"] - np.multiply(4, np.multiply(60,60))),
          'delay_mean' : np.append(accum["delay_mean"], x["delay_mean"]),
          'delay_median' : np.append(accum["delay_median"], x["delay_median"]),
          'delay_sd' : np.append(accum["delay_sd"], x["delay_sd"])}
    return temp

def main(ps):
    for hit in cmsLocate["locations"]:
        hcc_single = hcc_rdd.filter(lambda x: str(x["Site"]).lower() in [hit])
        if not hcc_single.isEmpty():
            hcc_sites = hcc_single.flatMap(lambda x: x["DataLocations"]).collect()
            hcc_reduce = hcc_single.reduce(hccCpu)
            for note in list(set(hcc_sites)):
#                through_single = through_rdd.filter(lambda x: x["srcSite"] == sitesArray[hit]) \
#                                            .filter(lambda x: x["destSite"] == note)
#                packet_single = packet_rdd.filter(lambda x: x["srcSite"] == sitesArray[hit]) \
#                                          .filter(lambda x: x["destSite"] == note)
                latency_single = latency_rdd.filter(lambda x: x["srcSite"] == sitesArray[hit]) \
                                            .filter(lambda x: x["destSite"] == note)
 
#                through_src = through_single.filter(lambda x: x["src"] == x["MA"]).reduce(throughRed)
#                through_dest = through_single.filter(lambda x: x["dest"] == x["MA"]).reduce(throughRed)
#                through_reduce = through_single.reduce(throughRed)
#                packet_src = packet_single.filter(lambda x: x["src"] == x["MA"]).reduce(packetRed)
#                packet_dest = packet_single.filter(lambda x: x["dest"] == x["MA"]).reduce(packetRed)
#                packet_reduce = packet_single.reduce(packetRed)
#                latency_src = latency_single.filter(lambda x: x["src"] == x["MA"]).reduce(latencyRed)
#                latency_dest = latency_single.filter(lambda x: x["dest"] == x["MA"]).reduce(latencyRed)
                latency_reduce = latency_single.reduce(latencyRed)
                figH, axH = plt.subplots(2, sharex=True)
                axH[1].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
                                                                  defaultfmt="%m-%d %H:%M"))
                figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                axH[0].plot(hcc_reduce["JobCurrentStartDate"], hcc_reduce["CpuEff"], 'bs')
                axH[0].hlines(hcc_reduce["CpuEff"], 
                              hcc_reduce["JobCurrentStartDate"], 
                              hcc_reduce["JobFinishedHookDone"])
                axH[0].set_ylabel("CpuEff")
                axH[0].set_title(str("2016 From " + hit + " To " + note))

                axH[1].plot(hcc_reduce["JobCurrentStartDate"], hcc_reduce["ChirpCMSSWEventRate"], 'bs')
                axH[1].hlines(hcc_reduce["ChirpCMSSWEventRate"], 
                              hcc_reduce["JobCurrentStartDate"], 
                              hcc_reduce["JobFinishedHookDone"])
                axH[1].set_ylabel("EventRate")
                ps.savefig(figH)
                plt.close(figH)
                #axA[2].xaxis.set_major_formatter(AutoDateFormatter(locator=AutoDateLocator(),
                #                                                   defaultfmt="%m-%d %H:%M"))
#                figA, axA = plt.subplots(2, sharex=True)
#                axA[0].set_title(str("2016 From " + \
#                                     hit + " (" + \
#                                     sitesArray[hit] + \
#                                     ")" + " To " + \
#                                     note + " (" + sitesArray[note.lower()] + ")"))
#                figA.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
#                axA[0].plot(packet_reduce["date"], packet_reduce["packet_loss"], 'bs')
#                axA[0].set_ylabel("Packet Loss")
#                axA[0].hlines(packet_reduce["packet_loss"],
#                              packet_reduce["datef"],
#                              packet_reduce["date"])

#                axA[1].set_ylabel("Throughput")
#                axA[1].plot(through_reduce["date"], through_reduce["throughput"], 'bs')
#                axA[1].hlines(through_reduce["throughput"],
#                              through_reduce["datef"],
#                              through_reduce["date"])
#                ps.savefig(figA)
#                plt.close(figA)

                if not latency_reduce.isEmpty():
                    figL, axL = plt.subplots(3, sharex=True)
                    axL[0].set_title(str("2016 Latency From " + \
                                         hit + " (" + \
                                         sitesArray[hit] + \
                                         ")" + " To " + \
                                         note + " (" + sitesArray[note.lower()] + ")"))
                    figL.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                    axL[0].set_ylabel("Mean")
                    axL[0].plot(latency_reduce["date"], latency_reduce["delay_mean"], 'bs', label="delay_mean")
                    axL[0].hlines(latency_reduce["delay_mean"],
                                  latency_reduce["datef"],
                                  latency_reduce["date"])
                    axL[1].set_ylabel("Median")
                    axL[1].plot(latency_reduce["date"], latency_reduce["delay_median"], 'rs', label="delay_median")
                    axL[1].hlines(latency_reduce["delay_median"],
                                  latency_reduce["datef"],
                                  latency_reduce["date"])
                    axL[2].set_ylabel("Std. Dev")
                    axL[2].plot(latency_reduce["date"], latency_reduce["delay_sd"], 'g^', label="delay_sd")
                    axL[2].hlines(latency_reduce["delay_sd"],
                                  latency_reduce["datef"],
                                  latency_reduce["date"])
                    ps.savefig(figL)
                    plt.close(figL)

#            stampStart = conAtlasTime(startDate) - tenMin
#            while stampStart <= stampEnd:
#                cpuCount = 0
#                hcc_stamp = hcc_single \
#                    .filter(lambda x: x["JobCurrentStartDate"] <= int(stampStart) and \
#                                   x["JobFinishedHookDone"] >= (int(stampStart) + tenMin))
#                hcc_count = hcc_stamp.count()
#                if not hcc_count == 0:
#                    hcc_reduce = hcc_stamp.reduce(hccCpu)
#                    hcc_reduce["CpuEff"] = hcc_reduce["CpuEff"] / hcc_count
#                    hcc_reduce["ChirpCMSSWEventRate"] = hcc_reduce["ChirpCMSSWEventRate"] / hcc_count
#                    print(hcc_reduce)
#                    print(hcc_count)
#                through_stamp = through_single.filter(lambda x: x["JobCurrentStartDate"] <= int(stampStart) and\
#                                                      x["JobFinishedHookDone"] >= (int(stampStart) + tenMin))
#                latency_stamp = latency_single.filter(lambda x: x["JobCurrentStartDate"] <= int(stampStart) and\
#                                                      x["JobFinishedHookDone"] >= (int(stampStart) + tenMin))
#                packet_stamp = packet_single.filter(lambda x: x["JobCurrentStartDate"] <= int(stampStart) and \
#                                                    x["JobFinishedHookDone"] >= (int(stampStart) + tenMin))
#                through_count = through_stamp.count()
#                latency_count = latency_stamp.count()
#                packet_count = packet_stamp.count()
#                if not through_count == 0:
#                    through_reduce_src = through_stamp.reduce(srcThrough)
#                stampStart = stampStart + tenMin
 
print("Started")
with PdfPages('CMS_SparkScatter.pdf') as ps:
    d = ps.infodict()
    d['Title'] = 'CMS Scatter Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs generated by Spark'
    d['Keywords'] = 'PdfPages matplotlib CMS grid spark'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()
    main(ps)
print("Finished")
