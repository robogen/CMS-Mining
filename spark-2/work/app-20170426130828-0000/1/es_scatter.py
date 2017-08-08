#from pyspark import SparkContext, SparkConf
import pyspark
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

startDate = "2017-02-13T00:00:00"
endDate = "2017-02-15T00:00:00"
oneDay = np.multiply(24, np.multiply(60,60))
utcStart = utcStamp(startDate)
utcEnd = utcStamp(endDate)
oneDay = np.multiply(24,np.multiply(60,60))
sparkConf = pyspark.SparkConf().setAppName("SparkScatter").setMaster("local[*]")
sc = pyspark.SparkContext(conf=sparkConf)

hcc_query=json.dumps(
           {"query" : 
              {"bool": {
                  "must": [
                      {"range" : {
                         "beginDate" : {
                             "gt" : int(utcStart),
                             "lt" : int(utcEnd)
                         }
                      }}
                          ]
                        }
              }
              }
)

hcc_conf= {
        "es.resource" : "net-health/DIGIRECO",
        "es.nodes" : contents[4],
        "es.port" : contents[5],
#        "es.nodes.wan.only" : "true",
#        "es.nodes.discovery": "false",
#        "es.nodes.data.only": "false",
        "es.query" : hcc_query,
        "es.http.timeout" : "20m",
        "es.http.retries" : "10",
        "es.scroll.keepalive" : "10m"
      }

hcc_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
                             keyClass="org.apache.hadoop.io.NullWritable",\
                             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
                             conf=hcc_conf).map(lambda x: x[1]) \
                             .persist(pyspark.StorageLevel.MEMORY_AND_DISK)

def stageSnap(accum, x):
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

def snapReduce(accum, x):
    temp={'CpuEff' : np.append(accum["CpuEff"], \
                               x["CpuEff"]),
          'DiskUsage' : np.append(accum["DiskUsage"], \
                                            x["DiskUsage"]),
          'EventRate' : np.append(accum["EventRate"], \
                                            x["EventRate"])}
    return temp

def main(ps):
    srcSite = hcc_rdd.map(lambda x: (str(x["src"]).lower(), int(1))) \
                     .aggregateByKey(0, lambda k,v: int(v)+k, lambda v,k: k+v) \
                     .map(lambda x: x[0]) \
                     .collect()
    destSite = hcc_rdd.map(lambda x: (str(x["dest"]).lower(), int(1))) \
                      .aggregateByKey(0, lambda k,v: int(v)+k, lambda v,k: k+v) \
                      .map(lambda x: x[0]) \
                      .collect()
    prevSite = hcc_rdd.map(lambda x: (str(x["LastRemoteHost"]).lower(), int(1))) \
                      .aggregateByKey(0, lambda k,v: int(v)+k, lambda v,k: k+v) \
                      .map(lambda x: x[0]) \
                      .collect()
    while utcStart <= utcEnd:
        hcc_time = hcc_rdd.filter(lambda x: x["beginDate"] >= int(utcStart)) \
                          .filter(lambda x: x["beginDate"] <= int((utcStart + oneDay))) \
                          .persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        for ping in srcSite:
            hcc_single = hcc_time.filter(lambda x: str(x["src"]).lower() == ping)
            if not hcc_single.isEmpty():
                for pong in destSite:
                    hcc_pong = hcc_single.filter(lambda x: x["dest"].lower() == pong)
                    if not hcc_pong.isEmpty():
                        for slot in prevSite:
                            hcc_slot = hcc_pong.filter(lambda x: x["LastRemoteHost"] == slot)
                            if not hcc_slot.isEmpty():
                                hcc_result = hcc_slot.reduce(snapReduce)

                                figH, axH = plt.subplots(2, sharex=True)
                                figH.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
                                axH[0].plot(hcc_result["DiskUsage"], \
                                            hcc_result["CpuEff"], 'bs')
                                axH[0].set_ylabel("CpuEff")
                                axH[0].set_title(str("2016 From " \
                                                     + ping + " To " \
                                                     + pong + " @ " + slot))
        
                                axH[1].plot(hcc_result["DiskUsage"], 
                                            hcc_result["EventRate"], 'bs')
                                axH[1].set_ylabel("EventRate")
                                ps.savefig(figH)
                                plt.close(figH)
        utcStart = utcStart + oneDay


print("Started")
with PdfPages('PDFOut/CMS_SparkScatter_%s.pdf' % str(sys.argv[1])) as ps:
    d = ps.infodict()
    d['Title'] = 'CMS Scatter Plots'
    d['Author'] = u'Jerrod T. Dixon\xe4nen'
    d['Subject'] = 'Plot of network affects on grid jobs generated by Spark'
    d['Keywords'] = 'PdfPages matplotlib CMS grid spark'
    d['CreationDate'] = dt.datetime.today()
    d['ModDate'] = dt.datetime.today()
    main(ps)
print("Finished")
