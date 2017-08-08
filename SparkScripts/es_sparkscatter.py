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
                      {"range" : {
                         "beginDate" : {
                             "gt" : int(utcStart),
                             "lt" : int(endDate)
                         }
                      }}]
              }
              }}
)

hcc_conf= {
        "es.resource" : "net-health/DIGIRECO",
        "es.nodes" : contents[4],
        "es.port" : contents[5],
        "es.nodes.wan.only" : "true",
        "es.nodes.discovery": "false",
        "es.nodes.data.only": "false",
        "es.query" : hcc_query,
        "es.http.timeout" : "20m",
        "es.http.retries" : "10",
        "es.scroll.keepalive" : "10m"
      }

hcc_rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",\
                             keyClass="org.apache.hadoop.io.NullWritable",\
                             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",\
                             conf=hcc_conf).map(lambda x: x[1]) \
                             .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

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
def main(ps):
    srcSites = hcc_rdd.flatMap(lambda x: x["src"]).collect()
    srcSites = list(set(srcSites))
    destSites = hcc_rdd.flatMap(lambda x: x["dest"]).collect()
    destSites = list(set(destSites))
    while utcStart <= utcEnd:
        for ping in srcSites:
            for pong in destSites:
                hcc_single = hcc_rdd.filter(lambda x: x["src"] == ping and x["dest"] == pong).filter(lambda x: x["beginDate"] >= int(utcStart) and x["beginDate"] <= int(utcStart + oneDay))
                if not hcc_single.isEmpty():
                    hcc_src_t = hcc_single.map(lambda x: x["srcThroughput"]).reduce(hccSrcT)
                    #hcc_dest_t = hcc_single.map(lambda x: x["destThroughput"]).reduce(hccDestT)
                    if not hcc_src_t.isEmpty():
                         figsT, axsT = plt.subplots(2, sharex=True)
                         axsT[0].scatter(hcc_src_t[:,0],hcc_src_t[:,1])
                         axsT[1].scatter(hcc_src_t[:,0],hcc_src_t[:,2])
                         axsT[0].set_ylabel("CpuEff")
                         axsT[1].set_ylabel("EventRate")
                         axsT[1].set_xlabel("Source Throughput")
                         axsT[0].set_title(str(ping + " to " + pong + " on " + workDate.strftime('%d-%B-%Y')))
                         pc.savefig(figsT)
                         plt.close(figsT)
    utcStart = utcStart + oneDay
   
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
