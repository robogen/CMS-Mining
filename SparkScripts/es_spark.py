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
        "es.nodes" : contents[4],
        "es.port" : contents[5],
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
        "es.nodes" : contents[4],
        "es.port" : contents[5],
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
        "es.nodes" : contents[4],
        "es.port" : contents[5],
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

print(hcc_rdd.first())
print(latency_rdd.first())
