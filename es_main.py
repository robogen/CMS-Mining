from elasticsearch import Elasticsearch
from string import rstrip
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import datetime as dt
from datetime import timedelta

# import pprint
# pp = pprint.PrettyPrinter(indent=4)

pp = PdfPages('CMS_Plots.pdf')

with open("config", "r+") as txt:
    contents = map(rstrip, txt)

host = contents[2]
port = contents[3]


es = Elasticsearch([{
    'host': host, 'port': port
}])

scrollPreserve="3m"

queryHCC={"query" : 
          {"bool": {
              "must": [
                  {"match" : 
                     {"Type" : "production"}
                  },
                  {"range" :
                     {"EventRate" : {"gte" : "0"}}
                  },
                  {"match" :
                     {"DataLocationsCount" : 1}
                  }
              ]
          }
      }
  }

queryAtlas={"query" :
              {"bool": {
                 "must": [
                   {"match" : 
                       #{"_type" : "packet_loss_rate"}
                       {"_type" : "latency" }
                   },
                   {"match" :
                       {"srcSite" : "RO-16-UAIC" }
                   },
                   {"match" :
                       {"destSite" : "AGLT2" }
                   },
                   {"range" : {
                       "timestamp" : {
                           "gt" : "2016-07-31T00:00:00",
                           "lt" : "2016-08-01T00:00:00"
                       }
                   }}
                 ]
               }
              }
           }

#scannerHCC = es.search(index="_all", 
#                       doc_type="job", 
#                       body=queryHCC, 
#                       search_type="scan", 
#                       scroll=scrollPreserve)
#scrollIdHCC = scannerHCC['_scroll_id']
#responseHCC = es.scroll(scroll_id=scrollIdHCC, 
#                     scroll=scrollPreserve)

scannerAtlas = es.search(index="network_weather_2-*", 
                         body=queryAtlas, 
                         search_type="scan", 
                         scroll=scrollPreserve)
scrollIdAtlas = scannerAtlas['_scroll_id']
atlasTotalRec = scannerAtlas["hits"]["total"]
dates = np.zeros(0)
datesF = np.zeros(0)
delayMean = np.zeros(0)
delayMedian = np.zeros(0)
delaySd = np.zeros(0)

while atlasTotalRec > 0:
    responseAtlas = es.scroll(scroll_id=scrollIdAtlas, 
                              scroll=scrollPreserve)
    for hit in responseAtlas["hits"]["hits"]:
        dates = np.append(dates, 
                      dt.datetime.strptime(hit["_source"]["timestamp"], '%Y-%m-%dT%X'))
        datesF = np.append(datesF, 
                       dt.datetime.strptime(hit["_source"]["timestamp"], '%Y-%m-%dT%X')
                          - timedelta(seconds=14400))
        delayMean = np.append(delayMean, float(hit["_source"]["delay_mean"]))
        delayMedian = np.append(delayMedian, float(hit["_source"]["delay_median"]))
        delaySd = np.append(delaySd, float(hit["_source"]["delay_sd"]))
    atlasTotalRec -= len(responseAtlas['hits']['hits'])

fig = plt.figure(figsize=(12, 6))
#hmean = fig.add_subplot(121)
#hmean.plot(dates, delayMean, 'bs')
#hmean.hlines(delayMean, datesF, dates)
#hmean.set_xlabel("Time Period")
#hmean.set_title("Delay Mean period")
#hmedian = fig.add_subplot(122)
#hmedian.plot(dates, delayMedian, 'gs')
#hmedian.hlines(delayMedian, datesF, dates)
#hmedian.set_xlabel("Time Period")
#hmedian.set_title("Delay Median period")
hsd = fig.add_subplot(111)
hsd.plot(dates, delaySd, 'rs')
hsd.hlines(delaySd, datesF, dates)
hsd.set_xlabel("Time Period")
hsd.set_title("Delay SD period")
fig.autofmt_xdate()
plt.show()
plt.savefig(pp, format='pdf')

#es.clear_scroll(scroll_id=scrollIdAtlas)

# Create trace for scatter plot

