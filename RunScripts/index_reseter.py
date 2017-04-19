from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import numpy as np
import datetime as dt
import math
import json
import pprint

with open("config", "r+") as txt:
    contents = list(map(str.rstrip, txt))

es = Elasticsearch([{
    'host': contents[4], 'port': contents[5]
}], timeout=30)

esCon = IndicesClient(es)

pp = pprint.PrettyPrinter(indent=4)

properties = {"properties":
                          {
                           "stdCoreHr":{"type":"double"},
                           "maxCoreHr":{"type":"double"},
			   "minCoreHr":{"type":"double"},
                           "medianCoreHr":{"type":"double"},
                           "meanCoreHr":{"type":"double"},

                           "stdCpuBadput":{"type":"double"},
                           "meanCpuBadput":{"type":"double"},
                           "medianCpuBadput":{"type":"double"},
                           "maxCpuBadput":{"type":"double"},
                           "minCpuBadput":{"type":"double"},

                           "minCpuEff":{"type":"double"},
                           "meanCpuEff":{"type":"double"},
                           "maxCpuEff":{"type":"double"},
                           "medianCpuEff":{"type":"double"},
                           "stdCpuEff":{"type":"double"},

                           "meanCpuTimeHr":{"type":"double"},
                           "minCpuTimeHr":{"type":"double"},
                           "maxCpuTimeHr":{"type":"double"},
                           "medianCpuTimeHr":{"type":"double"},
                           "stdCpuTimeHr":{"type":"double"},

                           "minEventRate":{"type":"double"},
                           "meanEventRate":{"type":"double"},
                           "maxEventRate":{"type":"double"},
                           "medianEventRate":{"type":"double"},
                           "stdEventRate":{"type":"double"},

                           "stdKEvents":{"type":"double"},
                           "meanKEvents":{"type":"double"},
                           "medianKEvents":{"type":"double"},
                           "minKEvents":{"type":"double"},
                           "maxKEvents":{"type":"double"},

                           "meanMemoryMB":{"type":"double"},
                           "medianMemoryMB":{"type":"double"},
             	           "minMemoryMB":{"type":"double"},
                           "maxMemoryMB":{"type":"double"},
                           "stdMemoryMB":{"type":"double"},

                           "Workflow":{"type":"keyword"},
                           "LastRemoteHost":{"type":"keyword"},

                           "meanInputGB":{"type":"double"},
                           "medianInputGB":{"type":"double"},
                           "stdInputGB":{"type":"double"},
                           "minInputGB":{"type":"double"},
                           "maxInputGB":{"type":"double"},

                           "stdLumosity":{"type":"double"},
                           "maxLumosity":{"type":"double"},
                           "minLumosity":{"type":"double"},
                           "medianLumosity":{"type":"double"},
                           "meanLumosity":{"type":"double"},

                           "meanQueueHrs":{"type":"double"},
                           "medianQueueHrs":{"type":"double"},
                           "maxQueueHrs":{"type":"double"},
                           "minQueueHrs":{"type":"double"},
                           "stdQueueHrs":{"type":"double"},

                           "meanRequestCpus":{"type":"double"},
                           "medianRequestCpus":{"type":"double"},
                           "minRequestCpus":{"type":"double"},
                           "maxRequestCpus":{"type":"double"},
                           "stdRequestCpus":{"type":"double"},

                           "meanRequestMemory":{"type":"double"},
                           "minRequestMemory":{"type":"double"},
                           "maxRequestMemory":{"type":"double"},
                           "medianRequestMemory":{"type":"double"},
                           "stdRequestMemory":{"type":"double"},

                           "meanWallClockHr":{"type":"double"},
                           "medianWallClockHr":{"type":"double"},
                           "maxWallClockHr":{"type":"double"},
                           "minWallClockHr":{"type":"double"},
                           "stdWallClockHr":{"type":"double"},

                           "stdCMSSWKLumis":{"type":"double"},
                           "meanCMSSWKLumis":{"type":"double"},
                           "medianCMSSWKLumis":{"type":"double"},
                           "maxCMSSWKLumis":{"type":"double"},
                           "minCMSSWKLumis":{"type":"double"},

                           "beginDate":{"type":"date","format":"epoch_second"},
                           "dest":{"type":"keyword"},

                           "maxdestLatency":{"type":"double"},
                           "mindestLatency":{"type":"double"},
                           "mediandestLatency":{"type":"double"},
                           "meandestLatency":{"type":"double"},
                           "mindestLatency":{"type":"double"},

                           "stddestPacket":{"type":"double"},
                           "maxdestPacket":{"type":"double"},
                           "mindestPacket":{"type":"double"},
                           "mediandestPacket":{"type":"double"},
                           "meandestPacket":{"type":"double"},

                           "stddestThroughput":{"type":"double"},
                           "meandestThroughput":{"type":"double"},
                           "mediandestThroughput":{"type":"double"},
                           "maxdestThroughput":{"type":"double"},
                           "mindestThroughput":{"type":"double"},

                           "endDate":{"type":"date","format":"epoch_second"},
                           "src":{"type":"keyword"},

                           "stdsrcLatency":{"type":"double"},
                           "minsrcLatency":{"type":"double"},
                           "maxsrcLatency":{"type":"double"},
                           "mediansrcLatency":{"type":"double"},
                           "meansrcLatency":{"type":"double"},

                           "stdsrcPacket":{"type":"double"},
                           "maxsrcPacket":{"type":"double"},
                           "minsrcPacket":{"type":"double"},
                           "mediansrcPacket":{"type":"double"},
                           "meansrcPacket":{"type":"double"},

                           "stdReadTimeMins":{"type":"double"},
                           "meanReadTimeMins":{"type":"double"},
                           "medianReadTimeMins":{"type":"double"},
                           "maxReadTimeMins":{"type":"double"},
                           "minReadTimeMins":{"type":"double"},

                           "stdsrcThroughput":{"type":"double"},
                           "meansrcThroughput":{"type":"double"},
                           "mediansrcThroughput":{"type":"double"},
                           "maxsrcThroughput":{"type":"double"},
                           "minsrcThroughput":{"type":"double"},

                          }
                      }

def esConQuery():
    queryBody={"mappings":
                  {"dev":properties,
                   "DataProcessing":properties,
                   "RECO":properties,
                   "DIGI":properties,
                   "DIGIRECO":properties
                  }
              }

    if esCon.exists(index="net-health"):
        pp.pprint(esCon.delete(index="net-health"))

    scannerCon = esCon.create(index="net-health",
                              body=queryBody)

    return scannerCon

#print(esConAgg("src"))
#print(esConAgg("dest"))
def main():
   temp = esConQuery()
   pp.pprint(temp)

# Run Main code
print("start")
main()
print("finished")
