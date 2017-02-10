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

def esConQuery():
    queryBody={"mappings":
                  {"dev":
                      {"properties":
                          {"CoreHr":{"type":"double"},
                           "CpuBadput":{"type":"double"},
                           "CpuEff":{"type":"double"},
                           "CpuTimeHr":{"type":"double"},
                           "EventRate":{"type":"double"},
                           "KEvents":{"type":"double"},
                           "MemoryMB":{"type":"double"},
                           "QueueHrs":{"type":"double"},
                           "RequestCpus":{"type":"double"},
                           "RequestMemory":{"type":"double"},
                           "WallClockHr":{"type":"double"},
                           "beginDate":{"type":"date","format":"epoch_second"},
                           "dest":{"type":"string"},
                           "destLatency":{"type":"double"},
                           "destPacket":{"type":"double"},
                           "destThroughput":{"type":"double"},
                           "endDate":{"type":"date","format":"epoch_second"},
                           "src":{"type":"string"},
                           "srcLatency":{"type":"double"},
                           "srcPacket":{"type":"double"},
                           "srcThroughput":{"type":"double"}
                          }
                      },
                   "gensim":
                      {"properties":
                          {"CoreHr":{"type":"double"},
                           "CpuBadput":{"type":"double"},
                           "CpuEff":{"type":"double"},
                           "CpuTimeHr":{"type":"double"},
                           "EventRate":{"type":"double"},
                           "KEvents":{"type":"double"},
                           "MemoryMB":{"type":"double"},
                           "QueueHrs":{"type":"double"},
                           "RequestCpus":{"type":"double"},
                           "RequestMemory":{"type":"double"},
                           "WallClockHr":{"type":"double"},
                           "beginDate":{"type":"date","format":"epoch_second"},
                           "dest":{"type":"string"},
                           "destLatency":{"type":"double"},
                           "destPacket":{"type":"double"},
                           "destThroughput":{"type":"double"},
                           "endDate":{"type":"date","format":"epoch_second"},
                           "src":{"type":"string"},
                           "srcLatency":{"type":"double"},
                           "srcPacket":{"type":"double"},
                           "srcThroughput":{"type":"double"}
                          }
                      },
                  "reco":
                      {"properties":
                          {"CoreHr":{"type":"double"},
                           "CpuBadput":{"type":"double"},
                           "CpuEff":{"type":"double"},
                           "CpuTimeHr":{"type":"double"},
                           "EventRate":{"type":"double"},
                           "KEvents":{"type":"double"},
                           "MemoryMB":{"type":"double"},
                           "QueueHrs":{"type":"double"},
                           "RequestCpus":{"type":"double"},
                           "RequestMemory":{"type":"double"},
                           "WallClockHr":{"type":"double"},
                           "beginDate":{"type":"date","format":"epoch_second"},
                           "dest":{"type":"string"},
                           "destLatency":{"type":"double"},
                           "destPacket":{"type":"double"},
                           "destThroughput":{"type":"double"},
                           "endDate":{"type":"date","format":"epoch_second"},
                           "src":{"type":"string"},
                           "srcLatency":{"type":"double"},
                           "srcPacket":{"type":"double"},
                           "srcThroughput":{"type":"double"}
                          }
                      },
                  "digi":
                      {"properties":
                          {"CoreHr":{"type":"double"},
                           "CpuBadput":{"type":"double"},
                           "CpuEff":{"type":"double"},
                           "CpuTimeHr":{"type":"double"},
                           "EventRate":{"type":"double"},
                           "KEvents":{"type":"double"},
                           "MemoryMB":{"type":"double"},
                           "QueueHrs":{"type":"double"},
                           "RequestCpus":{"type":"double"},
                           "RequestMemory":{"type":"double"},
                           "WallClockHr":{"type":"double"},
                           "beginDate":{"type":"date","format":"epoch_second"},
                           "dest":{"type":"string"},
                           "destLatency":{"type":"double"},
                           "destPacket":{"type":"double"},
                           "destThroughput":{"type":"double"},
                           "endDate":{"type":"date","format":"epoch_second"},
                           "src":{"type":"string"},
                           "srcLatency":{"type":"double"},
                           "srcPacket":{"type":"double"},
                           "srcThroughput":{"type":"double"}
                          }
                      }
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
