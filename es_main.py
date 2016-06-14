from elasticsearch import Elasticsearch
from string import rstrip
import pprint
pp = pprint.PrettyPrinter(indent=4)

with open("config", "r+") as txt:
    contents = map(rstrip, txt)

host = contents[0]
port = contents[1]

es = Elasticsearch([{
    'host': host, 'port': port
}])

query={"query" : {"match" : {"Type" : "production"}}}

# response = ns.execute()
scanner = es.search(index="_all", doc_type="job", body=query, search_type="scan", scroll="10m")
scrollId = scanner['_scroll_id']

response = es.scroll(scroll_id=scrollId, scroll="10m")
pp.pprint(response)
#for hit in response["hits"]["hits"]:
#    print(hit)
