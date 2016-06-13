from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch([
    'urlhere'
])

s = Search(using=es)

ns = Search().using(es).query("match", AccountingGroup="production.cmsdataops")

response = ns.execute()

for hit in ns:
    print(hit.CoreHr)
