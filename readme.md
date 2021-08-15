# Python client to work with elastic search

Just small personal project to interact with elastic on GCP. 
Written using requests library.



### usege:
Init:
```python
from es import ES, Index, Document, DataStream, Cluster
es = ES()
idx = Index(es=es)
doc = Document(es=es)
ds = DataStream(es=es)
cl = Cluster(es=es)
```

Get Cluster info:
```python
cl.get_health()
cl.get_nodes_info()
```

Get index:
```python
r = idx.get('nba')
```

Create document:
```python
doc.create("nba", data={"team_abbreviation": "MEM"})
```

Use SQL:
```python
df = es.sql(query="SELECT team_abbreviation FROM nba", response_format='df')
```

Search examples:
this is gold - > https://github.com/LisaHJung/Part-3-Running-full-text-queries-and-combined-queries-with-Elasticsearch-and-Kibana
```python
r = idx.search("nba", data={"query": {"match_all": {}}})
```

```python
r = idx.search("nba", data={"track_total_hits": "true"})
```

```python
r = idx.search("nba", data={"query": {"match": {"team_abbreviation": "DEN"}}})```
```

damn this is ugly:
```python
r = idx.search("nba", data={"aggs": {"by_category": {"terms": {"field": "team_abbreviation"}}}})
```

```python
r = idx.search("nba", data={"query": {"match": {"team_abbreviation": {"query": "DEN LAL LAC"}}}})
```

### notes:

Created deployment (elastic search, kibana etc. on elastic cloud):
https://cloud.elastic.co/deployments/

Automatically created a cluster with 3 instances and 170 shards
(is instance a node?)

cluster: group of elasticsearch nodes. Each node can have different roles:
- data nodes
- master nodes
- ingest nodes
- client nodes


### ES:
https://logz.io/blog/10-elasticsearch-concepts/


- field: data, feature (like column)
- meta_field: information about document
- document: collection of features (like table)
- index: collection of documents

btw. more than 1 index is indices not indexes

Index size can be a problem, so we use shards to distribute
indices over shards horizontally.



can use sql?
https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-params.html
https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions.html

can return csv, json, text, yaml, tsv :o
https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-format.html