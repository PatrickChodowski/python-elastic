
from es import ES, Index, Document, DataStream, Cluster
es = ES()
idx = Index(es=es)
doc = Document(es=es)
ds = DataStream(es=es)
cl = Cluster(es=es)


r = idx.search("nba", data={"query": {"match": {"team_abbreviation": {"query": "DEN LAL LAC"}}}})

# track exact number of hits
r = idx.search("nba", data={"query": {"match": {"team_abbreviation": "DEN"}}})

r = idx.search("nba", data={"aggs": {"by_category": {"terms": {"field": "team_abbreviation"}}}})