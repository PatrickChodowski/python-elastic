
from es import ES, Index, Document, DataStream, Cluster
es = ES()
idx = Index(es=es)
doc = Document(es=es)
ds = DataStream(es=es)
cl = Cluster(es=es)

r = idx.search("nba", data={"query": {"match_all": {}}})

