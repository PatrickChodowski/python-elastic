
from es import ES, Index, Document, DataStream, Cluster
es = ES()
idx = Index(es=es)
doc = Document(es=es)
ds = DataStream(es=es)
cl = Cluster(es=es)

r = idx.search("nba", data={"query": {"match_all": {}}})



# r = es.get_index('nba')
#a = es.create_document('nba', data={"team_abbreviation": "MEM"})

# df = es.sql(query="SELECT player FROM nba", response_format='json')

