
from es import ES, Index, Document, DataStream, Cluster
es = ES()
i = Index(es=es)
doc = Document(es=es)
cl = Cluster(es=es)

cl.get_health()
cl.get_nodes_info()

# r = es.get_index('nba')
#a = es.create_document('nba', data={"team_abbreviation": "MEM"})

# df = es.sql(query="SELECT player FROM nba", response_format='json')

