
from es import ES
es = ES()

# r = es.get_index('nba')
#a = es.create_document('nba', data={"team_abbreviation": "MEM"})

df = es.sql(query="SELECT team_abbreviation FROM nba", response_format='df')
