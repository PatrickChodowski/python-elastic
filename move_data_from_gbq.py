from google.cloud import bigquery
from google.oauth2 import service_account
from es import _read_config
import pandas as pd

gbq_config = _read_config('credentials/gbq_config.yaml')
credentials = service_account.Credentials.from_service_account_file(f"credentials/{gbq_config['sa_name']}")
client = bigquery.Client(credentials=credentials, project=gbq_config["project"])
df = client.query(query='SELECT * FROM `nbar_data.traditional`').to_dataframe()
df.to_csv('data/traditional.csv', index=False)
