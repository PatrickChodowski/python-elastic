import requests
import json
import pandas as pd


def _convert_to_df(res: requests.Response) -> pd.DataFrame:
    d = json.loads(res.content.decode())
    col_names = list()
    col_types = list()
    for col in d['columns']:
        col_names.append(col['name'])
        col_types.append(col['type'])
    df = pd.DataFrame(d['rows'], columns=col_names)
    return df
