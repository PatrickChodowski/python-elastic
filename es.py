import requests
import binascii
from utils import read_config, get_logger
from typing import Optional, Dict, Any
import json
import pandas as pd


class ES:
    def __init__(self,
                 config_path: str = 'credentials/config.yaml'):

        self.config_path = config_path
        self.parent_url = ""
        self.headers = dict()
        self.init()
        self.logger = get_logger('ES')
        self.res = None

    def init(self):
        config = read_config(self.config_path)
        # api_token = binascii.b2a_base64(api_auth_str).rstrip(b"\r\n").decode("utf-8")
        # api_auth_str = f"{config['api_id']}:{config['api_key']}".encode("utf-8")

        cred_auth_str = f"{config['username']}:{config['pwd']}".encode("utf-8")
        cred_token = binascii.b2a_base64(cred_auth_str).rstrip(b"\r\n").decode("utf-8")
        headers = {"Authorization": f"Basic {cred_token}",
                   "Content-Type": "application/json"}
        parent_url = f"https://{config['host']}:{config['port']}"
        res = requests.get(headers=headers, url=parent_url, verify=True)
        if res.status_code == 200:
            self.parent_url = parent_url
            self.headers = headers

    def _handle_request(self,
                        method: str,
                        url: str,
                        data: Optional[Dict] = None) -> requests.Response:
        self.logger.info(f"Request URL : {url}")
        self.logger.info(f"Method : {method}")

        if data is not None:
            if isinstance(data, dict):
                data = json.dumps(data)
            elif isinstance(data, str):
                pass

        res = requests.request(method=method,
                               headers=self.headers,
                               url=url,
                               verify=True,
                               data=data)
        self.logger.info(f"Status : {res.status_code}")
        self.logger.info(f"Content : {res.content}")
        return res

    def list_indices(self) -> requests.Response:
        url = self.parent_url + '/_cat/indices/'
        res = self._handle_request('GET', url)
        return res

    def create_index(self, index_name: str) -> requests.Response:
        url = self.parent_url + f'/{index_name}'
        res = self._handle_request('PUT', url)
        return res

    def get_index(self, index_name: str) -> requests.Response:
        url = self.parent_url + f'/{index_name}'
        res = self._handle_request('GET', url)
        return res

    def delete_index(self, index_name: str) -> requests.Response:
        url = self.parent_url + f'/{index_name}'
        res = self._handle_request('DELETE', url)
        return res

    def create_data_stream(self, data_stream_name: str) -> requests.Response:
        url = self.parent_url + f'/_data_stream/{data_stream_name}'
        res = self._handle_request('PUT', url)
        return res

    def get_data_stream(self, data_stream_name: str) -> requests.Response:
        url = self.parent_url + f'/_data_stream/{data_stream_name}'
        res = self._handle_request('GET', url)
        return res

    def migrate_to_data_stream(self, index_name: str) -> requests.Response:
        url = self.parent_url + f'/_data_stream/_migrate/{index_name}'
        res = self._handle_request('POST', url)
        return res

    def create_document(self,
                        target_name: str,
                        document_id: Optional[str] = None,
                        data: Optional[Dict] = None
                        ) -> requests.Response:
        """
        one of?
        PUT /<target>/_doc/<_id>
        POST /<target>/_doc/
        PUT /<target>/_create/<_id>
        POST /<target>/_create/<_id>

        :param target_name:
        :param document_id:
        :param data:
        :return:
        """
        if (document_id is None) | (document_id == ''):
            # creating document with automatic document id
            url = self.parent_url + f'/{target_name}/_doc/'
            res = self._handle_request('POST', url, data=data)
        else:
            url = self.parent_url + f'/{target_name}/_create/{document_id}'
            res = self._handle_request('POST', url, data=data)
        return res

    def get_document(self, source_name: str, document_id: str) -> requests.Response:
        url = self.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self._handle_request('GET', url)
        return res

    def get_document_list(self, source_name: str) -> requests.Response:
        url = self.parent_url + f'/{source_name}/_mget'
        res = self._handle_request('GET', url)
        return res

    def delete_document(self, source_name: str, document_id: str) -> requests.Response:
        url = self.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self._handle_request('DELETE', url)
        return res

    def search(self, source_name: str) -> requests.Response:
        url = self.parent_url + f'/{source_name}/_search'
        res = self._handle_request('GET', url)
        return res

    def sql(self, query: str, response_format: str = 'json', size: int = 1000) -> Any:
        if response_format == 'df':
            _response_format = 'json'
        else:
            _response_format = response_format

        url = self.parent_url + f'/_sql?format={_response_format}'
        data = {"query": query, "fetch_size": size}
        res = self._handle_request('POST', url, data=data)

        if response_format == 'df':
            return self._convert_to_df(res)
        else:
            return res.content.decode()

    @staticmethod
    def _convert_to_df(res: requests.Response) -> pd.DataFrame:
        d = json.loads(res.content.decode())
        col_names = list()
        col_types = list()
        for col in d['columns']:
            col_names.append(col['name'])
            col_types.append(col['type'])
        df = pd.DataFrame(d['rows'], columns=col_names)
        return df
