
import requests
import binascii
from typing import Optional, Dict, Any, Union
import json
from .utils import _convert_to_df, _read_config, _get_logger, get_dict


class ES:
    def __init__(self,
                 config_path: str = 'credentials/config.yaml',
                 log_content: bool = False,
                 return_dict: bool = True):
        """ Base class that would provide functions for all ES apis. Heart of class composition design"""

        self.config_path = config_path
        self.parent_url = ""
        self.headers = dict()
        self.init()
        self.logger = _get_logger('ES')
        self.res = None
        self.log_content = log_content
        self.return_dict = return_dict

    def init(self) -> None:
        """
        Intialize: authorize connection to ES, create headers and parenty_url
        """
        config = _read_config(self.config_path)
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

    def handle_request(self,
                       method: str,
                       url: str,
                       data: Optional[Dict] = None) -> Union[requests.Response, Dict]:

        """
        Inner method for handling request
        :param method: Method name (one of GET, POST, DELETE, PUT)
        :param url: URL to send to request to
        :param data: Optional data to be sent in request body
        :return: Requsts response or dict
        """
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
        if self.log_content:
            self.logger.info(f"Content : {res.content}")

        if self.return_dict:
            return get_dict(res)
        else:
            return res

    def sql(self, query: str, response_format: str = 'json', size: int = 1000) -> Any:
        """
        Method to send SQL query to ES
        :param query: Query string
        :param response_format: Response format - one of json, csv, tsv, df (pandas dataframe)
        :param size: size limit of the query
        :return: Data in some format (df or response content)
        """
        if response_format == 'df':
            _response_format = 'json'
        else:
            _response_format = response_format

        url = self.parent_url + f'/_sql?format={_response_format}'
        data = {"query": query, "fetch_size": size}
        res = self.handle_request('POST', url, data=data)

        if response_format == 'df':
            return _convert_to_df(res)
        else:
            return res.content.decode()
