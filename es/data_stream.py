from .es_api import ES
import requests
from typing import Union, Dict


class DataStream:
    def __init__(self, es: ES):
        """
        Initializes ES DataSteam api
        :param es: ES api object
        """
        self.es = es

    def create(self, data_stream_name: str) -> Union[requests.Response, Dict]:
        """
        Create data stream
        :param data_stream_name: Data stream name to be created
        :return: Requests response or dict
        """
        url = self.es.parent_url + f'/_data_stream/{data_stream_name}'
        res = self.es.handle_request('PUT', url)
        return res

    def get(self, data_stream_name: str) -> Union[requests.Response, Dict]:
        """
        Reads data stream by bane
        :param data_stream_name: Data stream name to be read
        :return: Requests response or dict
        """
        url = self.es.parent_url + f'/_data_stream/{data_stream_name}'
        res = self.es.handle_request('GET', url)
        return res
