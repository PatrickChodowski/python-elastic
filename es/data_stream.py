from .es_api import ES
import requests


class DataStream:
    def __init__(self, es: ES):
        self.es = es

    def create(self, data_stream_name: str) -> requests.Response:
        url = self.es.parent_url + f'/_data_stream/{data_stream_name}'
        res = self.es.handle_request('PUT', url)
        return res

    def get(self, data_stream_name: str) -> requests.Response:
        url = self.es.parent_url + f'/_data_stream/{data_stream_name}'
        res = self.es.handle_request('GET', url)
        return res
