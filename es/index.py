from .es_api import ES
import requests


class Index:
    def __init__(self, es: ES):
        self.es = es

    def list(self) -> requests.Response:
        url = self.es.parent_url + '/_cat/indices/'
        res = self.es.handle_request('GET', url)
        return res

    def create(self, index_name: str) -> requests.Response:
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('PUT', url)
        return res

    def get(self, index_name: str) -> requests.Response:
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('GET', url)
        return res

    def delete(self, index_name: str) -> requests.Response:
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('DELETE', url)
        return res

    def migrate_to_data_stream(self, index_name: str) -> requests.Response:
        url = self.es.parent_url + f'/_data_stream/_migrate/{index_name}'
        res = self.es.handle_request('POST', url)
        return res

    def search(self, source_name: str) -> requests.Response:
        url = self.es.parent_url + f'/{source_name}/_search'
        res = self.es.handle_request('GET', url)
        return res
