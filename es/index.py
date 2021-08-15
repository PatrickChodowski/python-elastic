from .es_api import ES
import requests
from typing import Optional, Dict, Union


class Index:
    def __init__(self, es: ES):
        """
        Initializes ES Index API
        :param es: ES api object
        """
        self.es = es

    def list(self) -> Union[requests.Response, Dict]:
        """
        List indices
        :return: Request response or Dict
        """
        url = self.es.parent_url + '/_cat/indices/'
        res = self.es.handle_request('GET', url)
        return res

    def create(self, index_name: str) -> Union[requests.Response, Dict]:
        """
        Creates index
        :param index_name: Name of the index to create
        :return: Request response or Dict
        """
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('PUT', url)
        return res

    def get(self, index_name: str) -> Union[requests.Response, Dict]:
        """
        Get index information
        :param index_name: Name of the index to read
        :return: Request response or dict
        """
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('GET', url)
        return res

    def delete(self, index_name: str) -> Union[requests.Response, Dict]:
        """
        Deletes index
        :param index_name: Name of the index to delete
        :return: Request response or dict
        """
        url = self.es.parent_url + f'/{index_name}'
        res = self.es.handle_request('DELETE', url)
        return res

    def migrate_to_data_stream(self, index_name: str) -> Union[requests.Response, Dict]:
        """
        Migrates index to data stream
        :param index_name: Name of the index to be migrated
        :return: Request response or dict
        """
        url = self.es.parent_url + f'/_data_stream/_migrate/{index_name}'
        res = self.es.handle_request('POST', url)
        return res

    def search(self, index_name: str, data: Optional[Dict] = None) -> Union[requests.Response, Dict]:
        """
        Runs search in given index
        :param index_name: Name of the index to be searched
        :param data: Data to be added to search request
        :return: Request response or dict
        """
        url = self.es.parent_url + f'/{index_name}/_search'
        res = self.es.handle_request('GET', url, data=data)
        return res


