from .es_api import ES
import requests
from typing import Optional, Dict, Union


class Search:
    def __init__(self, es: ES, index_name: str):
        """
        Initializes ES Search API. It is supposed to prettify the search requests as they look a bit messy
        :param es: ES api object
        :param index_name: Index name to run search
        """
        self.es = es
        self.index_name = index_name

    def search(self, data: Optional[Dict] = None) -> Union[requests.Response, Dict]:
        """
        Runs search in given index
        :param data: Data to be added to search request
        :return: Request response or dict
        """
        url = self.es.parent_url + f'/{self.index_name}/_search'
        res = self.es.handle_request('GET', url, data=data)
        return res
