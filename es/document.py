from .es_api import ES
import requests
from typing import Optional, Dict


class Document:
    def __init__(self, es: ES):
        self.es = es

    def create(self,
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
            url = self.es.parent_url + f'/{target_name}/_doc/'
            res = self.es.handle_request('POST', url, data=data)
        else:
            url = self.es.parent_url + f'/{target_name}/_create/{document_id}'
            res = self.es.handle_request('POST', url, data=data)
        return res

    def get(self, source_name: str, document_id: str) -> requests.Response:
        url = self.es.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self.es.handle_request('GET', url)
        return res

    def list(self, source_name: str) -> requests.Response:
        url = self.es.parent_url + f'/{source_name}/_mget'
        res = self.es.handle_request('GET', url)
        return res

    def delete(self, source_name: str, document_id: str) -> requests.Response:
        url = self.es.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self.es.handle_request('DELETE', url)
        return res

    def update(self, source_name: str, document_id: str) -> requests.Response:
        url = self.es.parent_url + f'/{source_name}/_update/{document_id}'
        res = self.es.handle_request('POST', url)
        return res
