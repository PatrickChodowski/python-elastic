from .es_api import ES
import requests
from typing import Optional, Dict, Union


class Document:
    def __init__(self, es: ES):
        """
        Initializes ES Documents api
        :param es: ES api object
        """
        self.es = es

    def create(self,
               target_name: str,
               data: Dict,
               document_id: Optional[str] = None
               ) -> Union[requests.Response, Dict]:
        """
        Creates document

        Uses one of:
        PUT /<target>/_doc/<_id>
        POST /<target>/_doc/
        PUT /<target>/_create/<_id>
        POST /<target>/_create/<_id>

        :param target_name: Index or data stream name
        :param data: Data to be added to document
        :param document_id: Document ID (optional, if not given ES will assign random ID)
        :return: Request Response or dict
        """
        if data is None:
            raise ValueError("Data cannot be empty")
        if not isinstance(data, dict):
            raise ValueError("Data has to be of type dict")

        if (document_id is None) | (document_id == ''):
            # creating document with automatic document id
            url = self.es.parent_url + f'/{target_name}/_doc/'
            res = self.es.handle_request('POST', url, data=data)
        else:
            url = self.es.parent_url + f'/{target_name}/_create/{document_id}'
            res = self.es.handle_request('POST', url, data=data)
        return res

    def get(self, source_name: str, document_id: str) -> Union[requests.Response, Dict]:
        """
        Reads document by ID
        :param source_name: Index or data stream name
        :param document_id: Document ID
        :return: Request Response or dict
        """
        url = self.es.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self.es.handle_request('GET', url)
        return res

    def list(self, source_name: str) -> Union[requests.Response, Dict]:
        """
        Returns list of all documents
        :param source_name: Index or data stream name
        :return: Request Response or dict
        """
        url = self.es.parent_url + f'/{source_name}/_mget'
        res = self.es.handle_request('GET', url)
        return res

    def delete(self, source_name: str, document_id: str) -> Union[requests.Response, Dict]:
        """
        Deletes document by ID
        :param source_name: Index or data stream name
        :param document_id: Document ID
        :return: Request Response or dict
        """
        url = self.es.parent_url + f'/{source_name}/_doc/{document_id}'
        res = self.es.handle_request('DELETE', url)
        return res

    def update(self, source_name: str, document_id: str) -> Union[requests.Response, Dict]:
        """
        Updates data in document
        :param source_name: Index or data stream name
        :param document_id: Document ID
        :return: Request Response or dict
        """
        url = self.es.parent_url + f'/{source_name}/_update/{document_id}'
        res = self.es.handle_request('POST', url)
        return res
