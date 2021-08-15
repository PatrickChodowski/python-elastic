from .es_api import ES
import requests
from typing import Union, Dict


class Cluster:
    def __init__(self, es: ES):
        """ Initializes ES Documents api"""
        self.es = es

    def get_health(self) -> Union[requests.Response, Dict]:
        """
        Get health of the cluster
        :return: Requests response or dict
        """
        url = self.es.parent_url + '/_cluster/health/'
        res = self.es.handle_request('GET', url)
        return res

    def get_nodes_info(self) -> Union[requests.Response, Dict]:
        """
        Get nodes info
        :return: Requests response or dict
        """
        url = self.es.parent_url + '/_nodes/stats'
        res = self.es.handle_request('GET', url)
        return res
