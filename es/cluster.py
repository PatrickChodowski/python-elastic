from .es_api import ES
import requests


class Cluster:
    def __init__(self, es: ES):
        """ Initializes ES Documents api"""
        self.es = es

    def get_health(self) -> requests.Response:
        """
        Get health of the cluster
        :return: Requests response
        """
        url = self.es.parent_url + '/_cluster/health/'
        res = self.es.handle_request('GET', url)
        return res

    def get_nodes_info(self) -> requests.Response:
        """
        Get nodes info
        :return: Requests response
        """
        url = self.es.parent_url + '/_nodes/stats'
        res = self.es.handle_request('GET', url)
        return res
