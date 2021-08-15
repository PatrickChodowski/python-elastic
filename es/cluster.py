from .es_api import ES
import requests


class Cluster:
    def __init__(self, es: ES):
        self.es = es

    def get_health(self) -> requests.Response:
        url = self.es.parent_url + '/_cluster/health/'
        res = self.es.handle_request('GET', url)
        return res

    def get_nodes_info(self) -> requests.Response:
        url = self.es.parent_url + '/_nodes/stats'
        res = self.es.handle_request('GET', url)
        return res