"""
ZK client using kazoo
"""

from solrcloudpy.zkclient import ZKClient
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

class NativeZKClient(ZKClient):

    def __init__(self, conn_string):
        self.client = KazooClient(hosts=conn_string, read_only=True)
        self.client.start()

    def get_data(self, path):
        try:
            data, _ = self.client.retry(self.client.get, path)
            return data
        except NoNodeError:
            return None

    def get_children(self, path):
        try:
            return self.client.retry(self.client.get_children, path)
        except NoNodeError:
            return None
