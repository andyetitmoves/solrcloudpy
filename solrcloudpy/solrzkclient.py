"""
Abstraction of a ZK client using Solr's /zookeeper endpoint.
"""
import semver

from solrcloudpy.zkclient import ZKClient
from solrcloudpy.utils import _Request

class SolrZKClient(ZKClient):

    def __init__(self, connection):
        self.client = _Request(connection)
        if semver.match(connection.version, '<5.4.0'):
            self.zk_path = '/{webappdir}/zookeeper'.format(webappdir=connection.webappdir)
        else:
            self.zk_path = '/{webappdir}/admin/zookeeper'.format(webappdir=connection.webappdir)

    def get_data(self, path):
        params = {'detail': 'true', 'path': path}
        response = self.client.get(
            self.zk_path, params).result
        return response['znode']['data']

    def get_children(self, path):
        params = {'detail': 'false', 'path': path}
        response = self.client.get(
            self.zk_path, params).result
        basename = path.rsplit('/', 1)[-1]
        children = []
        for branch in response['tree']:
            name = branch['data']['title']
            if name == basename or \
               name == ("/" + basename):
                return [node['data']['title'] \
                        for node in branch['children']]
        return None
