import unittest
from solr_instance import SolrInstance
import time
import os
from solrcloudpy import SolrConnection, SolrCollection, NativeZKClient

solrprocess = None


class TestConnection(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(TestConnection, self).__init__(methodName)
        self.conn = SolrConnection(version=os.getenv('SOLR_VERSION', '6.1.0'))

    def setUp(self):
        self.collparams = {}
        confname = os.getenv('SOLR_CONFNAME', '')
        if confname != '':
            self.collparams['collection_config_name'] = confname

    def test_list(self):
        self.conn['foo'].create(**self.collparams)
        colls = self.conn.list()
        self.assertTrue(len(colls) >= 1)
        self.conn['foo'].drop()

    def test_live_nodes(self):
        nodes = self.conn.live_nodes
        # to support easy use of solrcloud gettingstarted
        self.assertTrue(len(nodes) >= 1)

    def test_cluster_leader(self):
        leader = self.conn.cluster_leader
        self.assertTrue(leader is not None)

    def test_create_collection(self):
        coll = self.conn.create_collection('test2', **self.collparams)
        self.assertTrue(isinstance(coll, SolrCollection))
        self.conn.test2.drop()

class TestNativeConnection(TestConnection):
    def __init__(self, methodName="runTest"):
        super(TestNativeConnection, self).__init__(methodName)
        # overwrite self.conn from above to test using native ZK
        self.conn = SolrConnection(server=NativeZKClient(conn_string="127.0.0.1:9983"),
                                   version=os.getenv('SOLR_VERSION', '6.1.0'))

def setUpModule():
    if os.getenv('SKIP_STARTUP', False):
        return
    # start solr
    solrprocess = SolrInstance("solr2")
    solrprocess.start()
    solrprocess.wait_ready()
    time.sleep(1)


def tearDownModule():
    if os.getenv('SKIP_STARTUP', False):
        return
    if solrprocess:
        solrprocess.terminate()


if __name__ == '__main__':
    unittest.main()
