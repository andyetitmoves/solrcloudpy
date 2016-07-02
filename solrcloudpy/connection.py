"""
Connecting to a set of solr servers.

To get a :class:`~solrcloudpy.SolrCollection` instance from a :class:`SolrConnection` use either dictionary-style or attribute-style access:


    >>> from solrcloudpy.connection import SolrConnection
    >>> conn = SolrConnection()
    >>> conn.list()
    [u'collection1']
    >>> conn['collection1']
    SolrCollection<collection1>


"""
import semver

import solrcloudpy.collection as collection
from solrcloudpy.zkclient import ZKClient
from solrcloudpy.solrzkstate import SolrZKState

MIN_SUPPORTED_VERSION = '>=4.6.0'
MAX_SUPPORTED_VERSION = '<=6.1.0'

class SolrConnection(object):

    """
    Connection to a solr server or several ones

    :param server: The server. Can be a single one or a list of servers. Example  ``localhost:8983`` or ``[localhost,solr1.domain.com:8983]``.
    :type server: str
    :param detect_live_nodes: whether to detect live nodes automativally or not. This assumes that one is able to access the IPs listed by Zookeeper. The default value is ``False``.
    :type detect_live_nodes: bool

    :param user: HTTP basic auth user name
    :type user: str
    :param password: HTTP basic auth password
    :type password: str
    :param timeout: timeout for HTTP requests
    :type timeout: int
    :param webappdir: the solr webapp directory; defaults to 'solr'
    :type webappdir: str
    :param version: the solr version we're currently running. defaults to 5.3.0 for backwards compatibility. must be semver compliant
    :type version: str
    """

    def __init__(self, server="localhost:8983",
                 detect_live_nodes=False,
                 user=None,
                 password=None,
                 timeout=10,
                 webappdir='solr',
                 version='5.3.0'):
        self.user = user
        self.password = password
        self.timeout = timeout
        self.webappdir = webappdir
        self.version = version

        if not semver.match(version, MIN_SUPPORTED_VERSION) and \
           semver.match(version, MAX_SUPPORTED_VERSION):
            raise StandardError("Unsupported version %s" % version)

        self.url_template = 'http://{{server}}/{webappdir}/'.format(webappdir=self.webappdir)

        if isinstance(server, ZKClient):
            self._zk_state = SolrZKState(server)
            self.servers = self.live_nodes
        else:
            if type(server) == str:
                url = self.url_template.format(server=server)
                self.servers = [url, url]
                if type(server) == list:
                    self.servers = [self.url_template.format(server=a) for a in server]

            from solrcloudpy.solrzkclient import SolrZKClient
            self._zk_state = SolrZKState(SolrZKClient(self))

            if detect_live_nodes:
                self.servers = self.live_nodes

    def detect_nodes(self, _):
        """
        Queries Zookeeper interface for live nodes

        DEPRECATED

        :return: a list of sorl URLs corresponding to live nodes in solrcloud
        :rtype: list
        """
        return self.live_nodes

    def list(self):
        """
        Lists out the current collections in the cluster
        This should probably be a recursive function but I'm not in the mood today

        :return: a list of collection names
        :rtype: list
        """
        return self._zk_state.list_collections()

    def _list_cores(self):
        """
        Retrieves a list of cores from solr admin
        :return: a list of cores
        :rtype: list
        """
        if self._client is None:
            from solrcloudpy.utils import _Request
            self._client = _Request(self)
        params = {'wt': 'json', }
        response = self._client.get(
            ('/{webappdir}/admin/cores'.format(webappdir=self.webappdir)), params).result
        cores = response.get('status', {}).keys()
        return cores

    @property
    def cluster_health(self):
        """
        Determine the state of all nodes and collections in the cluster. Problematic nodes or
        collections are returned, along with their state, otherwise an `OK` message is returned

        :return: a dict representing the status of the cluster
        :rtype: dict
        """
        return self._zk_state.cluster_health

    @property
    def cluster_leader(self):
        """
        Gets the cluster leader

        :rtype: dict
        :return: a dict with the json loaded from the zookeeper response related to the cluster leader request
        """
        return self._zk_state.cluster_leader

    @property
    def live_nodes(self):
        """
        Lists all nodes that are currently online

        :return: a list of urls related to live nodes
        :rtype: list
        """
        nodes = self._zk_state.live_nodes
        return [self.url_template.format(server=a) for a in nodes]

    def create_collection(self, collname, *args, **kwargs):
        r"""
        Create a collection.

        :param collname: The collection name
        :type collname: str
        :param \*args: additional arguments
        :param \*\*kwargs: additional named parameters

        :return: the created collection
        :rtype: SolrCollection
        """
        coll = collection.SolrCollection(self, collname)
        return coll.create(*args, **kwargs)

    def __getattr__(self, name):
        """
        Convenience method for retrieving a solr collection
        :param name: the name of the collection
        :type name: str
        :return: SolrCollection
        """
        return collection.SolrCollection(self, name)

    def __getitem__(self, name):
        """
        Convenience method for retrieving a solr collection
        :param name: the name of the collection
        :type name: str
        :return: SolrCollection
        """
        return collection.SolrCollection(self, name)

    def __dir__(self):
        """
        Convenience method for viewing collections available in this cloud

        :return: a list of collections
        :rtype: list
        """
        return self.list()

    def __repr__(self):
        """
        Representation in Python outputs
        :return: string representation
        :rtype: str
        """
        return "SolrConnection %s" % str(self.servers)
