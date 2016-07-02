"""
ZK state for a Solr Cloud

"""
import json

class SolrZKState(object):

    def __init__(self, zk_client):
        self.zk_client = zk_client

    def list_collections(self):
        return self.zk_client.get_children('/collections')

    @property
    def cluster_health(self):
        data = json.loads(self.zk_client.get_data('/clusterstate.json'))
        res = []
        collections = self.list_collections()
        for coll in collections:
            shards = data[coll]['shards']
            for shard, shard_info in shards.iteritems():
                replicas = shard_info['replicas']
                for replica, info in replicas.iteritems():
                    state = info['state']
                    if state != 'active':
                        item = {"collection": coll,
                                "replica": replica,
                                "shard": shard,
                                "info": info,
                                }
                        res.append(item)

        if not res:
            return {"status": "OK"}

        return {"status": "NOT OK", "details": res}

    @property
    def cluster_leader(self):
        return json.loads(self.zk_client.get_data('/overseer_elect/leader'))

    @property
    def live_nodes(self):
        children = self.zk_client.get_children('/live_nodes')
        return [c.replace('_solr', '') for c in children]
