from elasticsearch import Elasticsearch
from elasticsearch import helpers
from django.utils.encoding import iri_to_uri
import networkx as nx

from Constants import *
from Mapper import Mapper


class ElasticSearchUtility:
    """
    class to communicate with elasticsearch
    """

    def __init__(self):
        self.es = Elasticsearch(hosts=[ES_HOST], timeout=750)
        self.web_graph = nx.DiGraph()

    def create_index(self, index_name, body):
        if self.es.indices.exists(index_name):
            print("deleting '%s' index..." % index_name)
            res = self.es.indices.delete(index=index_name)
            print(" response: '%s'" % res)

        print("creating '%s' index..." % index_name)
        res = self.es.indices.create(index=index_name, body=body)
        print(" response: '%s'" % res)

    def links_to_web_graph(self, links_index_name, links_index_type, web_graph_index_name, web_graph_index_type):
        # query scroll
        scroll = self.es.search(
            index=links_index_name,
            doc_type=links_index_type,
            scroll='10m',
            size=10000,
            body={
                "query": {
                    "match_all": {}
                }
            })

        scroll_size = scroll['hits']['total']

        size = 0

        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled %s \n" % size
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
            self.create_web_graph_from_links(hits_list)
        self.index_web_graph_from_links(web_graph_index_name, web_graph_index_type)
        self.web_graph = nx.DiGraph()
    
    def encoded_links_to_web_graph(self, links_index, links_type, web_graph_index, web_graph_type, link_map):
        # query scroll
        scroll = self.es.search(
            index=links_index,
            doc_type=links_type,
            scroll='10m',
            size=10000,
            body={
                "query": {
                    "match_all": {}
                }
            })

        scroll_size = scroll['hits']['total']

        size = 0

        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled %s \n" % size
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
            self.create_encoded_web_graph_from_links(hits_list, link_map)
        self.index_web_graph_from_links(web_graph_index, web_graph_type)
        self.web_graph = nx.DiGraph()

    def create_encoded_web_graph_from_links(self, hits_list, link_map):
        for hit in hits_list:
            src_link = iri_to_uri(hit['_source']['SRC_LINK'])
            dst_link = iri_to_uri(hit['_source']['DST_LINK'])
            self.web_graph.add_edge(link_map[src_link], link_map[dst_link])

    def create_web_graph_from_links(self, hits_list):
        for hit in hits_list:
            self.web_graph.add_edge(hit['_source']['SRC_LINK'], hit['_source']['DST_LINK'])

    def index_web_graph_from_links(self, web_graph_index_name, web_graph_index_type):
        bulk_actions = []
        index_meta = {
            "_index": web_graph_index_name,
            "_type": web_graph_index_type
        }
        count = 0
        for node in self.web_graph:
            # print "NODE", node
            # print "IN", webGraph.predecessors(node)
            # print "OUT", webGraph.successors(node)
            source = {
                "inlinks": self.web_graph.predecessors(node),
                "outlinks": self.web_graph.successors(node)
            }
            index_meta.update({"_id": node, "_source": source})
            bulk_actions.append(index_meta)
            index_meta = {
                "_index": web_graph_index_name,
                "_type": web_graph_index_type
            }
            if len(bulk_actions) == 10000:
                count += 10000
                print "graph nodes added ", str(count)
                helpers.bulk(self.es, bulk_actions)
                bulk_actions = []
        if len(bulk_actions) > 0:
            print "graph nodes added " + str(count + len(bulk_actions))
            helpers.bulk(self.es, bulk_actions)

    def load_wt2g_links_index(self):
        bulk_actions = []
        index_meta = {
            "_index": WT2G_LINKS_INDEX,
            "_type": WT2G_LINKS_TYPE
        }
        with open(WT2G_FILE_PATH, 'r') as f:
            lines = f.read().splitlines()
        for line in lines:
            links = line.strip().split(" ")
            destination = links[0]
            if len(links) == 1:
                pass
            else:
                sources = links[1:]
                for source in sources:
                    source_body = {
                        "SRC_LINK": source,
                        "DST_LINK": destination
                    }
                    index_meta.update({"_id": source + "#" + destination, "_source": source_body})
                    bulk_actions.append(index_meta)
                    index_meta = {
                        "_index": WT2G_LINKS_INDEX,
                        "_type": WT2G_LINKS_TYPE
                    }
        if len(bulk_actions) > 0:
            print "links added " + str(len(bulk_actions))
            helpers.bulk(self.es, bulk_actions)

    def get_web_graph(self, web_graph_index, web_graph_type):
        web_graph = dict()
        # query scroll
        scroll = self.es.search(
            index=web_graph_index,
            doc_type=web_graph_type,
            scroll='10m',
            size=1000,
            body={
                "query": {
                    "match_all": {}
                }
            })

        scroll_size = scroll['hits']['total']

        size = 0

        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            for hit in hits_list:
                # id: (inlinks, outlinks)
                web_graph[hit['_id']] = (hit['_source']['inlinks'], hit['_source']['outlinks'])
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled %s \n" % size
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
        return web_graph

    def encode_crawled_links(self, links_index, links_type, links_map, encoded_links_index, encoded_links_type):
        bulk_actions = []
        index_meta = {
            "_index": encoded_links_index,
            "_type": encoded_links_type
        }
        # query scroll
        scroll = self.es.search(
            index=links_index,
            doc_type=links_type,
            scroll='10m',
            size=10000,
            body={
                "query": {
                    "match_all": {}
                }
            })
        scroll_size = scroll['hits']['total']
        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            for hit in hits_list:
                src_link = iri_to_uri(hit['_source']['SRC_LINK'])
                dst_link = iri_to_uri(hit['_source']['DST_LINK'])
                source_body = {
                    "SRC_LINK": links_map[src_link],
                    "DST_LINK": links_map[dst_link]
                }
                index_meta.update({"_id": src_link + "#" + dst_link, "_source": source_body})
                bulk_actions.append(index_meta)
                index_meta = {
                    "_index": encoded_links_index,
                    "_type": encoded_links_type
                }
            helpers.bulk(self.es, bulk_actions)
            print "links added " + str(len(bulk_actions))
            bulk_actions = []
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')

    def create_links_map(self, links_index, links_type):
        mapper = Mapper()
        # query scroll
        scroll = self.es.search(
            index=links_index,
            doc_type=links_type,
            scroll='10m',
            size=10000,
            body={
                "query": {
                    "match_all": {}
                }
            })
        scroll_size = scroll['hits']['total']
        size = 0
        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            for hit in hits_list:
                src_link = hit['_source']['SRC_LINK']
                dst_link = hit['_source']['DST_LINK']
                mapper.map(src_link)
                mapper.map(dst_link)
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled %s \n" % size
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
        mapper.write(MAPPINGS_PATH)

    def get_top_n(self, index_name, index_type, query, n):
        """
        Returns top n search hits from given index based on given query

        :param index_name: Name of the index
        :param index_type: Type of the index
        :param query: Query string
        :param n: Result size
        :return: Top n search hits
        """
        result = self.es.search(
            index=index_name,
            doc_type=index_type,
            size=n,
            fields=['_id'],
            body={
                "query": {
                    "query_string": {
                        "default_field": "TEXT",
                        "query": query
                    }
                }
            })
        return result['hits']['hits']

    def get_all_ids(self, index_name, index_type):
        """
        Returns all ids of given index

        :param index_name: Name of the index
        :param index_type: Type of the index
        :return: List of ids of entire index
        """
        # query scroll
        id_list = []
        link_map = Mapper.fromFile(MAPPING_FILE_NAME).mappings

        scroll = self.es.search(
            index=index_name,
            doc_type=index_type,
            scroll='10m',
            size=10000,
            fields=['_id'],
            body={
                "query": {
                    "match_all": {}
                }
            })
        scroll_size = scroll['hits']['total']
        size = 0
        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            for hit in hits_list:
                url = hit['_id']
                encoded_id = link_map[iri_to_uri(url)]
                id_list.append(encoded_id)
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled %s \n" % size
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
        return id_list


def create_wt2g_graph():
    es_util = ElasticSearchUtility()
    es_util.create_index(WT2G_LINKS_INDEX, CREATE_WT2G_LINKS)
    es_util.create_index(WT2G_GRAPH_INDEX, CREATE_WT2G_GRAPH)
    es_util.load_wt2g_links_index()
    es_util.links_to_web_graph(WT2G_LINKS_INDEX, WT2G_LINKS_TYPE, WT2G_GRAPH_INDEX, WT2G_GRAPH_TYPE)


def create_encoded_graph():
    es_util = ElasticSearchUtility()
    # mapper = Mapper()

    es_util.create_index(WEB_GRAPH_INDEX, CREATE_WEB_GRAPH)
    # es_util.create_index(ENCODED_LINKS_INDEX, CREATE_ENCODED_LINKS)
    # es_util.create_links_map(LINKS_INDEX, LINKS_TYPE)
    # mapper = None
    link_map = Mapper.fromFile(MAPPING_FILE_NAME).mappings
    # es_util.encode_crawled_links(LINKS_INDEX, LINKS_TYPE, link_map, ENCODED_LINKS_INDEX, ENCODED_LINKS_TYPE)
    es_util.encoded_links_to_web_graph(LINKS_INDEX, LINKS_TYPE, WEB_GRAPH_INDEX, WEB_GRAPH_TYPE, link_map)

if __name__ == "__main__":
    create_wt2g_graph()
