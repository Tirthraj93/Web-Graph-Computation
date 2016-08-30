from ElasticSearchUtil import ElasticSearchUtility
from Constants import *
from RetrievalModels import page_rank
from RetrievalModels import hits
from FIleUtility import write
from FIleUtility import write_list
from FIleUtility import read_file_to_list
from Mapper import Mapper

# query for fetching the results
QUERY_STRING = 'great modernist artists'
OUTPUT_SIZE = 500


def execute_page_rank(url_set, graph_index, graph_type, out_file, reverse_map=False):
    es_util = ElasticSearchUtility()
    web_graph = es_util.get_web_graph(graph_index, graph_type)
    page_rank_dict = page_rank(url_set, web_graph)

    # clear memory
    web_graph = None

    sorted_tuples = sorted(page_rank_dict.items(), key=lambda x: x[1], reverse=True)[:OUTPUT_SIZE]
    # clear memory
    page_rank_dict = None

    if reverse_map:
        print 'getting reverse url map...'
        url_reverse_map = Mapper.fromFile(MAPPING_FILE_NAME, reverse=True).mappings

        decoded_tuples = []
        for t in sorted_tuples:
            decoded_url = url_reverse_map[t[0]]  # decode url
            score = t[1]  # score as it is
            decoded_tuple = (decoded_url, score)
            decoded_tuples.append(decoded_tuple)
    else:
        decoded_tuples = sorted_tuples

    print 'writing pagerank results...'
    write(out_file, decoded_tuples)


def execute_hits(crawl_index_name, crawl_index_type, graph_index, graph_type):
    es_util = ElasticSearchUtility()
    web_graph = es_util.get_web_graph(graph_index, graph_type)
    link_map = Mapper.fromFile(MAPPING_FILE_NAME).mappings
    hubs, authorities = hits(crawl_index_name, crawl_index_type, web_graph, QUERY_STRING, link_map)

    # clear memory
    web_graph = None
    link_map = None

    print 'sorting hubs...'
    sorted_hubs = sorted(hubs.items(), key=lambda x: x[1], reverse=True)[:OUTPUT_SIZE]

    # clear memory
    hubs = None

    print 'sorting authorities...'
    sorted_auth = sorted(authorities.items(), key=lambda x: x[1], reverse=True)[:OUTPUT_SIZE]

    # clear memory
    authorities = None

    print 'getting reverse url map...'
    url_reverse_map = Mapper.fromFile(MAPPING_FILE_NAME, reverse=True).mappings

    sorted_hubs_decoded = []
    for t in sorted_hubs:
        decoded_url = url_reverse_map[t[0]]  # decode url
        score = t[1]  # score as it is
        decoded_tuple = (decoded_url, score)
        sorted_hubs_decoded.append(decoded_tuple)

    sorted_auth_decoded = []
    for t in sorted_auth:
        decoded_url = url_reverse_map[t[0]]  # decode url
        score = t[1]  # score as it is
        decoded_tuple = (decoded_url, score)
        sorted_auth_decoded.append(decoded_tuple)

    print 'writing hubs...'
    write(HUBS_PATH, sorted_hubs_decoded)
    print 'writing authorities...'
    write(AUTH_PATH, sorted_auth_decoded)


def get_wt2g_base_url_set():
    url_set = []
    with open(WT2G_FILE_PATH, 'r') as f:
        lines = f.read().splitlines()
    for line in lines:
        links = line.strip().split(" ")
        url_set.append(links[0])
    return url_set


def create_base_url_file(index_name, index_type):
    es_util = ElasticSearchUtility()
    url_list = es_util.get_all_ids(index_name, index_type)
    write_list(URL_SET_PATH, url_list)


def get_crawled_base_url_set(index_name, index_type):
    return read_file_to_list(URL_SET_PATH)


if __name__ == "__main__":
    # create_base_url_file(CRAWL_INDEX, CRAWL_TYPE)
    base_url_set = get_wt2g_base_url_set()
    execute_page_rank(base_url_set, WT2G_GRAPH_INDEX, WT2G_GRAPH_TYPE, WT2G_PAGE_RANK_PATH)
    # base_url_set = get_crawled_base_url_set(CRAWL_INDEX, CRAWL_TYPE)
    # execute_page_rank(base_url_set, WEB_GRAPH_INDEX, WEB_GRAPH_TYPE, CRAWLED_PAGE_RANK_PATH, True)
    # execute_hits(CRAWL_INDEX, CRAWL_TYPE, WEB_GRAPH_INDEX, WEB_GRAPH_TYPE)
