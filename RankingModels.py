from __future__ import division

from collections import defaultdict

from GraphUtil import get_sink_nodes
import math
import random
from django.utils.encoding import iri_to_uri

from ElasticSearchUtil import ElasticSearchUtility

# teleportation factor
d = 0.85
PERPLEXITY_TRACK_COUNT = 4
# initial root set size
INIT_ROOT_SET_SIZE = 1000
# root set expansion upper limit
EXPANSION_LIMIT = 10000
# limit for pages pointing to root page
AUTHORITY_LIMIT = 200


def page_rank(base_url_set, web_graph):
    sink_nodes = get_sink_nodes(base_url_set, web_graph)
    # assign initial value
    total_normal_nodes = len(base_url_set)
    initial_rank = 1.0 / total_normal_nodes
    page_rank_dict = dict()

    for key in base_url_set:
        page_rank_dict[key] = initial_rank

    last_perplexity = []
    iteration = 1
    while not converged(last_perplexity):
        sink_pr = 0
        new_pr = dict()
        for key in sink_nodes:
            sink_pr += page_rank_dict[key]
        for key in base_url_set:
            new_pr[key] = (1 - d) / total_normal_nodes
            new_pr[key] += d * sink_pr / total_normal_nodes
            value = web_graph.get(key, None)
            if value is not None:
                for link in value[0]:
                    link_pr = page_rank_dict.get(link, 0)
                    new_pr[key] += d * link_pr / len(web_graph[link][1])
        page_rank_dict = new_pr
        last_perplexity = add_perplexity(last_perplexity, page_rank_dict)
        print iteration
        iteration += 1
    return page_rank_dict


def hits(crawl_index_name, crawl_index_type, web_graph, query, link_map):
    """
    Compute hits and authorities score for given crawled data and query

    :param crawl_index_name: index of the crawled web data
    :param crawl_index_type: index type of the crawled web data
    :param web_graph: graph containing url, its in-links and out-links
    :param query: query to be applied for HITS
    :return: the hits and authorities score for given data in that order
    """
    # Obtain the root set of INIT_ROOT_SET_SIZE documents by ranking all pages using an IR function
    print 'creating root set'
    initial_root_set = get_initial_root_set(crawl_index_name, crawl_index_type, query, INIT_ROOT_SET_SIZE, link_map)
    print 'expanding root set'
    expanded_root_set = get_expanded_root_set(set(initial_root_set), web_graph, EXPANSION_LIMIT, AUTHORITY_LIMIT)
    print 'calculating hits'
    return compute_hits(expanded_root_set[:INIT_ROOT_SET_SIZE], web_graph)


# Create a root set: Obtain the root set of about 1000 documents by ranking all pages using an IR function
#    (e.g. BM25, ES Search). You will need to use your topic as your query
def get_initial_root_set(crawl_index_name, crawl_index_type, query, n, url_map):
    """
    Queries crawled data in elasticsearch for given query
    to fetch top n results based on elasticsearch score

    :param crawl_index_name: Name of the index containing crawled data
    :param crawl_index_type: Type of the index containing crawled data
    :param query: Query string
    :param n: Result size
    :param url_map: map of url to its unique id number
    :return: List of top n urls for given query from given index
    """
    # get top n hits from ES
    es_util = ElasticSearchUtility()
    search_hits = es_util.get_top_n(crawl_index_name, crawl_index_type, query, n)
    # create list of n urls
    top_n_list = []
    for hit in search_hits:
        url = iri_to_uri(hit['_id'])
        top_n_list.append(url_map[url])
    return top_n_list


# Repeat few two or three time this expansion to get a base set of about 10,000 pages:
#   - For each page in the set, add all pages that the page points to
#   - For each page in the set, obtain a set of pages that pointing to the page
#       - if the size of the set is less than or equal to d, add all pages in the set to the root set
#       - if the size of the set is greater than d, add an RANDOM (must be random) set of d pages
#         from the set to the root set
#       - Note: The constant d can be 200. The idea of it is trying to include more possibly strong hubs into
#         the root set while constraining the size of the root size.
def get_expanded_root_set(initial_root_set, web_graph, expansion_limit, authority_limit):
    """
    Expands given root set up to given limit by adding out-links of all urls in the root set
    and getting in-links of a page up to given limit (if below limit then add all, otherwise add random
    urls of limit count)

    :param initial_root_set: Top INIT_ROOT_SET_SIZE urls for given query
    :param web_graph: crawled web graph whose node contains a url, its in-links and it out-links
    :param expansion_limit: limit for expanding root set
    :param authority_limit: limit for getting in-links
    :return: expanded set up to given limit
    """
    expansion_size = len(initial_root_set)
    expanded_root_set = initial_root_set
    while expansion_size <= expansion_limit:
        # add out-links
        temp_expanded_set = expanded_root_set.union(get_out_links(expanded_root_set, web_graph))
        # add in-links
        temp_expanded_set = temp_expanded_set.union(get_in_links(expanded_root_set, web_graph, authority_limit))
        expanded_root_set = expanded_root_set.union(temp_expanded_set)
        expansion_size = len(expanded_root_set)
        print '\texpansion size ', expansion_size
    return expanded_root_set


def get_out_links(root_set, web_graph):
    """
    Fetches out-links for given root set to expanded_root_set

    :param root_set: set to get out-links of
    :param web_graph: graph containing url, its in-links and out-links
    :return: list of out-links
    """
    out_links = []
    for url in root_set:
        out_links += web_graph[url][1]
    return out_links


def get_in_links(root_set, web_graph, authority_limit):
    """
    Fetches all in-links of a page if its count is less than limit;
    otherwise fetches random in-links of limit's count

    :param root_set: set to get in-links of
    :param web_graph: graph containing url, its in-links and out-links
    :param authority_limit: limit for in-links
    :return: fetched in-links
    """
    in_links = []
    for url in root_set:
        if len(web_graph[url][0]) <= authority_limit:
            in_links += web_graph[url][0]
        else:
            in_links += random.sample(web_graph[url][0], authority_limit)
    return in_links


# Compute HITS. For each web page, initialize its authority and hub scores to 1.
#    Update hub and authority scores for each page in the base set until convergence
#    - Authority Score Update: Set each web page's authority score in the root set to
#      the sum of the hub score of each web page that points to it
#    - Hub Score Update: Set each web pages's hub score in the base set to the sum of the authority score
#      of each web page that it is pointing to
#    - After every iteration, it is necessary to normalize the hub and authority scores.
def compute_hits(expanded_root_set, web_graph):
    """
    Compute HITS on given set and return computed hubs and authorities scores
    in that order

    :param expanded_root_set: set on which HITS is to be computed
    :param web_graph: graph containing url, its in-links and out-links
    :return: hubs and authorities scores in that order
    """
    # initialize hubs and authorities
    hubs = dict()
    authorities = dict()
    for key in expanded_root_set:
        hubs[key] = [1, 0]
        authorities[key] = [1, 0]

    # update scores until convergence
    last_hubs_perplexity = []
    last_auth_perplexity = []
    iteration = 0
    while (not converged(last_hubs_perplexity)) and (not converged(last_auth_perplexity)):
        normalize = 0
        # calculate auth scores for all pages in base set
        for url in expanded_root_set:
            # auth score of url = sum of hubs score of each in-link
            auth_score = 0
            in_link_count = 0
            for in_link in web_graph[url][0]:
                values = hubs.get(in_link, None)
                if values is not None:
                    hub_score = values[0]
                    in_link_count += 1
                else:
                    hub_score = 0
                auth_score += hub_score
            authorities[url] = [auth_score, in_link_count]
            normalize += math.pow(auth_score, 2)
        normalize = math.pow(normalize, 0.5)
        # normalize auth scores of all pages in the base set
        for url in expanded_root_set:
            authorities[url][0] /= normalize

        normalize = 0
        # calculate hubs scores for all pages in base set
        for url in expanded_root_set:
            # hubs score of url = sum of auth score of each out-link
            hubs_score = 0
            out_link_count = 0
            for out_link in web_graph[url][1]:
                values = authorities.get(out_link, None)
                if values is not None:
                    auth_score = values[0]
                    out_link_count += 1
                else:
                    auth_score = 0
                hubs_score += auth_score
            hubs[url] = [hubs_score, out_link_count]
            normalize += math.pow(hubs_score, 2)
        normalize = math.pow(normalize, 0.5)
        # normalize hubs scores of all pages in the base set
        for url in expanded_root_set:
            hubs[url][0] /= normalize

        # inject perplexities
        last_hubs_perplexity = add_perplexity(last_hubs_perplexity, hubs, True)
        last_auth_perplexity = add_perplexity(last_auth_perplexity, authorities, True)
        # print iteration
        iteration += 1
        print iteration
    return hubs, authorities


def add_perplexity(last_perplexity, score_dict, is_hits=False):
    if is_hits:
        perplexity = get_hits_perplexity(score_dict)
    else:
        perplexity = get_perplexity(score_dict)
    if len(last_perplexity) != PERPLEXITY_TRACK_COUNT:
        last_perplexity.append(perplexity)
    else:
        del last_perplexity[0]
        last_perplexity.append(perplexity)
    return last_perplexity


def get_hits_perplexity(score_dict):
    entropy = 0
    for _, values in score_dict.iteritems():
        try:
            score = values[0]
            entropy += score * math.log(score, 2)
        except:
            pass
    entropy *= -1
    perplexity = math.pow(2, entropy)
    return perplexity


def get_perplexity(page_rank_dict):
    entropy = 0
    for _, pr in page_rank_dict.iteritems():
        try:
            entropy += pr * math.log(pr, 2)
        except:
            pass
    entropy *= -1
    perplexity = math.pow(2, entropy)
    return perplexity


def converged(last_perplexity):
    if len(last_perplexity) < PERPLEXITY_TRACK_COUNT:
        return False
    else:
        summation = 0
        for i in range(0, len(last_perplexity) - 1):
            diff = abs(last_perplexity[i + 1] - last_perplexity[i])
            print '\tvalue', abs(diff)
            if diff > 1:
                return False
                summation += diff
        if summation < 1:
            is_converged = True
        else:
            is_converged = False
    return is_converged
