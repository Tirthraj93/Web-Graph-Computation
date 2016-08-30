WT2G_FILE_PATH = "D:\\NEU\\Sem 2 - Summer\\IR\\IR_data\\wt2g_inlinks.txt"
MAPPINGS_PATH = "D:\\NEU\\Sem 2 - Summer\\IR\\Assignment\\CS6200_Tirthraj_Parmar\\Python\\WebGraphComputation"
MAPPING_FILE_NAME = 'link_map.txt'
WT2G_PAGE_RANK_PATH = MAPPINGS_PATH + "\\wt2g_page_rank.txt"
CRAWLED_PAGE_RANK_PATH = MAPPINGS_PATH + "\\crawled_page_rank.txt"
HUBS_PATH = MAPPINGS_PATH + "\\hubs.txt"
AUTH_PATH = MAPPINGS_PATH + "\\authorities.txt"
URL_SET_PATH = MAPPINGS_PATH + "\\url_base_set.txt"

ES_HOST = dict(host="localhost", port=9200)

CRAWL_INDEX = "1512_great_mordenist_artist"
CRAWL_TYPE = "document"

LINKS_INDEX = "links"
LINKS_TYPE = "map"

WEB_GRAPH_INDEX = "web_graph"
WEB_GRAPH_TYPE = "nodes"
CREATE_WEB_GRAPH = {
    "settings": {
        "index": {
            "store": {
                "type": "default"
            },
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        WEB_GRAPH_TYPE: {
            "properties": {
                "inlinks": {
                    "type": "string",
                    "store": "true",
                },
                "outlinks": {
                    "type": "string",
                    "store": "true",
                }
            }
        }
    },
    "analysis": {
        "analyzer": {
            "my_keyword": {
                "type": "keyword",
                "filter": "lowercase"
            }
        }
    }
}

WT2G_GRAPH_INDEX = "wt2g_web_graph"
WT2G_GRAPH_TYPE = "wt2g_nodes"
CREATE_WT2G_GRAPH = {
    "settings": {
        "index": {
            "store": {
                "type": "default"
            },
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        WT2G_GRAPH_TYPE: {
            "properties": {
                "inlinks": {
                    "type": "string",
                    "store": "true",
                },
                "outlinks": {
                    "type": "string",
                    "store": "true",
                }
            }
        }
    },
    "analysis": {
        "analyzer": {
            "my_keyword": {
                "type": "keyword",
                "filter": "lowercase"
            }
        }
    }
}

WT2G_LINKS_INDEX = "wt2g_links"
WT2G_LINKS_TYPE = "wt2g_map"
CREATE_WT2G_LINKS = {
    "settings": {
        "index": {
            "store": {
                "type": "default"
            },
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        WT2G_LINKS_TYPE: {
            "properties": {
                "SRC_LINK": {
                    "type": "string",
                    "store": "true",
                },
                "DST_LINK": {
                    "type": "string",
                    "store": "true",
                }
            }
        }
    },
    "analysis": {
        "analyzer": {
            "my_keyword": {
                "type": "keyword",
                "filter": "lowercase"
            }
        }
    }
}

ENCODED_LINKS_INDEX = "encoded_links"
ENCODED_LINKS_TYPE = "encoded_map"
CREATE_ENCODED_LINKS = {
    "settings": {
        "index": {
            "store": {
                "type": "default"
            },
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    },
    "mappings": {
        ENCODED_LINKS_TYPE: {
            "properties": {
                "SRC_LINK": {
                    "type": "string",
                    "store": "true",
                },
                "DST_LINK": {
                    "type": "string",
                    "store": "true",
                }
            }
        }
    },
    "analysis": {
        "analyzer": {
            "my_keyword": {
                "type": "keyword",
                "filter": "lowercase"
            }
        }
    }
}
