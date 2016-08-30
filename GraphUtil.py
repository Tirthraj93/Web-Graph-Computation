def get_sink_nodes(base_url_set, web_graph):
    sink_nodes = dict()
    for url in base_url_set:
        # for no out links
        values = web_graph.get(url, None)
        if (values is not None) and (len(values[1]) == 0):
            sink_nodes[url] = values
    return sink_nodes
