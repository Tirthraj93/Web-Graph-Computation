class WebGraphNode:
    """
    class representing a node in a web graph
    """

    def __init__(self, id, in_links_list, total_out_links):
        """
            id: page id
            inLinksList: links pointing to id
            outLinksList: links id is pointing to
        """
        self.id = id
        self.in_links_list = in_links_list
        self.total_out_links = total_out_links

    def get_id(self):
        return self.id

    def get_in_links_list(self):
        return self.in_links_list

    def get_total_out_links(self):
        return self.total_out_links