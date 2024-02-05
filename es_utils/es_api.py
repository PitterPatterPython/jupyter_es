from elasticsearch import Elasticsearch


class ElasticAPI:

    # https://elasticsearch-py.readthedocs.io/en/v8.12.0/api/elasticsearch.html
    def __init__(self, host, port, username, password, **kwargs):

        es_options = {
            "use_ssl": False,
            "verify_certs": False,
            "ca_certs": None,
            "client_cert": None,
            "client_key": None,
            "http_compress": None,
            "ssl_show_warn": False,
            "sniff_on_start": False,
            "sniff_on_connection_fail": False,
            "sniffer_timeout": None
            }

        es_options.update(kwargs)

        self.session = Elasticsearch(
            hosts=f"{host}:{port}",
            basic_auth=(username, password),
            **es_options
        )

    def _handler(self, command, **kwargs):
        """Broker Elasticsearch commands"""

        return getattr(self, command)(**kwargs)

    def get_indices(self, **kwargs):
        """Retrieve a list of index names from the Elasticsearch cluster, without
            including system indexes

        Returns:
            list: a list of index names from the cluster
        """

        indices = [idx for idx in self.session.indices.get_alias(index="*")]

        return indices

    def search(self, **kwargs):
        """Executes a user's Elasticsearch query against the specified index

        Args:
            kwargs (dict): we should be passing in the user's parsed input,
                which will include the index, user's query, scroll_size, and scroll_time.
                The scroll_size, scroll_time, and max_search_results are all set by default
                in the integration, but a user can adjust them.

        Returns:
            list: a list of hits from the API call
        """

        index = kwargs.get("index")
        user_query = kwargs.get("query")
        scroll_size = kwargs.get("es_scroll_size")
        scroll_time = kwargs.get("es_scroll_time")
        max_search_results = kwargs.get("es_max_results")
        total_results = 0

        search_results = []

        query = {
                "query": {
                    "query_string": {
                        "query": user_query
                    }
                },
                "size": scroll_size,
                "from": 0
            }

        response = self.session.search(
            index=index,
            scroll=scroll_time,
            body=query
        )

        scroll_id = response["_scroll_id"]
        scroll_size = len(response["hits"]["hits"])

        while scroll_size > 0 and total_results < max_search_results:
            for hit in response["hits"]["hits"]:
                search_results.append(hit)

            # Update the total number of results we've processed so far
            total_results += scroll_size

            # Scroll to the next batch
            response = self.session.scroll(scroll_id=scroll_id, scroll=scroll_time)

            # Update the scroll size and scroll_id
            scroll_id = response["_scroll_id"]
            scroll_size = len(response["hits"]["hits"])

        self.session.clear_scroll(scroll_id=scroll_id)

        return search_results
