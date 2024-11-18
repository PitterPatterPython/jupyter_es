from elasticsearch import Elasticsearch


class ElasticAPI:

    # https://elasticsearch-py.readthedocs.io/en/v8.12.0/api/elasticsearch.html
    def __init__(self, host, port, scheme, username, password, **kwargs):

        es_options = {
            "verify_certs": False,
            "ssl_show_warn": False
            }

        # Remove namedpw and namedsecret keys from kwargs
        # because it will break the ES connection initialization
        try:
            del kwargs["namedpw"]
            del kwargs["namedsecret"]
        except KeyError:
            pass

        # IMPORTANT NOTE: the kwargs we pass in come from es_full when we instantiate
        # this class. The line below (es_options.update(kwargs)) allows us to set options
        # in our environment variables, and have them automatically added to es_options.
        # Consider it a way to add to, or override, default values. It's cool, I know. I wrote it.
        es_options.update(kwargs)

        self.session = Elasticsearch(
            [{"host": host, "port": port, "scheme": scheme}],
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

        indices = [idx["index"] for idx in self.session.cat.indices(format="json") if not idx["index"].startswith(".")]

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
                "bool": {
                    "minimum_should_match": 1,
                    "should": [
                        {
                            "query_string": {
                                "query": user_query,
                                "default_operator": "AND"
                            }
                        }
                    ]
                }
            },
            "size": scroll_size,
            "from": 0
        }

        response = self.session.search(
            index=index,
            scroll=scroll_time,
            body=query,
            timeout='30s'
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
