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

        indices = [idx for idx in self.session.indices.get_alias(index="*")]

        return indices

    def search(self, **kwargs):

        user_query = kwargs.get("query")

        search_results = self.session.search(
            index=kwargs.get("index"),
            body={
                "query": {
                    "query_string": {
                        "query": user_query
                    }
                }
            }
        )

        return search_results
