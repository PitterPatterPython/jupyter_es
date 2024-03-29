class ResponseParser:

    def __init__(self):
        pass

    def _handler(self, response, **kwargs):
        """Brokers response parsing on behalf of the calling function

        Args:
            response (int, str, list, or dict): the response from our API
                call in its original form, hence the varying types

        Returns:
            Whatever is passed back to it from one of the functions below.
        """

        issued_command = kwargs.get("command")

        return getattr(self, issued_command)(response, **kwargs)

    def get_indices(self, response, **kwargs):
        """Sort the indexes and format them as Markdown"""
        instance = kwargs.get("instance")
        response.sort()
        formatted_index_names = "".join(f"* {idx}\n" for idx in response if not idx.startswith("."))
        formatted_index_list = (f"#### Indices in `{instance}`\n"
                                "***\n"
                                f"{formatted_index_names}")

        return formatted_index_list

    def search(self, response, **kwargs):
        """Turn the API response into a list of hits for our dataframe"""

        data = [hit["_source"] for hit in response]

        return data
