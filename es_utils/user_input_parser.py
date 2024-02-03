from argparse import ArgumentParser
import json
from es_utils.es_api import ElasticAPI


class UserInputParser(ArgumentParser):

    def __init__(self, *args, **kwargs):
        self.valid_commands = list(filter(lambda func: not func.startswith("_") and
                                          hasattr(getattr(ElasticAPI, func), "__call__"), dir(ElasticAPI)))

        self.line_parser = ArgumentParser(prog=r"%es")
        self.cell_parser = ArgumentParser(prog=r"%%es")

        self.line_subparsers = self.line_parser.add_subparsers(dest="command")
        self.cell_subparsers = self.cell_parser.add_subparsers(dest="command")

        # LINE SUBPARSERS #
        # Subparser for "get_indices"
        self.parser_get_indices = self.line_subparsers.add_parser("get_indices", help="Return a list \
            of indices in the cluster")
        self.parser_get_indices.add_argument("-i", "--instance", required=True, help="The name of the \
            Elasticsearch instance (defined in Jupyter) to use")

        # CELL SUBPARSERS #
        # Subparser for "search"
        self.parser_search = self.cell_subparsers.add_parser("search", help="Perform a search against \
            an index in Elasticsearch")
        self.parser_search.add_argument("-i", "--instance", required=True, help="The name of the \
            Elasticsearch instance (defined in Jupyter) to use")
        self.parser_search.add_argument("-d", "--index", required=True, help="The name of the index in \
            the Elasticsearch cluster to search")

    def display_help(self, command):
        self.parser.parse_args([command], "--help")

    # def transform_query(self, query):
    #     """Transform the user's query to a JSON object. The user's query
    #        is passed as a string (i.e. - '"category" : "*Men\'s"'). This
    #        will wrap it in curly braces, and then json.loads() it to
    #        transform it into an appropriate JSON object that Elasticsearch
    #        understands.

    #     Args:
    #         query (str): The user's query, as inputted into the Jupyter cell

    #     Returns:
    #         dict: a JSON object containing the user's query
    #     """
    #     query = f"{{{query}}}"

    #     return json.loads(query)

    def parse_input(self, input, type):
        """Parses the user's line magic from Jupyter

        Args:
            input (str): the entire contents of the line from Jupyter

        Returns:
            parsed_input (dict): an object containing an error status, a message,
                and parsed command from argparse.parse()
        """

        parsed_input = {
            "type": type,
            "error": False,
            "message": None,
            "input": {}
        }

        # Process line magics
        if type == "line":
            try:
                if len(input.strip().split("\n")) > 1:
                    parsed_input["error"] = True
                    parsed_input["message"] = r"The line magic is more than one line and shouldn't be. \
                        Try `%splunk --help` or `%splunk -h` for proper formatting"

                else:
                    parsed_user_command = self.line_parser.parse_args(input.split())
                    parsed_input["input"].update(vars(parsed_user_command))

            except SystemExit:
                parsed_input["error"] = True
                parsed_input["message"] = r"Invalid input received, see the output above. \
                    Try `%mongo --help` or `%mongo -h`"

        # Process cell magics
        if type == "cell":

            # Split the cell magic by newline
            split_user_input = input.strip().split("\n")

            try:
                if len(split_user_input) == 1:
                    parsed_user_command = self.cell_parser.parse_args(split_user_input[0].split())
                    parsed_input["input"].update(vars(parsed_user_command))
                    parsed_input["error"] = True
                    parsed_input["message"] = "Expected to get 2 lines in your cell magic, but got 1. \
                        Did you forget to include a query?\nTry `--help` or `-h`"

                elif len(split_user_input) == 2:
                    parsed_user_command = self.cell_parser.parse_args(split_user_input[0].split())
                    parsed_user_query = split_user_input[1]

                    # # Transform the user's query into a JSON object
                    # transformed_query = self.transform_query(parsed_user_query)

                    parsed_input["input"].update(vars(parsed_user_command))
                    parsed_input["input"].update({"query": parsed_user_query})

                else:
                    parsed_input["error"] = True
                    parsed_input["message"] = f"Expected to get 2 lines in your cell magic, \
                        but got {len(split_user_input)}. Try `--help` or `-h`"

            except SystemExit:
                parsed_input["error"] = True
                parsed_input["message"] = r"Invalid input received, see the output above. \
                    Try `%%mongo --help` or `%%mongo -h`"

            except Exception as e:
                parsed_input["error"] = True
                parsed_input["message"] = f"Exception while parsing user input: {e}"

        return parsed_input
