import pandas as pd
from IPython.core.magic import (magics_class, line_cell_magic)
from integration_core import Integration
import jupyter_integrations_utility as jiu
from es_core._version import __desc__
from es_utils.es_api import ElasticAPI
from es_utils.user_input_parser import UserInputParser
from es_utils.api_response_parser import ResponseParser


@magics_class
class Es(Integration):
    # Static Variables
    # The name of the integration
    name_str = "es"
    instances = {}
    custom_evars = ["es_conn_default", "es_max_results", "es_scroll_size", "es_scroll_time"]

    # These are the variables in the opts dict that allowed to be set by the user. These are specific
    # to this custom integration and are joined with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["es_conn_default", "es_max_results", "es_scroll_size", "es_scroll_time"]

    myopts = {}
    myopts["es_conn_default"] = ["default", "Default instance to connect with"]
    myopts["es_max_results"] = [10000, "Maximum number of hits to return in your results set."]
    myopts["es_scroll_size"] = [1000, "This number represents the number of hits to process \
        at once in your results set."]
    myopts["es_scroll_time"] = ["2m", "Specifies to the server how long to keep scrollable \
        results available while the client accepts and processes results. Only change this \
        if you're receiving errors."]

    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Es, self).__init__(shell, debug=debug)
        self.debug = debug

        # Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        # Transform self.opts to a regular dictionary to be used alongside
        # calls to the Elasticsearch API
        self.search_opts = {}
        for k, v in self.myopts.items():
            self.search_opts[k] = v[0]

        self.user_input_parser = UserInputParser()
        self.response_parser = ResponseParser()
        self.load_env(self.custom_evars)
        self.parse_instances()

    def customAuth(self, instance):
        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            jiu.display_error(f"Instance {instance} not found in instances - Connection Failed")
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst["session"] = None
            mypass = ""
            if inst["enc_pass"] is not None:
                mypass = self.ret_dec_pass(inst["enc_pass"])
                inst["connect_pass"] = ""

            try:
                print(inst)
                inst["session"] = ElasticAPI(
                    inst["host"],
                    inst["port"],
                    inst["scheme"],
                    inst["user"],
                    mypass,
                    **inst["options"]
                )

                result = 0

            except Exception as e:
                jiu.display_error(f"Error connecting to {instance}: {e}")
                result = -2

        return result

    def customQuery(self, query, instance, reconnect=True):
        dataframe = None
        status = ""

        try:
            parsed_input = self.user_input_parser.parse_input(query, type="cell", **self.search_opts)

            if self.debug:
                jiu.displayMD(f"**[ Dbg ]** parsed_input\n{parsed_input}")

            response = self.instances[instance]["session"]._handler(**parsed_input["input"])

            parsed_response = self.response_parser._handler(response, **parsed_input["input"])

            dataframe = pd.DataFrame(parsed_response)

        except Exception as e:
            raise
            dataframe = None
            status = str(e)

        return dataframe, status

    def retCustomDesc(self):
        return __desc__

    def customHelp(self, current_output):
        out = current_output
        out += self.retQueryHelp(None)

        return out

    def retQueryHelp(self, q_examples=None):
        # Our current customHelp function doesn't support a table for line magics
        # (it's built in to integration_base.py) so I'm overriding it.

        magic_name = self.magic_name
        magic = f"%{magic_name}"

        cell_magic_helper_text = (f"\n## Running {magic_name} cell magics\n"
                                  "--------------------------------\n"
                                  f"\n#### When running {magic} cell magics, {magic} and the instance name \
                                      will be on the 1st of your cell, and then the command to run \
                                      will be on the 2nd line of your cell.\n"
                                  "\n### Cell magic examples\n"
                                  "-----------------------\n")

        cell_magic_table = ("| Cell Magic | Description |\n"
                            "| ---------- | ----------- |\n"
                            "| %%es instance<br>--help | Display usage syntax help for `%%es` cell magics |\n"
                            "| %%es instance<br>command --help | Display the help syntax for a command below |\n"
                            "| %%es instance<br>search -i instance -d index<br>field1: (hello OR goodbye) \
                                AND datetime: now-7d/d | Perform a query using Elasticsearch's **Query String \
                                Query** syntax. https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html |\n")

        line_magic_helper_text = (f"\n## Running {magic_name} line magics\n"
                                  "-------------------------------\n"
                                  f"\n#### To see a line magic's command syntax, type `%es --help`\n"
                                  "\n### Line magic examples\n"
                                  "-----------------------\n")

        line_magic_table = ("| Line Magic | Description |\n"
                            "| ---------- | ----------- |\n"
                            "| %es --help | Display usage syntax help for `%es` line magics |\n"
                            "| %es command --help | Display usage syntax help for a command below |\n"
                            "| %es instance<br>get_indices | Retrieve a list of indices from the \
                                Elasticsearch cluster |\n")

        help_out = cell_magic_helper_text + cell_magic_table + line_magic_helper_text + line_magic_table

        return help_out

    # This is the magic name.
    @line_cell_magic
    def es(self, line, cell=None):

        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)

            if self.debug:
                jiu.displayMD(f"**[ Dbg ]** line: {line}")
                jiu.displayMD(f"**[ Dbg ]** cell: {cell}")

            if not line_handled:  # We based on this we can do custom things for integrations.

                try:
                    parsed_input = self.user_input_parser.parse_input(line, type="line")

                    if self.debug:
                        jiu.displayMD(f"**[ Dbg ]** Parsed Query: `{parsed_input}`")

                    if parsed_input["error"] is True:
                        jiu.display_error(f"{parsed_input['message']}")

                    else:
                        instance = parsed_input["input"]["instance"]

                        if instance not in self.instances.keys():
                            jiu.display_error(f"Instance **{instance}** not found in instances")

                        else:
                            response = self.instances[instance]["session"]._handler(**parsed_input["input"])
                            parsed_response = self.response_parser._handler(response, **parsed_input["input"])
                            jiu.displayMD(parsed_response)

                except Exception as e:
                    jiu.display_error(f"There was an error in your line magic: {e}")

        else:  # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)
