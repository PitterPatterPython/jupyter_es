#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
from integration_core import Integration
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML

# Your Specific integration imports go here, make sure they are in requirements!

from es_core._version import __desc__

# from pandasticsearch import Select # Doing it without pandasticsearch
from elasticsearch import Elasticsearch
import jupyter_integrations_utility as jiu
#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Es(Integration):
    # Static Variables
    # The name of the integration
    name_str = "es"
    instances = {} 
    custom_evars = ['es_conn_default', 'es_max_results', 'es_batch_size', 'es_scroll_time']
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base

    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["es_conn_default", "es_max_results"]


    # Suportted Search Languages
    search_langs = ['eql', 'dsl', 'basic', 'sql', 'sqltrans']


    myopts = {}
    myopts['es_conn_default'] = ["default", "Default instance to connect with"]
    myopts['es_max_results'] = [10000, "Number of max results to return. Under 10000 this number is exact, above 10000 this number will an estimate with the real results being greater than es_max_results by up to es_batch_size"]
    myopts['es_batch_size'] = [1000, "Number of results to take in matches when using scroll api"]
    myopts['es_scroll_time'] = ["2s", "Scroll Windows size"]

    # Class Init function - Obtain a reference to the get_ipython()

    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Es, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()



    def customAuth(self, instance):
        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            print("Instance %s not found in instances - Connection Failed" % instance)
        else:
            inst = self.instances[instance]
        if inst is not None:
            inst['session'] = None
            mypass = ""
            if inst['enc_pass'] is not None:
                mypass = self.ret_dec_pass(inst['enc_pass'])
                inst['connect_pass'] = ""

            es_def_opts = {
                            "use_ssl": False, "verify_certs": False, "ca_certs": None, "client_cert": None, "client_key": None,
                            "http_compress": None, "ssl_show_warn": False, "sniff_on_start": False, "sniff_on_connection_fail": False,
                            "sniffer_timeout": None
                          }

            if 'no_auth' in inst['options'] and inst['options']['no_auth'] == 1:
                myauth = False
            else:
                myauth = True


            oururls = []
            if myauth:
                base_url = inst['scheme'] + "://" + inst['user'] + ":" + mypass + "@" +  inst['host'] + ":" + str(inst['port']) + "/"
            else:
                base_url = inst['scheme'] + "://" + inst['host'] + ":" + str(inst['port']) + "/"

            oururls.append(base_url)
            if 'hosts' in inst['options']:
                for h in inst['options']['hosts'].split(","):
                    if myauth:
                        this_url = inst['scheme'] + "://" + inst['user'] + ":" + mypass + "@" + h + ":" + str(inst['port']) + "/"
                    else:
                        this_url = inst['scheme'] + "://" + h + ":" + str(inst['port']) + "/"

                    if this_url != base_url:
                        oururls.append(this_url)
            our_opts = {}
            for o in es_def_opts.keys():
                if o in inst['options']:
                    our_opts[o] = inst['options'][o]
                else:
                    our_opts[o] = es_def_opts[o]

            try:
                inst['session'] = Elasticsearch(oururls, use_ssl=our_opts['use_ssl'], verify_certs=our_opts['verify_certs'],ca_certs=our_opts['ca_certs'], client_cert=our_opts['client_cert'], 
                                                client_key=our_opts['client_key'], http_compress=our_opts['http_compress'], ssl_show_warn=our_opts['ssl_show_warn'], sniff_on_start=our_opts['sniff_on_start'],
                                                sniff_on_connection_fail=our_opts['sniff_on_connection_fail'], sniffer_timeout=our_opts['sniffer_timeout'])
                result = 0
            except:
                print("Unable to connect to Elastic Search instance %s at %s" % (instance, inst["conn_url"]))
                result = -2

        return result

    def req_password(self, instance):
        opts = None
        retval = True
        try:
            opts = self.instances[instance]['options']
        except:
            print("Instance %s options not found" % instance)
        try:
            if opts['no_auth'] == 1:
                retval = False
                if self.debug:
                    print("Password Not Required")
            else:
                if self.debug:
                    print("Password Required")
        except:
            if self.debug:
                print("Password Required")
        return retval

    def req_username(self, instance):
        opts = None
        retval = True
        try:
            opts = self.instances[instance]['options']
        except:
            print("Instance %s options not found" % instance)
        try:
            if opts['no_auth'] == 1:
                retval = False
                if self.debug:
                    print("Usename not Required")
            else:
                if self.debug:
                    print("Usename Required")
        except:
            if self.debug:
                print("Usename Required")
        return retval

    def validateQuery(self, query, instance):
        bRun = True
        bReRun = False
        
        qlang, qindex, qpost, myquery = self.es_parse(query)


        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True
        # Ok, we know if we are rerun or not, so let's now set the last_query 
        self.instances[instance]['last_query'] = query
        self.instances[instance]['last_query_ts'] = int(time.time())
        if qlang == "dsl":
            try:
                dictquery = json.loads(myquery)
                if not isinstance(dictquery, dict):
                    print("The query provided for dsl is not a dictionary, therefore will not be processed as a dsl query")
                    print("Query:\n%s" % myquery)
                    bRun = False
            except:
               print("The query provided for dsl is not a dictionary or had a conversion error, therefore will not be processed as a dsl query")
               print("Query:\n%s" % myquery)
               bRun = False


        if qlang == "eql":
            try:
                dictquery = eval(myquery)
                if not isinstance(dictquery, dict):
                    print("The query provided for eql is not a dictionary, therefore will not be processed as a eql query")
                    print("Query:\n%s" % myquery)
                    bRun = False
            except:
               print("The query provided for eql is not a dictionary, therefore will not be processed as a eql query")
               print("Query:\n%s" % myquery)
               bRun = False

        # Example Validation
        # Warn only - Don't change bRun
        # Basically, we print a warning but don't change the bRun variable and the bReRun doesn't matter


        if qlang is None:
            print("No language specified - Defauling to basic queries")

        if qindex is None and qlang not in ["sql", "sqltrans"]:
            print("No index provided - Stopping execution to ensure you wish to search all indexes (run query again to override)")
            if bReRun == False:
                bRun = False

        if qpost is not None:
            print("Post processing things provided, we don't support that yet - Still running, just ignoring")



        # Warn and do not allow submission
        # There is no way for a user to submit this query 
#        if query.lower().find('limit ") < 0:
#            print("ERROR - All queries must have a limit clause - Query will not submit without out")
#            bRun = False
        return bRun


    def resolve_fields(self, row):
        fields = {}
        for field in row:
            nested_fields = {}
            if isinstance(row[field], dict):
                nested_fields = self.resolve_fields(row[field])
                for n_field, val in nested_fields.items():
                    fields["{}.{}".format(field, n_field)] = val
            else:
                fields[field] = row[field]
        return fields

    def hit_to_row(self, hit):
        row = {}
        for k in hit.keys():
            if k == '_source':
                solved_fields = self.resolve_fields(hit['_source'])
                row.update(solved_fields)
            elif k.startswith('_'):
                row[k] = hit[k]
        return row


    def es_parse(self, query):
        # This handles the specialness of ES queries
        myquerymeta = query.split("\n")[0]
        myquery = "\n".join(query.split("\n")[1:])

        qlist = myquerymeta.split(" ")
        if len(qlist) > 1:
            if qlist[0].strip() in self.search_langs:
            # They specified a Query language so we are going to go with it
                qlang = qlist[0]
                qindex = qlist[1]
                if len(qlist) > 2:
                # They provided some post processing instructions maybe?
                    qpost = " ".join(qlist[2:])
                else:
                    qpost = None
        elif len(qlist) == 1: # Then this probably just an index?
            if qlist[0].strip() != "": # Well let's check if there is anything here?
                if qlist[0].strip() in self.search_langs: # well it's a lang, 
                    qlang = qlist[0]
                    qindex = None
                    qpost = None
                else: # Let's assume it's a index
                    qindex = qlist[0]
                    qlang = "basic" # If no language, then let's assume basic
                    qpost = None
            else: #Nothing was provided... 
                qlang = "basic"
                qindex = None
                qpost = None
        return qlang, qindex, qpost, myquery


    def customQuery(self, query, instance, reconnect=True):

        qlang, qindex, qpost, myquery = self.es_parse(query)
        mydf = None
        status = ""
        str_err = ""
        all_hits = []
        max_results = int(self.opts['es_max_results'][0])
        batch_size = int(self.opts['es_batch_size'][0])
        scroll_time = self.opts['es_scroll_time'][0]
        if qlang in ['basic', 'dsl']:
            bGood = True
            try:
                if qlang == 'basic':
                    myres = self.instances[instance]['session'].search(q=myquery, index=qindex,  size=batch_size, scroll=scroll_time)
                elif qlang == 'dsl':
                    mbody = json.loads(myquery)
                    myres = self.instances[instance]['session'].search(body=mbody, index=qindex,  size=batch_size, scroll=scroll_time)
            except Exception as e:
                bGood = False
                mydf = None
                str_err +="\nES Query error: %s" % str(e)
            if bGood:
                myscrollid = myres['_scroll_id']
                all_hits = myres['hits']['hits']
                while len(myres['hits']['hits']) and len(all_hits) < max_results:
                    myres = self.instances[instance]['session'].scroll(scroll_id = myscrollid, scroll=scroll_time)
                    if myscrollid != myres['_scroll_id']:
                        myscrollid = myres['_scroll_id']
                        if self.debug:
                            print("Got new Scroll ID")
                    all_hits += myres['hits']['hits']

        elif qlang == "eql":
            try:
                mybody = eval(myquery)
                results = self.instances[instance]['session'].eql.search(body=mybody, index=qindex)
                status = "Query Success"
            except Exception as e:
                mydf = None
                str_err += "EQL error: %s" % str(e)
        elif qlang == "sqltrans":
            try:
                body = {"query": myquery, "fetch_size": batch_size}
                myres = self.instances[instance]['session'].sql.translate(body=body, format="json")
                str_err = "Other: \n" + json.dumps(myres, sort_keys=True, indent=4, separators=(',', ': '))
            except Exception as e:
                str_err += "\n SQL Translate error Occured: %s" % str(e)

        elif qlang == "sql":
            bGood = True
            mycurs = None
            clear_curs = None
            cols = []
            rows = []
            try:
                body = {"query": myquery, "fetch_size": batch_size}
                myres = self.instances[instance]['session'].sql.query(body=body, format="json")
            except Exception as e:
                str_err += "\n SQL Query error: %s" % str(e)
                bGood = False
            if bGood:
                try:
                    cols = myres['columns']
                    rows = myres['rows']
                    if 'cursor' in myres.keys():
                        mycurs = myres['cursor']
                except Exception as e:
                    str_err += "\n SQL fetch rows/cols error: %s" % str(e)
                    bGood = False
            if bGood:
                while mycurs is not None:
                    try:
                        body = {"cursor": mycurs, "fetch_size": batch_size}
                        myres = self.instances[instance]['session'].sql.query(body=body, format="json")
                        if "cursor" in myres.keys():
                            mycurs = myres['cursor']
                        else:
                            clear_curs = mycurs
                            mycurs = None
                        rows += myres['rows']
                        if len(rows) >= max_results:
                            clear_curs = mycurs
                            mycurs = None
                        if mycurs is None:
                            # Clear Cursosr
                            finalres = self.instances[instance]['session'].sql.clear_cursor(body={"cursor": clear_curs})
                    except Exception as e:
                        str_err += "Pagination sql error: %s" % str(e)
                        bGood = False
            if bGood:
                col_names = [c["name"] for c in cols]
                all_hits = []
                all_hits = [dict(zip(col_names, r)) for r in rows]



# New Pandas version
        if qlang == "eql":
            try:
                res_list = results['hits']['events']
            except Exception as e:
                mydf = None
                str_err = "Error - EQL hits -> events\n"
                str_err += str(e)
                if self.debug:
                    print("Failed to find hits -> events array in eql results")
                    print(str_err)
        elif qlang in ['basic', 'dsl', 'sql']:
            try:
                res_list = all_hits
            except Exception as e:
                mydf = None
                str_err = "Error - hits->hits\n"
                str_err += str(e)
                if self.debug:
                    print("Failed to find hits -> hits array in results")
                    print(str_err)



        if str_err == "":
            # Update the last results
#            self.ipy.user_ns['prev_' + self.name_str + "_" + instance + "_js"] = res_list
            # Make Data frame

            try:
                if qlang == "sql":
                    flat_res_list = all_hits
                else:
                    flat_res_list = [self.hit_to_row(hit) for hit in res_list]
                mydf = pd.DataFrame(flat_res_list)
            except Exception as e:
                mydf = None
                str_err += "\nPandas conversion error\n" + str(e)


        if str_err.find("error") < 0 and str_err.find("Warning") < 0 and str_err.find("Other: ") < 0:
            if mydf is not  None:
                str_err = "Success"
                if len(mydf) == max_results:
                    print("Warning: the number of returned results is the same as your max_hits - It's likely that there are more hits not returned in your data set")
            else:
               mydf = None
               str_err = "Success - No Results"
        

        if str_err.find("Success") >= 0:
            pass
        elif str_err.find("Other: ") >= 0:
            status = str_err
            mydf = None
        else:
            status = "Failure - query_error: \n" + str_err

        return mydf, status


# Display Help can be customized
    def customOldHelp(self):
        self.displayIntegrationHelp()
        self.displayQueryHelp('basic myindex\nfield: value AND otherfield: othervalue')

    def retCustomDesc(self):
        return __desc__


    def customHelp(self, curout):
        n = self.name_str
        mn = self.magic_name
        m = "%" + mn
        mq = "%" + m
        table_header = "| Magic | Description |\n"
        table_header += "| -------- | ----- |\n"
        out = curout
        qexamples = []
        qexamples.append(["myinstance", "basic<br>field: value AND otherfield: othervalue", "Run a basic syntax query against myinstance"])
        qexamples.append(["", "basic<br>field: value AND otherfield: othervalue", "Run a basic syntax query against the default instance"])
        out += self.retQueryHelp(qexamples)

        return out






    # This is the magic name.
    @line_cell_magic
    def es(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)

