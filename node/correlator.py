import json
import os
import threading
import time
import requests
import urllib
import redis
import logging

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class NodeCorrelator:
    def __init__(self, ctx, node_name, new_node_address, old_uuid_2_address_path):
        self.ctx = ctx
        self.node_idx = len(self.ctx.nodes)
        self.node_name = node_name
        self.new_node_address = new_node_address
        self.log_lines = []

    def info(self, str):
        if self.ctx.only_not_correlated:
            self.log_lines.append(str)
        else:
            self.ctx.logger.info(str)

    def print_log_lines(self):
        for line in self.log_lines:
            self.ctx.logger.info(line)
        self.log_lines = []

    def raise_error(self, str):
        if self.ctx.only_not_correlated:
            self.print_log_lines()
        raise ValueError(str)

    def correlate(self):
        self.info("")
        self.info("Correlating node #"+str(self.node_idx+1)+": " + self.node_name + ": " + self.new_node_address)

        old_equivalents = self.get_equivalents(self.ctx.old_handler_url, self.node_name)
        new_equivalents = self.get_equivalents(self.ctx.new_handler_url, self.new_node_address)

        old_equivalents_converted = len(old_equivalents) * [None]
        for i in range(len(old_equivalents)):
            old_equivalents_converted[i] = self.ctx.eq_map(old_equivalents[i])

        list.sort(old_equivalents_converted)
        list.sort(new_equivalents)

        self.info("\told_equivalents="+str(old_equivalents_converted))
        self.info("\tnew_equivalents="+str(new_equivalents))

        if str(old_equivalents_converted) != str(new_equivalents):
            self.raise_error("Error: Node " + self.node_name + " Old and New equivalents differs'")

        old_trust_lines = dict()
        for equivalent in old_equivalents:
            trust_lines = self.get_trust_lines(self.ctx.old_handler_url, self.node_name, equivalent)
            equivalent = self.ctx.eq_map(equivalent)
            old_trust_lines[equivalent] = trust_lines

        new_trust_lines = dict()
        for equivalent in new_equivalents:
            trust_lines = self.get_trust_lines(self.ctx.new_handler_url, self.new_node_address, equivalent)
            new_trust_lines[equivalent] = trust_lines

        old_tl_cmp_list, new_tl_cmp_list = self.form_trust_lines_cmp_lists(old_trust_lines, new_trust_lines)
        if len(old_tl_cmp_list) != len(new_tl_cmp_list):
            self.raise_error("Error: Node " + self.node_name + " Sizes of Old and New equivalents differs: " +
                             str(len(old_tl_cmp_list)) + "!=" + str(len(new_tl_cmp_list)))

        for i in range(len(old_tl_cmp_list)):
            old_tr_list = old_tl_cmp_list[i]
            new_tr_list = new_tl_cmp_list[i]
            if len(old_tr_list) != len(new_tr_list):
                self.raise_error("Error: Node " + self.node_name + " Sizes of Old and New trust lines differs: " +
                                str(len(old_tr_list)) + "!=" + str(len(new_tr_list)))
            for j in range(len(old_tr_list)):
                if str(old_tr_list[j]) != str(new_tr_list[j]):
                    self.raise_error("Error: Node " + self.node_name + " Old and New trust line differs: \n" +
                                     str(old_tr_list[j]) + "\n" + str(new_tr_list[j]))

        #if str(old_tl_cmp_list) != str(new_tl_cmp_list):
        #    self.raise_error("Error: Node " + self.node_name + " Old and New trust lines differs'")

    def form_trust_lines_cmp_lists(self, old_trust_lines, new_trust_lines):
        old_list = []
        new_list = []

        for equivalent in old_trust_lines:
            trust_lines = old_trust_lines[equivalent]
            tls = []
            for trust_line in trust_lines:
                address = self.ctx.gns_addresses[trust_line["uuid"]]
                if address is None:
                    self.raise_error("Error: Node " + self.node_name +
                                     " No gns address for uuid '"+trust_line["uuid"]+"' \ntrustline: "+str(trust_line))
                tl = ""
                tl += "address=" + address + "; "
                tl += "incoming_trust_amount=" + trust_line["incoming_trust_amount"] + "; "
                tl += "outgoing_trust_amount=" + trust_line["outgoing_trust_amount"] + "; "
                tl += "balance=" + trust_line["balance"] + "; "
                tl += "state=" + "2" + "; "
                tls.append(tl)
            list.sort(tls)
            old_list.append(tls)

        for equivalent in new_trust_lines:
            trust_lines = new_trust_lines[equivalent]
            tls = []
            for trust_line in trust_lines:
                tl = ""
                tl += "address=" + trust_line["contractor"] + "; "
                tl += "incoming_trust_amount=" + trust_line["incoming_trust_amount"] + "; "
                tl += "outgoing_trust_amount=" + trust_line["outgoing_trust_amount"] + "; "
                tl += "balance=" + trust_line["balance"] + "; "
                tl += "state=" + trust_line["state"] + "; "
                tls.append(tl)
            list.sort(tls)
            new_list.append(tls)

        list.sort(old_list)
        list.sort(new_list)

        return old_list, new_list

    def get_equivalents(self, url, address):
        try:
            node_name = address
            self.info("\tRetrieving equivalents for node '" + node_name + "'...")
            address = urllib.parse.quote_plus(address)
            r = requests.get(
                "http://" + url + "/api/v1/nodes/" + address + "/equivalents/"
            )
            response = r.json()
            if self.ctx.debug:
                self.info("\t\tResponse: " + str(response))

            redis_response = json.loads(self.get_from_redis(response["data"]["response_uuid"]).decode('utf8').replace("'", '"'))
            if self.ctx.debug:
                self.info("\t\tRedis response: " + str(redis_response))
        except Exception as e:
            self.raise_error(e)

        if redis_response["status"] != 200:
            self.raise_error("Error: Node " + node_name + " handler returned '" + str(redis_response["status"]) + "': ")

        ret = redis_response["data"]["equivalents"]
        return ret

    def get_trust_lines(self, url, address, equivalent):
        try:
            node_name = address
            self.info("\tRetrieving trust lines for node '" + node_name + "', equivalent '" + str(equivalent) + "'...")
            address = urllib.parse.quote_plus(address)
            r = requests.get(
                "http://" + url + "/api/v1/nodes/" + address +
                    "/contractors/trust-lines/" + str(equivalent) + "/"
            )
            response = r.json()
            if self.ctx.debug:
                self.info("\t\tResponse: " + str(response))

            redis_response = json.loads(self.get_from_redis(response["data"]["response_uuid"]).decode('utf8').replace("'", '"'))
            if self.ctx.debug:
                self.info("\t\tRedis response: " + str(redis_response))
        except Exception as e:
            self.raise_error(e)

        if redis_response["status"] != 200:
            self.raise_error("Error: Node " + node_name + " handler returned '" + str(redis_response["status"]) + "': ")

        ret = redis_response["data"]["trust_lines"]
        return ret

    def get_from_redis(self, key):
        tries = 1000
        while True:
            val = self.ctx.redis.get(key)
            if val == None and tries > 0:
                tries -= 1
                time.sleep(0.1)
                continue
            return val
