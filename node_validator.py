import os, sys
import json
import threading
import time

import context

from node_executor import NodeExecutor


class NodeValidator(NodeExecutor):
    def __init__(self, ctx, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path)

        self.trust_lines = dict()
        self.checked = False

    def init(self, verbose=True):
        new_node_result_fifo_thread = \
            threading.Thread(target=self.open_node_result_fifo, args=(self.new_result_fifo_path, verbose))
        new_node_result_fifo_thread.start()

        new_node_run_thread = threading.Thread(target=self.run_node,
                                               args=(self.new_node_path, self.new_client_path, verbose))
        new_node_run_thread.start()

    def validate(self):
        print()
        self.init()

        contractors = self.get_contractors()
        for c_tuple in contractors:
            contractor_id = c_tuple[0]
            node = c_tuple[1]
            node.init(False)
            id_on_contractor_side = node.get_contractors(self.new_node_address, False)
            if id_on_contractor_side is None:
                assert False, "Can't confirm channel between nodes: " +\
                              self.node_name + ", " + node.node_name
            print("\tFound channel (" + str(contractor_id) + "," + str(id_on_contractor_side) + ")")

            node.clean(False)

        self.clean()

    def get_trust_lines(self):
        print("Requesting contractors for node " + str(self.node_name) + "...")
        result_ct = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors-all\n'). \
            decode("utf-8")
        result_ct = result_ct.split('\t')
        ct_count = int(result_ct[2])
        print("\tFound " + str(ct_count) + " contractors")
        contractors = []
        for t in range(ct_count):
            trust_line_array_shift = t * 2 + 3
            contractor_id = int(result_ct[trust_line_array_shift + 0])
            addresses = result_ct[trust_line_array_shift + 1]
            address = addresses[addresses.find(' ')+1:].rstrip('\n')
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address '" + address + "'"
            print("\t\tContractor " + str(t+1) + ":" +
                  " id="+str(contractor_id) + ";" +
                  " address=" + str(address) + ";" +
                  " uuid=" + str(node.node_name))
            contractors.append((contractor_id, node))
        return contractors

    def get_contractors(self, search_address=None, verbose=True):
        if verbose:
            print("Requesting contractors for node " + str(self.node_name) + "...")
        result_ct = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors-all\n'). \
            decode("utf-8")
        result_ct = result_ct.split('\t')
        ct_count = int(result_ct[2])
        if verbose:
            print("\tFound " + str(ct_count) + " contractors")
        contractors = []
        for t in range(ct_count):
            trust_line_array_shift = t * 2 + 3
            contractor_id = int(result_ct[trust_line_array_shift + 0])
            addresses = result_ct[trust_line_array_shift + 1]
            address = addresses[addresses.find(' ')+1:].rstrip('\n')
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address '" + address + "'"
            if verbose:
                print("\t\tContractor " + str(t+1) + ":" +
                      " id="+str(contractor_id) + ";" +
                      " address=" + str(address) + ";" +
                      " uuid=" + str(node.node_name))
            contractors.append((contractor_id, node))
            if search_address is not None:
                if search_address == address:
                    return contractor_id
        if search_address is not None:
            return None
        return contractors
