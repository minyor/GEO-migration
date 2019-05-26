import os, sys
import json
import threading
import time

import context

from node_executor import NodeExecutor


class NodeValidator(NodeExecutor):
    class TrustLine:
        def __init__(self):
            self.node = None
            self.contractor_id = None
            self.address = None
            self.state = None
            self.own_keys = None
            self.contractor_keys = None
            self.incoming_trust_amount = None
            self.outgoing_trust_amount = None
            self.balance = None

    def __init__(self, ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path)

        self.trust_lines = None
        self.checked = False

    def init(self, verbose=True):
        new_node_result_fifo_thread = \
            threading.Thread(target=self.open_node_result_fifo, args=(self.new_result_fifo_path, verbose))
        new_node_result_fifo_thread.start()

        new_node_run_thread = threading.Thread(target=self.run_node,
                                               args=(self.new_node_path, self.new_client_path, verbose))
        new_node_run_thread.start()
        time.sleep(0.1)

    def validate(self):
        print()
        self.init()

        if 0 != 0:
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

        self.acquire_trust_lines()

        for trust_line in self.trust_lines:
            node = trust_line.node

        self.clean()

    def acquire_trust_lines(self):
        print("Acquisition of trust lines for node " + str(self.node_name) + "...")
        self.trust_lines = self.get_trust_lines()
        for trust_line in self.trust_lines:
            node = trust_line.node
            if node.trust_lines is None:
                node.init(False)
                node.trust_lines = node.get_trust_lines()
                node.clean(False)

    def get_trust_lines(self):
        print("\tRequesting equivalents for node " + str(self.node_name) + "...")
        result_eq = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n').decode("utf-8")
        result_eq = result_eq.split('\t')
        eq_count = int(result_eq[2])
        print("\tFound " + str(eq_count) + " equivalents")
        trust_lines = []
        for e in range(eq_count):
            eq = int(result_eq[e + 3])
            print("\t\tRequesting trust lines for equivalent " + str(eq) + "...")
            result_tl = self.run_command(
                self.new_commands_fifo_path,
                '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t' + str(eq) + '\n'). \
                decode("utf-8")
            result_tl = result_tl.split('\t')
            tl_count = int(result_tl[2])
            print("\t\tFound " + str(tl_count) + " trust lines")
            for t in range(tl_count):
                trust_line_array_shift = t * 8 + 3
                contractor_id = int(result_tl[trust_line_array_shift + 0])
                addresses = result_tl[trust_line_array_shift + 1]
                state = int(result_tl[trust_line_array_shift + 2])
                own_keys = int(result_tl[trust_line_array_shift + 3])
                contractor_keys = int(result_tl[trust_line_array_shift + 4])
                incoming_trust_amount = float(result_tl[trust_line_array_shift + 5])
                outgoing_trust_amount = float(result_tl[trust_line_array_shift + 6])
                balance = float(result_tl[trust_line_array_shift + 7])
                address = addresses[addresses.find(' ') + 1:]
                node = self.ctx.nodes_by_address.get(address)
                if node is None:
                    assert False, "Can't find node by address " + address
                print("\t\t\tTrust line " + str(t+1) + ":" +
                      " id="+str(node.node_name) + ";" +
                      " incoming=" + str(incoming_trust_amount) + ";" +
                      " outgoing=" + str(outgoing_trust_amount) + ";" +
                      " balance=" + str(balance))
                trust_line = NodeValidator.TrustLine()
                trust_lines.append(trust_line)
                trust_line.node = node
                trust_line.contractor_id = contractor_id
                trust_line.address = address
                trust_line.state = state
                trust_line.own_keys = own_keys
                trust_line.contractor_keys = contractor_keys
                trust_line.incoming_trust_amount = incoming_trust_amount
                trust_line.outgoing_trust_amount = outgoing_trust_amount
                trust_line.balance = balance
        return trust_lines

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
