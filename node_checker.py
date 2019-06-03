import os, sys
import sqlite3
import json
import subprocess
import tempfile
import binascii
import node_context

from node_generator import NodeGenerator


class NodeChecker(NodeGenerator):
    def __init__(self, ctx, node_name, old_node_path):
        super().__init__(ctx, node_name, old_node_path, None, None, False)

        self.checked = False
        self.old_com_storage_con = self.old_com_storage_cur = None
        self.transactions_count = 0
        self.communicator_messages_queue_count = 0

    def check(self):
        #print("Checking node: " + self.node_name)
        self.checked = True

        if self.transactions_count > 0:
            self.print_error(None, "Node has transactions_count: " + str(self.transactions_count))
            self.checked = False

        if self.communicator_messages_queue_count > 0:
            self.print_error(None, "Node has communicator_messages_queue_count: " + str(self.communicator_messages_queue_count))
            self.checked = False

        for trust_line1 in self.old_trust_lines:
            node = self.ctx.nodes.get(trust_line1.contractor_id)
            if node is None:
                self.print_error(trust_line1, "Node not found: "+trust_line1.contractor_id)
                self.checked = False
                continue
            ota1 = NodeChecker.read_amount(trust_line1.outgoing_amount)
            ita1 = NodeChecker.read_amount(trust_line1.incoming_amount)
            bal1 = NodeChecker.read_amount(trust_line1.balance)
            for trust_line2 in node.old_trust_lines:
                if trust_line2.equivalent != trust_line1.equivalent or \
                                trust_line2.contractor_id != self.node_name:
                    continue
                #print("\tChecking trust line: "+trust_line1.contractor_id+" eq=" +
                #      str(trust_line1.equivalent))
                ota2 = NodeChecker.read_amount(trust_line2.outgoing_amount)
                ita2 = NodeChecker.read_amount(trust_line2.incoming_amount)
                bal2 = NodeChecker.read_amount(trust_line2.balance)
                if ota1[0] != ita2[0] or ota1[1] != ita2[1]:
                    self.print_error(trust_line1, "Trust line ota does not match")
                    self.checked = False
                if ita1[0] != ota2[0] or ita1[1] != ota2[1]:
                    self.print_error(trust_line1, "Trust line ita does not match")
                    self.checked = False
                if (not NodeChecker.check_if_bal_is_null(bal1) or not NodeChecker.check_if_bal_is_null(bal2)) and \
                        (bal1[0] == bal2[0] or bal1[1] != bal2[1]):
                    self.print_error(trust_line1, "Trust line balance does not match")
                    self.checked = False

    def print_error(self, trust_line, msg):
        trust_line_str = ""
        if trust_line is not None:
            trust_line_str = " trust_line="+trust_line.contractor_id+" eq="+str(trust_line.equivalent)
        print("ERROR: node="+self.node_name+trust_line_str)
        print("\t"+msg)

    def retrieve_old_data(self):
        self.retrieve_old_trust_lines()

        self.old_storage_cur.execute(
            "SELECT COUNT(*) "
            "FROM transactions;")
        rows = self.old_storage_cur.fetchall()
        self.transactions_count = rows[0][0]

        self.old_com_storage_cur.execute(
            "SELECT COUNT(*) "
            "FROM communicator_messages_queue;")
        rows = self.old_com_storage_cur.fetchall()
        self.communicator_messages_queue_count = rows[0][0]

    def db_connect(self, verbose=True):
        super().db_connect(verbose)
        self.old_com_storage_con = sqlite3.connect(os.path.join(self.old_node_path, "io", "communicatorStorageDB"))
        self.old_com_storage_cur = self.old_com_storage_con.cursor()

    def db_disconnect(self, verbose=True):
        super().db_disconnect(verbose)
        self.old_com_storage_cur.close()
        self.old_com_storage_con = self.old_com_storage_cur = None
