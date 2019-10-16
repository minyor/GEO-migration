import os
import sqlite3

from node.generator import NodeGenerator
from node.context import CommunicatorMessage


class NodeChecker(NodeGenerator):
    def __init__(self, ctx, node_name, old_node_path):
        super().__init__(ctx, node_name, old_node_path, None, None, False)

        self.checked = False
        self.old_com_storage_con = self.old_com_storage_cur = None
        self.transactions_count = 0
        self.communicator_messages_queue_count = 0

        self.transactions = []
        self.communicator_messages = []

    def check(self):
        #print("Checking node: " + self.node_name)
        self.checked = True

        if self.transactions_count > 0:
            self.print_error(None, "Node has transactions_count: " + str(self.transactions_count))
            transaction_idx = 0
            for transaction in self.transactions:
                print("\t\tTransaction #"+str(transaction_idx) +
                      " transaction_uuid="+str(transaction)
                      )
            self.checked = False

        if self.communicator_messages_queue_count > 0:
            self.print_error(None, "Node has communicator_messages_queue_count: " +
                             str(self.communicator_messages_queue_count))
            message_idx = 0
            for message in self.communicator_messages:
                print("\t\tMessage #"+str(message_idx) +
                      " contractor_uuid="+str(message.contractor_uuid) +
                      " transaction_uuid="+str(message.transaction_uuid) +
                      " message_type="+str(message.message_type) +
                      " recording_time="+str(message.recording_time) +
                      " equivalent="+str(message.equivalent)
                      )
            self.checked = False

        non_null_bal = True
        gateway_only = True

        for trust_line1 in self.old_trust_lines:
            node = self.ctx.nodes.get(trust_line1.contractor_id)
            if node is None:
                self.print_error(trust_line1, "Node not found: "+trust_line1.contractor_id)
                self.checked = False
                continue
            ota1 = NodeChecker.read_amount(trust_line1.outgoing_amount)
            ita1 = NodeChecker.read_amount(trust_line1.incoming_amount)
            bal1 = NodeChecker.read_amount(trust_line1.balance)

            if NodeChecker.check_if_bal_is_null(bal1):
                non_null_bal = False

            for trust_line2 in node.old_trust_lines:
                if trust_line2.equivalent != trust_line1.equivalent or \
                                trust_line2.contractor_id != self.node_name:
                    continue
                if trust_line2.is_contractor_gateway == 0:
                    gateway_only = False
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

        if len(self.old_trust_lines) < 1:
            self.ctx.checking_tr_0_json = self.ctx.append_node_uuid(
                self.ctx.checking_tr_0_json, self.node_name)
        elif gateway_only:
            if non_null_bal:
                self.ctx.checking_tr_gw_bal_json = self.ctx.append_node_uuid(
                    self.ctx.checking_tr_gw_bal_json, self.node_name)
            else:
                self.ctx.checking_tr_gw_non_bal_json = self.ctx.append_node_uuid(
                    self.ctx.checking_tr_gw_non_bal_json, self.node_name)

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

        if self.transactions_count > 0:
            self.old_storage_cur.execute(
                "SELECT transaction_uuid "
                "FROM transactions;")
            rows = self.old_storage_cur.fetchall()
            for row in rows:
                transaction = self.read_uuid(row[0])
                self.transactions.append(transaction)

        if self.communicator_messages_queue_count > 0:
            self.old_com_storage_cur.execute(
                "SELECT contractor_uuid, transaction_uuid, message_type, recording_time, equivalent "
                "FROM communicator_messages_queue;")
            rows = self.old_com_storage_cur.fetchall()
            for row in rows:
                message = CommunicatorMessage()
                self.communicator_messages.append(message)
                message.contractor_uuid = self.read_uuid(row[0])
                message.transaction_uuid = self.read_uuid(row[1])
                message.message_type = row[2]
                message.recording_time = row[3]
                message.equivalent = row[4]

    def db_connect(self, verbose=True):
        super().db_connect(verbose)
        self.old_com_storage_con = sqlite3.connect(os.path.join(self.old_node_path, "io", "communicatorStorageDB"))
        self.old_com_storage_cur = self.old_com_storage_con.cursor()

    def db_disconnect(self, verbose=True):
        super().db_disconnect(verbose)
        self.old_com_storage_cur.close()
        self.old_com_storage_con = self.old_com_storage_cur = None
