import os, sys
import sqlite3
import json
import subprocess
import tempfile
import node_context

from node_generator import NodeGenerator


class NodeChecker(NodeGenerator):
    def __init__(self, ctx, node_name, old_node_path):
        super().__init__(ctx, node_name, old_node_path, None, None, False)

        self.old_com_storage_con = self.old_com_storage_cur = None
        self.transactions_count = 0
        self.communicator_messages_queue_count = 0

    def check(self):
        print("Checking node: " + self.node_name)

        if self.transactions_count > 0:
            print("NODE HAS transactions_count: " + str(self.transactions_count))

        if self.communicator_messages_queue_count > 0:
            print("NODE HAS communicator_messages_queue_count: " + str(self.communicator_messages_queue_count))

        for trust_line1 in self.old_trust_lines:
            print("cid1="+self.node_name+" eq="+str(trust_line1.equivalent) +
                  " ota="+str(trust_line1.outgoing_amount) + " ita="+str(trust_line1.incoming_amount))
            node = self.ctx.nodes.get(trust_line1.contractor_id)
            if node is None:
                print("NODE NOT FOUND: "+trust_line1.contractor_id)
            for trust_line2 in node.old_trust_lines:
                if trust_line2.equivalent != trust_line1.equivalent or \
                                trust_line2.contractor_id != self.node_name:
                    continue
                print("cid2="+trust_line2.contractor_id+" eq="+str(trust_line2.equivalent))# +
                      #" ota="+str(trust_line2.outgoing_amount) + " ita="+str(trust_line2.incoming_amount))

        print()

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

        print("transactions_count="+str(self.transactions_count) +
              " communicator_messages_queue_count="+str(self.communicator_messages_queue_count))

    def db_connect(self, verbose=True):
        super().db_connect(verbose)
        self.old_com_storage_con = sqlite3.connect(os.path.join(self.old_node_path, "io", "communicatorStorageDB"))
        self.old_com_storage_cur = self.old_com_storage_con.cursor()

    def db_disconnect(self, verbose=True):
        super().db_disconnect(verbose)
        self.old_com_storage_cur.close()
        self.old_com_storage_con = self.old_com_storage_cur = None
