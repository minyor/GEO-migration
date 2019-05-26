import os, sys
import sqlite3
import json
import subprocess
import tempfile
import context

from node_generator import NodeGenerator


class NodeMigrator(NodeGenerator):
    def __init__(self, ctx, old_node_path, new_node_path, new_node_address, client_path):
        super().__init__(ctx, old_node_path, new_node_path, new_node_address)
        self.client_path = client_path

        self.channel_idx = 0
        self.old_trust_lines = []
        self.old_history = []

        self.channels = dict()
        self.trust_lines = dict()
        self.own_keys = None

    def add_channel(self, pk, sk, ok, id_on_contractor_side, contractor_address):
        if not self.ctx.in_memory:
            self.db_connect(False)
        self.new_storage_cur.execute(
            "insert into contractors ('id', 'id_on_contractor_side', 'crypto_key', 'is_confirmed') "
            "values (?, ?, "
                "?, "
                "'1'"
            ");",
            (self.channel_idx, id_on_contractor_side, sqlite3.Binary(pk + sk + ok))
        )

        channel = context.Channel()
        self.channels[self.channel_idx] = channel
        channel.id = self.channel_idx
        channel.id_on_contractor_side = id_on_contractor_side

        self.new_storage_cur.execute(
            "insert into contractors_addresses ('type', 'contractor_id', 'address_size', 'address') "
            "values ('12', ?, '7', ?);",
            (self.channel_idx, sqlite3.Binary(self.serialize_ipv4_with_port(contractor_address)))
        )

        self.channel_idx += 1
        if not self.ctx.in_memory:
            self.db_disconnect(False)

    def add_trust_lines(self, local_id, contractor_id):
        if not self.ctx.in_memory:
            self.db_connect(False)
        for old_trust_line in self.old_trust_lines:
            if old_trust_line.contractor_id != contractor_id:
                continue
            print(
                "\tGenerating trust line for node: " + self.node_name +
                " id: " + str(local_id) +
                " eq: " + str(old_trust_line.equivalent) +
                " gw: " + str(old_trust_line.is_contractor_gateway)
            )
            self.new_storage_cur.execute(
                "insert into trust_lines ('state', 'contractor_id', 'equivalent', 'is_contractor_gateway') "
                "values ('2', ?, ?, ?);",
                (local_id, old_trust_line.equivalent, old_trust_line.is_contractor_gateway)
            )
            old_trust_line.id = self.new_storage_cur.lastrowid

            trust_line = context.TrustLine()
            self.trust_lines[old_trust_line.id] = trust_line
            trust_line.id = old_trust_line.id
            trust_line.contractor_id = local_id
            trust_line.equivalent = old_trust_line.equivalent

            self.new_storage_cur.execute(
                "insert into audit ("
                    "'number', 'trust_line_id', 'our_key_hash', "
                    "'our_signature', 'contractor_key_hash', 'contractor_signature', "
                    "'own_keys_set_hash', 'contractor_keys_set_hash', 'balance', "
                    "'outgoing_amount', 'incoming_amount'"
                ") "
                "values ("
                    "'1', ?, X'00', X'00', X'00', X'00', "
                    "X'0000000000000000000000000000000000000000000000000000000000000000', "
                    "X'0000000000000000000000000000000000000000000000000000000000000000', "
                    "?, ?, ?"
                ");",
                (
                    old_trust_line.id,
                    sqlite3.Binary(old_trust_line.balance),
                    sqlite3.Binary(old_trust_line.outgoing_amount),
                    sqlite3.Binary(old_trust_line.incoming_amount)
                )
            )
        if not self.ctx.in_memory:
            self.db_disconnect(False)

    def add_contractor_key(self, own_key1, own_key2):
        self.new_storage_cur.execute(
            "insert into contractor_keys ("
                "'hash', 'trust_line_id', 'keys_set_sequence_number', "
                "'public_key', 'number', 'is_valid'"
            ") "
            "values (?, ?, ?, ?, ?, ?);",
            (
                own_key2.hash,
                own_key1.trust_line_id,
                own_key1.keys_set_sequence_number,
                own_key2.public_key,
                own_key1.number,
                own_key1.is_valid
            )
        )

    def update_audit_crypto(self, trust_line_id, contractor_key_hash, contractor_signature):
        if not self.ctx.in_memory:
            self.db_connect(False)
        self.new_storage_cur.execute(
            "update audit set "
                "contractor_key_hash = ?, "
                "contractor_signature = ? "
            "WHERE trust_line_id = ?;",
            (
                contractor_key_hash,
                contractor_signature,
                trust_line_id
            )
        )
        if not self.ctx.in_memory:
            self.db_disconnect(False)

    def retrieve_old_data(self):
        self.retrieve_old_trust_lines()
        self.retrieve_old_history()

    def retrieve_old_trust_lines(self):
        self.old_storage_cur.execute(
            "SELECT contractor, incoming_amount, outgoing_amount, balance, is_contractor_gateway, equivalent "
            "FROM trust_lines;")
        rows = self.old_storage_cur.fetchall()
        for row in rows:
            trust_line = context.TrustLine()
            self.old_trust_lines.append(trust_line)
            trust_line.contractor_id = self.read_uuid(row[0])
            trust_line.contractor = row[0]
            trust_line.incoming_amount = row[1]
            trust_line.outgoing_amount = row[2]
            trust_line.balance = row[3]
            trust_line.is_contractor_gateway = row[4]
            trust_line.equivalent = row[5]

    def retrieve_old_history(self):
        self.old_storage_cur.execute(
            "SELECT operation_uuid, operation_timestamp, record_type, record_body, "
                "record_body_bytes_count, equivalent, command_uuid "
            "FROM history;")
        rows = self.old_storage_cur.fetchall()
        for row in rows:
            history = context.History()
            self.old_history.append(history)
            history.operation_uuid = row[0]
            history.operation_timestamp = row[1]
            history.record_type = row[2]
            history.record_body = row[3]
            history.record_body_bytes_count = row[4]
            history.equivalent = row[5]
            history.command_uuid = row[6]

    def retrieve_own_keys(self):
        self.run_and_wait()

    def load_own_keys(self):
        if not self.ctx.in_memory:
            self.db_connect(False)
        self.new_storage_cur.execute(
            "SELECT hash, trust_line_id, keys_set_sequence_number, public_key, private_key, number, is_valid "
            "FROM own_keys;")
        rows = self.new_storage_cur.fetchall()
        self.own_keys = []
        for row in rows:
            own_key = context.OwnKey()
            self.own_keys.append(own_key)
            own_key.hash = row[0]
            own_key.trust_line_id = row[1]
            own_key.keys_set_sequence_number = row[2]
            own_key.public_key = row[3]
            own_key.private_key = row[4]
            own_key.number = row[5]
            own_key.is_valid = row[6]

    def hash_audits(self):
        self.run_and_wait()
        if not self.ctx.in_memory:
            self.db_connect(False)
        self.new_storage_cur.execute(
            "SELECT number, trust_line_id, our_key_hash, our_signature, own_keys_set_hash, contractor_keys_set_hash "
            "FROM audit;")
        rows = self.new_storage_cur.fetchall()
        for row in rows:
            trust_line = self.trust_lines.get(row[1], None)
            if trust_line is None:
                continue
            trust_line.number = row[0]
            trust_line.our_key_hash = row[2]
            trust_line.our_signature = row[3]
            trust_line.own_keys_set_hash = row[4]
            trust_line.contractor_keys_set_hash = row[5]
        if not self.ctx.in_memory:
            self.db_disconnect(False)

    def migrate_history(self):
        trust_line_record_type = 1
        payment_record_type = 2
        payment_additional_record_type = 3

        operation_type_size = 1
        node_uuid_size = 16
        address_type = 12

        records_added = 0
        records_skipped = 0

        for history in self.old_history:
            operation_type = history.record_body[0]
            address_pos_begin = operation_type_size
            address_pos_end = (address_pos_begin + node_uuid_size)
            uuid = self.read_uuid(history.record_body[address_pos_begin:address_pos_end])

            node = self.ctx.nodes.get(uuid)
            if node is None:
                #print("\tHistory for " + uuid + " skipping...")
                records_skipped += 1
                continue

            addresses_bytes = bytearray(b'\x01') + self.serialize_ipv4_with_port(node.new_node_address)
            #print(
            #    "\tHistory for " + uuid +
            #    " record_type=" + str(history.record_type) +
            #    " operation_type=" + str(operation_type) +
            #    " address=" + node.new_node_address
            #)

            history.record_body = \
                history.record_body[0:address_pos_begin] + \
                addresses_bytes + \
                history.record_body[address_pos_end:]

            if history.record_type == payment_record_type:
                history.record_body += bytearray(b'\x00')

            records_added += 1
            self.new_storage_cur.execute(
                "insert into history ("
                "'operation_uuid', 'operation_timestamp', 'record_type', 'record_body', "
                "'record_body_bytes_count', 'equivalent', 'command_uuid'"
                ") "
                "values (?, ?, ?, ?, ?, ?, ?);",
                (
                    history.operation_uuid,
                    history.operation_timestamp,
                    history.record_type,
                    history.record_body,
                    len(history.record_body),
                    history.equivalent,
                    history.command_uuid
                )
            )
        print(
            "Generating history for node: " + self.node_name +
            " added: " + str(records_added) +
            " skipped: " + str(records_skipped)
        )

    def migrate(self):
        self.trust_lines.clear()
        if not self.ctx.in_memory:
            self.db_connect(False)
        self.migrate_history()
        self.db_disconnect()
        print()

    def run_and_wait(self, index=1):
        if self.ctx.in_memory:
            self.db_disconnect()
        print("Starting node: " + self.node_name)
        #self.ctx.runner.run("cd " + self.new_node_path + ";" + self.client_path + "")
        if 0 == 0:
            with tempfile.TemporaryFile() as client_f:
                client_proc = None
                if self.ctx.verbose:
                    client_proc = subprocess.Popen(
                        ["bash", "-c", "cd " + self.new_node_path + ";" + self.client_path + ""]
                    )
                else:
                    client_proc = subprocess.Popen(
                        ["bash", "-c", "cd " + self.new_node_path + ";" + self.client_path + ""],
                        stdout=client_f, stderr=client_f
                    )
                client_proc.wait()
        if self.ctx.in_memory:
            self.db_connect()
