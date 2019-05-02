import os
import sqlite3
import json
import struct
import subprocess
import tempfile

from node_generator import NodeGenerator


class Channel:
    def __init__(self):
        self.id = None
        self.id_on_contractor_side = None


class TrustLine:
    def __init__(self):
        self.id = None
        self.contractor_id = None
        self.contractor = None
        self.incoming_amount = None
        self.outgoing_amount = None
        self.balance = None
        self.is_contractor_gateway = None
        self.equivalent = None


class OwnKey:
    def __init__(self):
        self.hash = None
        self.trust_line_id = None
        self.keys_set_sequence_number = None
        self.public_key = None
        self.private_key = None
        self.number = None
        self.is_valid = None


class NodeMigrator(NodeGenerator):
    def __init__(self, ctx, old_node_path, new_node_path, new_node_address, client_path):
        super().__init__(ctx, old_node_path, new_node_path, new_node_address)
        self.client_path = client_path

        self.channel_idx = 0
        self.old_trust_lines = []

        self.channels = dict()
        self.trust_lines = dict()
        self.own_keys = []

    def add_channel(self, pk, sk, ok, id_on_contractor_side, contractor_address):
        self.new_storage_cur.execute(
            "insert into contractors ('id', 'id_on_contractor_side', 'crypto_key', 'is_confirmed') "
            "values (?, ?, "
                "?, "
                "'1'"
            ");",
            (self.channel_idx, id_on_contractor_side, sqlite3.Binary(pk + sk + ok))
        )

        channel = Channel()
        self.channels[self.channel_idx] = channel
        channel.id = self.channel_idx
        channel.id_on_contractor_side = id_on_contractor_side

        ip_and_port = contractor_address.split(':')
        ip_and_port[0] = ip_and_port[0].split('.')
        address = bytearray(b'\x0c')
        for c in ip_and_port[0]:
            address.append(int(c))
        self.new_storage_cur.execute(
            "insert into contractors_addresses ('type', 'contractor_id', 'address_size', 'address') "
            "values ('12', ?, '7', ?);",
            (self.channel_idx, sqlite3.Binary(address + struct.pack("H", int(ip_and_port[1]))))
        )

        self.channel_idx += 1

    def add_trust_lines(self, local_id, contractor_id):
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

            trust_line = TrustLine()
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
                "'1', ?, X'00', X'00', X'00', X'00', X'00', X'00', ?, ?, ?);",
                (
                    old_trust_line.id,
                    sqlite3.Binary(old_trust_line.balance),
                    sqlite3.Binary(old_trust_line.outgoing_amount),
                    sqlite3.Binary(old_trust_line.incoming_amount)
                )
            )

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

    def retrieve_trust_lines(self):
        self.old_storage_cur.execute(
            "SELECT contractor, incoming_amount, outgoing_amount, balance, is_contractor_gateway, equivalent "
            "FROM trust_lines;")
        rows = self.old_storage_cur.fetchall()
        for row in rows:
            trust_line = TrustLine()
            self.old_trust_lines.append(trust_line)
            trust_line.contractor_id = self.read_uuid(row[0])
            trust_line.contractor = row[0]
            trust_line.incoming_amount = row[1]
            trust_line.outgoing_amount = row[2]
            trust_line.balance = row[3]
            trust_line.is_contractor_gateway = row[4]
            trust_line.equivalent = row[5]

    def retrieve_own_keys(self):
        self.run_and_wait()
        self.new_storage_cur.execute(
            "SELECT hash, trust_line_id, keys_set_sequence_number, public_key, private_key, number, is_valid "
            "FROM own_keys;")
        rows = self.new_storage_cur.fetchall()
        for row in rows:
            own_key = OwnKey()
            self.own_keys.append(own_key)
            own_key.hash = row[0]
            own_key.trust_line_id = row[1]
            own_key.keys_set_sequence_number = row[2]
            own_key.public_key = row[3]
            own_key.private_key = row[4]
            own_key.number = row[5]
            own_key.is_valid = row[6]

    def migrate(self):
        self.db_disconnect()

    def run_and_wait(self):
        self.db_disconnect()
        print("Starting node: " + self.node_name)
        with tempfile.TemporaryFile() as client_f:
            client_proc = subprocess.Popen(
                ["bash", "-c", "cd " + self.new_node_path + ";" + self.client_path + ""]
                #,stdout=client_f, stderr=client_f
            )
            client_proc.wait()
        self.db_connect()
