import os
import sqlite3
import json
import struct
import binascii
import context


class NodeGenerator:
    def __init__(self, ctx, old_node_path, new_node_path, new_node_address):
        self.ctx = ctx
        self.old_node_path = old_node_path
        self.new_node_path = new_node_path
        self.new_node_address = new_node_address
        self.old_node_address = None

        self.node_name = "none"
        self.read_conf_json()

        os.makedirs(os.path.join(self.new_node_path, "io"), exist_ok=True)

        self.old_storage_con = self.old_storage_cur = None
        self.new_storage_con = self.new_storage_cur = None
        if self.ctx.in_memory:
            self.db_connect(False)

    def generate(self):
        print("Generating node #"+str(len(self.ctx.nodes)+1)+": " + self.node_name)
        self.generate_conf_json()
        self.generate_tables()

        self.new_storage_cur.execute(
            "insert into features ('feature_name', 'feature_length', 'feature_value') "
            "values ('EQUIVALENTS_REGISTRY_ADDRESS', '3', 'eth');"
        )

    def read_conf_json(self):
        with open(os.path.join(self.old_node_path, "conf.json")) as conf_file:
            data = json.load(conf_file)
            node = data["node"]
            self.node_name = node.get("uuid", self.node_name)
            network = data["network"]
            self.old_node_address = network.get("interface", "127.0.0.1") + ":" + str(network.get("port", 2033))
            if self.new_node_address is None:
                self.new_node_address = self.old_node_address

    def generate_conf_json(self):
        data = dict()
        data['addresses'] = []
        data['addresses'].append({
            'type': 'ipv4',
            'address': self.new_node_address
        })
        data['observers'] = []
        for observer in self.ctx.observers:
            data['observers'].append({
                'type': 'ipv4',
                'address': observer
            })
        data['equivalents_registry_address'] = "eth"
        with open(os.path.join(self.new_node_path, "conf.json"), 'w') as conf_file:
            json.dump(data, conf_file, sort_keys=True, indent=4, ensure_ascii=False)

    def generate_tables(self):
        self.new_storage_cur.execute(
            "CREATE TABLE audit("
                "number INTEGER NOT NULL, "
                "trust_line_id INTEGER NOT NULL, "
                "our_key_hash BLOB NOT NULL, "
                "our_signature BLOB NOT NULL, "
                "contractor_key_hash BLOB DEFAULT NULL, "
                "contractor_signature BLOB DEFAULT NULL, "
                "own_keys_set_hash BLOB NOT NULL, "
                "contractor_keys_set_hash BLOB NOT NULL, "
                "balance BLOB NOT NULL, "
                "outgoing_amount BLOB NOT NULL, "
                "incoming_amount BLOB NOT NULL, "
                "FOREIGN KEY(trust_line_id) REFERENCES trust_lines(id) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE contractor_keys ("
                "hash BLOB PRIMARY KEY, "
                "trust_line_id INTEGER NOT NULL, "
                "keys_set_sequence_number INTEGER NOT NULL, "
                "public_key BLOB NOT NULL, "
                "number INTEGER NOT NULL, "
                "is_valid INTEGER NOT NULL DEFAULT 1, "
                "FOREIGN KEY(trust_line_id) REFERENCES trust_lines(id) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE contractors("
                "id INTEGER PRIMARY KEY, "
                "id_on_contractor_side INTEGER, "
                "crypto_key BLOB NOT NULL, "
                "is_confirmed INTEGER NOT NULL DEFAULT 0"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE contractors_addresses("
                "type INTEGER NOT NULL, "
                "contractor_id INTEGER NOT NULL, "
                "address_size INTEGER NOT NULL, "
                "address BLOB NOT NULL, "
                "FOREIGN KEY(contractor_id) REFERENCES contractors(id) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE features("
                "feature_name STRING NOT NULL, "
                "feature_length INTEGER NOT NULL, "
                "feature_value STRING NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE history("
                "operation_uuid BLOB NOT NULL, "
                "operation_timestamp INTEGER NOT NULL, "
                "record_type INTEGER NOT NULL, "
                "record_body BLOB NOT NULL, "
                "record_body_bytes_count INT NOT NULL, "
                "equivalent INTEGER NOT NULL, "
                "command_uuid BLOB"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE history_additional("
                "operation_uuid BLOB NOT NULL, "
                "operation_timestamp INTEGER NOT NULL, "
                "record_type INTEGER NOT NULL, "
                "record_body BLOB NOT NULL, "
                "record_body_bytes_count INT NOT NULL, "
                "equivalent INTEGER NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE incoming_receipt ("
                "trust_line_id INTEGER NOT NULL, "
                "audit_number INTEGER NOT NULL, "
                "transaction_uuid BLOB NOT NULL, "
                "contractor_public_key_hash BLOB NOT NULL, "
                "amount BLOB NOT NULL, "
                "contractor_signature BLOB NOT NULL, "
                "FOREIGN KEY(trust_line_id) REFERENCES trust_lines(id) ON DELETE CASCADE ON UPDATE CASCADE, "
                "FOREIGN KEY(contractor_public_key_hash) REFERENCES contractor_keys(hash) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE outgoing_receipt ("
                "trust_line_id INTEGER NOT NULL, "
                "audit_number INTEGER NOT NULL, "
                "transaction_uuid BLOB NOT NULL, "
                "own_public_key_hash BLOB NOT NULL, "
                "amount BLOB NOT NULL, "
                "FOREIGN KEY(trust_line_id) REFERENCES trust_lines(id) ON DELETE CASCADE ON UPDATE CASCADE, "
                "FOREIGN KEY(own_public_key_hash) REFERENCES own_keys(hash) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE own_keys ("
                "hash BLOB PRIMARY KEY, "
                "trust_line_id INTEGER NOT NULL, "
                "keys_set_sequence_number INTEGER NOT NULL, "
                "public_key BLOB NOT NULL, "
                "private_key BLOB NOT NULL, "
                "number INTEGER NOT NULL, "
                "is_valid INTEGER NOT NULL DEFAULT 1, "
                "FOREIGN KEY(trust_line_id) REFERENCES trust_lines(id) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE payment_keys ("
                "transaction_uuid BLOB NOT NULL, "
                "public_key BLOB NOT NULL, "
                "private_key BLOB NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE payment_participants_votes ("
                "transaction_uuid BLOB NOT NULL, "
                "contractor BLOB NOT NULL, "
                "payment_node_id INTEGER NOT NULL, "
                "public_key BLOB NOT NULL, "
                "signature BLOB NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE payment_transactions ("
                "uuid BLOB NOT NULL, "
                "maximal_claiming_block_number BLOB NOT NULL, "
                "observing_state INTEGER NOT NULL, "
                "recording_time INTEGER NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE transactions ("
                "transaction_uuid BLOB NOT NULL, "
                "transaction_body BLOB NOT NULL, "
                "transaction_bytes_count INT NOT NULL"
            ")"
        )
        self.new_storage_cur.execute(
            "CREATE TABLE trust_lines("
                "id INTEGER PRIMARY KEY, "
                "state INTEGER NOT NULL, "
                "contractor_id INTEGER NOT NULL, "
                "equivalent INTEGER NOT NULL, "
                "is_contractor_gateway INTEGER NOT NULL DEFAULT 0, "
                "FOREIGN KEY(contractor_id) REFERENCES contractors(id) ON DELETE CASCADE ON UPDATE CASCADE"
            ")"
        )

    def db_connect(self, verbose=True):
        if verbose:
            print("Connecting to db of node: " + self.node_name)
        self.old_storage_con = sqlite3.connect(os.path.join(self.old_node_path, "io", "storageDB"))
        self.old_storage_cur = self.old_storage_con.cursor()
        self.new_storage_con = sqlite3.connect(os.path.join(self.new_node_path, "io", "storageDB"))
        self.new_storage_cur = self.new_storage_con.cursor()

    def db_disconnect(self, verbose=True):
        if verbose:
            print("Disconnecting from db of node: " + self.node_name)
        self.old_storage_cur.close()
        self.new_storage_con.commit()
        self.new_storage_cur.close()
        self.old_storage_con = self.old_storage_cur = None
        self.new_storage_con = self.new_storage_cur = None

    @staticmethod
    def read_uuid(blob):
        uuid = str(binascii.hexlify(blob))
        return \
            uuid[2:10] + '-' + \
            uuid[10:14] + '-' + \
            uuid[14:18] + '-' + \
            uuid[18:22] + '-' + \
            uuid[22:34]

    @staticmethod
    def serialize_ipv4_with_port(address):
        ip_and_port = address.split(':')
        ip_and_port[0] = ip_and_port[0].split('.')
        address = bytearray(b'\x0c')
        for c in ip_and_port[0]:
            address.append(int(c))
        return address + struct.pack("H", int(ip_and_port[1]))

    @staticmethod
    def bytes_to_str(arr):
        val = ""
        for byte in arr:
            val += '{:02x}'.format(byte) + ' '
        return val
