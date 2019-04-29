import os
import sqlite3
import json
import struct

from node_generator import NodeGenerator


class NodeMigrator(NodeGenerator):
    def __init__(self, ctx, old_node_path, new_node_path, new_node_address):
        super().__init__(ctx, old_node_path, new_node_path, new_node_address)

        self.node_name = "none"
        self.read_conf_json()

        self.channel_idx = 0

    def read_conf_json(self):
        with open(os.path.join(self.old_node_path, "conf.json")) as conf_file:
            data = json.load(conf_file)
            node = data["node"]
            self.node_name = node.get("uuid", self.node_name)
            if self.new_node_address is None:
                network = data["network"]
                self.new_node_address = network.get("interface", "127.0.0.1") + ":" + str(network.get("port", 2033))

    def add_channel(self, pk, sk, ok, id_on_contractor_side, contractor_address):
        self.new_storage_cur.execute(
            "insert into contractors ('id', 'id_on_contractor_side', 'crypto_key', 'is_confirmed') "
            "values (?, ?, "
                "?, "
                "'1'"
            ");",
            (self.channel_idx, id_on_contractor_side, sqlite3.Binary(pk + sk + ok))
        )

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

    def migrate(self):
        self.new_storage_cur.execute(
            "insert into features ('feature_name', 'feature_length', 'feature_value') "
            "values ('EQUIVALENTS_REGISTRY_ADDRESS', '3', 'eth');"
        )

        self.new_storage_con.commit()
        self.new_storage_cur.close()
