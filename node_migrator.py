import os
import sqlite3
import json


class NodeMigrator():
    def __init__(self, ctx, old_node_path, new_node_path, new_node_address):
        self.ctx = ctx
        self.old_node_path = old_node_path
        self.new_node_path = new_node_path
        self.new_node_address = new_node_address

        os.makedirs(os.path.join(self.new_node_path, "io"), exist_ok=True)

        self.old_storage_con = sqlite3.connect(os.path.join(self.old_node_path, "io", "storageDB"))
        self.old_storage_cur = self.old_storage_con.cursor()
        self.new_storage_con = sqlite3.connect(os.path.join(self.new_node_path, "io", "storageDB"))
        self.new_storage_cur = self.new_storage_con.cursor()

        self.node_name = "none"
        self.read_conf_json()

    def read_conf_json(self):
        with open(os.path.join(self.old_node_path, "conf.json")) as conf_file:
            data = json.load(conf_file)
            node = data["node"]
            self.node_name = node.get("uuid", self.node_name)
            if self.new_node_address is None:
                network = data["network"]
                self.new_node_address = network.get("interface", "127.0.0.1") + ":" + str(network.get("port", 2033));

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

    def generate(self):
        print("Generating node: " + self.node_name)
        self.generate_conf_json()
        self.generate_tables()

    def migrate(self):
        self.new_storage_con.commit()
        self.new_storage_cur.close()
