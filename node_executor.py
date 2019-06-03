import os, sys
import json
import subprocess
import tempfile
import time
from random import randint

import node_context

from node_generator import NodeGenerator


class NodeExecutor(NodeGenerator):
    def __init__(self, ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, node_name, old_node_path, new_node_path, None, False)
        self.old_client_path = old_client_path
        self.new_client_path = new_client_path
        self.old_uuid_2_address_path = old_uuid_2_address_path
        self.old_result_fifo_path = os.path.join(self.old_node_path, "fifo", "results.fifo")
        self.new_result_fifo_path = os.path.join(self.new_node_path, "fifo", "results.fifo")
        self.old_commands_fifo_path = os.path.join(self.old_node_path, "fifo", "commands.fifo")
        self.new_commands_fifo_path = os.path.join(self.new_node_path, "fifo", "commands.fifo")
        self.db_disconnect(False)

        self.result_fifo_handler = None
        self.command_result = None

        self.update_conf_json()
        self.read_new_conf_json()
        self.node_idx = len(self.ctx.nodes)

    def read_new_conf_json(self):
        with open(os.path.join(self.new_node_path, "conf.json")) as conf_file:
            data = json.load(conf_file)
            addresses = data["addresses"]
            self.new_node_address = addresses[0].get("address", self.new_node_address)

    def update_conf_json(self):
        try:
            with open(os.path.join(self.old_node_path, "conf.json")) as conf_file:
                data = json.load(conf_file)
                uuid2address = data["uuid2address"]
                uuid2address["host"] = "127.0.0.1"
                uuid2address["port"] = 1500
                with open(os.path.join(self.old_node_path, "conf.json"), 'w') as conf_file_out:
                    json.dump(data, conf_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        except:
            data = {
                "gateway": [],
                "network": {
                    "interface": "127.0.0.2",
                    "port": 30000 + randint(0, 30000-1)
                },
                "node": {
                    "uuid": self.node_name
                },
                "uuid2address": {
                    "host": "127.0.0.1",
                    "port": 1500
                }
            }
            with open(os.path.join(self.old_node_path, "conf.json"), 'w') as conf_file_out:
                json.dump(data, conf_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    def open_node_result_fifo(self, result_fifo_path, verbose=True):
        if verbose:
            print("Opening result FIFO for node " + self.node_name)
        while True:
            try:
                #self.result_fifo_handler = os.fdopen(os.open(result_fifo_path, os.O_RDONLY | os.O_NONBLOCK), 'rb')
                self.result_fifo_handler = os.fdopen(os.open(result_fifo_path, os.O_NONBLOCK), 'rb')
                break
            except:
                continue
        while self.result_fifo_handler is not None:
            data = None
            try:
                data = self.result_fifo_handler.read()
            except:
                None
            if data is None or len(data) == 0:
                continue
            self.command_result = data

    def run_node(self, node_path, client_path, verbose=True):
        if verbose:
            print("Starting node: " + self.node_name)
        with tempfile.TemporaryFile() as client_f:
            client_proc = None
            if self.ctx.verbose:
                client_proc = subprocess.Popen(
                    client_path,
                    cwd=node_path
                )
                return client_proc
            else:
                client_proc = subprocess.Popen(
                    client_path,
                    cwd=node_path,
                    stdout=client_f, stderr=client_f
                )
                return client_proc

    def run_command(self, fifo, line):
        line = line.replace("\\t", '\t').replace("\\n", "\n")
        line = line.encode()

        fifo_write = open(fifo, 'wb')
        fifo_write.write(line)
        fifo_write.flush()
        fifo_write.close()

        try_count = 0
        max_count = 10 * 60
        while self.command_result is None and try_count <= max_count:
            time.sleep(0.1)
            try_count += 1
        if try_count > max_count:
            assert False, "No response from node " + self.node_name
        result = self.command_result
        self.command_result = None
        return result

    def clean(self, also_clients=True):
        #if also_clients:
        #    with tempfile.TemporaryFile() as client_f:
        #        client_proc = subprocess.Popen(['killall', '-q', self.new_client_path], stdout=client_f, stderr=client_f)
        #        client_proc = subprocess.Popen(['killall', '-q', self.old_client_path], stdout=client_f, stderr=client_f)
        self.result_fifo_handler.close()
        self.result_fifo_handler = None
