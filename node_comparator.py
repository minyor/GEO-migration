import os, sys
import json
import threading
import subprocess
import tempfile
import time
from csv import excel

import context

from node_generator import NodeGenerator


class NodeComparator(NodeGenerator):
    def __init__(self, ctx, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, old_node_path, new_node_path, None)
        self.old_client_path = old_client_path
        self.new_client_path = new_client_path
        self.old_uuid_2_address_path = old_uuid_2_address_path
        self.old_result_fifo_path = os.path.join(self.old_node_path, "fifo", "results.fifo")
        self.new_result_fifo_path = os.path.join(self.new_node_path, "fifo", "results.fifo")
        self.old_commands_fifo_path = os.path.join(self.old_node_path, "fifo", "commands.fifo")
        self.new_commands_fifo_path = os.path.join(self.new_node_path, "fifo", "commands.fifo")
        self.db_disconnect()

        self.result_fifo_handler = None
        self.command_result = None

    def update_conf_json(self):
        with open(os.path.join(self.old_node_path, "conf.json")) as conf_file:
            data = json.load(conf_file)
            uuid2address = data["uuid2address"]
            uuid2address["host"] = "127.0.0.1"
            with open(os.path.join(self.old_node_path, "conf.json"), 'w') as conf_file_out:
                json.dump(data, conf_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    def compare(self):
        print()
        self.update_conf_json()

        self.retrieve_data_from_old_node()
        self.clean()

        self.retrieve_data_from_new_node()
        self.clean()

    def retrieve_data_from_old_node(self):
        print("Process old node...")
        commands_fifo_path = self.old_commands_fifo_path

        old_node_result_fifo_thread = threading.Thread(target=self.open_node_result_fifo, args=(self.old_result_fifo_path,))
        old_node_result_fifo_thread.start()

        old_node_run_thread = threading.Thread(target=self.run_node, args=(self.old_node_path, self.old_client_path))
        old_node_run_thread.start()

        self.ctx.old_comparision_json[self.node_name] = {}
        json_node = self.ctx.old_comparision_json[self.node_name]

        time.sleep(0.1)
        print("Requesting equivalents...")
        result_eq = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n').decode("utf-8")
        result_eq = result_eq.split('\t')
        eq_count = int(result_eq[2])
        print("Found " + str(eq_count) + " equivalents")
        for e in range(eq_count):
            eq = int(result_eq[e + 3])
            self.retrieve_tl_from_old_node(commands_fifo_path, json_node, eq)
            self.retrieve_h_tl_from_old_node(commands_fifo_path, json_node, eq)
            self.retrieve_h_p_from_old_node(commands_fifo_path, json_node, eq)

    def retrieve_tl_from_old_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t'+str(eq)+'\n').decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " trust lines")
        for t in range(tl_count):
            trust_line_array_shift = t * 4 + 3
            contractor_id = result_tl[trust_line_array_shift + 0]
            incoming_trust_amount = float(result_tl[trust_line_array_shift + 1])
            outgoing_trust_amount = float(result_tl[trust_line_array_shift + 2])
            balance = float(result_tl[trust_line_array_shift + 3])
            print("\t\tTrust line " + str(t+1) + ":" +
                  " id="+str(contractor_id) + ";" +
                  " incoming=" + str(incoming_trust_amount) + ";" +
                  " outgoing=" + str(outgoing_trust_amount) + ";" +
                  " balance=" + str(balance))
            json_node["tl_"+str(eq)+","+contractor_id] = {
                "equivalent": eq,
                "contractor_id": contractor_id,
                "incoming_trust_amount": incoming_trust_amount,
                "outgoing_trust_amount": outgoing_trust_amount,
                "balance": balance
            }

    def retrieve_h_tl_from_old_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting history trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/trust-lines\t0\t100000\tnull\tnull\t'+str(eq)+'\n').\
            decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " history trust lines")
        for t in range(tl_count):
            trust_line_array_shift = t * 5 + 3
            transaction_uuid = result_tl[trust_line_array_shift + 0]
            timestamp = int(result_tl[trust_line_array_shift + 1])
            addresses = result_tl[trust_line_array_shift + 2]
            operation_type = result_tl[trust_line_array_shift + 3]
            summ = float(result_tl[trust_line_array_shift + 4])
            node = self.ctx.nodes.get(addresses)
            if node is None:
                print("\t\tHistory tl " + str(t+1) + " Skip unknown node " + addresses)
                continue
            print("\t\tHistory tl " + str(t+1) + ":" +
                  " id="+str(transaction_uuid) + ";" +
                  " timestamp=" + str(timestamp) + ";" +
                  " type=" + str(operation_type) + ";" +
                  " summ=" + str(summ))
            json_node["h_tl_"+str(eq)+","+transaction_uuid] = {
                "equivalent": eq,
                "transaction_uuid": transaction_uuid,
                "timestamp": timestamp,
                "address": addresses,
                "operation_type": operation_type,
                "sum": summ
            }

    def retrieve_h_p_from_old_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting history payments for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/payments\t0\t100000\tnull\tnull\tnull\tnull\tnull\t' +
            str(eq) + '\n'). \
            decode("utf-8")
        #print('Result: "{0}"'.format(result_tl))
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " history payments")
        for t in range(tl_count):
            trust_line_array_shift = t * 6 + 3
            transaction_uuid = result_tl[trust_line_array_shift + 0]
            timestamp = int(result_tl[trust_line_array_shift + 1])
            addresses = result_tl[trust_line_array_shift + 2]
            payment_type = result_tl[trust_line_array_shift + 3]
            summ = float(result_tl[trust_line_array_shift + 4])
            balance = float(result_tl[trust_line_array_shift + 5])
            node = self.ctx.nodes.get(addresses)
            if node is None:
                print("\t\tHistory p " + str(t+1) + " Skip unknown node " + addresses)
                continue
            print("\t\tHistory p " + str(t+1) + ":" +
                  " id="+str(transaction_uuid) + ";" +
                  " timestamp=" + str(timestamp) + ";" +
                  " type=" + str(payment_type) + ";" +
                  " summ=" + str(summ) + ";" +
                  " balance=" + str(balance))
            json_node["h_p_"+str(eq)+","+transaction_uuid] = {
                "equivalent": eq,
                "transaction_uuid": transaction_uuid,
                "timestamp": timestamp,
                "address": addresses,
                "payment_type": payment_type,
                "sum": summ,
                "balance": balance
            }

    def retrieve_data_from_new_node(self):
        print("Process new node...")
        commands_fifo_path = self.new_commands_fifo_path

        new_node_result_fifo_thread = threading.Thread(target=self.open_node_result_fifo, args=(self.new_result_fifo_path,))
        new_node_result_fifo_thread.start()

        new_node_run_thread = threading.Thread(target=self.run_node, args=(self.new_node_path, self.new_client_path))
        new_node_run_thread.start()

        self.ctx.new_comparision_json[self.node_name] = {}
        json_node = self.ctx.new_comparision_json[self.node_name]

        time.sleep(0.1)
        print("Requesting equivalents...")
        result_eq = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n').decode("utf-8")
        result_eq = result_eq.split('\t')
        eq_count = int(result_eq[2])
        print("Found " + str(eq_count) + " equivalents")
        for e in range(eq_count):
            eq = int(result_eq[e + 3])
            self.retrieve_tl_from_new_node(commands_fifo_path, json_node, eq)
            self.retrieve_h_tl_from_new_node(commands_fifo_path, json_node, eq)
            self.retrieve_h_p_from_new_node(commands_fifo_path, json_node, eq)

    def retrieve_tl_from_new_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t'+str(eq)+'\n').decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " trust lines")
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
            address = addresses[addresses.find(' ')+1:]
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address " + address
            print("\t\tTrust line " + str(t+1) + ":" +
                  " id="+str(node.node_name) + ";" +
                  " incoming=" + str(incoming_trust_amount) + ";" +
                  " outgoing=" + str(outgoing_trust_amount) + ";" +
                  " balance=" + str(balance))
            json_node["tl_"+str(eq)+","+node.node_name] = {
                "equivalent": eq,
                "contractor_id": node.node_name,
                "incoming_trust_amount": incoming_trust_amount,
                "outgoing_trust_amount": outgoing_trust_amount,
                "balance": balance
            }

    def retrieve_h_tl_from_new_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting history trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/trust-lines\t0\t100000\tnull\tnull\t'+str(eq)+'\n').\
            decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " history trust lines")
        for t in range(tl_count):
            trust_line_array_shift = t * 5 + 3
            transaction_uuid = result_tl[trust_line_array_shift + 0]
            timestamp = int(result_tl[trust_line_array_shift + 1])
            addresses = result_tl[trust_line_array_shift + 2]
            operation_type = result_tl[trust_line_array_shift + 3]
            summ = float(result_tl[trust_line_array_shift + 4])
            address = addresses[addresses.find(' ')+1:]
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address " + address
            print("\t\tHistory tl " + str(t+1) + ":" +
                  " id="+str(transaction_uuid) + ";" +
                  " timestamp=" + str(timestamp) + ";" +
                  " type=" + str(operation_type) + ";" +
                  " summ=" + str(summ))
            json_node["h_tl_"+str(eq)+","+transaction_uuid] = {
                "equivalent": eq,
                "transaction_uuid": transaction_uuid,
                "timestamp": timestamp,
                "address": node.node_name,
                "operation_type": operation_type,
                "sum": summ
            }

    def retrieve_h_p_from_new_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting history payments for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/payments\t0\t100000\tnull\tnull\tnull\tnull\tnull\t' +
            str(eq) + '\n'). \
            decode("utf-8")
        #print('Result: "{0}"'.format(result_tl))
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        print("\tFound " + str(tl_count) + " history payments")
        for t in range(tl_count):
            trust_line_array_shift = t * 7 + 3
            transaction_uuid = result_tl[trust_line_array_shift + 0]
            timestamp = int(result_tl[trust_line_array_shift + 1])
            addresses = result_tl[trust_line_array_shift + 2]
            payment_type = result_tl[trust_line_array_shift + 3]
            summ = float(result_tl[trust_line_array_shift + 4])
            balance = float(result_tl[trust_line_array_shift + 5])
            address = addresses[addresses.find(' ')+1:]
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address " + address
            print("\t\tHistory p " + str(t+1) + ":" +
                  " id="+str(transaction_uuid) + ";" +
                  " timestamp=" + str(timestamp) + ";" +
                  " type=" + str(payment_type) + ";" +
                  " summ=" + str(summ) + ";" +
                  " balance=" + str(balance))
            json_node["h_p_"+str(eq)+","+transaction_uuid] = {
                "equivalent": eq,
                "transaction_uuid": transaction_uuid,
                "timestamp": timestamp,
                "address": node.node_name,
                "payment_type": payment_type,
                "sum": summ,
                "balance": balance
            }

    def open_node_result_fifo(self, result_fifo_path):
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

    def run_node(self, node_path, client_path):
        print("Starting node: " + self.node_name)
        with tempfile.TemporaryFile() as client_f:
            client_proc = None
            if self.ctx.verbose:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + node_path + ";" + client_path + ""]
                )
            else:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + node_path + ";" + client_path + ""],
                    stdout=client_f, stderr=client_f
                )
            client_proc.wait()

    def run_command(self, fifo, line):
        line = line.replace("\\t", '\t').replace("\\n", "\n")
        line = line.encode()
        while True:
            try:
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
            except:
                print("Failed to run command, retrying...")
                time.sleep(0.1)
                continue

    def clean(self):
        with tempfile.TemporaryFile() as client_f:
            subprocess.Popen(['killall', '-q', self.new_client_path], stdout=client_f, stderr=client_f)
            subprocess.Popen(['killall', '-q', self.old_client_path], stdout=client_f, stderr=client_f)
        self.result_fifo_handler.close()
        self.result_fifo_handler = None
