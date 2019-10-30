import json
import os
import threading
import time

from node.executor import NodeExecutor


class NodeComparator(NodeExecutor):
    def __init__(self, ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path)

    def compare(self):
        compared_file_path = os.path.join(self.new_node_path, "compared.json")
        if os.path.isfile(compared_file_path):
            return

        print()
        print("Comparing node #"+str(self.node_idx+1)+": " + self.node_name)

        self.retrieve_data_from_old_node()
        self.retrieve_data_from_new_node()

        with open(compared_file_path, 'w') as cpm_file_out:
            json.dump({}, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        self.ctx.save_comparision_files()
        self.ctx.nodes_count_processed += 1

    def clear(self, node_handle):
        node_handle.terminate()
        self.clean()

    def retrieve_data_from_old_node(self):
        print("Process old node...")
        commands_fifo_path = self.old_commands_fifo_path

        old_node_result_fifo_thread =\
            threading.Thread(target=self.open_node_result_fifo, args=(self.old_result_fifo_path,))
        old_node_result_fifo_thread.start()

        node_handle = self.run_node(self.old_node_path, self.old_client_path, True, False)

        self.ctx.old_comparision_json[self.node_name] = {}
        json_node = self.ctx.old_comparision_json[self.node_name]

        self.ctx.old_ignored_json[self.node_name] = {}
        json_ignored_node = self.ctx.old_ignored_json[self.node_name]

        try:
            time.sleep(0.2)
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
                self.retrieve_h_tl_from_old_node(commands_fifo_path, json_node, json_ignored_node, eq)
                self.retrieve_h_p_from_old_node(commands_fifo_path, json_node, json_ignored_node, eq)
        except Exception as e:
            print(e)
            self.clear(node_handle)
            assert False, "Reassert "

        self.clear(node_handle)

    def retrieve_tl_from_old_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t'+str(eq)+'\n').\
            decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        eq = self.ctx.eq_map(eq)
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

    def retrieve_h_tl_from_old_node(self, commands_fifo_path, json_node, json_ignored_node, eq):
        print("\tRequesting history trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/trust-lines\t0\t100000\tnull\tnull\t'+str(eq)+'\n').\
            decode("utf-8")
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        eq = self.ctx.eq_map(eq)
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
                print("\t\tHistory tl " + str(t+1) + " Ignore unknown node " + addresses)
                json_ignored_node["h_tl_"+str(eq)+","+transaction_uuid] = {
                    "equivalent": eq,
                    "transaction_uuid": transaction_uuid,
                    "timestamp": timestamp,
                    "address": addresses,
                    "operation_type": operation_type,
                    "sum": summ
                }
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

    def retrieve_h_p_from_old_node(self, commands_fifo_path, json_node, json_ignored_node, eq):
        print("\tRequesting history payments for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:history/payments\t0\t100000\tnull\tnull\tnull\tnull\tnull\t' +
            str(eq) + '\n'). \
            decode("utf-8")
        #print('Result: "{0}"'.format(result_tl))
        result_tl = result_tl.split('\t')
        tl_count = int(result_tl[2])
        eq = self.ctx.eq_map(eq)
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
                print("\t\tHistory p " + str(t+1) + " Ignore unknown node " + addresses)
                json_ignored_node["h_p_"+str(eq)+","+transaction_uuid] = {
                    "equivalent": eq,
                    "transaction_uuid": transaction_uuid,
                    "timestamp": timestamp,
                    "address": addresses,
                    "payment_type": payment_type,
                    "sum": summ,
                    "balance": balance
                }
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

        new_node_result_fifo_thread =\
            threading.Thread(target=self.open_node_result_fifo,args=(self.new_result_fifo_path,))
        new_node_result_fifo_thread.start()

        node_handle = self.run_node(self.new_node_path, self.new_client_path, True, False)

        self.ctx.new_comparision_json[self.node_name] = {}
        json_node = self.ctx.new_comparision_json[self.node_name]

        try:
            time.sleep(0.2)
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
        except Exception as e:
            print(e)
            self.clear(node_handle)
            assert False, "Reassert "

        self.clear(node_handle)

    def retrieve_tl_from_new_node(self, commands_fifo_path, json_node, eq):
        print("\tRequesting trust lines for equivalent " + str(eq) + "...")
        result_tl = self.run_command(
            commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t'+str(eq)+'\n').\
            decode("utf-8")
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
