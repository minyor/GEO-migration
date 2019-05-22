import os, sys
import json
import threading
import subprocess
import tempfile
import time
import context

from node_generator import NodeGenerator


class NodeComparator(NodeGenerator):
    def __init__(self, ctx, old_node_path, new_node_path, old_client_path, new_client_path):
        super().__init__(ctx, old_node_path, new_node_path, None)
        self.old_client_path = old_client_path
        self.new_client_path = new_client_path
        self.old_result_fifo_path = os.path.join(self.old_node_path, "fifo", "results.fifo")
        self.new_result_fifo_path = os.path.join(self.new_node_path, "fifo", "results.fifo")
        self.old_commands_fifo_path = os.path.join(self.old_node_path, "fifo", "commands.fifo")
        self.new_commands_fifo_path = os.path.join(self.new_node_path, "fifo", "commands.fifo")
        self.db_disconnect()

        self.result_fifo_handler = None
        self.command_result = None

    def compare(self):
        print()

        new_node_result_fifo_thread = threading.Thread(target=self.open_new_node_result_fifo, args=())
        new_node_result_fifo_thread.start()

        new_node_run_thread = threading.Thread(target=self.run_new_node, args=())
        new_node_run_thread.start()

        time.sleep(0.1)
        print("Requesting equivalents...")
        result_eq = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n').decode("utf-8")
        result_eq = result_eq.split('\t')
        eq_count = int(result_eq[2])
        print("Found " + str(eq_count) + " equivalents")
        for e in range(eq_count):
            eq = int(result_eq[e + 3])
            print("\tRequesting trust lines for equivalent " + str(eq) + "...")
            result_tl = self.run_command(
                self.new_commands_fifo_path,
                '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100\t1\n').decode("utf-8")
            #print('Result: "{0}"'.format(result_tl))
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
                print("\t\tTrust line " + str(t+1) + ":" +
                      " incoming_trust_amount="+str(incoming_trust_amount) + ";" +
                      " outgoing_trust_amount=" + str(outgoing_trust_amount) + ";" +
                      " balance=" + str(balance))

        self.clean()

    def open_new_node_result_fifo(self):
        print("Opening result FIFO for new node " + self.node_name)
        self.result_fifo_handler = os.fdopen(os.open(self.new_result_fifo_path, os.O_RDONLY | os.O_NONBLOCK), 'rb')
        while self.result_fifo_handler is not None:
            try:
                data = self.result_fifo_handler.read()
            except:
                None
            if data is None or len(data) == 0:
                continue
            self.command_result = data

    def run_new_node(self):
        print("Starting new node: " + self.node_name)
        with tempfile.TemporaryFile() as client_f:
            client_proc = None
            if self.ctx.verbose:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + self.new_node_path + ";" + self.new_client_path + ""]
                )
            else:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + self.new_node_path + ";" + self.new_client_path + ""],
                    stdout=client_f, stderr=client_f
                )
            client_proc.wait()

    def run_command(self, fifo, line):
        line = line.replace("\\t", '\t').replace("\\n", "\n")
        line = line.encode()
        file_handler = os.fdopen(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK), 'wb')
        file_handler.write(line)
        file_handler.close()
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

    def clean(self):
        with tempfile.TemporaryFile() as client_f:
            subprocess.Popen(['killall', '-q', self.new_client_path], stdout=client_f, stderr=client_f)
            subprocess.Popen(['killall', '-q', self.old_client_path], stdout=client_f, stderr=client_f)
        self.result_fifo_handler.close()
        self.result_fifo_handler = None
