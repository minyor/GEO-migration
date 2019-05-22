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
        result = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n')
        print('Result: "{0}"'.format(result))

        result = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100\t1\n')
        print('Result: "{0}"'.format(result))

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
        print("Running cmd: " + line)
        line = line.encode()
        file_handler = os.fdopen(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK), 'wb')
        file_handler.write(line)
        file_handler.close()
        while self.command_result is None:
            time.sleep(0.1)
        result = self.command_result
        self.command_result = None
        return result

    def clean(self):
        with tempfile.TemporaryFile() as client_f:
            subprocess.Popen(['killall', '-q', self.new_client_path], stdout=client_f, stderr=client_f)
            subprocess.Popen(['killall', '-q', self.old_client_path], stdout=client_f, stderr=client_f)
        self.result_fifo_handler.close()
        self.result_fifo_handler = None
