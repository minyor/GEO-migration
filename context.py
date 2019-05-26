import os
import subprocess
import tempfile


class Context:
    def __init__(self):
        # For all operations:
        self.verbose = False
        self.nodes = dict()
        self.nodes_by_address = dict()

        # Migration operation specific:
        self.address = None
        self.observers = "127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002"
        self.in_memory = False
        self.migration_error_json = None

        # Comparision operation specific:
        self.old_comparision_json = {}
        self.new_comparision_json = {}
        self.old_ignored_json = {}
        self.new_ignored_json = {}

    def run_uuid_2_address(self, node_path, client_path):
        print("Starting uuid_2_address...")
        with tempfile.TemporaryFile() as client_f:
            client_proc = None
            if self.verbose:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + node_path + ";" + client_path + ""]
                )
            else:
                client_proc = subprocess.Popen(
                    ["bash", "-c", "cd " + node_path + ";" + client_path + ""],
                    stdout=client_f, stderr=client_f
                )
            client_proc.wait()
            client_proc.kill()

    def append_migration_error(self, entry):
        if self.migration_error_json is None:
            self.migration_error_json = {}
        self.migration_error_json[str(len(self.migration_error_json)+1)] = entry

    @staticmethod
    def terminate():
        with tempfile.TemporaryFile() as client_f:
            client_proc = subprocess.Popen(['kill', '-9', str(os.getpid())], stdout=client_f, stderr=client_f)
            client_proc.kill()


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

        self.number = None
        self.our_key_hash = None
        self.our_signature = None
        self.own_keys_set_hash = None
        self.contractor_keys_set_hash = None


class OwnKey:
    def __init__(self):
        self.hash = None
        self.trust_line_id = None
        self.keys_set_sequence_number = None
        self.public_key = None
        self.private_key = None
        self.number = None
        self.is_valid = None


class History:
    def __init__(self):
        self.operation_uuid = None
        self.operation_timestamp = None
        self.record_type = None
        self.record_body = None
        self.record_body_bytes_count = None
        self.equivalent = None
        self.command_uuid = None
