import os, sys
import subprocess
import logging
import tempfile
import pickle
import json
from datetime import datetime

from settings import migration_conf


class Context:
    def __init__(self):
        # For all operations:
        self.verbose = False
        self.clean = False
        self.nodes_array = []
        self.nodes = dict()
        self.nodes_by_address = dict()
        self.gns_addresses = None
        self.new_equivalents = None

        # Checking operation specific:
        self.checking_tr_0_json = None
        self.checking_tr_gw_bal_json = None
        self.checking_tr_gw_non_bal_json = None
        self.checking_no_gns_address_json = None
        self.checking_stats_tl = dict()

        # Migration operation specific:
        self.address = None
        self.observers = "127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002"
        self.in_memory = False
        self.migration_error_json = None
        self.channels = None

        # Comparision operation specific:
        self.old_comparision_json = {}
        self.new_comparision_json = {}
        self.old_ignored_json = {}
        self.new_ignored_json = {}
        self.nodes_count_processed = 0
        self.nodes_count_max = sys.maxsize
        self.threads = None

        # Correlation operation specific:
        self.redis = None
        self.loop_period_in_sec = 10

        self.debug = migration_conf.get("debug")
        self.old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        self.new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        self.gns_address_separator = migration_conf.get("gns_address_separator")

        self.__init_logging()

    def get_tl_stat(self, eq):
        stat = self.checking_stats_tl.get(eq, None)
        if stat is None:
            stat = TrustLineStat()
            stat.eq = eq
            self.checking_stats_tl[eq] = stat
        return stat

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

    def append_migration_error(self, entry):
        if self.migration_error_json is None:
            self.migration_error_json = {}
        self.migration_error_json[str(len(self.migration_error_json)+1)] = entry

    @staticmethod
    def append_node_uuid(json_obj, uuid, entry=None):
        if json_obj is None:
            json_obj = {}
        json_obj[str(uuid)] = entry
        return json_obj

    @staticmethod
    def save_json(json_obj, filename):
        with open(filename, 'w') as cpm_file_out:
            json.dump(json_obj, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    def save(self):
        pass
        #binary_file = open('./pickled_migration.bin', mode='wb')
        #pickle.dump(self, binary_file)
        #binary_file.close()

    @staticmethod
    def load():
        if os.path.isfile('./pickled_migration.bin'):
            binary_file = open('./pickled_migration.bin', mode='rb')
            obl = pickle.load(binary_file)
            binary_file.close()
            return obl
        return None

    def eq_map(self, eq):
        if self.new_equivalents is not None:
            new_eq = self.new_equivalents.get(eq, None)
            if new_eq is not None:
                #print("eq " + str(eq) + " is swapped to " + str(new_eq))
                return new_eq
        return eq

    @staticmethod
    def terminate():
        with tempfile.TemporaryFile() as client_f:
            subprocess.Popen(['kill', '-9', str(os.getpid())], stdout=client_f, stderr=client_f)

    def __init_logging(self) -> None:
        self.file_handler = None
        self.errors_handler = None
        self.logger = None

        self.logger = logging.getLogger()
        stream_handler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s: %(message)s')
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)

        self.reinit_logging()

    def reinit_logging(self):
        if self.file_handler is not None:
            self.logger.removeHandler(self.file_handler)
            self.logger.removeHandler(self.errors_handler)
            self.file_handler.close()
            self.errors_handler.close()
            postfix = "_" + str(datetime.now())
        else:
            postfix = ""

        self.file_handler = logging.FileHandler('operations'+postfix+'.log')
        self.errors_handler = logging.FileHandler('errors'+postfix+'.log')
        self.errors_handler.setLevel(logging.ERROR)

        formatter = logging.Formatter('%(asctime)s: %(message)s')
        self.file_handler.setFormatter(formatter)
        self.errors_handler.setFormatter(formatter)

        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.errors_handler)

        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)


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


class TrustLineStat:
    def __init__(self):
        self.eq = 0
        self.count_all = 0
        self.count_0_bal = 0
        self.count_non_0_bal = 0


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


class CommunicatorMessage:
    def __init__(self):
        self.contractor_uuid = None
        self.transaction_uuid = None
        self.message_type = None
        self.recording_time = None
        self.equivalent = None
