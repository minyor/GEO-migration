import getopt
import json
import os
import shutil
import sys
import time
import csv

from node import context
from node.migrator import NodeMigrator

from node.channel import NodeChannel
from settings import migration_conf


class Main(context.Context):
    def __init__(self):
        super().__init__()
        try:
            opts, args = getopt.getopt(sys.argv[1:], "ha:o:vm", ["help"])
        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            sys.exit(2)
        for o, a in opts:
            if o == "-v":
                self.verbose = True
            elif o == "-m":
                self.in_memory = True
            elif o in ("-h", "--help"):
                self.usage()
                sys.exit()
            elif o in ("-a", "--address"):
                self.address = a
            elif o in ("-o", "--observers"):
                self.observers = a
            else:
                assert False, "unhandled option"
        self.observers = self.observers.split(',')

        self.mod_network_client_path = migration_conf.get("mod_network_client_path")
        self.unknown_address = migration_conf.get("unknown_address")

    def migrate(self):
        shutil.rmtree(self.new_infrastructure_path, ignore_errors=True)

        # Reading GNS addresses from file
        self.gns_addresses = dict()
        with open('users_addresses.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                self.gns_addresses[row[0]] = row[1]

        # Reading new equivalents from file
        self.new_equivalents = dict()
        with open('new_equivalents.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                self.new_equivalents[row[0]] = row[1]
                self.new_equivalents[int(row[0])] = int(row[1])

        print()
        new_node_address = self.address
        nodes = os.listdir(self.old_infrastructure_path)
        for path in nodes:
            old_node_path = os.path.join(self.old_infrastructure_path, path)
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if os.path.isdir(new_node_path):
                continue
            node_migrator = NodeMigrator(self, path, old_node_path,
                                         new_node_path, new_node_address, self.mod_network_client_path)
            if not self.in_memory:
                node_migrator.db_connect(False)
            node_migrator.generate()
            node_migrator.retrieve_old_data()
            if not self.in_memory:
                node_migrator.db_disconnect(False)
            self.nodes[node_migrator.node_name] = node_migrator
            new_node_address = self.increment_node_address(new_node_address)

        channels = NodeChannel.construct_channels(self.nodes)
        self.channels = channels

        print()
        for channel in channels.values():
            channel.generate_channels()
        for channel in channels.values():
            channel.generate_trust_lines()

        print()
        for node_migrator in self.nodes.values():
            node_migrator.retrieve_own_keys()
            print()

        self.save()

        #exit(0)
        self.resume()

    def resume(self):
        for channel in self.channels.values():
            channel.generate_contractor_keys()
            print()

        for node_migrator in self.nodes.values():
            node_migrator.hash_audits()
            print()

        for channel in self.channels.values():
            channel.generate_audit_crypto()
            print()

        for node_migrator in self.nodes.values():
            node_migrator.migrate()

        if self.migration_error_json is not None:
            print("THERE ARE ERRORS!!")
            print("Saving 'migration_error.json' file...")
            print()
            migration_error_file_path = os.path.join(self.new_infrastructure_path, "migration_error.json")
            with open(migration_error_file_path, 'w') as cpm_file_out:
                json.dump(self.migration_error_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    @staticmethod
    def increment_node_address(node_address):
        if node_address is None:
            return node_address
        port_idx = node_address.rfind(':') + 1
        port = node_address[port_idx:]
        return node_address[:port_idx] + str(int(port) + 1)

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython migrate.py [-v] [-m] [-a address] [-o observers]")
        print("Example:")
        print("\tpython migrate.py -o 127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002")
        print("\tNote: -v is verbose output")
        print("\tNote: -m is 'in_memory' mode (Faster but high RAM usage!!)")


if __name__ == "__main__":
    start_time = time.time()
    main = Main.load()
    if main is None:
        Main().migrate()
    else:
        print("Migration is Resumed!!")
        main.resume()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
