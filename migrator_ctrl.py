import getopt
import json
import os
import requests
import sys
import time
import csv

from node.checker import NodeChecker

from node import context
from settings import migration_conf


class Main(context.Context):
    def __init__(self):
        super().__init__()
        self.rename = False
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hvr",
                    ["help", "verbose", "rename"])
        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            sys.exit(2)
        for o, a in opts:
            if o in ("-v", "--verbose"):
                self.verbose = True
            elif o in ("-h", "--help"):
                self.usage()
                sys.exit()
            elif o in ("-r", "--rename"):
                self.rename = True
            else:
                assert False, "unhandled option"

        os.makedirs(self.new_infrastructure_path, exist_ok=True)

    def ctrl(self):
        nodes = os.listdir(self.new_infrastructure_path)

        if self.rename:
            print()
            print("Renaming nodes...")
            self.rename_nodes(nodes)

    def rename_nodes(self, nodes):
        for path in nodes:
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(new_node_path):
                continue
            address_type = None
            address = None
            try:
                with open(os.path.join(new_node_path, "conf.json")) as conf_file:
                    data = json.load(conf_file)
                    address_type = data["addresses"][0]["type"]
                    address = data["addresses"][0]["address"]
            except:
                print("Error: Node " + path + " Cannot read 'conf.json'")

            if address_type != "gns":
                continue

            gns_address = address[:address.find(":")]
            print("Renaming Node "+path+" to "+gns_address)
            self.rename_node(new_node_path, gns_address)

    @staticmethod
    def rename_node(path, name):
        path_new = os.path.abspath(
            os.path.join(path, "..", name))
        os.rename(path, path_new)

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython migrator_ctrl.py [-v]")
        print("\t[-h --help] : Show this information")
        print("\t[-v --verbose] : Verbose output")
        print("\t[-r --rename] : Try to rename all node's folders from UUID to gns addresses if present")
        print("Example:")
        print("\tpython migrator_ctrl.py --rename")


if __name__ == "__main__":
    start_time = time.time()
    Main().ctrl()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
