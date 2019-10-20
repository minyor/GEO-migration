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
        self.name = None
        self.key = None
        self.ping = None
        self.lookup = None
        self.register_address = None
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hvn:k:p:l:r",
                    ["help", "verbose", "name=", "key=", "ping=", "lookup=", "register="])
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
            elif o in ("-n", "--name"):
                self.name = a
            elif o in ("-k", "--key"):
                self.key = a
            elif o in ("-p", "--ping"):
                self.ping = a
            elif o in ("-l", "--lookup"):
                self.lookup = a
            elif o in ("-r", "--register"):
                self.register_address = a
            else:
                assert False, "unhandled option"

        os.makedirs(self.new_infrastructure_path, exist_ok=True)

    def ctrl(self):
        nodes = os.listdir(self.new_infrastructure_path)

        print()
        print("Reconfigure nodes...")
        self.reconfigure_nodes(nodes)

        if self.register_address is not None:
            print()
            print("Registering nodes...")
            self.registering_nodes(nodes)

    def reconfigure_nodes(self, nodes):
        for path in nodes:
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(new_node_path):
                continue
            with open(os.path.join(new_node_path, "conf.json")) as conf_file:
                data = json.load(conf_file)
                try:
                    if self.name is not None:
                        data["providers"][0]["name"] = self.name
                    if self.key is not None:
                        data["providers"][0]["key"] = self.key
                    if self.ping is not None:
                        data["providers"][0]["addresses"][0]["address"] = self.ping
                    if self.lookup is not None:
                        data["providers"][0]["addresses"][1]["address"] = self.lookup
                except:
                    print("Error: Node " + path + " Cannot update 'conf.json'")
                with open(os.path.join(new_node_path, "conf.json"), 'w') as conf_file_out:
                    json.dump(data, conf_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    def registering_nodes(self, nodes):
        for path in nodes:
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(new_node_path):
                continue
            address_type = None
            address = None
            key = None
            participant_id = None
            try:
                with open(os.path.join(new_node_path, "conf.json")) as conf_file:
                    data = json.load(conf_file)
                    address_type = data["addresses"][0]["type"]
                    address = data["addresses"][0]["address"]
                    key = data["providers"][0]["key"]
                    participant_id = data["providers"][0]["participant_id"]
            except:
                print("Error: Node " + path + " Cannot read 'conf.json'")

            if address_type != "gns":
                continue

            username = address[:address.find(self.gns_address_separator)]
            print("Registering Node "+path+" participant_id+"+str(participant_id) +
                  " username="+username+" key="+key)
            self.register_node(path, participant_id, username, key)

    def register_node(self, path, participant_id, username, key):
        r = requests.post(
            "http://" + self.register_address + "/api/v1/users/",
            data={
                'id': participant_id,
                'username': username,
                'key': key
            }
        )
        if r.json()["status"] != "success":
            print("Error: Node " + path + " Cannot register in provider '" + self.register_address + "': " +
                  r.json()["msg"])

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython provider_ctrl.py [-v]")
        print("\t[-h --help] : Show this information")
        print("\t[-v --verbose] : Verbose output")
        print("\t[-n --name] : Specify provider name for all nodes")
        print("\t[-k --key] : Specify provider key for all nodes")
        print("\t[-p --ping] : Specify provider ping ip for all nodes")
        print("\t[-l --lookup] : Specify provider lookup ip for all nodes")
        print("\t[-r --register] : Try to register each node in specified provider")
        print("Example:")
        print("\tpython provider_ctrl.py --name geo.pay --key 1237 --ping 127.0.0.1:2010 --lookup 127.0.0.1:2011")
        print("\tpython provider_ctrl.py --register 127.0.0.1:2012")


if __name__ == "__main__":
    start_time = time.time()
    Main().ctrl()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
