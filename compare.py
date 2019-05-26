import os, sys
import json
import getopt
import shutil
import threading
import time
import context

from settings import migration_conf
from node_comparator import NodeComparator


class Main(context.Context):
    def __init__(self):
        super().__init__()
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hv", ["help"])
        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            sys.exit(2)
        for o, a in opts:
            if o == "-v":
                self.verbose = True
            elif o in ("-h", "--help"):
                self.usage()
                sys.exit()
            else:
                assert False, "unhandled option"
        self.in_memory = True

    def compare(self):
        old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        old_network_client_path = migration_conf.get("old_network_client_path")
        new_network_client_path = migration_conf.get("new_network_client_path")
        old_uuid_2_address_path = migration_conf.get("old_uuid_2_address_path")

        old_uuid_2_address_dir = old_uuid_2_address_path[:old_uuid_2_address_path.rindex('/')]
        print("old_uuid_2_address_dir="+old_uuid_2_address_dir)
        old_uuid_2_address_thread = threading.Thread(
            target=self.run_uuid_2_address,
            args=(old_uuid_2_address_dir, old_uuid_2_address_path))
        old_uuid_2_address_thread.start()

        print()
        nodes = os.listdir(old_infrastructure_path)
        for path in nodes:
            old_node_path = os.path.join(old_infrastructure_path, path)
            new_node_path = os.path.join(new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if not os.path.isdir(new_node_path):
                assert False, "Migrated node " + path + " is not found"
            print("Loading node #"+str(len(self.nodes)+1)+": " + path)
            node_comparator = NodeComparator(
                self, old_node_path, new_node_path,
                old_network_client_path, new_network_client_path,
                old_uuid_2_address_path)
            self.nodes[node_comparator.node_name] = node_comparator
            self.nodes_by_address[node_comparator.new_node_address] = node_comparator

        for node_comparator in self.nodes.values():
            node_comparator.compare()

        print()
        print("Saving 'compare.json' files...")
        old_cpm_file_path = os.path.join(old_infrastructure_path, "compare.json")
        new_cpm_file_path = os.path.join(new_infrastructure_path, "compare.json")
        with open(old_cpm_file_path, 'w') as cpm_file_out:
            json.dump(self.old_comparision_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        with open(new_cpm_file_path, 'w') as cpm_file_out:
            json.dump(self.new_comparision_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

        print("Saving 'ignored.json' files...")
        old_ign_file_path = os.path.join(old_infrastructure_path, "ignored.json")
        new_ign_file_path = os.path.join(new_infrastructure_path, "ignored.json")
        with open(old_ign_file_path, 'w') as cpm_file_out:
            json.dump(self.old_ignored_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        with open(new_ign_file_path, 'w') as cpm_file_out:
            json.dump(self.new_ignored_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

        print()
        print("Calculating migration outcome...")
        old_cpm_file = str(json.load(open(old_cpm_file_path)))
        new_cpm_file = str(json.load(open(new_cpm_file_path)))
        if old_cpm_file == new_cpm_file:
            print("Success: old and new comparision json files are equal!")
        else:
            print("Failure: old and new comparision json files differs!")
        print()

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython compare.py [-v]")
        print("Example:")
        print("\tpython compare.py")


if __name__ == "__main__":
    start_time = time.time()
    Main().compare()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    Main.terminate()
