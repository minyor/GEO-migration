import os, sys
import getopt
import shutil
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
        self.observers = self.observers.split(',')

    def compare(self):
        old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        old_network_client_path = migration_conf.get("old_network_client_path")
        new_network_client_path = migration_conf.get("new_network_client_path")

        print()
        nodes = os.listdir(old_infrastructure_path)
        for path in nodes:
            old_node_path = os.path.join(old_infrastructure_path, path)
            new_node_path = os.path.join(new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if not os.path.isdir(new_node_path):
                assert False, "Migrated node " + path + " is not found"
            node_comparator = NodeComparator(
                self, old_node_path, new_node_path,
                old_network_client_path, new_network_client_path)
            self.nodes[node_comparator.node_name] = node_comparator

        for node_comparator in self.nodes.values():
            node_comparator.compare()

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
