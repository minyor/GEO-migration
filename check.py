import getopt
import os
import sys
import time

from node.checker import NodeChecker

from node import context
from settings import migration_conf


class Main(context.Context):
    def __init__(self):
        super().__init__()
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hvm", ["help"])
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

        self.old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        self.new_infrastructure_path = migration_conf.get("new_infrastructure_path")

    def check(self):
        new_node_address = self.address
        nodes = os.listdir(self.old_infrastructure_path)

        print()
        print("Loading nodes...")
        for path in nodes:
            old_node_path = os.path.join(self.old_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            node_checker = NodeChecker(self, path, old_node_path)
            node_checker.db_connect(False)
            node_checker.retrieve_old_data()
            node_checker.db_disconnect(False)
            self.nodes[node_checker.node_name] = node_checker

        print()
        print("Checking nodes...")
        for node_checker in self.nodes.values():
            node_checker.check()

        print()

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython check.py [-v]")
        print("Example:")
        print("\tpython check.py")
        print("\tNote: -v is verbose output")


if __name__ == "__main__":
    start_time = time.time()
    Main().check()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
