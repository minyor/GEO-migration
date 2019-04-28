import os, sys
import getopt

from settings import migration_conf
from node_migrator import NodeMigrator


class Context:
    address = None
    observers = "127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002"
    verbose = False
    nodes = dict()


class Main(Context):
    def __init__(self):
        try:
            opts, args = getopt.getopt(sys.argv[1:], "ha:o:v", ["help"])
        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            sys.exit(2)
        for o, a in opts:
            if o == "-v":
                verbose = True
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
        self.migrate()

    def migrate(self):
        old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        nodes = os.listdir(old_infrastructure_path)
        new_node_address = self.address;
        for path in nodes:
            old_node_path = os.path.join(old_infrastructure_path, path)
            new_node_path = os.path.join(new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if os.path.isdir(new_node_path):
                continue
            node_migrator = NodeMigrator(self, old_node_path, new_node_path, new_node_address)
            node_migrator.generate()
            self.nodes[node_migrator.new_node_address] = node_migrator
            new_node_address = self.increment_node_address(new_node_address)

        for node_address, node_migrator in self.nodes.items():
            node_migrator.migrate()

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
        print("\tpython migrate.py [-v] [-a address] [-o observers]")
        print("Example:")
        print("\tpython migrate.py -o 127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002")


if __name__ == "__main__":
    Main()
