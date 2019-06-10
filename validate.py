import getopt
import operator
import os
import sys
import time

from node.validator import NodeValidator

from node import context
from settings import migration_conf


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

    def validate(self):
        old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        old_network_client_path = migration_conf.get("old_network_client_path")
        new_network_client_path = migration_conf.get("new_network_client_path")
        old_uuid_2_address_path = migration_conf.get("old_uuid_2_address_path")

        print()
        nodes = os.listdir(old_infrastructure_path)
        for path in nodes:
            old_node_path = os.path.join(old_infrastructure_path, path)
            new_node_path = os.path.join(new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if not os.path.isdir(new_node_path):
                assert False, "Migrated node " + path + " is not found"
            node_validator = NodeValidator(
                self, path, old_node_path, new_node_path,
                old_network_client_path, new_network_client_path,
                old_uuid_2_address_path)
            self.nodes[node_validator.node_name] = node_validator
            self.nodes_by_address[node_validator.new_node_address] = node_validator

        nodes_succeeded_count = 0
        nodes_failed_count = 0
        sorted_nodes = sorted(self.nodes.values(), key=operator.attrgetter('trust_lines_count'), reverse=True)
        for node_validator in sorted_nodes:
            try:
                node_validator.validate()
            except Exception as e:
                print(e)
                print("Failed to validate node #" + str(node_validator.node_idx + 1) + ": " + node_validator.node_name)
            if node_validator.checked:
                nodes_succeeded_count += 1
            else:
                nodes_failed_count += 1

        print("Calculating node_validation outcome...")
        print("Succeeded=" + str(nodes_succeeded_count) + " Failed=" + str(nodes_failed_count) +
              " All=" + str(len(self.nodes)))

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython validate.py [-v]")
        print("Example:")
        print("\tpython validate.py")


if __name__ == "__main__":
    start_time = time.time()
    Main().validate()
    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    Main.terminate()
