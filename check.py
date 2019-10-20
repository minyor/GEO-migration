import getopt
import os
import sys
import time
import csv

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

        os.makedirs(self.new_infrastructure_path, exist_ok=True)

    def check(self):
        # Reading GNS addresses from file
        self.gns_addresses = dict()
        with open('users_addresses.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                self.gns_addresses[row[0]] = row[1]

        # Construct checking result file's paths
        checking_tr_0_path = os.path.join(self.new_infrastructure_path, "checking_tr_0.json")
        checking_tr_gw_bal_path = os.path.join(self.new_infrastructure_path, "checking_tr_gw_bal.json")
        checking_tr_gw_non_bal_path = os.path.join(self.new_infrastructure_path, "checking_tr_gw_non_bal.json")
        checking_no_gns_address_path = os.path.join(self.new_infrastructure_path, "checking_no_gns_address.json")

        # Remove previous checking result files
        if os.path.exists(checking_tr_0_path):
            os.remove(checking_tr_0_path)
        if os.path.exists(checking_tr_gw_bal_path):
            os.remove(checking_tr_gw_bal_path)
        if os.path.exists(checking_tr_gw_non_bal_path):
            os.remove(checking_tr_gw_non_bal_path)
        if os.path.exists(checking_no_gns_address_path):
            os.remove(checking_no_gns_address_path)

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

        # Save checking result files
        if self.checking_tr_0_json is not None:
            self.save_json(self.checking_tr_0_json, checking_tr_0_path)
        if self.checking_tr_gw_bal_json is not None:
            self.save_json(self.checking_tr_gw_bal_json, checking_tr_gw_bal_path)
        if self.checking_tr_gw_non_bal_json is not None:
            self.save_json(self.checking_tr_gw_non_bal_json, checking_tr_gw_non_bal_path)
        if self.checking_no_gns_address_json is not None:
            self.save_json(self.checking_no_gns_address_json, checking_no_gns_address_path)

        print("Trust line stats:")
        print(
            '{0: <8}|'.format("EQ") +
            '{0: <10}|'.format("Count all") +
            '{0: <12}|'.format("Count 0 bal") +
            '{0: <16}|'.format("Count non 0 bal")
        )
        print("--------------------------------------------------")
        for tl_stat in self.checking_stats_tl.values():
            print(
                '{0: <8}|'.format(str(tl_stat.eq)) +
                '{0: <10}|'.format(str(tl_stat.count_all)) +
                '{0: <12}|'.format(str(tl_stat.count_0_bal)) +
                '{0: <16}|'.format(str(tl_stat.count_non_0_bal))
            )


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
