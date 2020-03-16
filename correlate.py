import datetime
import getopt
import json
import os
import sys
import threading
import time
import csv
import redis
import logging

from node.correlator import NodeCorrelator

from node import context
from settings import migration_conf


class Main(context.Context):
    def __init__(self, custom_args=None, files_prefix=""):
        super().__init__()
        try:
            if custom_args is None:
                custom_args = sys.argv[1:]
            opts, args = getopt.getopt(custom_args, "ht:p:v", ["help", "threads=", "period="])
        except getopt.GetoptError as err:
            self.logger.error(str(err))
            self.usage()
            sys.exit(2)
        for o, a in opts:
            if o == "-v":
                self.verbose = True
            elif o in ("-h", "--help"):
                self.usage()
                sys.exit()
            elif o in ("-t", "--threads"):
                self.threads = int(a)
            elif o in ("-p", "--period"):
                self.loop_period_in_sec = int(a)
            else:
                assert False, "unhandled option"
        self.in_memory = True
        self.old_network_client_path = migration_conf.get("old_network_client_path")
        self.new_network_client_path = migration_conf.get("new_network_client_path")
        self.old_uuid_2_address_path = migration_conf.get("old_uuid_2_address_path")
        self.old_handler_url = migration_conf.get("old_handler_url")
        self.new_handler_url = migration_conf.get("new_handler_url")
        self.only_not_correlated = migration_conf.get("only_not_correlated")

        logging.basicConfig(filename='correlation.log', level=logging.DEBUG)

    def load_nodes(self):
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

        old_uuid_2_address_dir = self.old_uuid_2_address_path[:self.old_uuid_2_address_path.rindex('/')]
        self.logger.info("old_uuid_2_address_dir="+old_uuid_2_address_dir)
        old_uuid_2_address_thread = threading.Thread(
            target=self.run_uuid_2_address,
            args=(old_uuid_2_address_dir, self.old_uuid_2_address_path))
        old_uuid_2_address_thread.start()

        # Connect to redis
        self.redis = redis.Redis(
            host=migration_conf.get("redis_host"),
            port=migration_conf.get("redis_port"),
            password=migration_conf.get("redis_password"),
            db=migration_conf.get("redis_db"))

        self.logger.info("")
        for path in self.gns_addresses:
            self.logger.info("Loading node #"+str(len(self.nodes)+1)+": " + path)
            node_correlator = NodeCorrelator(
                self, path, self.gns_addresses[path], self.old_uuid_2_address_path)
            self.nodes_array.append(node_correlator)
            self.nodes[node_correlator.node_name] = node_correlator
            self.nodes_by_address[node_correlator.new_node_address] = node_correlator

    def correlate(self):
        self.reinit_logging()
        logging.info('Beginning new correlation sequence...')
        for node_correlator in self.nodes_array:
            try:
                node_correlator.correlate()
            except Exception as e:
                logging.error(e)
                logging.error("Failed to correlate node #" + str(node_correlator.node_idx + 1) + ": " + node_correlator.node_name)

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython correlate.py [-v] [-p period in sec] [-t threads number]")
        print("\t[-p --period] : Specify correlation period in seconds")
        print("\t[-t --threads] : Specify number of threads")
        print("Example:")
        print("\tpython correlate.py --period 10 -t 32")


if __name__ == "__main__":
    start_time = time.time()
    main = Main()
    main.load_nodes()

    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    main.logger.info("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))

    main.logger.info("")
    main.logger.info("Starting correlation loop...")
    start_time = time.time() - main.loop_period_in_sec
    while True:
        while (time.time() - start_time) < main.loop_period_in_sec:
            time.sleep(0.1)
        start_time = time.time()
        main.logger.info("\n")
        main.correlate()

    Main.terminate()
