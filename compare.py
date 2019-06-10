import os, sys
import json
import getopt
import shutil
import threading
import time
import datetime
import node_context

from settings import migration_conf
from node_comparator import NodeComparator


class Main(node_context.Context):
    def __init__(self, custom_args=None, files_prefix=""):
        super().__init__()
        try:
            if custom_args is None:
                custom_args = sys.argv[1:]
            opts, args = getopt.getopt(custom_args, "hm:t:v", ["help"])
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
            elif o in ("-m", "--max-nodes"):
                self.nodes_count_max = int(a)
            elif o in ("-t", "--threads"):
                self.threads = int(a)
            else:
                assert False, "unhandled option"
        self.in_memory = True
        self.old_infrastructure_path = migration_conf.get("old_infrastructure_path")
        self.new_infrastructure_path = migration_conf.get("new_infrastructure_path")
        self.old_network_client_path = migration_conf.get("old_network_client_path")
        self.new_network_client_path = migration_conf.get("new_network_client_path")
        self.old_uuid_2_address_path = migration_conf.get("old_uuid_2_address_path")
        self.old_cpm_file_path = os.path.join(self.old_infrastructure_path, files_prefix + "compare.json")
        self.new_cpm_file_path = os.path.join(self.new_infrastructure_path, files_prefix + "compare.json")
        self.old_ign_file_path = os.path.join(self.old_infrastructure_path, files_prefix + "ignored.json")
        self.new_ign_file_path = os.path.join(self.new_infrastructure_path, files_prefix + "ignored.json")

    def batch(self, batch_thread_info):
        try:
            thread_index = batch_thread_info[1]
            nodes = batch_thread_info[2]
            files_prefix = "thread_" + str(thread_index) + "_"
            main = Main("", files_prefix)
            batch_thread_info[4] = main
            if len(nodes) > 0:
                main.nodes = self.nodes
                main.nodes_by_address = self.nodes_by_address
                main.verbose = self.verbose
                main.compare(nodes)
        except Exception as e:
            print(e)

        batch_thread_info[3] = True

    def start_batch(self):
        nodes = os.listdir(self.old_infrastructure_path)
        pending_nodes = []
        number_of_valid_nodes = 0
        for path in nodes:
            old_node_path = os.path.join(self.old_infrastructure_path, path)
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if not os.path.isdir(new_node_path):
                assert False, "Migrated node " + path + " is not found"
            number_of_valid_nodes += 1

            print("Loading node #" + str(len(self.nodes) + 1) + ": " + path)
            node_comparator = NodeComparator(
                self, path, old_node_path, new_node_path,
                self.old_network_client_path, self.new_network_client_path,
                self.old_uuid_2_address_path)
            self.nodes[node_comparator.node_name] = node_comparator
            self.nodes_by_address[node_comparator.new_node_address] = node_comparator

            compared_file_path = os.path.join(new_node_path, "compared.json")
            if os.path.isfile(compared_file_path):
                continue
            if len(pending_nodes) >= self.nodes_count_max:
                continue
            pending_nodes.append(path)

        nodes_per_thread = int(len(pending_nodes) / self.threads + 1)
        nodes_reserved = 0
        batch_threads_info = []
        for t in range(self.threads):
            nodes_end = min(nodes_reserved + nodes_per_thread, len(pending_nodes))
            curr_nodes = pending_nodes[nodes_reserved:nodes_end]
            nodes_reserved = nodes_end
            batch_thread_info = [None, t, curr_nodes, False, None]
            batch_thread = \
                threading.Thread(target=self.batch, args=(batch_thread_info, ))
            batch_thread.start()
            batch_thread_info[0] = batch_thread
            batch_threads_info.append(batch_thread_info)

        for batch_thread_info in batch_threads_info:
            while not batch_thread_info[3]:
                time.sleep(1)

        print("Calculating migration outcome...")
        compared_nodes_sum = 0
        for batch_thread_info in batch_threads_info:
            if batch_thread_info[4] is None:
                continue
            print("[Thread: "+str(batch_thread_info[1]+1)+"] ", end="")
            compared_nodes_sum += batch_thread_info[4].calculating_migration_outcome()
        print("Nodes compared "+str(compared_nodes_sum)+"/"+str(len(pending_nodes))+"/"+str(number_of_valid_nodes) +
              " : " + str(number_of_valid_nodes - compared_nodes_sum) + " nodes left")

    def compare(self, nodes=None):
        if self.threads is not None:
            self.start_batch()
            return

        old_uuid_2_address_dir = self.old_uuid_2_address_path[:self.old_uuid_2_address_path.rindex('/')]
        print("old_uuid_2_address_dir="+old_uuid_2_address_dir)
        old_uuid_2_address_thread = threading.Thread(
            target=self.run_uuid_2_address,
            args=(old_uuid_2_address_dir, self.old_uuid_2_address_path))
        old_uuid_2_address_thread.start()

        self.load_comparision_files()

        print()
        all_nodes = os.listdir(self.old_infrastructure_path) if nodes is None else nodes
        for path in all_nodes:
            old_node_path = os.path.join(self.old_infrastructure_path, path)
            new_node_path = os.path.join(self.new_infrastructure_path, path)
            if not os.path.isdir(old_node_path):
                continue
            if not os.path.isdir(new_node_path):
                assert False, "Migrated node " + path + " is not found"
            print("Loading node #"+str(len(self.nodes)+1)+": " + path)
            node_comparator = NodeComparator(
                self, path, old_node_path, new_node_path,
                self.old_network_client_path, self.new_network_client_path,
                self.old_uuid_2_address_path)
            self.nodes_array.append(node_comparator)
            self.nodes[node_comparator.node_name] = node_comparator
            self.nodes_by_address[node_comparator.new_node_address] = node_comparator

        for node_comparator in self.nodes_array:
            if self.nodes_count_processed >= self.nodes_count_max:
                break
            try:
                node_comparator.compare()
            except:
                print("Failed to compare node #" + str(node_comparator.node_idx + 1) + ": " + node_comparator.node_name)
                self.load_comparision_files()

    def calculating_migration_outcome(self):
        try:
            old_cpm_obj = json.load(open(self.old_cpm_file_path))
            old_cpm_file = str(old_cpm_obj)
            new_cpm_file = str(json.load(open(self.new_cpm_file_path)))
            if old_cpm_file == new_cpm_file:
                print("SUCCESS["+str(len(old_cpm_obj))+"]: old and new comparision json files are equal!")
            else:
                print("FAILURE["+str(len(old_cpm_obj))+"]: old and new comparision json files differs!")

            if 0 != 0:
                #print()
                #print("Renaming comparision json files as old...")
                curr_time = str(datetime.datetime.now())
                os.rename(self.old_cpm_file_path, self.old_cpm_file_path + "." + curr_time)
                os.rename(self.new_cpm_file_path, self.new_cpm_file_path + "." + curr_time)
                os.rename(self.old_ign_file_path, self.old_ign_file_path + "." + curr_time)
                os.rename(self.new_ign_file_path, self.new_ign_file_path + "." + curr_time)

            return len(old_cpm_obj)
        except:
            print("FAILURE: there are nothing to compare!")
            return 0

    def load_comparision_files(self):
        print("Loading 'compare.json' files...")
        try:
            old_cmp_file = open(self.old_cpm_file_path)
            new_cmp_file = open(self.new_cpm_file_path)
            old_ign_file = open(self.old_ign_file_path)
            new_ign_file = open(self.new_ign_file_path)
            if old_cmp_file is None or new_cmp_file is None or old_ign_file is None or new_ign_file:
                assert "start anew"
            self.old_comparision_json = json.load(old_cmp_file)
            self.new_comparision_json = json.load(new_cmp_file)
            self.old_ignored_json = json.load(old_ign_file)
            self.new_ignored_json = json.load(new_ign_file)
        except:
            print("Cannot load 'compare.json' files. Starting anew...")
            self.old_comparision_json = {}
            self.new_comparision_json = {}
            self.old_ignored_json = {}
            self.new_ignored_json = {}

    def save_comparision_files(self):
        print()
        print("Saving 'compare.json' files...")
        with open(self.old_cpm_file_path, 'w') as cpm_file_out:
            json.dump(self.old_comparision_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        with open(self.new_cpm_file_path, 'w') as cpm_file_out:
            json.dump(self.new_comparision_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

        print("Saving 'ignored.json' files...")
        with open(self.old_ign_file_path, 'w') as cpm_file_out:
            json.dump(self.old_ignored_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)
        with open(self.new_ign_file_path, 'w') as cpm_file_out:
            json.dump(self.new_ignored_json, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    @staticmethod
    def usage():
        print("Usage:")
        print("\tpython compare.py [-v] [-m max nodes to process]")
        print("Example:")
        print("\tpython compare.py")


if __name__ == "__main__":
    start_time = time.time()
    main = Main()
    main.compare()
    if main.threads is None:
        print()
        print("Calculating migration outcome...")
        compared_nodes_sum = main.calculating_migration_outcome()
        print("Nodes compared "+str(compared_nodes_sum)+"/"+str(len(main.nodes)) +
              " : " + str(len(main.nodes) - compared_nodes_sum) + " nodes left")
        print()

    hours, rem = divmod(time.time() - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
    Main.terminate()
