import os
import threading
import time
import json

from node.executor import NodeExecutor


class NodeValidator(NodeExecutor):
    class TrustLine:
        def __init__(self):
            self.node = None
            self.equivalent = None
            self.contractor_id = None
            self.address = None
            self.state = None
            self.own_keys = None
            self.contractor_keys = None
            self.incoming_trust_amount = None
            self.outgoing_trust_amount = None
            self.balance = None

    def __init__(self, ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, node_name, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path)

        self.node_handle = None
        self.trust_lines = None
        self.checked = False
        self.trust_lines_count = int(0)

        self.db_connect(False)
        self.new_storage_cur.execute(
            "SELECT COUNT(*) "
            "FROM trust_lines;")
        rows = self.new_storage_cur.fetchall()
        self.trust_lines_count = rows[0][0]
        self.db_disconnect(False)

    def init(self, verbose=True):
        new_node_result_fifo_thread = \
            threading.Thread(target=self.open_node_result_fifo, args=(self.new_result_fifo_path, verbose))
        new_node_result_fifo_thread.start()

        self.node_handle = self.run_node(self.new_node_path, self.new_client_path, verbose)
        time.sleep(0.2)

    def validate(self):
        validated_file_path = os.path.join(self.new_node_path, "validated.json")
        if os.path.isfile(validated_file_path):
            self.checked = True
            return

        print()
        self.init()

        if 0 != 0:
            contractors = self.get_contractors()
            for c_tuple in contractors:
                contractor_id = c_tuple[0]
                node = c_tuple[1]
                node.init(False)
                id_on_contractor_side = node.get_contractors(self.new_node_address, False)
                if id_on_contractor_side is None:
                    assert False, "Can't confirm channel between nodes: " +\
                                  self.node_name + ", " + node.node_name
                print("\tFound channel (" + str(contractor_id) + "," + str(id_on_contractor_side) + ")")

                node.clear()

        print("Acquisition of trust lines for node " + str(self.node_name) + "...")
        self.trust_lines = self.get_trust_lines()

        checked = True
        for trust_line1 in self.trust_lines:
            node = trust_line1.node
            if node.checked:
                continue
            node.init(False)
            if node.trust_lines is None:
                node.trust_lines = node.get_trust_lines()
            for trust_line2 in node.trust_lines:
                if trust_line1.equivalent != trust_line2.equivalent or self.new_node_address != trust_line2.address:
                    continue
                new_outgoing_trust_amount1 = trust_line1.outgoing_trust_amount + 1
                new_outgoing_trust_amount2 = trust_line2.outgoing_trust_amount + 1
                trust_line_changed1 = self.change_trust_line(trust_line1, self, new_outgoing_trust_amount1)
                trust_line_changed2 = self.change_trust_line(trust_line2, node, new_outgoing_trust_amount2)

                if trust_line_changed1.outgoing_trust_amount == new_outgoing_trust_amount1 and \
                    trust_line_changed1.state == 2 and \
                    trust_line_changed1.own_keys == 1 and trust_line_changed1.contractor_keys == 1 and \
                    trust_line_changed2.outgoing_trust_amount == new_outgoing_trust_amount2 and \
                    trust_line_changed2.state == 2 and \
                    trust_line_changed2.own_keys == 1 and trust_line_changed2.contractor_keys == 1:
                    print("SUCCESS: Both trust lines changed, no errors")
                else:
                    checked = False
                    print("FAILURE: Changed trust lines does not match their targets")

            node.clear()

        self.clear()

        if checked:
            self.checked = True
            with open(validated_file_path, 'w') as cpm_file_out:
                json.dump({}, cpm_file_out, sort_keys=True, indent=4, ensure_ascii=False)

    @staticmethod
    def change_trust_line(trust_line, node, new_outgoing_trust_amount):
        print("\tTrust line before: ota=" + str(trust_line.outgoing_trust_amount) +
              " state=" + str(trust_line.state) +
              " keys=(" + str(trust_line.own_keys) +
              ":" + str(trust_line.contractor_keys) + ")")
        print("\tTrying to change trust line ota to " + str(new_outgoing_trust_amount) + "...")

        result_tl = node.run_command(
            node.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tSET:contractors/trust-lines\t' +
            str(trust_line.contractor_id) + '\t' +
            str(int(new_outgoing_trust_amount)) + '\t' +
            str(trust_line.equivalent) + '\n'). \
            decode("utf-8")
        #print('Result: "{0}"'.format(result_tl))
        result_tl = result_tl.split('\t')
        tl_success = int(result_tl[1])
        print("\t\t" + ("SUCCESS: " if tl_success == 200 else "FAILURE: ") + str(tl_success))

        state_get_retr_count = 0
        while True:
            trust_line_changed = node.get_trust_line(trust_line.contractor_id, trust_line.equivalent)
            print("\tTrust line after: ota=" + str(trust_line_changed.outgoing_trust_amount) +
                  " state=" + str(trust_line_changed.state) +
                  " keys=(" + str(trust_line_changed.own_keys) +
                  ":" + str(trust_line_changed.contractor_keys) + ")")
            curr_state = trust_line_changed.state
            if curr_state == 2:
                break
            state_get_retr_count += 1
            if state_get_retr_count > 10:
                print("\tError: Waiting for state 2 is over")
                break
            print("\twaiting 0.5 sec...")
            time.sleep(0.5)
        return trust_line_changed

    def clear(self):
        self.node_handle.terminate()
        #os.killpg(os.getpgid(self.node_handle.pid), signal.SIGTERM)
        self.clean()

    def get_trust_lines(self):
        print("\tRequesting equivalents for node " + str(self.node_name) + "...")
        result_eq = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:equivalents\n').decode("utf-8")
        result_eq = result_eq.split('\t')
        eq_count = int(result_eq[2])
        print("\tFound " + str(eq_count) + " equivalents")
        trust_lines = []
        for e in range(eq_count):
            eq = int(result_eq[e + 3])
            print("\t\tRequesting trust lines for equivalent " + str(eq) + "...")
            result_tl = self.run_command(
                self.new_commands_fifo_path,
                '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines\t0\t100000\t' + str(eq) + '\n'). \
                decode("utf-8")
            result_tl = result_tl.split('\t')
            tl_count = int(result_tl[2])
            print("\t\tFound " + str(tl_count) + " trust lines")
            for t in range(tl_count):
                trust_line_array_shift = t * 8 + 3
                contractor_id = int(result_tl[trust_line_array_shift + 0])
                addresses = result_tl[trust_line_array_shift + 1]
                state = int(result_tl[trust_line_array_shift + 2])
                own_keys = int(result_tl[trust_line_array_shift + 3])
                contractor_keys = int(result_tl[trust_line_array_shift + 4])
                incoming_trust_amount = float(result_tl[trust_line_array_shift + 5])
                outgoing_trust_amount = float(result_tl[trust_line_array_shift + 6])
                balance = float(result_tl[trust_line_array_shift + 7])
                address = addresses[addresses.find(' ') + 1:]
                node = self.ctx.nodes_by_address.get(address)
                if node is None:
                    assert False, "Can't find node by address " + address
                print("\t\t\tTrust line " + str(t+1) + ":" +
                      " id="+str(node.node_name) + ";" +
                      " incoming=" + str(incoming_trust_amount) + ";" +
                      " outgoing=" + str(outgoing_trust_amount) + ";" +
                      " balance=" + str(balance))
                trust_line = NodeValidator.TrustLine()
                trust_lines.append(trust_line)
                trust_line.node = node
                trust_line.equivalent = eq
                trust_line.contractor_id = contractor_id
                trust_line.address = address
                trust_line.state = state
                trust_line.own_keys = own_keys
                trust_line.contractor_keys = contractor_keys
                trust_line.incoming_trust_amount = incoming_trust_amount
                trust_line.outgoing_trust_amount = outgoing_trust_amount
                trust_line.balance = balance
        return trust_lines

    def get_trust_line(self, contractor_id, equivalent):
        print("\t\tRequesting trust line for contractor_id=" + str(contractor_id) +
              " equivalent " + str(equivalent) + "...")
        result_tl = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines/one/id\t' +
            str(contractor_id) + '\t' + str(equivalent) + '\n'). \
            decode("utf-8")
        result_tl = result_tl.split('\t')
        trust_line_array_shift = 2
        contractor_id = int(result_tl[trust_line_array_shift + 0])
        state = int(result_tl[trust_line_array_shift + 1])
        own_keys = int(result_tl[trust_line_array_shift + 2])
        contractor_keys = int(result_tl[trust_line_array_shift + 3])
        incoming_trust_amount = float(result_tl[trust_line_array_shift + 4])
        outgoing_trust_amount = float(result_tl[trust_line_array_shift + 5])
        balance = float(result_tl[trust_line_array_shift + 6])
        print("\t\t\tTrust line:" +
              " incoming=" + str(incoming_trust_amount) + ";" +
              " outgoing=" + str(outgoing_trust_amount) + ";" +
              " balance=" + str(balance))
        trust_line = NodeValidator.TrustLine()
        trust_line.equivalent = equivalent
        trust_line.contractor_id = contractor_id
        trust_line.state = state
        trust_line.own_keys = own_keys
        trust_line.contractor_keys = contractor_keys
        trust_line.incoming_trust_amount = incoming_trust_amount
        trust_line.outgoing_trust_amount = outgoing_trust_amount
        trust_line.balance = balance
        return trust_line

    def get_trust_line_by_address(self, address, equivalent):
        print("\t\tRequesting trust line for address=" + str(address) +
              " equivalent " + str(equivalent) + "...")
        result_tl = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors/trust-lines/one/address\t1\t12\t' +
            str(address) + '\t' + str(equivalent) + '\n'). \
            decode("utf-8")
        result_tl = result_tl.split('\t')
        trust_line_array_shift = 2
        contractor_id = int(result_tl[trust_line_array_shift + 0])
        state = int(result_tl[trust_line_array_shift + 1])
        own_keys = int(result_tl[trust_line_array_shift + 2])
        contractor_keys = int(result_tl[trust_line_array_shift + 3])
        incoming_trust_amount = float(result_tl[trust_line_array_shift + 4])
        outgoing_trust_amount = float(result_tl[trust_line_array_shift + 5])
        balance = float(result_tl[trust_line_array_shift + 6])
        print("\t\t\tTrust line:" +
              " incoming=" + str(incoming_trust_amount) + ";" +
              " outgoing=" + str(outgoing_trust_amount) + ";" +
              " balance=" + str(balance))
        trust_line = NodeValidator.TrustLine()
        trust_line.equivalent = equivalent
        trust_line.contractor_id = contractor_id
        trust_line.state = state
        trust_line.own_keys = own_keys
        trust_line.contractor_keys = contractor_keys
        trust_line.incoming_trust_amount = incoming_trust_amount
        trust_line.outgoing_trust_amount = outgoing_trust_amount
        trust_line.balance = balance
        return trust_line

    def get_contractors(self, search_address=None, verbose=True):
        if verbose:
            print("Requesting contractors for node " + str(self.node_name) + "...")
        result_ct = self.run_command(
            self.new_commands_fifo_path,
            '13e5cf8c-5834-4e52-b65b-f9281dd1ff91\tGET:contractors-all\n'). \
            decode("utf-8")
        result_ct = result_ct.split('\t')
        ct_count = int(result_ct[2])
        if verbose:
            print("\tFound " + str(ct_count) + " contractors")
        contractors = []
        for t in range(ct_count):
            trust_line_array_shift = t * 2 + 3
            contractor_id = int(result_ct[trust_line_array_shift + 0])
            addresses = result_ct[trust_line_array_shift + 1]
            address = addresses[addresses.find(' ')+1:].rstrip('\n')
            node = self.ctx.nodes_by_address.get(address)
            if node is None:
                assert False, "Can't find node by address '" + address + "'"
            if verbose:
                print("\t\tContractor " + str(t+1) + ":" +
                      " id="+str(contractor_id) + ";" +
                      " address=" + str(address) + ";" +
                      " uuid=" + str(node.node_name))
            contractors.append((contractor_id, node))
            if search_address is not None:
                if search_address == address:
                    return contractor_id
        if search_address is not None:
            return None
        return contractors
