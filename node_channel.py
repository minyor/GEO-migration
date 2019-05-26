import pysodium
import itertools


class NodeChannel:
    def __init__(self, node1, node2):
        self.node1 = node1
        self.node2 = node2
        self.id_on_contractor_side1 = None
        self.id_on_contractor_side2 = None

    def generate_channels(self):
        pk1, sk1 = pysodium.crypto_box_keypair()
        pk2, sk2 = pysodium.crypto_box_keypair()
        self.id_on_contractor_side1 = self.node2.channel_idx
        self.id_on_contractor_side2 = self.node1.channel_idx

        print("Generating channel(" +
              str(self.id_on_contractor_side2) + ":" + str(self.id_on_contractor_side1) +
              ") between nodes: " + self.node1.node_name + ", " + self.node2.node_name)

        self.node1.add_channel(
            pk1, sk1, pk2,
            self.id_on_contractor_side1,
            self.node2.new_node_address
        )
        self.node2.add_channel(
            pk2, sk2, pk1,
            self.id_on_contractor_side2,
            self.node1.new_node_address
        )

    def generate_trust_lines(self):
        print()
        print("Generating trust lines between nodes: " +
              self.node1.node_name + ", " + self.node2.node_name)

        self.node1.add_trust_lines(
            self.id_on_contractor_side2,
            self.node2.node_name
        )
        self.node2.add_trust_lines(
            self.id_on_contractor_side1,
            self.node1.node_name
        )

    def generate_contractor_keys(self):
        print("Generating contractor keys between nodes: " +
              self.node1.node_name + ", " + self.node2.node_name)

        print("generate_contractor_keys DEBUG 01")
        if not self.node1.ctx.in_memory:
            self.node1.db_connect(False)
        print("generate_contractor_keys DEBUG 02")
        if not self.node2.ctx.in_memory:
            self.node2.db_connect(False)

        print("generate_contractor_keys DEBUG 03")
        self.node1.load_own_keys()
        print("generate_contractor_keys DEBUG 04")
        self.node2.load_own_keys()

        print("generate_contractor_keys DEBUG 05")
        for trust_line1 in self.node1.trust_lines.values():
            print("generate_contractor_keys DEBUG 051")
            if trust_line1.contractor_id != self.id_on_contractor_side2:
                continue
            print("generate_contractor_keys DEBUG 052")
            channel1 = self.node1.channels.get(trust_line1.contractor_id, None)
            print("generate_contractor_keys DEBUG 053")
            channel2 = self.node2.channels.get(channel1.id_on_contractor_side, None)

            print("generate_contractor_keys DEBUG 054")
            for trust_line2 in self.node2.trust_lines.values():
                print("generate_contractor_keys DEBUG 0541")
                if trust_line2.contractor_id != self.id_on_contractor_side1:
                    continue
                print("generate_contractor_keys DEBUG 0542")
                if trust_line2.contractor_id != channel2.id or \
                        trust_line1.equivalent != trust_line2.equivalent:
                    continue

                print("generate_contractor_keys DEBUG 0543")
                print(
                    "\tGenerating contractor keys for trust lines" +
                    "(" + str(trust_line1.id) + ":" + str(trust_line2.id) + ")"
                )
                print("generate_contractor_keys DEBUG 0544")
                for own_key1 in self.node1.own_keys:
                    print("generate_contractor_keys DEBUG 05441")
                    if own_key1.trust_line_id != trust_line1.id:
                        continue
                    print("generate_contractor_keys DEBUG 05442")
                    for own_key2 in self.node2.own_keys:
                        print("generate_contractor_keys DEBUG 054421")
                        if own_key2.trust_line_id != trust_line2.id or \
                                own_key1.number != own_key2.number:
                            continue
                        print("generate_contractor_keys DEBUG 054422")
                        self.node1.add_contractor_key(
                            own_key1,
                            own_key2
                        )
                        print("generate_contractor_keys DEBUG 054423")
                        self.node2.add_contractor_key(
                            own_key2,
                            own_key1
                        )
                        print("generate_contractor_keys DEBUG 054424")

        print("generate_contractor_keys DEBUG 06")
        self.node1.own_keys.clear()
        print("generate_contractor_keys DEBUG 07")
        self.node1.own_keys = None
        print("generate_contractor_keys DEBUG 08")
        self.node2.own_keys.clear()
        print("generate_contractor_keys DEBUG 09")
        self.node2.own_keys = None

        print("generate_contractor_keys DEBUG 10")
        if not self.node1.ctx.in_memory:
            self.node1.db_disconnect(False)
        print("generate_contractor_keys DEBUG 11")
        if not self.node2.ctx.in_memory:
            self.node2.db_disconnect(False)
        print("generate_contractor_keys DEBUG 12")

    def generate_audit_crypto(self):
        print("Generating audit hashes and signatures between nodes: " +
              self.node1.node_name + ", " + self.node2.node_name)

        for trust_line1 in self.node1.trust_lines.values():
            if trust_line1.contractor_id != self.id_on_contractor_side2:
                continue
            channel1 = self.node1.channels.get(trust_line1.contractor_id, None)
            channel2 = self.node2.channels.get(channel1.id_on_contractor_side, None)

            for trust_line2 in self.node2.trust_lines.values():
                if trust_line2.contractor_id != self.id_on_contractor_side1:
                    continue
                if trust_line2.contractor_id != channel2.id or \
                        trust_line1.equivalent != trust_line2.equivalent:
                    continue

                print("\tGenerating audit(" +
                      str(trust_line1.id) + ":" + str(trust_line2.id) + ")")
                self.node1.update_audit_crypto(
                    trust_line1.id,
                    trust_line2.our_key_hash,
                    trust_line2.our_signature
                )
                self.node2.update_audit_crypto(
                    trust_line2.id,
                    trust_line1.our_key_hash,
                    trust_line1.our_signature
                )

    @staticmethod
    def construct_channels(nodes):
        channels = dict()
        for node_migrator in nodes.values():
            for trust_line in node_migrator.old_trust_lines:
                contractor_tuple = NodeChannel.construct_contractor_tuple(
                    node_migrator.node_name, trust_line.contractor_id)
                contractor_node = nodes.get(trust_line.contractor_id)
                if contractor_node is None:
                    node_migrator.ctx.append_migration_error({
                        "error": "Channels: trust line. Unknown contractor_id",
                        "node": node_migrator.node_name,
                        "contractor_id": trust_line.contractor_id
                    })
                    continue
                channels[contractor_tuple] = NodeChannel(node_migrator, contractor_node)
        return channels

    @staticmethod
    def construct_contractor_tuple(node_name1, node_name2):
        node_key_pair = [node_name1, node_name2]
        node_key_pair.sort()
        return tuple(node_key_pair)
