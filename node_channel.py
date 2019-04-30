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

    @staticmethod
    def construct_channels(nodes):
        channels = dict()
        for node_migrator in nodes.values():
            for trust_line in node_migrator.trust_lines:
                contractor_tuple = NodeChannel.construct_contractor_tuple(
                    node_migrator.node_name, trust_line.contractor_id)
                channels[contractor_tuple] = \
                    NodeChannel(node_migrator, nodes[trust_line.contractor_id])
        return channels

    @staticmethod
    def construct_contractor_tuple(node_name1, node_name2):
        node_key_pair = [node_name1, node_name2]
        node_key_pair.sort()
        return tuple(node_key_pair)
