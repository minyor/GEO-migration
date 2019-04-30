import pysodium
import itertools


class NodeChannel:
    def __init__(self, contractor, node1, node2):
        self.contractor = contractor
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

    def generate_trust_lines(self, channel):
        print("\tGenerating trust lines between nodes: " +
              self.node1.node_name + ", " + self.node2.node_name)

        self.node1.add_trust_lines(
            channel.id_on_contractor_side2,
            self.contractor.contractor_id
        )
        self.node2.add_trust_lines(
            channel.id_on_contractor_side1,
            self.contractor.contractor_id
        )

    @staticmethod
    def construct_contractor_tuple(node_name1, node_name2):
        node_key_pair = [node_name1, node_name2]
        node_key_pair.sort()
        return tuple(node_key_pair)

    @staticmethod
    def list(contractors):
        channels = dict()
        trust_lines = dict()
        for name, contractor in contractors.items():
            print("Generating contractor: " + name + " nodes: " + str(len(contractor.nodes)))
            nodes = list(contractor.nodes.values())
            node_pairs = list(itertools.combinations(range(0, len(nodes)), r=2))
            for pair in node_pairs:
                node1 = nodes[pair[0]]
                node2 = nodes[pair[1]]
                contractor_tuple = NodeChannel.construct_contractor_tuple(node1.node_name, node2.node_name)
                node_channel = NodeChannel(contractor, node1, node2)
                channels[contractor_tuple] = node_channel
                trust_lines[(contractor.contractor_id, contractor_tuple[0], contractor_tuple[1])] = node_channel

        return [channels, trust_lines]
