import pysodium


class NodeChannel:
    def __init__(self, ctx, node1, node2):
        self.ctx = ctx
        self.node1 = node1
        self.node2 = node2

    def generate(self):
        print("Generating channel between nodes: " +
              self.node1.node_name + ", " + self.node2.node_name)

        pk1, sk1 = pysodium.crypto_box_keypair()
        pk2, sk2 = pysodium.crypto_box_keypair()
        id_on_contractor_side1 = self.node2.channel_idx
        id_on_contractor_side2 = self.node1.channel_idx

        self.node1.add_channel(
            pk1, sk1, pk2,
            id_on_contractor_side1,
            self.node2.new_node_address
        )
        self.node2.add_channel(
            pk2, sk2, pk1,
            id_on_contractor_side2,
            self.node1.new_node_address
        )
