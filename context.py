

class Context:
    def __init__(self):
        self.address = None
        self.observers = "127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002"
        self.verbose = False
        self.in_memory = False
        self.nodes = dict()
        self.nodes_by_address = dict()
        self.old_comparision_json = {}
        self.new_comparision_json = {}


class Channel:
    def __init__(self):
        self.id = None
        self.id_on_contractor_side = None


class TrustLine:
    def __init__(self):
        self.id = None
        self.contractor_id = None
        self.contractor = None
        self.incoming_amount = None
        self.outgoing_amount = None
        self.balance = None
        self.is_contractor_gateway = None
        self.equivalent = None

        self.number = None
        self.our_key_hash = None
        self.our_signature = None
        self.own_keys_set_hash = None
        self.contractor_keys_set_hash = None


class OwnKey:
    def __init__(self):
        self.hash = None
        self.trust_line_id = None
        self.keys_set_sequence_number = None
        self.public_key = None
        self.private_key = None
        self.number = None
        self.is_valid = None


class History:
    def __init__(self):
        self.operation_uuid = None
        self.operation_timestamp = None
        self.record_type = None
        self.record_body = None
        self.record_body_bytes_count = None
        self.equivalent = None
        self.command_uuid = None
