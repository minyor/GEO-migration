import os, sys
import json
import threading
import time

import context

from node_executor import NodeExecutor


class NodeValidator(NodeExecutor):
    def __init__(self, ctx, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path):
        super().__init__(ctx, old_node_path, new_node_path, old_client_path, new_client_path, old_uuid_2_address_path)

    def validate(self):
        print()

        self.clean()
