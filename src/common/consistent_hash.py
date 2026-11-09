import hashlib


class ConsistentHash:
    def __init__(self, nodes, virtual_nodes=150):

        self.ring = {}
        self.sorted_keys = []

        for node in nodes:
            for index in range(virtual_nodes):
                key = self._hash(f"{node}:{index}")

                self.ring[key] = node

        self.sorted_keys = sorted(self.ring)

    @staticmethod
    def _hash(value):

        return int(hashlib.sha256(value.encode()).hexdigest(), 16)

    def get_nodes(self, key, count=3):

        if not self.ring:
            return []

        position = self._hash(key)

        result = []

        for ring_position in self.sorted_keys:
            if ring_position < position:
                continue

            node = self.ring[ring_position]

            if node not in result:
                result.append(node)

            if len(result) == count:
                break

        if len(result) < count:
            for ring_position in self.sorted_keys:
                node = self.ring[ring_position]

                if node not in result:
                    result.append(node)

                if len(result) == count:
                    break

        return result
