import os

STORAGE_NODES_ENV = "DFSHA_STORAGE_NODES"

DEFAULT_STORAGE_NODES = (
    ("storage-1", "storage-1:50061"),
    ("storage-2", "storage-2:50061"),
    ("storage-3", "storage-3:50061"),
    ("storage-4", "storage-4:50061"),
)


def parse_storage_nodes(value=None):
    raw_value = value if value is not None else os.getenv(
        STORAGE_NODES_ENV
    )

    if not raw_value:
        return list(DEFAULT_STORAGE_NODES)

    nodes = []
    seen_ids = set()

    for item in raw_value.split(","):
        entry = item.strip()

        if not entry:
            continue

        node_id, separator, endpoint = entry.partition("=")
        node_id = node_id.strip()
        endpoint = endpoint.strip()

        if not separator or not node_id or not endpoint:
            raise ValueError(
                f"Invalid {STORAGE_NODES_ENV} entry {entry!r}: "
                "expected id=host:port"
            )

        if node_id in seen_ids:
            raise ValueError(
                f"Invalid {STORAGE_NODES_ENV}: duplicated node {node_id!r}"
            )

        seen_ids.add(node_id)
        nodes.append((node_id, endpoint))

    if len(nodes) < 2:
        raise ValueError(
            f"Invalid {STORAGE_NODES_ENV}: at least two nodes are required"
        )

    return nodes
