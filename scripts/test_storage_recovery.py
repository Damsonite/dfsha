import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient
from src.common import data_pb2, data_pb2_grpc, metadata_pb2, metadata_pb2_grpc
from src.common.auth import (
    DEFAULT_CLIENT_TOKEN,
    DEFAULT_SERVICE_TOKEN,
    ClientTokenInterceptor,
)

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"
FAILED_NODE = "storage-3"
FAILED_CONTAINER = "dfsha-storage-3"


def wait_for_channel(channel, name):
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError as error:
        raise RuntimeError(f"{name} is not reachable") from error


def docker_exec(container, *arguments):
    return subprocess.run(
        ["docker", "exec", container, *arguments],
        capture_output=True,
        text=True,
        check=True,
    )


def storage_block_count(node_number):
    result = docker_exec(
        f"dfsha-storage-{node_number}",
        "sh",
        "-c",
        "ls /data/*.block 2>/dev/null | wc -l",
    )

    return int(result.stdout.strip() or "0")


def block_counts():
    return tuple(storage_block_count(node) for node in (1, 2, 3))


def node_status(node_id):
    result = docker_exec(
        "dfsha-postgres",
        "psql",
        "-U",
        "dfsha",
        "-d",
        "dfsha",
        "-tAc",
        f"select status from storage_nodes where id='{node_id}';",
    )

    return result.stdout.strip()


def wait_for_node_status(node_id, expected, timeout=20):
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if node_status(node_id) == expected:
            return

        time.sleep(0.5)

    raise AssertionError(
        f"node {node_id} did not reach {expected}; current={node_status(node_id)}"
    )


def locate_block(metadata_stub, file_id):
    locations = metadata_stub.LocateBlocks(
        metadata_pb2.LocateBlocksRequest(
            owner_id=OWNER_ID, file_id=file_id
        ),
        timeout=10,
    )

    assert len(locations.blocks) == 1, len(locations.blocks)

    return locations.blocks[0]


def endpoint_count(metadata_stub, file_id):
    return len(locate_block(metadata_stub, file_id).node_endpoints)


def main():
    metadata_channel = grpc.intercept_channel(
        grpc.insecure_channel(METADATA_ENDPOINT),
        ClientTokenInterceptor(DEFAULT_CLIENT_TOKEN),
    )
    wait_for_channel(metadata_channel, "Metadata Service")
    metadata_stub = metadata_pb2_grpc.MetadataServiceStub(metadata_channel)

    data_channel = grpc.intercept_channel(
        grpc.insecure_channel(DATA_ENDPOINT),
        ClientTokenInterceptor(DEFAULT_SERVICE_TOKEN),
    )
    wait_for_channel(data_channel, "Data Service")
    data_stub = data_pb2_grpc.DataServiceStub(data_channel)

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    try:
        root_name = f"recovery-{uuid.uuid4().hex[:8]}"
        root_path = f"/{root_name}"
        root = client.mkdir_path(root_path, OWNER_ID)

        counts_before = block_counts()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            repair_file = temporary_path / "repair.bin"
            repair_file.write_bytes(b"repair replica\n" * 10000)

            repair_upload = client.upload(
                repair_file, OWNER_ID, root.entry_id
            )

            block = locate_block(metadata_stub, repair_upload.file_id)
            endpoint = block.node_endpoints[0]
            container = f"dfsha-{endpoint.split(':', 1)[0]}"

            docker_exec(
                container,
                "sh",
                "-c",
                f"rm -f /data/{block.block_id}.block "
                f"/data/{block.block_id}.json",
            )

            print(f"OK   deleted replica of {block.block_id} from {endpoint}")

            repair_result = data_stub.Repair(
                data_pb2.RepairRequest(target_replicas=3), timeout=120
            )

            assert repair_result.success, list(repair_result.failures)

            block_after = locate_block(metadata_stub, repair_upload.file_id)

            assert len(block_after.node_endpoints) == 3, block_after

            print(
                "OK   missing replica repaired: "
                f"{repair_result.replicas_repaired} replica(s)"
            )

            client.remove(repair_upload.file_id, OWNER_ID)

            health_file = temporary_path / "health.bin"
            health_file.write_bytes(b"health tracking\n" * 10000)

            health_upload = client.upload(
                health_file, OWNER_ID, root.entry_id
            )

            assert endpoint_count(metadata_stub, health_upload.file_id) == 3

            subprocess.run(
                ["docker", "stop", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "INACTIVE")

            assert endpoint_count(metadata_stub, health_upload.file_id) == 2

            print("OK   unhealthy node excluded from block locations")

            subprocess.run(
                ["docker", "start", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "ACTIVE")

            assert endpoint_count(metadata_stub, health_upload.file_id) == 3

            print("OK   recovered node returned to block locations")

            client.remove(health_upload.file_id, OWNER_ID)

            replicate_file = temporary_path / "replicate.bin"
            replicate_file.write_bytes(b"replicate replica\n" * 10000)

            subprocess.run(
                ["docker", "stop", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "INACTIVE")

            replicate_upload = client.upload(
                replicate_file, OWNER_ID, root.entry_id
            )

            assert endpoint_count(metadata_stub, replicate_upload.file_id) == 2

            print("OK   upload completed with two replicas while node was down")

            subprocess.run(
                ["docker", "start", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "ACTIVE")

            replicate_result = data_stub.Repair(
                data_pb2.RepairRequest(target_replicas=3), timeout=120
            )

            assert replicate_result.success, list(replicate_result.failures)

            assert endpoint_count(metadata_stub, replicate_upload.file_id) == 3

            print(
                "OK   missing replica re-replicated after node returned: "
                f"{replicate_result.replicas_repaired} replica(s)"
            )

            client.remove(replicate_upload.file_id, OWNER_ID)

            partial_file = temporary_path / "partial-rm.bin"
            partial_file.write_bytes(b"partial rm\n" * 10000)

            partial_upload = client.upload(
                partial_file, OWNER_ID, root.entry_id
            )

            subprocess.run(
                ["docker", "stop", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "INACTIVE")

            try:
                client.remove(partial_upload.file_id, OWNER_ID)
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.FAILED_PRECONDITION, (
                    error.code(),
                    error.details(),
                )

                print("OK   partial rm rejected while a replica was unreachable")
            else:
                raise AssertionError("rm succeeded with an unreachable replica")

            assert client.stat(partial_upload.file_id, OWNER_ID).type == "FILE"

            print("OK   entry preserved after failed rm")

            subprocess.run(
                ["docker", "start", FAILED_CONTAINER],
                check=True,
                capture_output=True,
            )

            wait_for_node_status(FAILED_NODE, "ACTIVE")

            client.remove(partial_upload.file_id, OWNER_ID)

            try:
                client.stat(partial_upload.file_id, OWNER_ID)
            except grpc.RpcError as error:
                assert error.code() == grpc.StatusCode.NOT_FOUND, error.code()
            else:
                raise AssertionError("entry still exists after retried rm")

            print("OK   retried rm completed after node recovery")

        client.remove_path(root_path, OWNER_ID)

        counts_after = block_counts()

        assert counts_after == counts_before, (
            f"recovery tests left physical blocks behind: "
            f"{counts_before} -> {counts_after}"
        )

        print(f"OK   physical block count restored: {counts_after}")

    finally:
        subprocess.run(
            ["docker", "start", FAILED_CONTAINER],
            check=False,
            capture_output=True,
        )
        client.close()
        data_channel.close()
        metadata_channel.close()

    print("\nStorage recovery tests passed.")


if __name__ == "__main__":
    main()
