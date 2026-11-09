import hashlib
import subprocess
import tempfile
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient
from src.common import metadata_pb2, metadata_pb2_grpc
from src.common.auth import DEFAULT_CLIENT_TOKEN, ClientTokenInterceptor

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"
POSTGRES_CONTAINER = "dfsha-postgres"


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


def container_for(endpoint):
    host = endpoint.split(":", 1)[0]
    return f"dfsha-{host}"


def backup_and_corrupt(endpoint, block_id):
    container = container_for(endpoint)
    backup_path = f"/tmp/{block_id}.block.bak"

    docker_exec(container, "cp", f"/data/{block_id}.block", backup_path)
    docker_exec(
        container,
        "sh",
        "-c",
        f"printf 'corrupted by test' > /data/{block_id}.block",
    )

    return container, backup_path


def restore_block(container, block_id, backup_path):
    docker_exec(
        container,
        "mv",
        backup_path,
        f"/data/{block_id}.block",
    )


def expect_rpc_error(action, expected_code, label):
    try:
        action()
    except grpc.RpcError as error:
        if error.code() != expected_code:
            raise AssertionError(
                f"{label}: expected {expected_code.name}, got "
                f"{error.code().name}: {error.details()}"
            ) from error

        print(f"OK   {label}: {error.code().name}")
        return

    raise AssertionError(f"{label}: expected {expected_code.name}, call succeeded")


def set_checksum(file_id, checksum):
    sql = f"UPDATE entries SET checksum = '{checksum}' WHERE id = '{file_id}';"

    docker_exec(
        POSTGRES_CONTAINER,
        "psql",
        "-U",
        "dfsha",
        "-d",
        "dfsha",
        "-c",
        sql,
    )


def main():
    metadata_channel = grpc.intercept_channel(
        grpc.insecure_channel(METADATA_ENDPOINT),
        ClientTokenInterceptor(DEFAULT_CLIENT_TOKEN),
    )
    wait_for_channel(metadata_channel, "Metadata Service")

    metadata_stub = metadata_pb2_grpc.MetadataServiceStub(metadata_channel)

    directory_name = f"download-validation-{uuid.uuid4().hex[:8]}"

    directory = metadata_stub.Mkdir(
        metadata_pb2.MkdirRequest(owner_id=OWNER_ID, name=directory_name),
        timeout=10,
    )

    print(f"OK   directory created: {directory_name} ({directory.entry_id})")

    payload = b"download validation payload\n" * 4096
    expected_checksum = hashlib.sha256(payload).hexdigest()

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    restored_blocks = []

    try:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            source = temporary_path / "payload.bin"
            source.write_bytes(payload)

            uploaded = client.upload(source, OWNER_ID, directory.entry_id)

            assert uploaded.success, uploaded.message
            assert uploaded.block_count == 1, uploaded.block_count
            assert uploaded.checksum == expected_checksum, uploaded.checksum

            locations = metadata_stub.LocateBlocks(
                metadata_pb2.LocateBlocksRequest(
                    owner_id=OWNER_ID, file_id=uploaded.file_id
                ),
                timeout=10,
            )

            assert len(locations.blocks) == 1, len(locations.blocks)

            block = locations.blocks[0]

            assert len(block.node_endpoints) == 3, block.node_endpoints

            first_endpoint = block.node_endpoints[0]

            container, backup_path = backup_and_corrupt(
                first_endpoint, block.block_id
            )

            restored_blocks.append((container, block.block_id, backup_path))

            destination = temporary_path / "one-replica-corrupt.bin"

            client.download(uploaded.file_id, OWNER_ID, destination)

            assert destination.read_bytes() == payload

            print(
                "OK   corrupted replica skipped: "
                f"{first_endpoint} -> {block.node_endpoints[1]}"
            )

            restore_block(container, block.block_id, backup_path)
            restored_blocks.remove((container, block.block_id, backup_path))

            all_backups = []

            try:
                for endpoint in block.node_endpoints:
                    node_container, node_backup = backup_and_corrupt(
                        endpoint, block.block_id
                    )
                    all_backups.append(
                        (node_container, block.block_id, node_backup)
                    )

                destination = temporary_path / "all-replicas-corrupt.bin"

                expect_rpc_error(
                    lambda: client.download(
                        uploaded.file_id, OWNER_ID, destination
                    ),
                    grpc.StatusCode.DATA_LOSS,
                    "all replicas corrupted rejected",
                )

                assert not destination.exists(), "destination must not be created"
                assert not destination.with_name(
                    destination.name + ".part"
                ).exists(), "partial file must be removed"

            finally:
                for node_container, node_block_id, node_backup in all_backups:
                    restore_block(node_container, node_block_id, node_backup)

            destination = temporary_path / "restored.bin"

            client.download(uploaded.file_id, OWNER_ID, destination)

            assert destination.read_bytes() == payload

            print("OK   download works again after restoring replicas")

            metadata_only = metadata_stub.RegisterFile(
                metadata_pb2.RegisterFileRequest(
                    owner_id=OWNER_ID,
                    parent_id=directory.entry_id,
                    name="metadata-only.bin",
                    size=10,
                    checksum=hashlib.sha256(b"0123456789").hexdigest(),
                ),
                timeout=10,
            )

            destination = temporary_path / "metadata-only-out.bin"

            expect_rpc_error(
                lambda: client.download(
                    metadata_only.entry_id, OWNER_ID, destination
                ),
                grpc.StatusCode.DATA_LOSS,
                "metadata-only file rejected",
            )

            assert not destination.exists(), "destination must not be created"
            assert not destination.with_name(
                destination.name + ".part"
            ).exists(), "partial file must be removed"

            metadata_stub.Remove(
                metadata_pb2.RemoveRequest(
                    owner_id=OWNER_ID, entry_id=metadata_only.entry_id
                ),
                timeout=10,
            )

            original_checksum = metadata_stub.Stat(
                metadata_pb2.StatRequest(
                    owner_id=OWNER_ID, entry_id=uploaded.file_id
                ),
                timeout=10,
            ).checksum

            wrong_checksum = (
                "1" * 64 if original_checksum != "1" * 64 else "2" * 64
            )

            set_checksum(uploaded.file_id, wrong_checksum)

            try:
                destination = temporary_path / "bad-checksum.bin"

                expect_rpc_error(
                    lambda: client.download(
                        uploaded.file_id, OWNER_ID, destination
                    ),
                    grpc.StatusCode.DATA_LOSS,
                    "final checksum mismatch rejected",
                )

                assert not destination.exists(), "destination must not be created"
                assert not destination.with_name(
                    destination.name + ".part"
                ).exists(), "partial file must be removed"

            finally:
                set_checksum(uploaded.file_id, original_checksum)

            destination = temporary_path / "checksum-restored.bin"

            client.download(uploaded.file_id, OWNER_ID, destination)

            assert destination.read_bytes() == payload

            print("OK   download works again after restoring metadata checksum")

    finally:
        for container, block_id, backup_path in restored_blocks:
            restore_block(container, block_id, backup_path)

        client.close()
        metadata_channel.close()

    print("\nDownload validation tests passed.")


if __name__ == "__main__":
    main()
