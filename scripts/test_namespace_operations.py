import subprocess
import tempfile
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"


def wait_for_channel(channel, name):
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError as error:
        raise RuntimeError(f"{name} is not reachable") from error


def storage_block_count(node_number):
    container = f"dfsha-storage-{node_number}"

    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "sh",
            "-c",
            "ls /data/*.block 2>/dev/null | wc -l",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    return int(result.stdout.strip() or "0")


def block_counts():
    return tuple(storage_block_count(node) for node in (1, 2, 3))


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


def main():
    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    try:
        wait_for_channel(client.channel, "Data Service")
        wait_for_channel(client.metadata_channel, "Metadata Service")

        root_name = f"namespace-{uuid.uuid4().hex[:8]}"

        root = client.mkdir(root_name, OWNER_ID)

        assert root.type == "DIRECTORY", root.type
        print(f"OK   mkdir root: {root_name} ({root.entry_id})")

        root_listing = client.list_dir(OWNER_ID)

        assert any(
            entry.entry_id == root.entry_id for entry in root_listing.entries
        ), "root directory not listed"

        print(f"OK   ls root: {len(root_listing.entries)} entries")

        duplicate_name = f"duplicate-{uuid.uuid4().hex[:8]}"
        duplicate = client.mkdir(duplicate_name, OWNER_ID, root.entry_id)

        expect_rpc_error(
            lambda: client.mkdir(duplicate_name, OWNER_ID, root.entry_id),
            grpc.StatusCode.ALREADY_EXISTS,
            "duplicate mkdir rejected",
        )

        client.remove(duplicate.entry_id, OWNER_ID)

        docs = client.mkdir("docs", OWNER_ID, root.entry_id)

        root_listing = client.list_dir(OWNER_ID, root.entry_id)

        assert any(entry.entry_id == docs.entry_id for entry in root_listing.entries)

        docs_stat = client.stat(docs.entry_id, OWNER_ID)

        assert docs_stat.type == "DIRECTORY", docs_stat.type
        assert docs_stat.name == "docs", docs_stat.name

        print("OK   mkdir and stat nested directory")

        counts_before = block_counts()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            main_file = temporary_path / "main.bin"
            pattern = b"namespace payload\n"
            main_payload = (
                pattern * (5 * 1024 * 1024 // len(pattern) + 1)
            )[: 5 * 1024 * 1024 + 123]
            main_file.write_bytes(main_payload)

            main_upload = client.upload(main_file, OWNER_ID, docs.entry_id)

            assert main_upload.block_count == 2, main_upload.block_count

            counts_after_upload = block_counts()
            expected_after_upload = tuple(count + 2 for count in counts_before)

            assert counts_after_upload == expected_after_upload, (
                f"expected {expected_after_upload}, got {counts_after_upload}"
            )

            print(
                "OK   upload nested file: "
                f"{main_upload.block_count} blocks, {main_upload.size} bytes"
            )

            docs_listing = client.list_dir(OWNER_ID, docs.entry_id)

            assert any(
                entry.entry_id == main_upload.file_id
                for entry in docs_listing.entries
            )

            main_stat = client.stat(main_upload.file_id, OWNER_ID)

            assert main_stat.type == "FILE", main_stat.type
            assert main_stat.size == len(main_payload), main_stat.size
            assert main_stat.checksum == main_upload.checksum, main_stat.checksum

            print("OK   ls and stat nested file")

            removed = client.remove(main_upload.file_id, OWNER_ID)

            assert removed.success, removed.message

            expect_rpc_error(
                lambda: client.stat(main_upload.file_id, OWNER_ID),
                grpc.StatusCode.NOT_FOUND,
                "removed file stat rejected",
            )

            docs_listing = client.list_dir(OWNER_ID, docs.entry_id)

            assert not docs_listing.entries, docs_listing.entries

            counts_after_remove = block_counts()

            assert counts_after_remove == counts_before, (
                "physical blocks were not removed: "
                f"{counts_before} -> {counts_after_remove}"
            )

            print(f"OK   rm file removed physical blocks: {counts_after_remove}")

            blocked_file = temporary_path / "blocked.bin"
            blocked_file.write_bytes(b"blocking file")

            blocked_upload = client.upload(
                blocked_file, OWNER_ID, docs.entry_id
            )

            expect_rpc_error(
                lambda: client.remove(docs.entry_id, OWNER_ID),
                grpc.StatusCode.FAILED_PRECONDITION,
                "non-empty directory removal rejected",
            )

            assert client.stat(blocked_upload.file_id, OWNER_ID).type == "FILE"

            client.remove(blocked_upload.file_id, OWNER_ID)
            client.remove(docs.entry_id, OWNER_ID)

            expect_rpc_error(
                lambda: client.stat(docs.entry_id, OWNER_ID),
                grpc.StatusCode.NOT_FOUND,
                "removed directory stat rejected",
            )

            print("OK   rm empty directory")

        expect_rpc_error(
            lambda: client.remove("00000000-0000-0000-0000-000000000000", OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "unknown entry removal rejected",
        )

        client.remove(root.entry_id, OWNER_ID)

        expect_rpc_error(
            lambda: client.stat(root.entry_id, OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "removed root stat rejected",
        )

        print("OK   rm root directory")

        final_counts = block_counts()

        assert final_counts == counts_before, (
            "namespace tests left physical blocks behind: "
            f"{counts_before} -> {final_counts}"
        )

        print(f"OK   final physical block count restored: {final_counts}")

    finally:
        client.close()

    print("\nNamespace operation tests passed.")


if __name__ == "__main__":
    main()
