import hashlib
import subprocess
import tempfile
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient, DownloadIntegrityError
from src.common.path_utils import InvalidPath

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"


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


def expect_exception(action, expected_type, label):
    try:
        action()
    except expected_type as error:
        print(f"OK   {label}: {type(error).__name__}")
        return

    raise AssertionError(f"{label}: expected {expected_type.__name__}")


def main():
    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    try:
        grpc.channel_ready_future(client.channel).result(timeout=10)
        grpc.channel_ready_future(client.metadata_channel).result(timeout=10)

        root_name = f"paths-{uuid.uuid4().hex[:8]}"
        root_path = f"/{root_name}"

        root = client.mkdir_path(root_path, OWNER_ID)

        assert root.type == "DIRECTORY", root.type
        print(f"OK   mkdir path: {root_path} ({root.entry_id})")

        nested_path = f"{root_path}/a/b"

        expect_rpc_error(
            lambda: client.mkdir_path(nested_path, OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "mkdir without parents rejected",
        )

        nested = client.mkdir_path(
            nested_path, OWNER_ID, create_parents=True
        )

        assert nested.type == "DIRECTORY", nested.type
        assert nested.name == "b", nested.name

        print(f"OK   mkdir -p: {nested_path}")

        existing = client.mkdir_path(
            nested_path, OWNER_ID, create_parents=True
        )

        assert existing.entry_id == nested.entry_id, existing.entry_id

        print("OK   mkdir -p is idempotent")

        expect_rpc_error(
            lambda: client.mkdir_path(nested_path, OWNER_ID),
            grpc.StatusCode.ALREADY_EXISTS,
            "duplicate mkdir rejected",
        )

        root_entry = client.resolve_path("/", OWNER_ID)

        assert root_entry.entry_id == "", root_entry.entry_id
        assert root_entry.type == "DIRECTORY", root_entry.type

        print("OK   resolve root path")

        normalized = client.resolve_path(
            f"{root_path}//a/./b", OWNER_ID
        )

        assert normalized.entry_id == nested.entry_id, normalized.entry_id

        parent_normalized = client.resolve_path(
            f"{root_path}/a/../a/b", OWNER_ID
        )

        assert parent_normalized.entry_id == nested.entry_id, (
            parent_normalized.entry_id
        )

        print("OK   path normalization with '.' and '..'")

        expect_rpc_error(
            lambda: client.resolve_path(f"{root_path}/missing", OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "missing path rejected",
        )

        expect_rpc_error(
            lambda: client.resolve_path("/" + "x" * 256, OWNER_ID),
            grpc.StatusCode.INVALID_ARGUMENT,
            "overlong path component rejected",
        )

        counts_before = block_counts()

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            report = temporary_path / "report.bin"
            report_payload = b"path report\n" * 512
            report.write_bytes(report_payload)

            uploaded = client.upload_path(
                report, f"{nested_path}/report.bin", OWNER_ID
            )

            assert uploaded.filename == "report.bin", uploaded.filename
            assert uploaded.checksum == hashlib.sha256(
                report_payload
            ).hexdigest()

            print("OK   upload to explicit file path")

            copy = temporary_path / "copy.bin"
            copy_payload = b"directory target\n" * 128
            copy.write_bytes(copy_payload)

            copied = client.upload_path(copy, f"{nested_path}/", OWNER_ID)

            assert copied.filename == "copy.bin", copied.filename

            copy2 = temporary_path / "copy2.bin"
            copy2_payload = b"existing directory target\n" * 64
            copy2.write_bytes(copy2_payload)

            copied2 = client.upload_path(copy2, nested_path, OWNER_ID)

            assert copied2.filename == "copy2.bin", copied2.filename

            print("OK   upload to directory paths")

            listing = client.list_dir_path(nested_path, OWNER_ID)

            names = {entry.name for entry in listing.entries}

            assert names == {"report.bin", "copy.bin", "copy2.bin"}, names

            print("OK   ls by path")

            report_stat = client.stat_path(
                f"{nested_path}/report.bin", OWNER_ID
            )

            assert report_stat.type == "FILE", report_stat.type
            assert report_stat.size == len(report_payload), report_stat.size

            print("OK   stat by path")

            destination = temporary_path / "report-out.bin"

            client.download_path(
                f"{nested_path}/report.bin", OWNER_ID, destination
            )

            assert destination.read_bytes() == report_payload

            print("OK   download by path")

            expect_rpc_error(
                lambda: client.resolve_path(
                    f"{nested_path}/report.bin/child", OWNER_ID
                ),
                grpc.StatusCode.FAILED_PRECONDITION,
                "path through a file rejected",
            )

            expect_exception(
                lambda: client.list_dir_path(
                    f"{nested_path}/report.bin", OWNER_ID
                ),
                NotADirectoryError,
                "ls on a file rejected",
            )

            expect_exception(
                lambda: client.download_path(
                    nested_path, OWNER_ID, temporary_path / "dir.bin"
                ),
                DownloadIntegrityError,
                "download of a directory rejected",
            )

            expect_rpc_error(
                lambda: client.upload_path(
                    copy,
                    f"{root_path}/missing/parent/copy.bin",
                    OWNER_ID,
                ),
                grpc.StatusCode.NOT_FOUND,
                "upload with missing parent rejected",
            )

            removed = client.remove_path(
                f"{nested_path}/report.bin", OWNER_ID
            )

            assert removed.success, removed.message

            expect_rpc_error(
                lambda: client.stat_path(
                    f"{nested_path}/report.bin", OWNER_ID
                ),
                grpc.StatusCode.NOT_FOUND,
                "removed path stat rejected",
            )

            print("OK   rm by path")

            expect_rpc_error(
                lambda: client.remove_path(nested_path, OWNER_ID),
                grpc.StatusCode.FAILED_PRECONDITION,
                "non-empty path removal rejected",
            )

            client.remove_path(f"{nested_path}/copy.bin", OWNER_ID)
            client.remove_path(f"{nested_path}/copy2.bin", OWNER_ID)
            client.remove_path(nested_path, OWNER_ID)
            client.remove_path(f"{root_path}/a", OWNER_ID)
            client.remove_path(root_path, OWNER_ID)

            print("OK   rm nested directories by path")

        expect_exception(
            lambda: client.remove_path("/", OWNER_ID),
            InvalidPath,
            "root removal rejected",
        )

        counts_after = block_counts()

        assert counts_after == counts_before, (
            "path tests left physical blocks behind: "
            f"{counts_before} -> {counts_after}"
        )

        print(f"OK   physical block count restored: {counts_after}")

    finally:
        client.close()

    print("\nPath operation tests passed.")


if __name__ == "__main__":
    main()
