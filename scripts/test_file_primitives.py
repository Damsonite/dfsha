import hashlib
import subprocess
import sys
import uuid

import grpc

from src.client.dfs_client import DFSClient
from src.common.path_utils import InvalidPath, join_path

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
    return tuple(storage_block_count(node) for node in (1, 2, 3, 4))


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

    raise AssertionError(
        f"{label}: expected {expected_code.name}, call succeeded"
    )


def expect_exception(action, expected_type, label):
    try:
        action()
    except expected_type as error:
        print(f"OK   {label}: {type(error).__name__}")
        return

    raise AssertionError(
        f"{label}: expected {expected_type.__name__}, call succeeded"
    )


def test_join_path():
    assert join_path("/", "a") == "/a"
    assert join_path("/a/b", "../c") == "/a/c"
    assert join_path("/a/b", "/x/./y") == "/x/y"
    assert join_path("/a/b", "") == "/a/b"

    print("OK   relative path resolution")


def main():
    test_join_path()

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    counts_before = block_counts()
    root_name = f"primitives-{uuid.uuid4().hex[:8]}"
    root_path = f"/{root_name}"

    try:
        client.mkdir_path(root_path, OWNER_ID)
        file_path = f"{root_path}/stream.bin"
        payload = b"line one\n" * 5000 + b"line two\n" * 5000

        expect_rpc_error(
            lambda: client.open(file_path, "r", OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "open missing file rejected",
        )

        handle = client.open(file_path, "w", OWNER_ID)
        written = 0

        for offset in range(0, len(payload), 777):
            chunk = payload[offset : offset + 777]
            written += handle.write(chunk)

        assert written == len(payload), written

        entry = handle.close()
        expected_checksum = hashlib.sha256(payload).hexdigest()

        assert entry.success, entry.message
        assert entry.size == len(payload), entry.size
        assert entry.checksum == expected_checksum, entry.checksum

        print("OK   open/write/close created file with valid checksum")

        expect_exception(
            lambda: client.open(file_path, "w", OWNER_ID),
            FileExistsError,
            "write mode rejects existing file",
        )

        reader = client.open(file_path, "r", OWNER_ID)
        first = reader.read(100)
        second = reader.read(len(payload) - 100)

        assert first + second == payload
        assert reader.read() == b""
        reader.close()

        print("OK   open/read returns the complete payload")

        reader = client.open(file_path, "r", OWNER_ID)

        assert reader.read() == payload
        reader.close()

        print("OK   read without size returns the complete payload")

        empty_path = f"{root_path}/empty.bin"
        empty_handle = client.open(empty_path, "w", OWNER_ID)
        empty_entry = empty_handle.close()

        assert empty_entry.size == 0, empty_entry.size

        empty_reader = client.open(empty_path, "r", OWNER_ID)
        assert empty_reader.read() == b""
        empty_reader.close()

        print("OK   empty file round trip")

        locked_path = f"{root_path}/locked.bin"
        lease_id = client.lock(locked_path, OWNER_ID)

        expect_rpc_error(
            lambda: client.lock(locked_path, OWNER_ID),
            grpc.StatusCode.ABORTED,
            "second lock on same name rejected",
        )

        expect_rpc_error(
            lambda: client.open(locked_path, "w", OWNER_ID),
            grpc.StatusCode.ABORTED,
            "open for write while locked rejected",
        )

        released = client.unlock(lease_id, OWNER_ID)

        assert released.success, released.message

        lease_id = client.lock(locked_path, OWNER_ID)
        released = client.unlock(lease_id, OWNER_ID)

        assert released.success, released.message

        print("OK   lock/unlock releases the name")

        handle = client.open(locked_path, "w", OWNER_ID)
        handle.write(b"locked payload")

        expect_rpc_error(
            lambda: client.open(locked_path, "w", OWNER_ID),
            grpc.StatusCode.ABORTED,
            "concurrent open while handle is open rejected",
        )

        handle.close()

        print("OK   open handle reserves the target until close")

        expect_rpc_error(
            lambda: client.remove_path(root_path, OWNER_ID),
            grpc.StatusCode.FAILED_PRECONDITION,
            "rmdir on non-empty directory rejected",
        )

        subdir_path = f"{root_path}/rmdir-target"
        client.mkdir_path(subdir_path, OWNER_ID)

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "src.client.cli",
                "rmdir",
                subdir_path,
                "--user",
                OWNER_ID,
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr

        expect_rpc_error(
            lambda: client.stat_path(subdir_path, OWNER_ID),
            grpc.StatusCode.NOT_FOUND,
            "rmdir removed the directory",
        )

        print("OK   rmdir command removes an empty directory")

        expect_exception(
            lambda: client.remove_path("/", OWNER_ID),
            InvalidPath,
            "root removal rejected",
        )

        client.remove_path(file_path, OWNER_ID)
        client.remove_path(empty_path, OWNER_ID)
        client.remove_path(locked_path, OWNER_ID)
        client.remove_path(root_path, OWNER_ID)

        counts_after = block_counts()

        assert counts_after == counts_before, (
            "file primitive tests left physical blocks behind: "
            f"{counts_before} -> {counts_after}"
        )

        print(f"OK   physical block count restored: {counts_after}")

    finally:
        client.close()

    print("\nFile primitive tests passed.")


if __name__ == "__main__":
    main()
