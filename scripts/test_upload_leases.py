import hashlib
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent import futures
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient
from src.common import metadata_pb2, metadata_pb2_grpc

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


def acquire(stub, parent_id, name, ttl_seconds=60):
    return stub.AcquireUploadLease(
        metadata_pb2.AcquireUploadLeaseRequest(
            owner_id=OWNER_ID,
            parent_id=parent_id,
            name=name,
            ttl_seconds=ttl_seconds,
        ),
        timeout=10,
    )


def release(stub, lease_id):
    return stub.ReleaseUploadLease(
        metadata_pb2.ReleaseUploadLeaseRequest(
            owner_id=OWNER_ID,
            lease_id=lease_id,
        ),
        timeout=10,
    )


def main():
    metadata_channel = grpc.insecure_channel(METADATA_ENDPOINT)
    wait_for_channel(metadata_channel, "Metadata Service")
    metadata_stub = metadata_pb2_grpc.MetadataServiceStub(metadata_channel)

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

    try:
        wait_for_channel(client.channel, "Data Service")

        root_name = f"leases-{uuid.uuid4().hex[:8]}"
        root_path = f"/{root_name}"
        root = client.mkdir_path(root_path, OWNER_ID)

        lease_a = acquire(metadata_stub, root.entry_id, "leased.bin")

        assert lease_a.success, lease_a.message
        assert lease_a.lease_id, lease_a.lease_id
        assert lease_a.expires_at > int(time.time()), lease_a.expires_at

        print(f"OK   lease acquired: {lease_a.lease_id}")

        expect_rpc_error(
            lambda: acquire(metadata_stub, root.entry_id, "leased.bin"),
            grpc.StatusCode.ABORTED,
            "concurrent lease rejected",
        )

        lease_b = acquire(metadata_stub, root.entry_id, "other.bin")
        release(metadata_stub, lease_b.lease_id)

        print("OK   independent target lease acquired and released")

        renewed = metadata_stub.RenewUploadLease(
            metadata_pb2.RenewUploadLeaseRequest(
                owner_id=OWNER_ID,
                lease_id=lease_a.lease_id,
                ttl_seconds=120,
            ),
            timeout=10,
        )

        assert renewed.success, renewed.message
        assert renewed.expires_at >= lease_a.expires_at, (
            lease_a.expires_at,
            renewed.expires_at,
        )

        print("OK   lease renewed")

        release(metadata_stub, lease_a.lease_id)
        release(metadata_stub, lease_a.lease_id)

        reacquired = acquire(metadata_stub, root.entry_id, "leased.bin")
        release(metadata_stub, reacquired.lease_id)

        print("OK   release is idempotent and target can be reused")

        expiring = acquire(
            metadata_stub, root.entry_id, "expiring.bin", ttl_seconds=1
        )

        time.sleep(2)

        expect_rpc_error(
            lambda: metadata_stub.RenewUploadLease(
                metadata_pb2.RenewUploadLeaseRequest(
                    owner_id=OWNER_ID,
                    lease_id=expiring.lease_id,
                    ttl_seconds=60,
                ),
                timeout=10,
            ),
            grpc.StatusCode.DEADLINE_EXCEEDED,
            "expired lease renewal rejected",
        )

        replacement = acquire(metadata_stub, root.entry_id, "expiring.bin")

        assert replacement.lease_id != expiring.lease_id, replacement.lease_id

        release(metadata_stub, replacement.lease_id)

        print("OK   expired lease was replaced")

        empty_checksum = hashlib.sha256(b"").hexdigest()

        expect_rpc_error(
            lambda: metadata_stub.CommitFile(
                metadata_pb2.CommitFileRequest(
                    owner_id=OWNER_ID,
                    parent_id=root.entry_id,
                    name="invalid-lease.bin",
                    size=0,
                    checksum=empty_checksum,
                    lease_id="",
                ),
                timeout=10,
            ),
            grpc.StatusCode.FAILED_PRECONDITION,
            "commit without lease rejected",
        )

        expect_rpc_error(
            lambda: metadata_stub.CommitFile(
                metadata_pb2.CommitFileRequest(
                    owner_id=OWNER_ID,
                    parent_id=root.entry_id,
                    name="invalid-lease.bin",
                    size=0,
                    checksum=empty_checksum,
                    lease_id=str(uuid.uuid4()),
                ),
                timeout=10,
            ),
            grpc.StatusCode.FAILED_PRECONDITION,
            "commit with unknown lease rejected",
        )

        expect_rpc_error(
            lambda: client.resolve_path(
                f"{root_path}/invalid-lease.bin", OWNER_ID
            ),
            grpc.StatusCode.NOT_FOUND,
            "rejected commit created no entry",
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            barrier = threading.Barrier(2)

            payloads = {
                "first": b"first concurrent payload\n" * 50000,
                "second": b"second concurrent payload\n" * 50000,
            }

            def attempt(label):
                local_file = temporary_path / f"race-{label}.bin"
                local_file.write_bytes(payloads[label])

                thread_client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)

                try:
                    barrier.wait(timeout=10)

                    response = thread_client.upload(
                        local_file,
                        OWNER_ID,
                        root.entry_id,
                        filename="race.bin",
                    )

                    return label, response, None
                except grpc.RpcError as error:
                    return label, None, error
                finally:
                    thread_client.close()

            counts_before = block_counts()

            with futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(attempt, payloads))

            successes = [result for result in results if result[1] is not None]

            failures = [result for result in results if result[2] is not None]

            assert len(successes) == 1, successes
            assert len(failures) == 1, failures

            winning_label, winning_response, _ = successes[0]
            losing_label, _, losing_error = failures[0]

            assert losing_error.code() in (
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.ALREADY_EXISTS,
            ), (losing_error.code(), losing_error.details())

            print(
                "OK   concurrent upload: "
                f"{winning_label} won, {losing_label} got "
                f"{losing_error.code().name}"
            )

            counts_after_race = block_counts()
            expected_after_race = tuple(count + 1 for count in counts_before)

            assert counts_after_race == expected_after_race, (
                f"expected {expected_after_race}, got {counts_after_race}"
            )

            print(f"OK   no orphan blocks from concurrent upload: {counts_after_race}")

            listing = client.list_dir_path(root_path, OWNER_ID)

            race_entries = [
                entry for entry in listing.entries if entry.name == "race.bin"
            ]

            assert len(race_entries) == 1, race_entries

            destination = temporary_path / "race-out.bin"

            client.download(
                race_entries[0].entry_id, OWNER_ID, destination
            )

            assert destination.read_bytes() == payloads[winning_label]

            print("OK   committed concurrent upload matches winning payload")

            client.remove(race_entries[0].entry_id, OWNER_ID)

            counts_after_remove = block_counts()

            assert counts_after_remove == counts_before, (
                f"{counts_before} -> {counts_after_remove}"
            )

            print(f"OK   physical block count restored: {counts_after_remove}")

        client.remove_path(root_path, OWNER_ID)

    finally:
        client.close()
        metadata_channel.close()

    print("\nUpload lease tests passed.")


if __name__ == "__main__":
    main()
