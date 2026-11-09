import hashlib
import subprocess
import tempfile
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient
from src.common import data_pb2, data_pb2_grpc, metadata_pb2, metadata_pb2_grpc
from src.common.auth import DEFAULT_CLIENT_TOKEN, ClientTokenInterceptor

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"
PAYLOAD_SIZE = 5 * 1024 * 1024 + 123
REPLICATION_FACTOR = 3


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

    raise AssertionError(f"{label}: expected {expected_code.name}, call succeeded")


def invalid_filename_upload(stub, parent_id, filename):
    def requests():
        yield data_pb2.UploadFileRequest(
            header=data_pb2.UploadHeader(
                owner_id=OWNER_ID, parent_id=parent_id, filename=filename
            )
        )
        yield data_pb2.UploadFileRequest(chunk=data_pb2.UploadChunk(data=b"x"))

    return stub.UploadFile(requests(), timeout=15)


def main():
    metadata_channel = grpc.intercept_channel(
        grpc.insecure_channel(METADATA_ENDPOINT),
        ClientTokenInterceptor(DEFAULT_CLIENT_TOKEN),
    )
    wait_for_channel(metadata_channel, "Metadata Service")

    metadata_stub = metadata_pb2_grpc.MetadataServiceStub(metadata_channel)

    directory_name = f"validation-{uuid.uuid4().hex[:8]}"

    directory = metadata_stub.Mkdir(
        metadata_pb2.MkdirRequest(owner_id=OWNER_ID, name=directory_name),
        timeout=10,
    )

    print(f"OK   directory created: {directory_name} ({directory.entry_id})")

    pattern = b"DFSha validation payload\n"
    payload = (pattern * (PAYLOAD_SIZE // len(pattern) + 1))[:PAYLOAD_SIZE]
    expected_checksum = hashlib.sha256(payload).hexdigest()

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_path = Path(temporary_directory)

        source = temporary_path / "payload.bin"
        source.write_bytes(payload)

        data_channel = grpc.intercept_channel(
            grpc.insecure_channel(DATA_ENDPOINT),
            ClientTokenInterceptor(DEFAULT_CLIENT_TOKEN),
        )
        wait_for_channel(data_channel, "Data Service")

        data_stub = data_pb2_grpc.DataServiceStub(data_channel)
        client = DFSClient(DATA_ENDPOINT)

        try:
            counts_before = block_counts()

            response = client.upload(source, OWNER_ID, directory.entry_id)

            assert response.success, response.message
            assert response.size == len(payload), response.size
            assert response.block_count == 2, response.block_count
            assert response.checksum == expected_checksum, response.checksum

            print(
                "OK   upload: "
                f"{response.block_count} blocks, {response.size} bytes, "
                f"sha256={response.checksum}"
            )

            destination = temporary_path / "downloaded.bin"

            client.download(response.file_id, OWNER_ID, destination)

            downloaded = destination.read_bytes()

            assert downloaded == payload, "downloaded bytes differ from source"

            print(
                "OK   download: bytes match, "
                f"sha256={hashlib.sha256(downloaded).hexdigest()}"
            )

            counts_after_upload = block_counts()
            expected_after_upload = (
                sum(counts_before) + response.block_count * REPLICATION_FACTOR
            )
            increments = [
                after - before
                for after, before in zip(counts_after_upload, counts_before)
            ]

            assert sum(counts_after_upload) == expected_after_upload, (
                f"expected {expected_after_upload} total blocks after upload, "
                f"got {sum(counts_after_upload)}"
            )

            assert all(
                0 <= increment <= response.block_count
                for increment in increments
            ), f"unexpected per-node block increments: {increments}"

            print(f"OK   replica count after upload: {counts_after_upload}")

            expect_rpc_error(
                lambda: client.upload(source, OWNER_ID, directory.entry_id),
                grpc.StatusCode.ALREADY_EXISTS,
                "duplicate upload rejected",
            )

            expect_rpc_error(
                lambda: client.upload(
                    source, OWNER_ID, "00000000-0000-0000-0000-000000000000"
                ),
                grpc.StatusCode.NOT_FOUND,
                "invalid parent rejected",
            )

            bad_owner = temporary_path / "bad-owner.bin"
            bad_owner.write_bytes(b"should not be stored")

            expect_rpc_error(
                lambda: client.upload(bad_owner, "missing-user", directory.entry_id),
                grpc.StatusCode.PERMISSION_DENIED,
                "owner mismatch rejected",
            )

            for invalid_name in ("", "nested/file.bin", ".", "x" * 256):
                expect_rpc_error(
                    lambda name=invalid_name: invalid_filename_upload(
                        data_stub, directory.entry_id, name
                    ),
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"invalid filename {invalid_name[:20]!r} rejected",
                )

            counts_after_failures = block_counts()

            assert counts_after_failures == counts_after_upload, (
                "failed uploads left orphan blocks: "
                f"{counts_after_upload} -> {counts_after_failures}"
            )

            print(f"OK   no orphan blocks: {counts_after_failures}")

            empty_source = temporary_path / "empty.bin"
            empty_source.write_bytes(b"")

            empty_response = client.upload(
                empty_source, OWNER_ID, directory.entry_id
            )

            assert empty_response.success, empty_response.message
            assert empty_response.size == 0, empty_response.size
            assert empty_response.block_count == 0, empty_response.block_count
            assert empty_response.checksum == hashlib.sha256(b"").hexdigest()

            empty_destination = temporary_path / "empty-out.bin"

            client.download(empty_response.file_id, OWNER_ID, empty_destination)

            assert empty_destination.read_bytes() == b""

            print("OK   empty file round trip")

        finally:
            client.close()
            data_channel.close()

    metadata_channel.close()

    print("\nUpload validation tests passed.")


if __name__ == "__main__":
    main()
