import tempfile
import uuid
from pathlib import Path

import grpc

from src.client.dfs_client import DFSClient
from src.common import (
    data_pb2,
    data_pb2_grpc,
    metadata_pb2,
    metadata_pb2_grpc,
    storage_pb2,
    storage_pb2_grpc,
)
from src.common.auth import (
    DEFAULT_CLIENT_TOKEN,
    DEFAULT_SECOND_USER_TOKEN,
    DEFAULT_SERVICE_TOKEN,
    DEFAULT_STORAGE_TOKEN,
    ClientTokenInterceptor,
)

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
STORAGE_ENDPOINT = "localhost:50061"
OWNER_ID = "user-001"


def wait_for_channel(channel, name):
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError as error:
        raise RuntimeError(f"{name} is not reachable") from error


def channel_with_token(endpoint, token):
    return grpc.intercept_channel(
        grpc.insecure_channel(endpoint),
        ClientTokenInterceptor(token),
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


def main():
    raw_metadata_channel = grpc.insecure_channel(METADATA_ENDPOINT)
    wait_for_channel(raw_metadata_channel, "Metadata Service")
    raw_metadata = metadata_pb2_grpc.MetadataServiceStub(raw_metadata_channel)

    raw_data_channel = grpc.insecure_channel(DATA_ENDPOINT)
    wait_for_channel(raw_data_channel, "Data Service")
    raw_data = data_pb2_grpc.DataServiceStub(raw_data_channel)

    raw_storage_channel = grpc.insecure_channel(STORAGE_ENDPOINT)
    wait_for_channel(raw_storage_channel, "Storage Node")
    raw_storage = storage_pb2_grpc.StorageNodeStub(raw_storage_channel)

    invalid_metadata_channel = channel_with_token(
        METADATA_ENDPOINT, "invalid-token"
    )
    invalid_metadata = metadata_pb2_grpc.MetadataServiceStub(
        invalid_metadata_channel
    )

    user2_metadata_channel = channel_with_token(
        METADATA_ENDPOINT, DEFAULT_SECOND_USER_TOKEN
    )
    user2_metadata = metadata_pb2_grpc.MetadataServiceStub(
        user2_metadata_channel
    )

    service_metadata_channel = channel_with_token(
        METADATA_ENDPOINT, DEFAULT_SERVICE_TOKEN
    )
    service_metadata = metadata_pb2_grpc.MetadataServiceStub(
        service_metadata_channel
    )

    user1_data_channel = channel_with_token(
        DATA_ENDPOINT, DEFAULT_CLIENT_TOKEN
    )
    user1_data = data_pb2_grpc.DataServiceStub(user1_data_channel)

    service_data_channel = channel_with_token(
        DATA_ENDPOINT, DEFAULT_SERVICE_TOKEN
    )
    service_data = data_pb2_grpc.DataServiceStub(service_data_channel)

    authenticated_storage_channel = channel_with_token(
        STORAGE_ENDPOINT, DEFAULT_STORAGE_TOKEN
    )
    authenticated_storage = storage_pb2_grpc.StorageNodeStub(
        authenticated_storage_channel
    )

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)
    user2_client = DFSClient(
        DATA_ENDPOINT,
        METADATA_ENDPOINT,
        token=DEFAULT_SECOND_USER_TOKEN,
    )

    try:
        assert raw_metadata.Health(
            metadata_pb2.HealthRequest(), timeout=10
        ).status == "HEALTHY"

        assert raw_data.Health(
            data_pb2.HealthRequest(), timeout=10
        ).status == "HEALTHY"

        assert raw_storage.Health(
            storage_pb2.HealthRequest(), timeout=10
        ).status == "HEALTHY"

        print("OK   health endpoints are public")

        root_name = f"auth-{uuid.uuid4().hex[:8]}"
        root_path = f"/{root_name}"
        root = client.mkdir_path(root_path, OWNER_ID)

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            private_file = temporary_path / "private.bin"
            private_file.write_bytes(b"private data\n" * 1000)

            uploaded = client.upload(
                private_file, OWNER_ID, root.entry_id
            )

            assert client.stat(uploaded.file_id, OWNER_ID).type == "FILE"

            print("OK   owner can access own file")

            expect_rpc_error(
                lambda: raw_metadata.Stat(
                    metadata_pb2.StatRequest(
                        owner_id=OWNER_ID, entry_id=uploaded.file_id
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.UNAUTHENTICATED,
                "metadata rejects missing token",
            )

            expect_rpc_error(
                lambda: invalid_metadata.Stat(
                    metadata_pb2.StatRequest(
                        owner_id=OWNER_ID, entry_id=uploaded.file_id
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.UNAUTHENTICATED,
                "metadata rejects invalid token",
            )

            expect_rpc_error(
                lambda: raw_data.RemoveEntry(
                    data_pb2.RemoveEntryRequest(
                        owner_id=OWNER_ID, entry_id=uploaded.file_id
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.UNAUTHENTICATED,
                "data service rejects missing token",
            )

            expect_rpc_error(
                lambda: user2_metadata.Stat(
                    metadata_pb2.StatRequest(
                        owner_id=OWNER_ID, entry_id=uploaded.file_id
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.PERMISSION_DENIED,
                "user-002 cannot stat user-001 entry",
            )

            expect_rpc_error(
                lambda: user2_metadata.ListDir(
                    metadata_pb2.ListDirRequest(
                        owner_id=OWNER_ID, parent_id=root.entry_id
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.PERMISSION_DENIED,
                "user-002 cannot list user-001 directory",
            )

            expect_rpc_error(
                lambda: user2_client.resolve_path(root_path, OWNER_ID),
                grpc.StatusCode.PERMISSION_DENIED,
                "user-002 cannot resolve user-001 path",
            )

            expect_rpc_error(
                lambda: user2_client.upload(
                    private_file, OWNER_ID, root.entry_id
                ),
                grpc.StatusCode.PERMISSION_DENIED,
                "user-002 cannot upload into user-001 directory",
            )

            expect_rpc_error(
                lambda: user2_client.remove(uploaded.file_id, OWNER_ID),
                grpc.StatusCode.PERMISSION_DENIED,
                "user-002 cannot remove user-001 file",
            )

            expect_rpc_error(
                lambda: user1_data.Repair(
                    data_pb2.RepairRequest(target_replicas=3), timeout=120
                ),
                grpc.StatusCode.PERMISSION_DENIED,
                "user token cannot run repair",
            )

            expect_rpc_error(
                lambda: client.metadata_stub.RegisterNode(
                    metadata_pb2.RegisterNodeRequest(
                        node_id="storage-1",
                        endpoint="storage-1:50061",
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.PERMISSION_DENIED,
                "user token cannot call service-only metadata RPC",
            )

            expect_rpc_error(
                lambda: raw_storage.BlockStatus(
                    storage_pb2.BlockStatusRequest(
                        block_id="auth-test-block"
                    ),
                    timeout=10,
                ),
                grpc.StatusCode.UNAUTHENTICATED,
                "storage node rejects missing token",
            )

            storage_status = authenticated_storage.BlockStatus(
                storage_pb2.BlockStatusRequest(block_id="auth-test-block"),
                timeout=10,
            )

            assert not storage_status.exists, storage_status

            service_status = service_metadata.SetNodeStatus(
                metadata_pb2.SetNodeStatusRequest(
                    node_id="storage-1", status="ACTIVE"
                ),
                timeout=10,
            )

            assert service_status.success, service_status.message

            repair_result = service_data.Repair(
                data_pb2.RepairRequest(target_replicas=3), timeout=120
            )

            assert repair_result.success, list(repair_result.failures)

            print("OK   service token can run maintenance RPCs")

            client.remove(uploaded.file_id, OWNER_ID)
            client.remove_path(root_path, OWNER_ID)

    finally:
        client.close()
        user2_client.close()
        raw_metadata_channel.close()
        raw_data_channel.close()
        raw_storage_channel.close()
        invalid_metadata_channel.close()
        user2_metadata_channel.close()
        service_metadata_channel.close()
        user1_data_channel.close()
        service_data_channel.close()
        authenticated_storage_channel.close()

    print("\nAuthentication and authorization tests passed.")


if __name__ == "__main__":
    main()
