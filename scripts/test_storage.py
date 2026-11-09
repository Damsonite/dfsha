import hashlib
import sys

import grpc

from src.common import storage_pb2, storage_pb2_grpc
from src.common.auth import DEFAULT_STORAGE_TOKEN, ClientTokenInterceptor


def test_node(address):

    channel = grpc.intercept_channel(
        grpc.insecure_channel(address),
        ClientTokenInterceptor(DEFAULT_STORAGE_TOKEN),
    )

    grpc.channel_ready_future(channel).result(timeout=5)

    stub = storage_pb2_grpc.StorageNodeStub(channel)

    # 1. Health
    health = stub.Health(storage_pb2.HealthRequest())

    print(f"\nNodo: {health.node_id}")
    print(f"Estado: {health.status}")

    # 2. WriteBlock
    data = b"Este es un bloque de prueba de DFSha."

    checksum = hashlib.sha256(data).hexdigest()

    write_response = stub.WriteBlock(
        storage_pb2.WriteBlockRequest(
            block_id="block_test_001", data=data, checksum=checksum, version=1
        )
    )

    print("\nEscritura:")
    print(write_response)

    # 3. ReadBlock
    read_response = stub.ReadBlock(
        storage_pb2.ReadBlockRequest(block_id="block_test_001")
    )

    print("\nLectura:")
    print(read_response)

    assert read_response.data == data
    assert read_response.checksum == checksum

    # 4. BlockStatus
    status = stub.BlockStatus(storage_pb2.BlockStatusRequest(block_id="block_test_001"))

    print("\nEstado del bloque:")
    print(status)

    assert status.exists
    assert status.valid

    # 5. DeleteBlock
    delete_response = stub.DeleteBlock(
        storage_pb2.DeleteBlockRequest(block_id="block_test_001")
    )

    print("\nEliminación:")
    print(delete_response)

    channel.close()


if __name__ == "__main__":
    address = sys.argv[1] if len(sys.argv) > 1 else "localhost:50061"

    test_node(address)
