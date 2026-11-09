import hashlib
import os
import uuid
from concurrent import futures

import grpc

from src.common import (
    data_pb2,
    data_pb2_grpc,
    metadata_pb2,
    metadata_pb2_grpc,
    storage_pb2,
    storage_pb2_grpc,
)
from src.common.consistent_hash import ConsistentHash

BLOCK_SIZE = 4 * 1024 * 1024

GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_receive_message_length", 5 * 1024 * 1024),
    ("grpc.max_send_message_length", 5 * 1024 * 1024),
]


class DataServicer(data_pb2_grpc.DataServiceServicer):
    def __init__(self):

        self.metadata = metadata_pb2_grpc.MetadataServiceStub(
            grpc.insecure_channel("metadata:50051")
        )

        self.nodes = {
            "storage-1": "storage-1:50061",
            "storage-2": "storage-2:50061",
            "storage-3": "storage-3:50061",
        }

        self.storage_stubs = {
            node_id: storage_pb2_grpc.StorageNodeStub(
                grpc.insecure_channel(endpoint, options=GRPC_CHANNEL_OPTIONS)
            )
            for node_id, endpoint in self.nodes.items()
        }

        self.ring = ConsistentHash(list(self.nodes.keys()), virtual_nodes=150)

    def UploadFile(self, request_iterator, context):

        header = None
        buffer = bytearray()
        blocks = []

        file_id = str(uuid.uuid4())

        file_hash = hashlib.sha256()
        total_size = 0
        block_index = 0

        try:
            for request in request_iterator:
                if request.WhichOneof("payload") == "header":
                    if header is not None:
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT, "Multiple upload headers"
                        )

                    header = request.header

                    if (
                        not header.owner_id
                        or not header.filename
                        or "/" in header.filename
                    ):
                        context.abort(
                            grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload metadata"
                        )

                    continue

                if header is None:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Upload header required first"
                    )

                if request.WhichOneof("payload") != "chunk":
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload request"
                    )

                chunk = request.chunk.data

                if not chunk:
                    continue

                file_hash.update(chunk)
                total_size += len(chunk)
                buffer.extend(chunk)

                while len(buffer) >= BLOCK_SIZE:
                    block_data = bytes(buffer[:BLOCK_SIZE])
                    del buffer[:BLOCK_SIZE]

                    block = self._store_block(file_id, block_index, block_data)

                    blocks.append(block)

                    block_index += 1

            if header is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Missing upload header")

            if buffer:
                block = self._store_block(file_id, block_index, bytes(buffer))

                blocks.append(block)

            checksum = file_hash.hexdigest()

            commit = self.metadata.CommitFile(
                metadata_pb2.CommitFileRequest(
                    owner_id=header.owner_id,
                    parent_id=header.parent_id,
                    name=header.filename,
                    size=total_size,
                    checksum=checksum,
                    blocks=blocks,
                ),
                timeout=30,
            )

            return data_pb2.UploadFileResponse(
                success=True,
                file_id=commit.entry_id,
                filename=header.filename,
                size=total_size,
                checksum=checksum,
                block_count=len(blocks),
                message="File uploaded and committed",
            )

        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

        except Exception as error:
            context.abort(grpc.StatusCode.INTERNAL, str(error))

    def _store_block(self, file_id, block_index, data):

        block_id = str(uuid.uuid4())

        checksum = hashlib.sha256(data).hexdigest()

        placement_key = f"{file_id}:{block_index}"

        nodes = self.ring.get_nodes(placement_key, count=3)

        acknowledgements = []
        failures = []

        for node_id in nodes:
            try:
                response = self.storage_stubs[node_id].WriteBlock(
                    storage_pb2.WriteBlockRequest(
                        block_id=block_id, data=data, checksum=checksum, version=1
                    ),
                    timeout=15,
                )

                if response.success:
                    acknowledgements.append(node_id)
                else:
                    failures.append(f"{node_id}: rejected: {response.message}")

            except grpc.RpcError as error:
                failures.append(f"{node_id}: {error.code().name}: {error.details()}")

        if len(acknowledgements) < 2:
            raise RuntimeError(
                f"Replication quorum not reached for block {block_id} "
                f"(acks={acknowledgements}, failures={failures})"
            )

        return metadata_pb2.CommittedBlock(
            block_id=block_id,
            block_index=block_index,
            offset=block_index * BLOCK_SIZE,
            size=len(data),
            checksum=checksum,
            version=1,
            node_ids=acknowledgements,
        )

    def DownloadFile(self, request, context):

        try:
            locations = self.metadata.LocateBlocks(
                metadata_pb2.LocateBlocksRequest(
                    owner_id=request.owner_id, file_id=request.file_id
                ),
                timeout=15,
            )

            for block in locations.blocks:
                recovered = False

                for endpoint in block.node_endpoints:
                    try:
                        channel = grpc.insecure_channel(
                            endpoint, options=GRPC_CHANNEL_OPTIONS
                        )

                        stub = storage_pb2_grpc.StorageNodeStub(channel)

                        response = stub.ReadBlock(
                            storage_pb2.ReadBlockRequest(block_id=block.block_id),
                            timeout=15,
                        )

                        channel.close()

                        if not response.success:
                            continue

                        calculated = hashlib.sha256(response.data).hexdigest()

                        if calculated != block.checksum:
                            continue

                        yield data_pb2.DownloadFileResponse(
                            block_index=block.block_index, data=response.data
                        )

                        recovered = True
                        break

                    except grpc.RpcError:
                        continue

                if not recovered:
                    context.abort(
                        grpc.StatusCode.DATA_LOSS,
                        f"No valid replica for block {block.block_id}",
                    )

        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

    def Health(self, request, context):

        return data_pb2.HealthResponse(status="HEALTHY")


def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_receive_message_length", 5 * 1024 * 1024),
            ("grpc.max_send_message_length", 5 * 1024 * 1024),
        ],
    )

    data_pb2_grpc.add_DataServiceServicer_to_server(DataServicer(), server)

    server.add_insecure_port("[::]:50052")

    server.start()

    print("Data Service listening on port 50052")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
