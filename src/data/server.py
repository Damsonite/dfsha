import hashlib
import logging
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
REPLICATION_FACTOR = 3
WRITE_QUORUM = 2
MAX_FILENAME_LENGTH = 255
DEFAULT_MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024

GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_receive_message_length", 5 * 1024 * 1024),
    ("grpc.max_send_message_length", 5 * 1024 * 1024),
]

logger = logging.getLogger(__name__)


class UploadRejected(Exception):
    """Internal control-flow exception for a rejected streaming upload."""

    def __init__(self, code, details, cleanup=True):
        super().__init__(details)
        self.code = code
        self.details = details
        self.cleanup = cleanup


class DownloadRejected(Exception):
    """Internal control-flow exception for a rejected streaming download."""

    def __init__(self, code, details):
        super().__init__(details)
        self.code = code
        self.details = details


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

        self.storage_stubs_by_endpoint = {
            endpoint: self.storage_stubs[node_id]
            for node_id, endpoint in self.nodes.items()
        }

        self.ring = ConsistentHash(list(self.nodes.keys()), virtual_nodes=150)

        self.max_upload_bytes = self._read_max_upload_bytes()

    @staticmethod
    def _read_max_upload_bytes():
        raw_value = os.getenv("MAX_UPLOAD_BYTES", str(DEFAULT_MAX_UPLOAD_BYTES))

        try:
            value = int(raw_value)
        except ValueError as error:
            raise RuntimeError(
                f"Invalid MAX_UPLOAD_BYTES={raw_value!r}: must be an integer"
            ) from error

        if value <= 0:
            raise RuntimeError(
                f"Invalid MAX_UPLOAD_BYTES={raw_value!r}: must be greater than zero"
            )

        return value

    def UploadFile(self, request_iterator, context):

        header = None
        buffer = bytearray()
        blocks = []
        written_blocks = []

        file_id = str(uuid.uuid4())

        file_hash = hashlib.sha256()
        total_size = 0
        block_index = 0

        try:
            for request in request_iterator:
                payload = request.WhichOneof("payload")

                if payload == "header":
                    if header is not None:
                        raise UploadRejected(
                            grpc.StatusCode.INVALID_ARGUMENT,
                            "Multiple upload headers",
                        )

                    header = request.header

                    self._validate_upload_header(header)
                    self._prepare_upload(header)

                    continue

                if header is None:
                    raise UploadRejected(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Upload header required first",
                    )

                if payload != "chunk":
                    raise UploadRejected(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload request"
                    )

                chunk = request.chunk.data

                if not chunk:
                    continue

                if total_size + len(chunk) > self.max_upload_bytes:
                    raise UploadRejected(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        f"Upload exceeds maximum size of {self.max_upload_bytes} bytes",
                    )

                file_hash.update(chunk)
                total_size += len(chunk)
                buffer.extend(chunk)

                while len(buffer) >= BLOCK_SIZE:
                    block_data = bytes(buffer[:BLOCK_SIZE])
                    del buffer[:BLOCK_SIZE]

                    block = self._store_block(
                        file_id, block_index, block_data, written_blocks
                    )

                    blocks.append(block)

                    block_index += 1

            if header is None:
                raise UploadRejected(
                    grpc.StatusCode.INVALID_ARGUMENT, "Missing upload header"
                )

            if buffer:
                block = self._store_block(
                    file_id, block_index, bytes(buffer), written_blocks
                )

                blocks.append(block)

            checksum = file_hash.hexdigest()

            try:
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
            except grpc.RpcError as error:
                # A deadline or transport failure may happen after the metadata
                # transaction committed. Deleting the blocks in that case would
                # leave a committed entry pointing to missing data, so retain
                # them for the future recovery/GC stage.
                ambiguous = error.code() in (
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.INTERNAL,
                    grpc.StatusCode.UNKNOWN,
                )
                raise UploadRejected(
                    error.code(), error.details(), cleanup=not ambiguous
                ) from error

            if not commit.success:
                raise UploadRejected(
                    grpc.StatusCode.INTERNAL,
                    commit.message or "File commit failed",
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

        except UploadRejected as error:
            if error.cleanup:
                self._cleanup_blocks(written_blocks)
            else:
                logger.warning(
                    "Commit status unknown; retaining %d block(s) for recovery",
                    len(written_blocks),
                )

            context.abort(error.code, error.details)

        except grpc.RpcError as error:
            self._cleanup_blocks(written_blocks)

            context.abort(error.code(), error.details())

        except Exception as error:
            self._cleanup_blocks(written_blocks)

            context.abort(grpc.StatusCode.INTERNAL, f"Upload failed: {error}")

    @staticmethod
    def _validate_upload_header(header):

        if not header.owner_id:
            raise UploadRejected(
                grpc.StatusCode.INVALID_ARGUMENT, "owner_id is required"
            )

        name = header.filename

        if (
            not name
            or len(name) > MAX_FILENAME_LENGTH
            or "/" in name
            or "\x00" in name
            or name in (".", "..")
        ):
            raise UploadRejected(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid filename"
            )

    def _prepare_upload(self, header):

        try:
            response = self.metadata.PrepareUpload(
                metadata_pb2.PrepareUploadRequest(
                    owner_id=header.owner_id,
                    parent_id=header.parent_id,
                    name=header.filename,
                ),
                timeout=15,
            )
        except grpc.RpcError as error:
            raise UploadRejected(error.code(), error.details()) from error

        if not response.success:
            raise UploadRejected(
                grpc.StatusCode.FAILED_PRECONDITION,
                response.message or "Upload validation failed",
            )

    def _cleanup_blocks(self, written_blocks):

        for block_id, node_ids in written_blocks:
            for node_id in node_ids:
                try:
                    self.storage_stubs[node_id].DeleteBlock(
                        storage_pb2.DeleteBlockRequest(block_id=block_id),
                        timeout=5,
                    )
                except Exception as error:
                    logger.warning(
                        "Could not delete block %s from %s during upload cleanup: %s",
                        block_id,
                        node_id,
                        error,
                    )

    def _store_block(self, file_id, block_index, data, written_blocks):

        block_id = str(uuid.uuid4())

        checksum = hashlib.sha256(data).hexdigest()

        placement_key = f"{file_id}:{block_index}"

        nodes = self.ring.get_nodes(placement_key, count=REPLICATION_FACTOR)

        # Record every candidate node before writing. Cleanup is deliberately
        # broader than the acknowledged replicas: a timed-out write may still
        # have created a block on a node that is not in the quorum.
        written_blocks.append((block_id, list(nodes)))

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

        if len(acknowledgements) < WRITE_QUORUM:
            raise UploadRejected(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Replication quorum not reached for block {block_id} "
                f"(acks={acknowledgements}, failures={failures})",
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
            metadata = self.metadata.Stat(
                metadata_pb2.StatRequest(
                    owner_id=request.owner_id, entry_id=request.file_id
                ),
                timeout=15,
            )

            if metadata.type != "FILE":
                raise DownloadRejected(
                    grpc.StatusCode.FAILED_PRECONDITION, "Entry is not a file"
                )

            if metadata.size < 0:
                raise DownloadRejected(
                    grpc.StatusCode.DATA_LOSS, "Invalid file size in metadata"
                )

            if len(metadata.checksum) != 64:
                raise DownloadRejected(
                    grpc.StatusCode.DATA_LOSS,
                    "File checksum is missing or invalid",
                )

            locations = self.metadata.LocateBlocks(
                metadata_pb2.LocateBlocksRequest(
                    owner_id=request.owner_id, file_id=request.file_id
                ),
                timeout=15,
            )

            blocks = list(locations.blocks)

            if metadata.size == 0:
                if blocks:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS, "Empty file has blocks"
                    )

                if metadata.checksum != hashlib.sha256(b"").hexdigest():
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        "Empty file checksum does not match metadata",
                    )

                return

            if not blocks:
                raise DownloadRejected(
                    grpc.StatusCode.DATA_LOSS, "File has no blocks"
                )

            expected_index = 0
            expected_offset = 0
            total_size = 0
            file_hash = hashlib.sha256()

            for block in blocks:
                if block.block_index != expected_index:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Unexpected block index {block.block_index}, "
                        f"expected {expected_index}",
                    )

                if block.offset != expected_offset:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Invalid block offset {block.offset}, "
                        f"expected {expected_offset}",
                    )

                if block.size <= 0 or block.size > BLOCK_SIZE:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Invalid block size {block.size}",
                    )

                if not block.node_endpoints:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Block {block.block_id} has no ready replicas",
                    )

                data, failures = self._read_block(block)

                if data is None:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"No valid replica for block {block.block_id}: {failures}",
                    )

                if len(data) != block.size:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Block {block.block_id} has {len(data)} bytes, "
                        f"expected {block.size}",
                    )

                if hashlib.sha256(data).hexdigest() != block.checksum:
                    raise DownloadRejected(
                        grpc.StatusCode.DATA_LOSS,
                        f"Block {block.block_id} checksum mismatch",
                    )

                file_hash.update(data)
                total_size += len(data)
                expected_offset += block.size
                expected_index += 1

                yield data_pb2.DownloadFileResponse(
                    block_index=block.block_index, data=data
                )

            if total_size != metadata.size:
                raise DownloadRejected(
                    grpc.StatusCode.DATA_LOSS,
                    f"Downloaded {total_size} bytes, expected {metadata.size}",
                )

            if file_hash.hexdigest() != metadata.checksum:
                raise DownloadRejected(
                    grpc.StatusCode.DATA_LOSS,
                    "File checksum mismatch after download",
                )

        except DownloadRejected as error:
            context.abort(error.code, error.details)

        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

    def _read_block(self, block):

        failures = []

        for endpoint in block.node_endpoints:
            channel = grpc.insecure_channel(
                endpoint, options=GRPC_CHANNEL_OPTIONS
            )

            try:
                stub = storage_pb2_grpc.StorageNodeStub(channel)

                response = stub.ReadBlock(
                    storage_pb2.ReadBlockRequest(block_id=block.block_id),
                    timeout=15,
                )
            except grpc.RpcError as error:
                failures.append(
                    f"{endpoint}: {error.code().name}: {error.details()}"
                )
                continue
            finally:
                channel.close()

            if not response.success:
                failures.append(f"{endpoint}: rejected: {response.message}")
                continue

            if response.block_id != block.block_id:
                failures.append(
                    f"{endpoint}: returned block {response.block_id}, "
                    f"expected {block.block_id}"
                )
                continue

            if response.checksum != block.checksum:
                failures.append(f"{endpoint}: checksum metadata mismatch")
                continue

            if len(response.data) != block.size:
                failures.append(
                    f"{endpoint}: returned {len(response.data)} bytes, "
                    f"expected {block.size}"
                )
                continue

            if hashlib.sha256(response.data).hexdigest() != block.checksum:
                failures.append(f"{endpoint}: data checksum mismatch")
                continue

            return response.data, failures

        return None, failures

    def RemoveEntry(self, request, context):

        try:
            entry = self.metadata.Stat(
                metadata_pb2.StatRequest(
                    owner_id=request.owner_id, entry_id=request.entry_id
                ),
                timeout=15,
            )

            if entry.type == "FILE":
                locations = self.metadata.LocateBlocks(
                    metadata_pb2.LocateBlocksRequest(
                        owner_id=request.owner_id, file_id=request.entry_id
                    ),
                    timeout=15,
                )

                failures = []

                for block in locations.blocks:
                    if not block.node_endpoints:
                        failures.append(
                            f"{block.block_id}: no ready replicas"
                        )
                        continue

                    for endpoint in block.node_endpoints:
                        stub = self.storage_stubs_by_endpoint.get(endpoint)

                        if stub is None:
                            failures.append(
                                f"{endpoint}: unknown storage endpoint"
                            )
                            continue

                        try:
                            response = stub.DeleteBlock(
                                storage_pb2.DeleteBlockRequest(
                                    block_id=block.block_id
                                ),
                                timeout=15,
                            )

                            if not response.success:
                                failures.append(
                                    f"{endpoint}: {response.message}"
                                )
                        except grpc.RpcError as error:
                            failures.append(
                                f"{endpoint}: {error.code().name}: "
                                f"{error.details()}"
                            )

                if failures:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "Could not delete all block replicas: "
                        + "; ".join(failures),
                    )

            self.metadata.Remove(
                metadata_pb2.RemoveRequest(
                    owner_id=request.owner_id, entry_id=request.entry_id
                ),
                timeout=15,
            )

            return data_pb2.RemoveEntryResponse(
                success=True, message="Entry removed"
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
