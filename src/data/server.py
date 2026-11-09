import hashlib
import logging
import os
import threading
import time
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
from src.common.auth import (
    DEFAULT_SERVICE_TOKEN,
    DEFAULT_STORAGE_TOKEN,
    ClientTokenInterceptor,
    extract_bearer_token,
)
from src.common.consistent_hash import ConsistentHash
from src.common.storage_nodes import parse_storage_nodes

BLOCK_SIZE = 4 * 1024 * 1024
REPLICATION_FACTOR = 3
WRITE_QUORUM = 2
MAX_FILENAME_LENGTH = 255
DEFAULT_MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_LEASE_TTL_SECONDS = 300
DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS = 5
DEFAULT_REPAIR_INTERVAL_SECONDS = 30

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

        self.nodes = dict(parse_storage_nodes())

        if len(self.nodes) < REPLICATION_FACTOR:
            raise RuntimeError(
                "DFSHA_STORAGE_NODES must define at least "
                f"{REPLICATION_FACTOR} storage nodes; got {len(self.nodes)}"
            )

        self.service_token = os.getenv(
            "DFSHA_SERVICE_TOKEN", DEFAULT_SERVICE_TOKEN
        )

        self.storage_token = os.getenv(
            "DFSHA_STORAGE_TOKEN", DEFAULT_STORAGE_TOKEN
        )

        self.storage_stubs = {}

        for node_id, endpoint in self.nodes.items():
            channel = grpc.intercept_channel(
                grpc.insecure_channel(
                    endpoint, options=GRPC_CHANNEL_OPTIONS
                ),
                ClientTokenInterceptor(self.storage_token),
            )

            self.storage_stubs[node_id] = storage_pb2_grpc.StorageNodeStub(
                channel
            )

        self.storage_stubs_by_endpoint = {
            endpoint: self.storage_stubs[node_id]
            for node_id, endpoint in self.nodes.items()
        }

        self.ring = ConsistentHash(list(self.nodes.keys()), virtual_nodes=150)

        self.max_upload_bytes = self._read_positive_int_env(
            "MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES
        )

        self.upload_lease_ttl_seconds = self._read_positive_int_env(
            "UPLOAD_LEASE_TTL_SECONDS", DEFAULT_LEASE_TTL_SECONDS
        )

        self.health_check_interval_seconds = self._read_positive_int_env(
            "HEALTH_CHECK_INTERVAL_SECONDS",
            DEFAULT_HEALTH_CHECK_INTERVAL_SECONDS,
        )

        self.repair_interval_seconds = self._read_positive_int_env(
            "REPAIR_INTERVAL_SECONDS", DEFAULT_REPAIR_INTERVAL_SECONDS
        )

        self.node_status = {}
        self.maintenance_stop = threading.Event()
        self.maintenance_thread = None

    @staticmethod
    def _read_positive_int_env(name, default):
        raw_value = os.getenv(name, str(default))

        try:
            value = int(raw_value)
        except ValueError as error:
            raise RuntimeError(
                f"Invalid {name}={raw_value!r}: must be an integer"
            ) from error

        if value <= 0:
            raise RuntimeError(
                f"Invalid {name}={raw_value!r}: must be greater than zero"
            )

        return value

    def _authenticate(self, context):
        token = extract_bearer_token(context.invocation_metadata())

        if not token:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Missing bearer token"
            )

        try:
            response = self.metadata.Authenticate(
                metadata_pb2.AuthenticateRequest(token=token),
                timeout=5,
            )
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

        if not response.success:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                response.message or "Invalid bearer token",
            )

        return token, response.user_id, response.is_service

    @staticmethod
    def _authorization_metadata(token):
        return (("authorization", f"Bearer {token}"),)

    def _metadata_call(self, method, request, token, timeout=15):
        return method(
            request,
            timeout=timeout,
            metadata=self._authorization_metadata(token),
        )

    @staticmethod
    def _authorize_owner(context, owner_id, user_id, is_service):
        if not is_service and owner_id != user_id:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED, "Owner mismatch"
            )

    def start_maintenance(self):
        if self.maintenance_thread and self.maintenance_thread.is_alive():
            return

        self.maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="dfsha-maintenance",
            daemon=True,
        )
        self.maintenance_thread.start()

    def _maintenance_loop(self):
        next_repair = 0.0

        while not self.maintenance_stop.is_set():
            self._refresh_node_health()

            if time.monotonic() >= next_repair:
                try:
                    result = self._repair(
                        REPLICATION_FACTOR, self.service_token
                    )

                    if result.failures:
                        logger.warning(
                            "Repair completed with %d failure(s): %s",
                            len(result.failures),
                            "; ".join(result.failures[:5]),
                        )
                    elif result.replicas_repaired:
                        logger.info(
                            "Repair checked %d block(s) and repaired %d replica(s)",
                            result.blocks_checked,
                            result.replicas_repaired,
                        )
                except Exception as error:
                    logger.warning("Repair cycle failed: %s", error)

                next_repair = time.monotonic() + self.repair_interval_seconds

            self.maintenance_stop.wait(self.health_check_interval_seconds)

    def _refresh_node_health(self):
        for node_id, stub in self.storage_stubs.items():
            try:
                response = stub.Health(
                    storage_pb2.HealthRequest(), timeout=2
                )
                status = (
                    "ACTIVE" if response.status == "HEALTHY" else "INACTIVE"
                )
            except grpc.RpcError:
                status = "INACTIVE"

            if self.node_status.get(node_id) == status:
                continue

            try:
                self._metadata_call(
                    self.metadata.SetNodeStatus,
                    metadata_pb2.SetNodeStatusRequest(
                        node_id=node_id,
                        status=status,
                    ),
                    self.service_token,
                    timeout=5,
                )
                self.node_status[node_id] = status
            except grpc.RpcError as error:
                logger.warning(
                    "Could not update health for %s: %s",
                    node_id,
                    error,
                )

    def UploadFile(self, request_iterator, context):

        token, user_id, is_service = self._authenticate(context)

        header = None
        buffer = bytearray()
        blocks = []
        written_blocks = []
        lease_id = None

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

                    if not is_service and header.owner_id != user_id:
                        raise UploadRejected(
                            grpc.StatusCode.PERMISSION_DENIED,
                            "Owner mismatch",
                        )

                    self._validate_upload_header(header)

                    if header.lease_id:
                        lease_id = header.lease_id
                        self._renew_lease(
                            lease_id, header.owner_id, token
                        )
                    else:
                        lease_id = self._acquire_lease(header, token)

                    lease_renew_at = time.monotonic() + max(
                        1.0, self.upload_lease_ttl_seconds / 2
                    )

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
                        block_index, block_data, written_blocks
                    )

                    blocks.append(block)

                    block_index += 1

                    if time.monotonic() >= lease_renew_at:
                        self._renew_lease(lease_id, header.owner_id, token)
                        lease_renew_at = time.monotonic() + max(
                            1.0, self.upload_lease_ttl_seconds / 2
                        )

            if header is None:
                raise UploadRejected(
                    grpc.StatusCode.INVALID_ARGUMENT, "Missing upload header"
                )

            if buffer:
                block = self._store_block(
                    block_index, bytes(buffer), written_blocks
                )

                blocks.append(block)

            checksum = file_hash.hexdigest()

            self._renew_lease(lease_id, header.owner_id, token)

            try:
                commit = self._metadata_call(
                    self.metadata.CommitFile,
                    metadata_pb2.CommitFileRequest(
                        owner_id=header.owner_id,
                        parent_id=header.parent_id,
                        name=header.filename,
                        size=total_size,
                        checksum=checksum,
                        blocks=blocks,
                        lease_id=lease_id,
                    ),
                    token,
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

            lease_id = None

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

            if lease_id:
                self._release_lease(lease_id, header.owner_id, token)

            context.abort(error.code, error.details)

        except grpc.RpcError as error:
            self._cleanup_blocks(written_blocks)

            if lease_id:
                self._release_lease(lease_id, header.owner_id, token)

            context.abort(error.code(), error.details())

        except Exception as error:
            self._cleanup_blocks(written_blocks)

            if lease_id:
                self._release_lease(lease_id, header.owner_id, token)

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

    def _acquire_lease(self, header, token):

        try:
            response = self._metadata_call(
                self.metadata.AcquireUploadLease,
                metadata_pb2.AcquireUploadLeaseRequest(
                    owner_id=header.owner_id,
                    parent_id=header.parent_id,
                    name=header.filename,
                    ttl_seconds=self.upload_lease_ttl_seconds,
                ),
                token,
                timeout=15,
            )
        except grpc.RpcError as error:
            raise UploadRejected(error.code(), error.details()) from error

        if not response.success:
            raise UploadRejected(
                grpc.StatusCode.FAILED_PRECONDITION,
                response.message or "Upload lease acquisition failed",
            )

        return response.lease_id

    def _renew_lease(self, lease_id, owner_id, token):

        try:
            response = self._metadata_call(
                self.metadata.RenewUploadLease,
                metadata_pb2.RenewUploadLeaseRequest(
                    owner_id=owner_id,
                    lease_id=lease_id,
                    ttl_seconds=self.upload_lease_ttl_seconds,
                ),
                token,
                timeout=15,
            )
        except grpc.RpcError as error:
            raise UploadRejected(error.code(), error.details()) from error

        if not response.success:
            raise UploadRejected(
                grpc.StatusCode.FAILED_PRECONDITION,
                response.message or "Upload lease renewal failed",
            )

    def _release_lease(self, lease_id, owner_id, token):

        try:
            self._metadata_call(
                self.metadata.ReleaseUploadLease,
                metadata_pb2.ReleaseUploadLeaseRequest(
                    owner_id=owner_id,
                    lease_id=lease_id,
                ),
                token,
                timeout=5,
            )
        except Exception as error:
            logger.warning(
                "Could not release upload lease %s: %s",
                lease_id,
                error,
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

    def _store_block(self, block_index, data, written_blocks):

        block_id = str(uuid.uuid4())

        checksum = hashlib.sha256(data).hexdigest()

        # The block ID is the stable placement key. Writes and repairs use
        # the same key, so a repaired replica returns to the original nodes.
        nodes = self.ring.get_nodes(block_id, count=REPLICATION_FACTOR)

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

        token, user_id, is_service = self._authenticate(context)

        self._authorize_owner(
            context, request.owner_id, user_id, is_service
        )

        try:
            metadata = self._metadata_call(
                self.metadata.Stat,
                metadata_pb2.StatRequest(
                    owner_id=request.owner_id, entry_id=request.file_id
                ),
                token,
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

            locations = self._metadata_call(
                self.metadata.LocateBlocks,
                metadata_pb2.LocateBlocksRequest(
                    owner_id=request.owner_id, file_id=request.file_id
                ),
                token,
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
            stub = self.storage_stubs_by_endpoint.get(endpoint)

            if stub is None:
                failures.append(f"{endpoint}: unknown storage endpoint")
                continue

            try:
                response = stub.ReadBlock(
                    storage_pb2.ReadBlockRequest(block_id=block.block_id),
                    timeout=15,
                )
            except grpc.RpcError as error:
                failures.append(
                    f"{endpoint}: {error.code().name}: {error.details()}"
                )
                continue

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

        token, user_id, is_service = self._authenticate(context)

        self._authorize_owner(
            context, request.owner_id, user_id, is_service
        )

        try:
            entry = self._metadata_call(
                self.metadata.Stat,
                metadata_pb2.StatRequest(
                    owner_id=request.owner_id, entry_id=request.entry_id
                ),
                token,
                timeout=15,
            )

            if entry.type == "FILE":
                locations = self._metadata_call(
                    self.metadata.LocateBlocks,
                    metadata_pb2.LocateBlocksRequest(
                        owner_id=request.owner_id,
                        file_id=request.entry_id,
                        include_inactive=True,
                    ),
                    token,
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

            self._metadata_call(
                self.metadata.Remove,
                metadata_pb2.RemoveRequest(
                    owner_id=request.owner_id, entry_id=request.entry_id
                ),
                token,
                timeout=15,
            )

            return data_pb2.RemoveEntryResponse(
                success=True, message="Entry removed"
            )

        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

    def Repair(self, request, context):

        token, _, is_service = self._authenticate(context)

        if not is_service:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "Repair requires a service token",
            )

        target_replicas = request.target_replicas or REPLICATION_FACTOR

        if target_replicas < 1 or target_replicas > REPLICATION_FACTOR:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"target_replicas must be between 1 and {REPLICATION_FACTOR}",
            )

        try:
            return self._repair(target_replicas, token)
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

    def _repair(self, target_replicas, token):

        repair_blocks = self._metadata_call(
            self.metadata.ListBlocksForRepair,
            metadata_pb2.ListBlocksForRepairRequest(
                target_replicas=target_replicas
            ),
            token,
            timeout=30,
        ).blocks

        blocks_checked = 0
        replicas_repaired = 0
        failures = []

        for block in repair_blocks:
            blocks_checked += 1

            active_node_ids = set(block.active_node_ids)
            targets = []

            for node_id in block.node_ids:
                if node_id not in active_node_ids:
                    # No spare node exists in a three-node RF=3 cluster.
                    # The replica is repaired when the node returns.
                    continue

                needs_repair, failure = self._replica_needs_repair(
                    node_id, block
                )

                if failure:
                    failures.append(failure)
                    continue

                if needs_repair:
                    targets.append((node_id, False))

            existing_node_ids = set(block.node_ids)

            if len(existing_node_ids) < target_replicas:
                for candidate in self.ring.get_nodes(
                    block.block_id, count=target_replicas
                ):
                    if candidate in existing_node_ids:
                        continue

                    if not self._node_is_healthy(candidate):
                        continue

                    targets.append((candidate, True))

            if not targets:
                continue

            source_data, source_failures = self._read_repair_source(block)

            if source_data is None:
                failures.append(
                    f"{block.block_id}: no valid source replica "
                    f"({'; '.join(source_failures)})"
                )
                continue

            for node_id, register_if_valid in targets:
                repaired, failure = self._repair_replica(
                    node_id,
                    block,
                    source_data,
                    token,
                    register_if_valid=register_if_valid,
                )

                if repaired:
                    replicas_repaired += 1

                if failure:
                    failures.append(failure)

        return data_pb2.RepairResponse(
            success=not failures,
            blocks_checked=blocks_checked,
            replicas_repaired=replicas_repaired,
            failures=failures,
            message=(
                "Repair completed"
                if not failures
                else "Repair completed with failures"
            ),
        )

    def _read_repair_source(self, block):

        failures = []

        for endpoint in block.active_node_endpoints:
            stub = self.storage_stubs_by_endpoint.get(endpoint)

            if stub is None:
                failures.append(f"{endpoint}: unknown storage endpoint")
                continue

            try:
                response = stub.ReadBlock(
                    storage_pb2.ReadBlockRequest(block_id=block.block_id),
                    timeout=15,
                )
            except grpc.RpcError as error:
                failures.append(
                    f"{endpoint}: {error.code().name}: {error.details()}"
                )
                continue

            if not response.success:
                failures.append(f"{endpoint}: rejected: {response.message}")
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

    def _node_is_healthy(self, node_id):

        stub = self.storage_stubs.get(node_id)

        if stub is None:
            return False

        try:
            response = stub.Health(storage_pb2.HealthRequest(), timeout=2)
        except grpc.RpcError:
            return False

        return response.status == "HEALTHY"

    def _replica_needs_repair(self, node_id, block):

        stub = self.storage_stubs.get(node_id)

        if stub is None:
            return True, f"{node_id}: unknown storage node"

        try:
            status = stub.BlockStatus(
                storage_pb2.BlockStatusRequest(block_id=block.block_id),
                timeout=10,
            )
        except grpc.RpcError as error:
            return (
                True,
                f"{node_id}: {error.code().name}: {error.details()}",
            )

        valid = (
            status.exists
            and status.valid
            and status.checksum == block.checksum
            and status.version == block.version
        )

        return (not valid), None

    def _repair_replica(
        self,
        node_id,
        block,
        source_data,
        token,
        register_if_valid=False,
    ):

        stub = self.storage_stubs.get(node_id)

        if stub is None:
            return False, f"{node_id}: unknown storage node"

        try:
            status = stub.BlockStatus(
                storage_pb2.BlockStatusRequest(block_id=block.block_id),
                timeout=10,
            )
        except grpc.RpcError as error:
            return (
                False,
                f"{node_id}: {error.code().name}: {error.details()}",
            )

        valid = (
            status.exists
            and status.valid
            and status.checksum == block.checksum
            and status.version == block.version
        )

        if valid and not register_if_valid:
            return False, None

        if valid and register_if_valid:
            try:
                self._metadata_call(
                    self.metadata.RegisterBlockReplica,
                    metadata_pb2.RegisterBlockReplicaRequest(
                        block_id=block.block_id,
                        node_id=node_id,
                        version=block.version,
                    ),
                    token,
                    timeout=10,
                )
            except grpc.RpcError as error:
                return (
                    False,
                    f"{node_id}: could not register replica: "
                    f"{error.code().name}: {error.details()}",
                )

            return True, None

        if status.exists:
            try:
                stub.DeleteBlock(
                    storage_pb2.DeleteBlockRequest(
                        block_id=block.block_id
                    ),
                    timeout=10,
                )
            except grpc.RpcError as error:
                return (
                    False,
                    f"{node_id}: could not delete invalid replica: "
                    f"{error.code().name}: {error.details()}",
                )

        try:
            response = stub.WriteBlock(
                storage_pb2.WriteBlockRequest(
                    block_id=block.block_id,
                    data=source_data,
                    checksum=block.checksum,
                    version=block.version,
                ),
                timeout=30,
            )
        except grpc.RpcError as error:
            return (
                False,
                f"{node_id}: write failed: "
                f"{error.code().name}: {error.details()}",
            )

        if not response.success:
            return False, f"{node_id}: write rejected: {response.message}"

        try:
            self._metadata_call(
                self.metadata.RegisterBlockReplica,
                metadata_pb2.RegisterBlockReplicaRequest(
                    block_id=block.block_id,
                    node_id=node_id,
                    version=block.version,
                ),
                token,
                timeout=10,
            )
        except grpc.RpcError as error:
            return (
                False,
                f"{node_id}: could not register replica: "
                f"{error.code().name}: {error.details()}",
            )

        return True, None

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

    servicer = DataServicer()

    data_pb2_grpc.add_DataServiceServicer_to_server(servicer, server)

    server.add_insecure_port("[::]:50052")

    server.start()

    servicer.start_maintenance()

    print("Data Service listening on port 50052")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
