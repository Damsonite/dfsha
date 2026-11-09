import re
from concurrent import futures
from datetime import datetime, timedelta, timezone
from uuid import UUID

import grpc
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from src.common import metadata_pb2, metadata_pb2_grpc
from src.common.auth import (
    AUTH_IS_SERVICE,
    AUTH_USER_ID,
    extract_bearer_token,
    hash_token,
)
from src.common.path_utils import InvalidPath, normalize_path
from src.metadata.database import SessionLocal
from src.metadata.models import (
    AuthToken,
    Block,
    BlockReplica,
    Entry,
    StorageNode,
    UploadLease,
    User,
)
from src.metadata.repository import initialize_database

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_BLOCK_SIZE = 4 * 1024 * 1024
DEFAULT_LEASE_TTL_SECONDS = 300
MAX_LEASE_TTL_SECONDS = 3600


def _lease_ttl(requested_ttl):
    if requested_ttl <= 0:
        return DEFAULT_LEASE_TTL_SECONDS

    if requested_ttl > MAX_LEASE_TTL_SECONDS:
        raise ValueError(
            f"Lease TTL cannot exceed {MAX_LEASE_TTL_SECONDS} seconds"
        )

    return requested_ttl


def _authorize_owner(context, owner_id):
    user_id = AUTH_USER_ID.get()

    if not user_id:
        context.abort(
            grpc.StatusCode.UNAUTHENTICATED, "Authentication required"
        )

    if not AUTH_IS_SERVICE.get() and owner_id != user_id:
        context.abort(grpc.StatusCode.PERMISSION_DENIED, "Owner mismatch")


def _authorize_service(context):
    if not AUTH_IS_SERVICE.get():
        context.abort(
            grpc.StatusCode.PERMISSION_DENIED, "Service token required"
        )


class MetadataAuthInterceptor(grpc.ServerInterceptor):
    PUBLIC_METHODS = frozenset(
        {
            "/dfsha.metadata.MetadataService/Health",
            "/dfsha.metadata.MetadataService/Authenticate",
        }
    )

    SERVICE_METHODS = frozenset(
        {
            "/dfsha.metadata.MetadataService/RegisterNode",
            "/dfsha.metadata.MetadataService/SetNodeStatus",
            "/dfsha.metadata.MetadataService/ListBlocksForRepair",
            "/dfsha.metadata.MetadataService/RegisterBlockReplica",
        }
    )

    def _deny(self, continuation, details, code, message):
        handler = continuation(details)

        if handler is None:
            return None

        def denied(request, context):
            context.abort(code, message)

        return grpc.unary_unary_rpc_method_handler(
            denied,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method

        if method in self.PUBLIC_METHODS:
            return continuation(handler_call_details)

        token = extract_bearer_token(
            handler_call_details.invocation_metadata
        )

        if not token:
            return self._deny(
                continuation,
                handler_call_details,
                grpc.StatusCode.UNAUTHENTICATED,
                "Missing bearer token",
            )

        try:
            with SessionLocal() as session:
                auth_token = session.scalar(
                    select(AuthToken).where(
                        AuthToken.token_hash == hash_token(token)
                    )
                )

                if not auth_token:
                    return self._deny(
                        continuation,
                        handler_call_details,
                        grpc.StatusCode.UNAUTHENTICATED,
                        "Invalid bearer token",
                    )

                user_id = auth_token.user_id
                is_service = auth_token.is_service
        except Exception as error:
            return self._deny(
                continuation,
                handler_call_details,
                grpc.StatusCode.INTERNAL,
                f"Authentication backend failed: {error}",
            )

        if method in self.SERVICE_METHODS and not is_service:
            return self._deny(
                continuation,
                handler_call_details,
                grpc.StatusCode.PERMISSION_DENIED,
                "Service token required",
            )

        handler = continuation(handler_call_details)

        if handler is None:
            return None

        if handler.unary_unary is None:
            return handler

        def authenticated(request, context):
            AUTH_USER_ID.set(user_id)
            AUTH_IS_SERVICE.set(is_service)

            return handler.unary_unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            authenticated,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def _root_entry_response():
    return metadata_pb2.EntryResponse(
        success=True,
        entry_id="",
        name="/",
        type="DIRECTORY",
        size=0,
        checksum="",
        version=0,
        message="Root directory",
    )


def _entry_response(entry, message):
    return metadata_pb2.EntryResponse(
        success=True,
        entry_id=str(entry.id),
        name=entry.name,
        type=entry.type,
        size=entry.size,
        checksum=entry.checksum or "",
        version=entry.version,
        message=message,
    )


def _get_child(session, owner_id, parent_id, name):
    return session.scalar(
        select(Entry).where(
            Entry.owner_id == owner_id,
            Entry.parent_id == parent_id,
            Entry.name == name,
        )
    )


class MetadataServicer(metadata_pb2_grpc.MetadataServiceServicer):
    def Mkdir(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            user = session.get(User, request.owner_id)

            if not user:
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid parent ID")

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "Parent directory not found"
                    )

            if not request.name or "/" in request.name:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid directory name"
                )

            existing = session.scalar(
                select(Entry).where(
                    Entry.owner_id == request.owner_id,
                    Entry.parent_id == parent_id,
                    Entry.name == request.name,
                )
            )

            if existing:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Entry already exists")

            entry = Entry(
                owner_id=request.owner_id,
                parent_id=parent_id,
                name=request.name,
                type="DIRECTORY",
            )

            session.add(entry)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS, "Entry already exists"
                )

            return metadata_pb2.EntryResponse(
                success=True,
                entry_id=str(entry.id),
                name=entry.name,
                type=entry.type,
                message="Directory created",
            )

    def ListDir(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid directory ID"
                    )

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(grpc.StatusCode.NOT_FOUND, "Directory not found")

            entries = session.scalars(
                select(Entry)
                .where(Entry.owner_id == request.owner_id, Entry.parent_id == parent_id)
                .order_by(Entry.name)
            ).all()

            return metadata_pb2.ListDirResponse(
                entries=[
                    metadata_pb2.EntryItem(
                        entry_id=str(entry.id),
                        name=entry.name,
                        type=entry.type,
                        size=entry.size,
                    )
                    for entry in entries
                ],
                message="Directory listed successfully",
            )

    def Stat(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            try:
                entry_id = UUID(request.entry_id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid entry ID")

            entry = session.get(Entry, entry_id)

            if not entry or entry.owner_id != request.owner_id:
                context.abort(grpc.StatusCode.NOT_FOUND, "Entry not found")

            return metadata_pb2.EntryResponse(
                success=True,
                entry_id=str(entry.id),
                name=entry.name,
                type=entry.type,
                size=entry.size,
                checksum=entry.checksum or "",
                version=entry.version,
                message="Entry found",
            )

    def Remove(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            try:
                entry_id = UUID(request.entry_id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid entry ID")

            entry = session.get(Entry, entry_id)

            if not entry or entry.owner_id != request.owner_id:
                context.abort(grpc.StatusCode.NOT_FOUND, "Entry not found")

            if entry.type == "DIRECTORY":
                children = session.scalar(
                    select(Entry.id).where(Entry.parent_id == entry.id).limit(1)
                )

                if children:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION, "Directory is not empty"
                    )

            if entry.type == "FILE":
                session.execute(
                    delete(BlockReplica).where(
                        BlockReplica.block_id.in_(
                            select(Block.id).where(Block.file_id == entry.id)
                        )
                    )
                )
                session.execute(delete(Block).where(Block.file_id == entry.id))

            session.delete(entry)
            session.commit()

            return metadata_pb2.OperationResponse(success=True, message="Entry removed")

    def RegisterFile(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            user = session.get(User, request.owner_id)

            if not user:
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid parent ID")

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "Parent directory not found"
                    )

            if not request.name or "/" in request.name:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid filename")

            if request.size < 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid file size")

            existing = session.scalar(
                select(Entry).where(
                    Entry.owner_id == request.owner_id,
                    Entry.parent_id == parent_id,
                    Entry.name == request.name,
                )
            )

            if existing:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Entry already exists")

            entry = Entry(
                owner_id=request.owner_id,
                parent_id=parent_id,
                name=request.name,
                type="FILE",
                size=request.size,
                checksum=request.checksum,
            )

            session.add(entry)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS, "Entry already exists"
                )

            return metadata_pb2.EntryResponse(
                success=True,
                entry_id=str(entry.id),
                name=entry.name,
                type=entry.type,
                size=entry.size,
                checksum=entry.checksum or "",
                version=entry.version,
                message="File registered",
            )

    def LocateBlocks(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            try:
                file_id = UUID(request.file_id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid file ID")

            entry = session.get(Entry, file_id)

            if not entry or entry.owner_id != request.owner_id or entry.type != "FILE":
                context.abort(grpc.StatusCode.NOT_FOUND, "File not found")

            blocks = session.scalars(
                select(Block)
                .where(Block.file_id == file_id)
                .order_by(Block.block_index)
            ).all()

            locations = []

            for block in blocks:
                conditions = [
                    BlockReplica.block_id == block.id,
                    BlockReplica.state == "READY",
                    BlockReplica.version == block.version,
                ]

                if not request.include_inactive:
                    conditions.append(StorageNode.status == "ACTIVE")

                replicas = session.execute(
                    select(BlockReplica, StorageNode)
                    .join(StorageNode, BlockReplica.node_id == StorageNode.id)
                    .where(*conditions)
                ).all()

                locations.append(
                    metadata_pb2.BlockLocation(
                        block_id=str(block.id),
                        block_index=block.block_index,
                        offset=block.offset,
                        size=block.size,
                        checksum=block.checksum,
                        version=block.version,
                        node_endpoints=[node.endpoint for replica, node in replicas],
                    )
                )

            return metadata_pb2.LocateBlocksResponse(blocks=locations)

    def RegisterNode(self, request, context):

        _authorize_service(context)

        if not request.node_id or not request.endpoint:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Node ID and endpoint are required"
            )

        with SessionLocal() as session:
            node = session.get(StorageNode, request.node_id)

            if node:
                node.endpoint = request.endpoint
                node.status = "ACTIVE"
            else:
                session.add(
                    StorageNode(
                        id=request.node_id, endpoint=request.endpoint, status="ACTIVE"
                    )
                )

            session.commit()

            return metadata_pb2.OperationResponse(
                success=True, message="Node registered"
            )

    def SetNodeStatus(self, request, context):

        _authorize_service(context)

        if request.status not in ("ACTIVE", "INACTIVE"):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Node status must be ACTIVE or INACTIVE",
            )

        with SessionLocal() as session:
            node = session.get(StorageNode, request.node_id)

            if not node:
                context.abort(grpc.StatusCode.NOT_FOUND, "Storage node not found")

            node.status = request.status
            session.commit()

            return metadata_pb2.OperationResponse(
                success=True, message=f"Node status set to {request.status}"
            )

    def ListBlocksForRepair(self, request, context):

        _authorize_service(context)

        if request.target_replicas <= 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "target_replicas must be greater than zero",
            )

        with SessionLocal() as session:
            blocks = session.scalars(
                select(Block).order_by(Block.file_id, Block.block_index)
            ).all()

            repair_blocks = []

            for block in blocks:
                replicas = session.execute(
                    select(BlockReplica, StorageNode)
                    .join(StorageNode, BlockReplica.node_id == StorageNode.id)
                    .where(
                        BlockReplica.block_id == block.id,
                        BlockReplica.state == "READY",
                    )
                ).all()

                node_ids = [node.id for _, node in replicas]

                active_replicas = [
                    (replica, node)
                    for replica, node in replicas
                    if node.status == "ACTIVE"
                ]

                repair_blocks.append(
                    metadata_pb2.RepairBlock(
                        block_id=str(block.id),
                        file_id=str(block.file_id),
                        block_index=block.block_index,
                        size=block.size,
                        checksum=block.checksum,
                        version=block.version,
                        node_ids=node_ids,
                        active_node_ids=[
                            node.id for _, node in active_replicas
                        ],
                        active_node_endpoints=[
                            node.endpoint for _, node in active_replicas
                        ],
                    )
                )

            return metadata_pb2.ListBlocksForRepairResponse(
                blocks=repair_blocks
            )

    def RegisterBlockReplica(self, request, context):

        _authorize_service(context)

        try:
            block_id = UUID(request.block_id)
        except ValueError:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid block ID")

        with SessionLocal() as session:
            block = session.get(Block, block_id)

            if not block:
                context.abort(grpc.StatusCode.NOT_FOUND, "Block not found")

            node = session.get(StorageNode, request.node_id)

            if not node:
                context.abort(grpc.StatusCode.NOT_FOUND, "Storage node not found")

            version = request.version if request.version > 0 else block.version

            replica = session.scalar(
                select(BlockReplica).where(
                    BlockReplica.block_id == block.id,
                    BlockReplica.node_id == node.id,
                )
            )

            if replica:
                replica.state = "READY"
                replica.version = version
            else:
                session.add(
                    BlockReplica(
                        block_id=block.id,
                        node_id=node.id,
                        state="READY",
                        version=version,
                    )
                )

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

                return metadata_pb2.OperationResponse(
                    success=True, message="Replica already registered"
                )

            return metadata_pb2.OperationResponse(
                success=True, message="Replica registered"
            )

    def Authenticate(self, request, context):

        token = request.token.strip()

        if not token:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Token is required"
            )

        with SessionLocal() as session:
            auth_token = session.scalar(
                select(AuthToken).where(
                    AuthToken.token_hash == hash_token(token)
                )
            )

            if not auth_token:
                context.abort(
                    grpc.StatusCode.UNAUTHENTICATED, "Invalid token"
                )

            return metadata_pb2.AuthenticateResponse(
                success=True,
                user_id=auth_token.user_id,
                is_service=auth_token.is_service,
                message="Authenticated",
            )

    def Health(self, request, context):

        return metadata_pb2.HealthResponse(status="HEALTHY")

    def CommitFile(self, request, context):

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            user = session.get(User, request.owner_id)

            if not user:
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid parent ID")

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "Parent directory not found"
                    )

            existing = session.scalar(
                select(Entry).where(
                    Entry.owner_id == request.owner_id,
                    Entry.parent_id == parent_id,
                    Entry.name == request.name,
                )
            )

            if existing:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Entry already exists")

            if (
                not request.name
                or len(request.name) > 255
                or "/" in request.name
                or "\x00" in request.name
                or request.name in (".", "..")
            ):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid filename")

            if not request.lease_id:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Upload lease is required",
                )

            try:
                lease_id = UUID(request.lease_id)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload lease ID"
                )

            lease = session.get(UploadLease, lease_id)

            if (
                not lease
                or lease.owner_id != request.owner_id
                or lease.parent_id != parent_id
                or lease.name != request.name
            ):
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Upload lease does not match target",
                )

            if lease.expires_at <= datetime.now(timezone.utc):
                session.delete(lease)
                session.commit()

                context.abort(
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    "Upload lease expired",
                )

            if request.size < 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid file size")

            if not SHA256_PATTERN.fullmatch(request.checksum):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid file checksum")

            if request.size == 0 and len(request.blocks) != 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Empty files cannot contain blocks",
                )

            if request.size > 0 and not request.blocks:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "File has no blocks")

            ordered_blocks = sorted(request.blocks, key=lambda block: block.block_index)

            expected_offset = 0

            for index, block in enumerate(ordered_blocks):
                if block.block_index != index:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid block sequence"
                    )

                if block.offset != expected_offset or block.size <= 0:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid block layout"
                    )

                if block.size > MAX_BLOCK_SIZE:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "Block exceeds maximum size",
                    )

                if not SHA256_PATTERN.fullmatch(block.checksum):
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid block checksum"
                    )

                if not block.node_ids:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION, "Block has no replicas"
                    )

                if len(set(block.node_ids)) < 2:
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        "Block has fewer than two replicas",
                    )

                expected_offset += block.size

            if expected_offset != request.size:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Block sizes do not match file size",
                )

            try:
                entry = Entry(
                    owner_id=request.owner_id,
                    parent_id=parent_id,
                    name=request.name,
                    type="FILE",
                    size=request.size,
                    checksum=request.checksum,
                )

                session.add(entry)
                session.flush()

                for block_data in ordered_blocks:
                    block_id = UUID(block_data.block_id)

                    block = Block(
                        id=block_id,
                        file_id=entry.id,
                        block_index=block_data.block_index,
                        offset=block_data.offset,
                        size=block_data.size,
                        checksum=block_data.checksum,
                        version=block_data.version,
                    )

                    session.add(block)

                    for node_id in block_data.node_ids:
                        node = session.get(StorageNode, node_id)

                        if not node:
                            raise ValueError(f"Unknown storage node: {node_id}")

                        session.add(
                            BlockReplica(
                                block_id=block_id,
                                node_id=node_id,
                                state="READY",
                                version=block_data.version,
                            )
                        )

                session.delete(lease)
                session.commit()

                return metadata_pb2.EntryResponse(
                    success=True,
                    entry_id=str(entry.id),
                    name=entry.name,
                    type=entry.type,
                    size=entry.size,
                    checksum=entry.checksum,
                    version=entry.version,
                    message="File committed successfully",
                )

            except IntegrityError as error:
                session.rollback()

                if "uq_entry_namespace" in str(error.orig):
                    context.abort(
                        grpc.StatusCode.ALREADY_EXISTS, "Entry already exists"
                    )

                context.abort(
                    grpc.StatusCode.INTERNAL, f"Database integrity error: {error.orig}"
                )

            except Exception as error:
                session.rollback()

                context.abort(grpc.StatusCode.INTERNAL, str(error))

    def PrepareUpload(self, request, context):
        """Validate an upload before the Data Service writes any block.

        This is an advisory check, not a lock: the final uniqueness guarantee
        remains the uq_entry_namespace index enforced by CommitFile.
        """

        _authorize_owner(context, request.owner_id)

        with SessionLocal() as session:
            if not session.get(User, request.owner_id):
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid parent ID")

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "Parent directory not found"
                    )

            if (
                not request.name
                or len(request.name) > 255
                or "/" in request.name
                or "\x00" in request.name
                or request.name in (".", "..")
            ):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid filename")

            existing = session.scalar(
                select(Entry).where(
                    Entry.owner_id == request.owner_id,
                    Entry.parent_id == parent_id,
                    Entry.name == request.name,
                )
            )

            if existing:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Entry already exists")

            return metadata_pb2.OperationResponse(
                success=True, message="Upload validated"
            )

    def AcquireUploadLease(self, request, context):

        _authorize_owner(context, request.owner_id)

        try:
            ttl_seconds = _lease_ttl(request.ttl_seconds)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

        with SessionLocal() as session:
            if not session.get(User, request.owner_id):
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            parent_id = None

            if request.parent_id:
                try:
                    parent_id = UUID(request.parent_id)
                except ValueError:
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid parent ID"
                    )

                parent = session.get(Entry, parent_id)

                if (
                    not parent
                    or parent.owner_id != request.owner_id
                    or parent.type != "DIRECTORY"
                ):
                    context.abort(
                        grpc.StatusCode.NOT_FOUND, "Parent directory not found"
                    )

            if (
                not request.name
                or len(request.name) > 255
                or "/" in request.name
                or "\x00" in request.name
                or request.name in (".", "..")
            ):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid filename")

            now = datetime.now(timezone.utc)

            session.execute(
                delete(UploadLease).where(UploadLease.expires_at <= now)
            )

            existing = _get_child(
                session, request.owner_id, parent_id, request.name
            )

            if existing:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "Entry already exists")

            active_lease = session.scalar(
                select(UploadLease).where(
                    UploadLease.owner_id == request.owner_id,
                    UploadLease.parent_id == parent_id,
                    UploadLease.name == request.name,
                )
            )

            if active_lease:
                context.abort(
                    grpc.StatusCode.ABORTED, "Upload already in progress"
                )

            lease = UploadLease(
                owner_id=request.owner_id,
                parent_id=parent_id,
                name=request.name,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )

            session.add(lease)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()

                context.abort(
                    grpc.StatusCode.ABORTED, "Upload already in progress"
                )

            return metadata_pb2.UploadLeaseResponse(
                success=True,
                lease_id=str(lease.id),
                expires_at=int(lease.expires_at.timestamp()),
                message="Upload lease acquired",
            )

    def RenewUploadLease(self, request, context):

        _authorize_owner(context, request.owner_id)

        try:
            lease_id = UUID(request.lease_id)
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload lease ID"
            )

        try:
            ttl_seconds = _lease_ttl(request.ttl_seconds)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

        with SessionLocal() as session:
            lease = session.get(UploadLease, lease_id)

            if not lease or lease.owner_id != request.owner_id:
                context.abort(
                    grpc.StatusCode.NOT_FOUND, "Upload lease not found"
                )

            now = datetime.now(timezone.utc)

            if lease.expires_at <= now:
                session.delete(lease)
                session.commit()

                context.abort(
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    "Upload lease expired",
                )

            lease.expires_at = now + timedelta(seconds=ttl_seconds)
            session.commit()

            return metadata_pb2.UploadLeaseResponse(
                success=True,
                lease_id=str(lease.id),
                expires_at=int(lease.expires_at.timestamp()),
                message="Upload lease renewed",
            )

    def ReleaseUploadLease(self, request, context):

        _authorize_owner(context, request.owner_id)

        try:
            lease_id = UUID(request.lease_id)
        except ValueError:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Invalid upload lease ID"
            )

        with SessionLocal() as session:
            lease = session.get(UploadLease, lease_id)

            if lease and lease.owner_id == request.owner_id:
                session.delete(lease)
                session.commit()

            return metadata_pb2.OperationResponse(
                success=True, message="Upload lease released"
            )

    def ResolvePath(self, request, context):

        _authorize_owner(context, request.owner_id)

        try:
            normalized = normalize_path(request.path)
        except InvalidPath as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

        with SessionLocal() as session:
            if not session.get(User, request.owner_id):
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            if normalized == "/":
                return _root_entry_response()

            parent_id = None
            entry = None

            components = normalized.strip("/").split("/")

            for index, name in enumerate(components):
                entry = _get_child(session, request.owner_id, parent_id, name)

                if not entry:
                    context.abort(
                        grpc.StatusCode.NOT_FOUND,
                        f"Path not found: {normalized}",
                    )

                if index < len(components) - 1 and entry.type != "DIRECTORY":
                    context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        f"Not a directory: {normalized}",
                    )

                parent_id = entry.id

            return _entry_response(entry, "Path resolved")

    def MkdirPath(self, request, context):

        _authorize_owner(context, request.owner_id)

        try:
            normalized = normalize_path(request.path)
        except InvalidPath as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))

        if normalized == "/":
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Cannot create the root directory",
            )

        with SessionLocal() as session:
            if not session.get(User, request.owner_id):
                context.abort(grpc.StatusCode.NOT_FOUND, "User not found")

            components = normalized.strip("/").split("/")

            parent_id = None
            last_entry = None

            try:
                for index, name in enumerate(components):
                    existing = _get_child(
                        session, request.owner_id, parent_id, name
                    )
                    is_last = index == len(components) - 1

                    if existing:
                        if existing.type != "DIRECTORY":
                            if is_last:
                                context.abort(
                                    grpc.StatusCode.ALREADY_EXISTS,
                                    "Entry already exists",
                                )

                            context.abort(
                                grpc.StatusCode.FAILED_PRECONDITION,
                                f"Not a directory: {normalized}",
                            )

                        if is_last:
                            if not request.create_parents:
                                context.abort(
                                    grpc.StatusCode.ALREADY_EXISTS,
                                    "Directory already exists",
                                )

                            return _entry_response(
                                existing, "Directory already exists"
                            )

                        parent_id = existing.id
                        continue

                    if not is_last and not request.create_parents:
                        context.abort(
                            grpc.StatusCode.NOT_FOUND,
                            "Parent directory not found",
                        )

                    entry = Entry(
                        owner_id=request.owner_id,
                        parent_id=parent_id,
                        name=name,
                        type="DIRECTORY",
                    )

                    session.add(entry)
                    session.flush()

                    parent_id = entry.id
                    last_entry = entry

                session.commit()

                return _entry_response(last_entry, "Directory created")

            except IntegrityError:
                session.rollback()

                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS, "Directory already exists"
                )


def serve():

    initialize_database()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=(MetadataAuthInterceptor(),),
    )

    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataServicer(), server)

    server.add_insecure_port("[::]:50051")

    server.start()

    print("Metadata Service listening on port 50051")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
