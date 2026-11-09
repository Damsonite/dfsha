import re
from concurrent import futures
from uuid import UUID

import grpc
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from src.common import metadata_pb2, metadata_pb2_grpc
from src.metadata.database import SessionLocal
from src.metadata.models import Block, BlockReplica, Entry, StorageNode, User
from src.metadata.repository import initialize_database

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_BLOCK_SIZE = 4 * 1024 * 1024


class MetadataServicer(metadata_pb2_grpc.MetadataServiceServicer):
    def Mkdir(self, request, context):

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
            session.commit()

            return metadata_pb2.EntryResponse(
                success=True,
                entry_id=str(entry.id),
                name=entry.name,
                type=entry.type,
                message="Directory created",
            )

    def ListDir(self, request, context):

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
            session.commit()

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
                replicas = session.execute(
                    select(BlockReplica, StorageNode)
                    .join(StorageNode, BlockReplica.node_id == StorageNode.id)
                    .where(
                        BlockReplica.block_id == block.id,
                        BlockReplica.state == "READY",
                        BlockReplica.version == block.version,
                        StorageNode.status == "ACTIVE",
                    )
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

    def Health(self, request, context):

        return metadata_pb2.HealthResponse(status="HEALTHY")

    def CommitFile(self, request, context):

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


def serve():

    initialize_database()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    metadata_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataServicer(), server)

    server.add_insecure_port("[::]:50051")

    server.start()

    print("Metadata Service listening on port 50051")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
