import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.metadata.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class Entry(Base):
    __tablename__ = "entries"

    __table_args__ = (
        Index(
            "uq_entry_namespace",
            text("owner_id"),
            text("COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            text("name"),
            unique=True,
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), nullable=False)

    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("entries.id"), nullable=True
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)

    type: Mapped[str] = mapped_column(String(20), nullable=False)

    size: Mapped[int] = mapped_column(BigInteger, default=0)

    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)

    version: Mapped[int] = mapped_column(Integer, default=1)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class UploadLease(Base):
    __tablename__ = "upload_leases"

    __table_args__ = (
        Index(
            "uq_upload_lease_namespace",
            text("owner_id"),
            text("COALESCE(parent_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            text("name"),
            unique=True,
        ),
        Index("ix_upload_lease_expires_at", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    owner_id: Mapped[str] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )

    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("entries.id", ondelete="CASCADE"),
        nullable=True,
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)

    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class StorageNode(Base):
    __tablename__ = "storage_nodes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)

    status: Mapped[str] = mapped_column(String(20), default="ACTIVE")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Block(Base):
    __tablename__ = "blocks"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    file_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("entries.id"), nullable=False)

    block_index: Mapped[int] = mapped_column(Integer, nullable=False)

    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    size: Mapped[int] = mapped_column(Integer, nullable=False)

    checksum: Mapped[str] = mapped_column(String(64), nullable=False)

    version: Mapped[int] = mapped_column(Integer, default=1)

    __table_args__ = (UniqueConstraint("file_id", "block_index", name="uq_file_block"),)


class BlockReplica(Base):
    __tablename__ = "block_replicas"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    block_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("blocks.id"), nullable=False)

    node_id: Mapped[str] = mapped_column(ForeignKey("storage_nodes.id"), nullable=False)

    state: Mapped[str] = mapped_column(String(20), default="PENDING")

    version: Mapped[int] = mapped_column(Integer, default=1)

    __table_args__ = (UniqueConstraint("block_id", "node_id", name="uq_block_node"),)
