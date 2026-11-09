import os
from datetime import datetime, timezone

from sqlalchemy import delete, text

from src.common.auth import hash_token
from src.common.storage_nodes import parse_storage_nodes
from src.metadata.database import Base, SessionLocal, engine
from src.metadata.models import (
    AuthToken,
    Block,
    BlockReplica,
    Entry,
    StorageNode,
    UploadLease,
    User,
)


def initialize_database():

    Base.metadata.create_all(bind=engine)

    with SessionLocal() as session:
        users = [
            ("user-001", "demo"),
            ("user-002", "demo2"),
            ("service", "service"),
        ]

        for user_id, username in users:
            if not session.get(User, user_id):
                session.add(User(id=user_id, username=username))

        session.flush()

        auth_tokens = [
            (
                "user-001",
                os.getenv("DFSHA_DEMO_TOKEN", "dev-demo-token"),
                False,
            ),
            (
                "user-002",
                os.getenv("DFSHA_SECOND_USER_TOKEN", "dev-second-token"),
                False,
            ),
            (
                "service",
                os.getenv("DFSHA_SERVICE_TOKEN", "dev-service-token"),
                True,
            ),
        ]

        for user_id, token, is_service in auth_tokens:
            session.execute(
                delete(AuthToken).where(AuthToken.user_id == user_id)
            )
            session.add(
                AuthToken(
                    user_id=user_id,
                    token_hash=hash_token(token),
                    is_service=is_service,
                )
            )

        # Registro de los Storage Nodes
        nodes = parse_storage_nodes()

        for node_id, endpoint in nodes:
            node = session.get(StorageNode, node_id)

            if not node:
                session.add(StorageNode(id=node_id, endpoint=endpoint, status="ACTIVE"))
            elif node.endpoint != endpoint:
                node.endpoint = endpoint

        session.execute(
            delete(UploadLease).where(
                UploadLease.expires_at <= datetime.now(timezone.utc)
            )
        )

        session.commit()
