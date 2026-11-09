from sqlalchemy import text

from src.metadata.database import Base, SessionLocal, engine
from src.metadata.models import Block, BlockReplica, Entry, StorageNode, User


def initialize_database():

    Base.metadata.create_all(bind=engine)

    with SessionLocal() as session:
        # Usuario inicial para pruebas
        user = session.get(User, "user-001")

        if not user:
            user = User(id="user-001", username="demo")

            session.add(user)

        # Registro de los Storage Nodes
        nodes = [
            ("storage-1", "storage-1:50061"),
            ("storage-2", "storage-2:50061"),
            ("storage-3", "storage-3:50061"),
        ]

        for node_id, endpoint in nodes:
            node = session.get(StorageNode, node_id)

            if not node:
                session.add(StorageNode(id=node_id, endpoint=endpoint, status="ACTIVE"))

        session.commit()
