import hashlib
from pathlib import Path

import grpc

from src.common import data_pb2, data_pb2_grpc

CHUNK_SIZE = 1024 * 1024


class DFSClient:
    def __init__(self, endpoint="localhost:50052"):

        self.channel = grpc.insecure_channel(
            endpoint,
            options=[
                ("grpc.max_receive_message_length", 8 * 1024 * 1024),
                ("grpc.max_send_message_length", 8 * 1024 * 1024),
            ],
        )

        self.stub = data_pb2_grpc.DataServiceStub(self.channel)

    def upload(self, path, owner_id, parent_id=""):

        path = Path(path)

        if not path.is_file():
            raise FileNotFoundError(path)

        def requests():

            yield data_pb2.UploadFileRequest(
                header=data_pb2.UploadHeader(
                    owner_id=owner_id, parent_id=parent_id, filename=path.name
                )
            )

            with open(path, "rb") as file:
                while True:
                    chunk = file.read(CHUNK_SIZE)

                    if not chunk:
                        break

                    yield data_pb2.UploadFileRequest(
                        chunk=data_pb2.UploadChunk(data=chunk)
                    )

        response = self.stub.UploadFile(requests(), timeout=300)

        return response

    def download(self, file_id, owner_id, destination):

        response = self.stub.DownloadFile(
            data_pb2.DownloadFileRequest(owner_id=owner_id, file_id=file_id),
            timeout=300,
        )

        destination = Path(destination)

        destination.parent.mkdir(parents=True, exist_ok=True)

        temp_path = destination.with_name(destination.name + ".part")

        file_hash = hashlib.sha256()

        try:
            with open(temp_path, "wb") as file:
                for block in response:
                    file.write(block.data)
                    file_hash.update(block.data)

                file.flush()

            temp_path.replace(destination)

            return {"path": str(destination), "checksum": file_hash.hexdigest()}

        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

    def close(self):

        self.channel.close()
