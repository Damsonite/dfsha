import hashlib
import os
from pathlib import Path

import grpc

from src.common import (
    data_pb2,
    data_pb2_grpc,
    metadata_pb2,
    metadata_pb2_grpc,
)

CHUNK_SIZE = 1024 * 1024
DEFAULT_DATA_ENDPOINT = os.getenv("DATA_ENDPOINT", "localhost:50052")
DEFAULT_METADATA_ENDPOINT = os.getenv("METADATA_ENDPOINT", "localhost:50051")


class DownloadIntegrityError(Exception):
    pass


class DFSClient:
    def __init__(self, endpoint=None, metadata_endpoint=None):

        endpoint = endpoint or DEFAULT_DATA_ENDPOINT
        metadata_endpoint = metadata_endpoint or DEFAULT_METADATA_ENDPOINT

        self.channel = grpc.insecure_channel(
            endpoint,
            options=[
                ("grpc.max_receive_message_length", 8 * 1024 * 1024),
                ("grpc.max_send_message_length", 8 * 1024 * 1024),
            ],
        )

        self.stub = data_pb2_grpc.DataServiceStub(self.channel)

        self.metadata_channel = grpc.insecure_channel(metadata_endpoint)

        self.metadata_stub = metadata_pb2_grpc.MetadataServiceStub(
            self.metadata_channel
        )

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

        metadata = self.metadata_stub.Stat(
            metadata_pb2.StatRequest(owner_id=owner_id, entry_id=file_id),
            timeout=30,
        )

        if metadata.type != "FILE":
            raise DownloadIntegrityError("Entry is not a file")

        if metadata.size < 0:
            raise DownloadIntegrityError("Metadata contains an invalid file size")

        if len(metadata.checksum) != 64:
            raise DownloadIntegrityError(
                "Metadata checksum is missing or invalid; refusing download"
            )

        response = self.stub.DownloadFile(
            data_pb2.DownloadFileRequest(owner_id=owner_id, file_id=file_id),
            timeout=300,
        )

        destination = Path(destination)

        destination.parent.mkdir(parents=True, exist_ok=True)

        temp_path = destination.with_name(destination.name + ".part")

        file_hash = hashlib.sha256()
        downloaded_size = 0
        expected_block_index = 0

        try:
            with open(temp_path, "wb") as file:
                for block in response:
                    if block.block_index != expected_block_index:
                        raise DownloadIntegrityError(
                            "Unexpected block index: "
                            f"got {block.block_index}, "
                            f"expected {expected_block_index}"
                        )

                    file.write(block.data)
                    file_hash.update(block.data)

                    downloaded_size += len(block.data)
                    expected_block_index += 1

                file.flush()

            actual_checksum = file_hash.hexdigest()

            if downloaded_size != metadata.size:
                raise DownloadIntegrityError(
                    f"Downloaded {downloaded_size} bytes, expected {metadata.size}"
                )

            if actual_checksum != metadata.checksum:
                raise DownloadIntegrityError(
                    f"Downloaded checksum {actual_checksum} "
                    f"does not match metadata checksum {metadata.checksum}"
                )

            temp_path.replace(destination)

            return {
                "path": str(destination),
                "checksum": actual_checksum,
                "size": downloaded_size,
            }

        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

    def close(self):

        self.channel.close()
        self.metadata_channel.close()
