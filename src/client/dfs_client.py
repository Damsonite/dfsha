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
from src.common.auth import DEFAULT_CLIENT_TOKEN, ClientTokenInterceptor
from src.common.path_utils import (
    InvalidPath,
    normalize_path,
    split_parent,
)

CHUNK_SIZE = 1024 * 1024
DEFAULT_DATA_ENDPOINT = os.getenv("DATA_ENDPOINT", "localhost:50052")
DEFAULT_METADATA_ENDPOINT = os.getenv("METADATA_ENDPOINT", "localhost:50051")


class DownloadIntegrityError(Exception):
    pass


class DFSClient:
    def __init__(
        self, endpoint=None, metadata_endpoint=None, token=None
    ):

        endpoint = endpoint or DEFAULT_DATA_ENDPOINT
        metadata_endpoint = metadata_endpoint or DEFAULT_METADATA_ENDPOINT
        self.token = token or DEFAULT_CLIENT_TOKEN

        data_channel = grpc.insecure_channel(
            endpoint,
            options=[
                ("grpc.max_receive_message_length", 8 * 1024 * 1024),
                ("grpc.max_send_message_length", 8 * 1024 * 1024),
            ],
        )

        self.channel = grpc.intercept_channel(
            data_channel, ClientTokenInterceptor(self.token)
        )

        self.stub = data_pb2_grpc.DataServiceStub(self.channel)

        metadata_channel = grpc.insecure_channel(metadata_endpoint)

        self.metadata_channel = grpc.intercept_channel(
            metadata_channel, ClientTokenInterceptor(self.token)
        )

        self.metadata_stub = metadata_pb2_grpc.MetadataServiceStub(
            self.metadata_channel
        )

    def mkdir(self, name, owner_id, parent_id=""):

        return self.metadata_stub.Mkdir(
            metadata_pb2.MkdirRequest(
                owner_id=owner_id,
                parent_id=parent_id,
                name=name,
            ),
            timeout=30,
        )

    def list_dir(self, owner_id, parent_id=""):

        return self.metadata_stub.ListDir(
            metadata_pb2.ListDirRequest(
                owner_id=owner_id,
                parent_id=parent_id,
            ),
            timeout=30,
        )

    def stat(self, entry_id, owner_id):

        return self.metadata_stub.Stat(
            metadata_pb2.StatRequest(
                owner_id=owner_id,
                entry_id=entry_id,
            ),
            timeout=30,
        )

    def resolve_path(self, path, owner_id):

        return self.metadata_stub.ResolvePath(
            metadata_pb2.ResolvePathRequest(
                owner_id=owner_id,
                path=path,
            ),
            timeout=30,
        )

    def mkdir_path(self, path, owner_id, create_parents=False):

        return self.metadata_stub.MkdirPath(
            metadata_pb2.MkdirPathRequest(
                owner_id=owner_id,
                path=path,
                create_parents=create_parents,
            ),
            timeout=30,
        )

    def stat_path(self, path, owner_id):

        return self.resolve_path(path, owner_id)

    def list_dir_path(self, path, owner_id):

        entry = self.resolve_path(path, owner_id)

        if entry.type != "DIRECTORY":
            raise NotADirectoryError(f"Not a directory: {path}")

        return self.list_dir(owner_id, entry.entry_id)

    def remove_path(self, path, owner_id):

        entry = self.resolve_path(path, owner_id)

        if not entry.entry_id:
            raise InvalidPath("Cannot remove the root directory")

        return self.remove(entry.entry_id, owner_id)

    def upload(self, path, owner_id, parent_id="", filename=None):

        path = Path(path)

        if not path.is_file():
            raise FileNotFoundError(path)

        remote_filename = filename or path.name

        def requests():

            yield data_pb2.UploadFileRequest(
                header=data_pb2.UploadHeader(
                    owner_id=owner_id,
                    parent_id=parent_id,
                    filename=remote_filename,
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

    def upload_path(self, local_path, remote_path, owner_id):

        local_path = Path(local_path)

        if not local_path.is_file():
            raise FileNotFoundError(local_path)

        remote_path = str(remote_path)
        normalized = normalize_path(remote_path)

        target_is_directory = remote_path.endswith("/") or normalized == "/"

        if not target_is_directory:
            try:
                resolved = self.resolve_path(remote_path, owner_id)
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.NOT_FOUND:
                    raise
            else:
                target_is_directory = resolved.type == "DIRECTORY"

        if target_is_directory:
            parent_path = normalized
            filename = local_path.name
        else:
            parent_path, filename = split_parent(remote_path)

        parent = self.resolve_path(parent_path, owner_id)

        if parent.type != "DIRECTORY":
            raise NotADirectoryError(f"Not a directory: {parent_path}")

        return self.upload(
            local_path,
            owner_id,
            parent.entry_id,
            filename=filename,
        )

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

    def download_path(self, remote_path, owner_id, destination):

        entry = self.resolve_path(remote_path, owner_id)

        if entry.type != "FILE":
            raise DownloadIntegrityError(f"Not a file: {remote_path}")

        return self.download(entry.entry_id, owner_id, destination)

    def remove(self, entry_id, owner_id):

        return self.stub.RemoveEntry(
            data_pb2.RemoveEntryRequest(
                owner_id=owner_id,
                entry_id=entry_id,
            ),
            timeout=60,
        )

    def close(self):

        self.channel.close()
        self.metadata_channel.close()
