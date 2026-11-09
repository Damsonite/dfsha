import hashlib
import os
import tempfile
from pathlib import Path
from uuid import UUID

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


def _is_uuid(value):
    try:
        UUID(str(value))
    except (AttributeError, ValueError):
        return False

    return True


class DownloadIntegrityError(Exception):
    pass


class FileHandle:
    def __init__(
        self,
        client,
        owner_id,
        mode,
        entry=None,
        path=None,
        parent_id=None,
        filename=None,
        lease_id=None,
    ):
        self.client = client
        self.owner_id = owner_id
        self.mode = mode
        self.entry = entry
        self.path = path
        self.parent_id = parent_id
        self.filename = filename
        self.lease_id = lease_id
        self.closed = False

        self._buffer = tempfile.SpooledTemporaryFile(
            max_size=8 * 1024 * 1024
        )
        self._download = None
        self._download_buffer = b""
        self._download_hash = hashlib.sha256()
        self._download_size = 0
        self._expected_block_index = 0
        self._eof = False

        if mode == "r":
            self._download = client.stub.DownloadFile(
                data_pb2.DownloadFileRequest(
                    owner_id=owner_id, file_id=entry.entry_id
                ),
                timeout=300,
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    def _ensure_open(self):
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def read(self, size=-1):
        self._ensure_open()

        if self.mode != "r":
            raise OSError("File is not open for reading")

        if size == 0:
            return b""

        if size is None or size < 0:
            while not self._eof:
                self._pull_block()

            data = self._download_buffer
            self._download_buffer = b""
            return data

        while len(self._download_buffer) < size and not self._eof:
            self._pull_block()

        data = self._download_buffer[:size]
        self._download_buffer = self._download_buffer[size:]
        return data

    def _pull_block(self):
        try:
            block = next(self._download)
        except StopIteration:
            self._finish_download()
            return

        if block.block_index != self._expected_block_index:
            raise DownloadIntegrityError(
                "Unexpected block index: "
                f"got {block.block_index}, "
                f"expected {self._expected_block_index}"
            )

        self._download_hash.update(block.data)
        self._download_size += len(block.data)
        self._download_buffer += block.data
        self._expected_block_index += 1

    def _finish_download(self):
        if self._download_size != self.entry.size:
            raise DownloadIntegrityError(
                f"Downloaded {self._download_size} bytes, "
                f"expected {self.entry.size}"
            )

        actual_checksum = self._download_hash.hexdigest()

        if actual_checksum != self.entry.checksum:
            raise DownloadIntegrityError(
                f"Downloaded checksum {actual_checksum} "
                f"does not match metadata checksum {self.entry.checksum}"
            )

        self._eof = True

    def write(self, data):
        self._ensure_open()

        if self.mode != "w":
            raise OSError("File is not open for writing")

        self._buffer.write(data)
        return len(data)

    def close(self):
        if self.closed:
            return None

        self.closed = True

        if self.mode == "r":
            if not self._eof and self._download is not None:
                self._download.cancel()

            return None

        try:
            self._buffer.seek(0)

            return self.client.upload_stream(
                self._buffer,
                self.owner_id,
                self.parent_id or "",
                self.filename,
                lease_id=self.lease_id or "",
            )
        finally:
            self._buffer.close()


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

        with open(path, "rb") as file:
            return self.upload_stream(
                file,
                owner_id,
                parent_id,
                remote_filename,
            )

    def upload_stream(
        self,
        fileobj,
        owner_id,
        parent_id="",
        filename="",
        lease_id="",
    ):
        def requests():

            yield data_pb2.UploadFileRequest(
                header=data_pb2.UploadHeader(
                    owner_id=owner_id,
                    parent_id=parent_id,
                    filename=filename,
                    lease_id=lease_id,
                )
            )

            while True:
                chunk = fileobj.read(CHUNK_SIZE)

                if not chunk:
                    break

                yield data_pb2.UploadFileRequest(
                    chunk=data_pb2.UploadChunk(data=chunk)
                )

        return self.stub.UploadFile(requests(), timeout=300)

    def open(self, target, mode, owner_id, ttl_seconds=300):
        if mode not in ("r", "w"):
            raise ValueError("Mode must be 'r' or 'w'")

        target = str(target)

        if mode == "r":
            if _is_uuid(target):
                entry = self.stat(target, owner_id)
            else:
                entry = self.resolve_path(target, owner_id)

            if entry.type != "FILE":
                raise IsADirectoryError(f"Not a file: {target}")

            if entry.size < 0:
                raise DownloadIntegrityError(
                    "Metadata contains an invalid file size"
                )

            if len(entry.checksum) != 64:
                raise DownloadIntegrityError(
                    "Metadata checksum is missing or invalid; refusing open"
                )

            return FileHandle(
                self, owner_id, "r", entry=entry, path=target
            )

        if _is_uuid(target):
            raise ValueError("Write mode requires a path, not an entry ID")

        normalized = normalize_path(target)
        parent_path, filename = split_parent(normalized)

        if not filename:
            raise InvalidPath("Cannot open the root directory for writing")

        parent = self.resolve_path(parent_path, owner_id)

        if parent.type != "DIRECTORY":
            raise NotADirectoryError(f"Not a directory: {parent_path}")

        try:
            self.resolve_path(normalized, owner_id)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.NOT_FOUND:
                raise
        else:
            raise FileExistsError(f"Entry already exists: {normalized}")

        try:
            lease = self.metadata_stub.AcquireUploadLease(
                metadata_pb2.AcquireUploadLeaseRequest(
                    owner_id=owner_id,
                    parent_id=parent.entry_id,
                    name=filename,
                    ttl_seconds=ttl_seconds,
                ),
                timeout=30,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise FileExistsError(
                    f"Entry already exists: {normalized}"
                ) from error

            raise

        if not lease.success:
            raise RuntimeError(
                lease.message or "Upload lease acquisition failed"
            )

        return FileHandle(
            self,
            owner_id,
            "w",
            path=normalized,
            parent_id=parent.entry_id,
            filename=filename,
            lease_id=lease.lease_id,
        )

    def lock(self, path, owner_id, ttl_seconds=300):
        parent_path, filename = split_parent(path)

        if not filename:
            raise InvalidPath("Cannot lock the root directory")

        parent = self.resolve_path(parent_path, owner_id)

        if parent.type != "DIRECTORY":
            raise NotADirectoryError(f"Not a directory: {parent_path}")

        try:
            lease = self.metadata_stub.AcquireUploadLease(
                metadata_pb2.AcquireUploadLeaseRequest(
                    owner_id=owner_id,
                    parent_id=parent.entry_id,
                    name=filename,
                    ttl_seconds=ttl_seconds,
                ),
                timeout=30,
            )
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise FileExistsError(
                    f"Entry already exists: {path}"
                ) from error

            raise

        if not lease.success:
            raise RuntimeError(
                lease.message or "Upload lease acquisition failed"
            )

        return lease.lease_id

    def unlock(self, lease_id, owner_id):
        return self.metadata_stub.ReleaseUploadLease(
            metadata_pb2.ReleaseUploadLeaseRequest(
                owner_id=owner_id,
                lease_id=lease_id,
            ),
            timeout=30,
        )

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
