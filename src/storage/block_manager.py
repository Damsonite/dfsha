import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from threading import RLock


class BlockManager:
    def __init__(self, storage_path="/data"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()

    def _validate_block_id(self, block_id):
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", block_id):
            raise ValueError("Invalid block ID")

    def _block_path(self, block_id):
        self._validate_block_id(block_id)
        return self.storage_path / f"{block_id}.block"

    def _metadata_path(self, block_id):
        self._validate_block_id(block_id)
        return self.storage_path / f"{block_id}.json"

    def _atomic_write(self, destination, content):

        fd, temp_path = tempfile.mkstemp(dir=self.storage_path, prefix=".tmp_")

        try:
            with os.fdopen(fd, "wb") as file:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temp_path, destination)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def write_block(self, block_id, data, checksum, version):

        self._validate_block_id(block_id)

        calculated_checksum = hashlib.sha256(data).hexdigest()

        if calculated_checksum != checksum:
            raise ValueError("Checksum mismatch")

        block_path = self._block_path(block_id)
        metadata_path = self._metadata_path(block_id)

        with self.lock:
            if metadata_path.exists():
                with open(metadata_path, "r") as file:
                    current = json.load(file)

                current_version = current["version"]

                if version < current_version:
                    raise ValueError("STALE_VERSION")

                if version == current_version:
                    if current["checksum"] != checksum:
                        raise ValueError("VERSION_CONFLICT")

                    if block_path.exists():
                        return current

            metadata = {
                "block_id": block_id,
                "checksum": checksum,
                "version": version,
                "size": len(data),
            }

            self._atomic_write(block_path, data)

            self._atomic_write(metadata_path, json.dumps(metadata).encode("utf-8"))

            return metadata

    def read_block(self, block_id):

        block_path = self._block_path(block_id)
        metadata_path = self._metadata_path(block_id)

        with self.lock:
            if not block_path.exists() or not metadata_path.exists():
                raise FileNotFoundError("Block not found")

            with open(block_path, "rb") as file:
                data = file.read()

            with open(metadata_path, "r") as file:
                metadata = json.load(file)

            calculated_checksum = hashlib.sha256(data).hexdigest()

            if calculated_checksum != metadata["checksum"]:
                raise ValueError("DATA_CORRUPTION")

            return data, metadata

    def delete_block(self, block_id):

        block_path = self._block_path(block_id)
        metadata_path = self._metadata_path(block_id)

        with self.lock:
            block_path.unlink(missing_ok=True)
            metadata_path.unlink(missing_ok=True)

    def block_status(self, block_id):

        try:
            _, metadata = self.read_block(block_id)

            return {"exists": True, "valid": True, **metadata}

        except FileNotFoundError:
            return {"exists": False, "valid": False}

        except ValueError:
            return {"exists": True, "valid": False}
