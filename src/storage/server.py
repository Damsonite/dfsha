import os
import secrets
from concurrent import futures

import grpc

from src.common import storage_pb2, storage_pb2_grpc
from src.common.auth import DEFAULT_STORAGE_TOKEN, extract_bearer_token
from src.storage.block_manager import BlockManager


class StorageAuthInterceptor(grpc.ServerInterceptor):
    PUBLIC_METHODS = frozenset(
        {"/dfsha.storage.StorageNode/Health"}
    )

    def __init__(self):
        self.token = os.getenv(
            "DFSHA_STORAGE_TOKEN", DEFAULT_STORAGE_TOKEN
        )

    def _deny(self, continuation, details, message):
        handler = continuation(details)

        if handler is None:
            return None

        def denied(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, message)

        return grpc.unary_unary_rpc_method_handler(
            denied,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )

    def intercept_service(self, continuation, handler_call_details):
        if handler_call_details.method in self.PUBLIC_METHODS:
            return continuation(handler_call_details)

        token = extract_bearer_token(
            handler_call_details.invocation_metadata
        )

        if not token or not secrets.compare_digest(token, self.token):
            return self._deny(
                continuation,
                handler_call_details,
                "Invalid storage node token",
            )

        return continuation(handler_call_details)


class StorageNodeServicer(storage_pb2_grpc.StorageNodeServicer):
    def __init__(self):
        self.node_id = os.getenv("NODE_ID", "storage-unknown")

        self.block_manager = BlockManager(os.getenv("STORAGE_PATH", "/data"))

    def WriteBlock(self, request, context):

        try:
            metadata = self.block_manager.write_block(
                block_id=request.block_id,
                data=request.data,
                checksum=request.checksum,
                version=request.version,
            )

            return storage_pb2.WriteBlockResponse(
                success=True,
                block_id=request.block_id,
                checksum=metadata["checksum"],
                version=metadata["version"],
                message="Block stored successfully",
            )

        except (ValueError, OSError) as error:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

            return storage_pb2.WriteBlockResponse(
                success=False, block_id=request.block_id, message=str(error)
            )

    def ReadBlock(self, request, context):

        try:
            data, metadata = self.block_manager.read_block(request.block_id)

            return storage_pb2.ReadBlockResponse(
                success=True,
                block_id=request.block_id,
                data=data,
                checksum=metadata["checksum"],
                version=metadata["version"],
                message="Block retrieved successfully",
            )

        except FileNotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)

            return storage_pb2.ReadBlockResponse(
                success=False, message="Block not found"
            )

        except ValueError:
            context.set_code(grpc.StatusCode.DATA_LOSS)

            return storage_pb2.ReadBlockResponse(
                success=False, message="Block integrity validation failed"
            )

    def DeleteBlock(self, request, context):

        try:
            self.block_manager.delete_block(request.block_id)

            return storage_pb2.DeleteBlockResponse(
                success=True, message="Block deleted successfully"
            )

        except ValueError as error:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

            return storage_pb2.DeleteBlockResponse(success=False, message=str(error))

    def BlockStatus(self, request, context):

        try:
            status = self.block_manager.block_status(request.block_id)

            return storage_pb2.BlockStatusResponse(
                exists=status["exists"],
                block_id=request.block_id,
                checksum=status.get("checksum", ""),
                version=status.get("version", 0),
                valid=status["valid"],
            )

        except ValueError as error:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

            return storage_pb2.BlockStatusResponse()

    def Health(self, request, context):

        return storage_pb2.HealthResponse(node_id=self.node_id, status="HEALTHY")


def serve():

    port = os.getenv("GRPC_PORT", "50061")

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=(StorageAuthInterceptor(),),
        options=[
            ("grpc.max_receive_message_length", 5 * 1024 * 1024),
            ("grpc.max_send_message_length", 5 * 1024 * 1024),
        ],
    )

    storage_pb2_grpc.add_StorageNodeServicer_to_server(StorageNodeServicer(), server)

    server.add_insecure_port(f"[::]:{port}")

    server.start()

    print(f"[{os.getenv('NODE_ID')}] Storage Node listening on port {port}")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
