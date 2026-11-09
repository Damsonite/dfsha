import contextvars
import hashlib
from os import getenv

import grpc

AUTH_USER_ID = contextvars.ContextVar("auth_user_id", default=None)
AUTH_IS_SERVICE = contextvars.ContextVar("auth_is_service", default=False)

DEFAULT_DEMO_TOKEN = getenv("DFSHA_DEMO_TOKEN", "dev-demo-token")
DEFAULT_SECOND_USER_TOKEN = getenv(
    "DFSHA_SECOND_USER_TOKEN", "dev-second-token"
)
DEFAULT_SERVICE_TOKEN = getenv("DFSHA_SERVICE_TOKEN", "dev-service-token")
DEFAULT_STORAGE_TOKEN = getenv("DFSHA_STORAGE_TOKEN", "dev-storage-token")
DEFAULT_CLIENT_TOKEN = getenv("DFSHA_TOKEN", DEFAULT_DEMO_TOKEN)


def hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def extract_bearer_token(metadata):
    if not metadata:
        return None

    for key, value in metadata:
        if key.lower() != "authorization":
            continue

        scheme, _, token = value.partition(" ")

        if scheme.lower() == "bearer" and token:
            return token.strip()

    return None


class ClientTokenInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, token):
        self.token = token

    def _with_auth(self, client_call_details):
        if not self.token:
            return client_call_details

        metadata = list(client_call_details.metadata or [])
        metadata.append(("authorization", f"Bearer {self.token}"))

        if hasattr(client_call_details, "_replace"):
            return client_call_details._replace(metadata=metadata)

        return client_call_details

    def intercept_unary_unary(
        self, continuation, client_call_details, request
    ):
        return continuation(
            self._with_auth(client_call_details), request
        )

    def intercept_unary_stream(
        self, continuation, client_call_details, request
    ):
        return continuation(
            self._with_auth(client_call_details), request
        )

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return continuation(
            self._with_auth(client_call_details), request_iterator
        )

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return continuation(
            self._with_auth(client_call_details), request_iterator
        )
