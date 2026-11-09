import grpc
import pytest

ENDPOINTS = {
    "Metadata Service": "localhost:50051",
    "Data Service": "localhost:50052",
}


@pytest.fixture(scope="session", autouse=True)
def require_running_stack():
    for name, endpoint in ENDPOINTS.items():
        channel = grpc.insecure_channel(endpoint)

        try:
            grpc.channel_ready_future(channel).result(timeout=3)
        except grpc.FutureTimeoutError:
            pytest.skip(
                f"{name} is not reachable at {endpoint}; "
                "start the Docker Compose stack first"
            )
        finally:
            channel.close()
