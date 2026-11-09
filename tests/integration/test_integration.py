import pytest

from scripts import (
    test_auth as auth_script,
    test_download_validation as download_validation_script,
    test_namespace_operations as namespace_script,
    test_path_operations as path_script,
    test_storage as storage_script,
    test_storage_recovery as recovery_script,
    test_upload_leases as lease_script,
    test_upload_validation as upload_validation_script,
)

pytestmark = pytest.mark.integration


def test_upload_validation():
    upload_validation_script.main()


def test_download_validation():
    download_validation_script.main()


def test_namespace_operations():
    namespace_script.main()


def test_path_operations():
    path_script.main()


def test_upload_leases():
    lease_script.main()


def test_authentication_and_authorization():
    auth_script.main()


@pytest.mark.parametrize("port", [50061, 50062, 50063])
def test_storage_node(port):
    storage_script.test_node(f"localhost:{port}")


def test_storage_recovery():
    recovery_script.main()
