import pytest
from unittest import mock

from botocore.credentials import Credentials

from dbt.adapters.duckdb.credentials import Attachment, DuckDBCredentials


def test_load_basic_settings():
    creds = DuckDBCredentials()
    creds.settings = {
        "s3_access_key_id": "abc",
        "s3_secret_access_key": "xyz",
        "s3_region": "us-west-2",
    }
    settings = creds.load_settings()
    assert creds.settings == settings


def test_add_secret_with_empty_name():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="s3",
                name="",
                key_id="abc",
                secret="xyz",
                region="us-west-2"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds.secrets[0].type.name == "S3"
    assert creds.secrets[0].key_id == "abc"
    assert creds.secrets[0].secret == "xyz"
    assert creds.secrets[0].region == "us-west-2"

    sql = creds.secrets[0].to_sql()
    assert sql == \
"""CREATE SECRET (
    type S3,
    key_id abc,
    secret xyz,
    region us-west-2
)"""


def test_add_secret_with_name():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="s3",
                name="my_secret",
                key_id="abc",
                secret="xyz",
                region="us-west-2"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds.secrets[0].type.name == "S3"
    assert creds.secrets[0].key_id == "abc"
    assert creds.secrets[0].secret == "xyz"
    assert creds.secrets[0].region == "us-west-2"

    sql = creds.secrets[0].to_sql()
    assert sql == \
"""CREATE OR REPLACE SECRET my_secret (
    type S3,
    key_id abc,
    secret xyz,
    region us-west-2
)"""


def test_add_unsupported_secret():
    with pytest.raises(ValueError):
        _ = DuckDBCredentials(
            secrets=[
                dict(
                    type="scrooge_mcduck",
                    name="money"
                )
            ]
        )


def test_add_unsupported_secret_param():
    with pytest.raises(ValueError):
        _ = DuckDBCredentials(
            secrets=[
                dict(
                    type="s3",
                    password="secret"
                )
            ]
        )


def test_add_azure_secret():
    creds = DuckDBCredentials(
        secrets=[
            dict(
                type="azure",
                name="",
                provider="service_principal",
                tenant_id="abc",
                client_id="xyz",
                client_certificate_path="foo\\bar\\baz",
                account_name="123"
            )
        ]
    )
    assert len(creds.secrets) == 1
    assert creds.secrets[0].type.name == "AZURE"
    assert creds.secrets[0].tenant_id == "abc"
    assert creds.secrets[0].client_id == "xyz"
    assert creds.secrets[0].client_certificate_path == "foo\\bar\\baz.pem"
    assert creds.secrets[0].account_name == "123"

    sql = creds.secrets[0].to_sql()
    assert sql == \
"""CREATE SECRET (
    type AZURE,
    provider SERVICE_PRINCIPAL,
    account_name 123,
    tenant_id abc,
    client_id xyz,
    client_certificate_path foo\\bar\\baz.pem
)"""


@mock.patch("boto3.session.Session")
def test_load_aws_creds(mock_session_class):
    mock_session_object = mock.Mock()
    mock_client = mock.Mock()

    mock_session_object.get_credentials.return_value = Credentials(
        "access_key", "secret_key", "token"
    )
    mock_session_object.client.return_value = mock_client
    mock_session_class.return_value = mock_session_object
    mock_client.get_caller_identity.return_value = {}

    creds = DuckDBCredentials(use_credential_provider="aws")
    creds.settings = {"some_other_setting": 1}

    settings = creds.load_settings()
    assert settings["s3_access_key_id"] == "access_key"
    assert settings["s3_secret_access_key"] == "secret_key"
    assert settings["s3_session_token"] == "token"
    assert settings["some_other_setting"] == 1


def test_attachments():
    creds = DuckDBCredentials()
    creds.attach = [
        {"path": "/tmp/f1234.db"},
        {"path": "/tmp/g1234.db", "alias": "g"},
        {"path": "/tmp/h5678.db", "read_only": 1},
        {"path": "/tmp/i9101.db", "type": "sqlite"},
        {"path": "/tmp/jklm.db", "alias": "jk", "read_only": 1, "type": "sqlite"},
    ]

    expected_sql = [
        "ATTACH '/tmp/f1234.db'",
        "ATTACH '/tmp/g1234.db' AS g",
        "ATTACH '/tmp/h5678.db' (READ_ONLY)",
        "ATTACH '/tmp/i9101.db' (TYPE sqlite)",
        "ATTACH '/tmp/jklm.db' AS jk (TYPE sqlite, READ_ONLY)",
    ]

    for i, a in enumerate(creds.attach):
        attachment = Attachment(**a)
        assert expected_sql[i] == attachment.to_sql()


def test_infer_database_name_from_path():
    payload = {}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "memory"

    payload = {"path": "local.duckdb"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "local"

    payload = {"path": "/tmp/f1234.db"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "f1234"

    payload = {"path": "md:?token=abc123"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "my_db"

    payload = {"path": "md:jaffle_shop?token=abc123"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "jaffle_shop"

    payload = {"database": "memory"}
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "memory"

    payload = {
        "database": "remote",
        "remote": {"host": "localhost", "port": 5433, "user": "test"},
    }
    creds = DuckDBCredentials.from_dict(payload)
    assert creds.database == "remote"
