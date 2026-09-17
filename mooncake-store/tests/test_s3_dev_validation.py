"""Cloud validation guardrails and exact provider-side verification."""

import io
import unittest
from unittest.mock import Mock

import s3_dev_validation as validation


class ValidationTests(unittest.TestCase):
    def cloud(self):
        return {
            "MOONCAKE_AWS_ACCESS_KEY_ID": "fake-cloud-id",
            "MOONCAKE_AWS_SECRET_ACCESS_KEY": "fake-cloud-secret",
            "MOONCAKE_AWS_BUCKET_NAME": "test-bucket",
            "MOONCAKE_AWS_REGION": "test-region",
            "MOONCAKE_AWS_S3_ENDPOINT": "https://oss.example.test",
            "MOONCAKE_S3_KEY_PREFIX": "mooncake-validation-example",
            "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING": "true",
        }

    def test_missing_cloud_values_never_fall_back_to_minio(self):
        for key in self.cloud():
            with self.subTest(key=key):
                env = self.cloud()
                del env[key]
                with self.assertRaises(ValueError):
                    validation.configure(env, cloud=True)

    def test_cloud_rejects_unsafe_or_unsupported_configuration(self):
        for key, value in (
            ("MOONCAKE_AWS_S3_ENDPOINT", "http://oss.example.test"),
            ("MOONCAKE_AWS_S3_ENDPOINT", "https://id:secret@oss.example.test"),
            ("MOONCAKE_AWS_S3_ENDPOINT", "https://oss.example.test/?secret=value"),
            ("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "yes"),
            ("MOONCAKE_AWS_USE_HTTPS", "false"),
            ("MOONCAKE_S3_KEY_PREFIX", "production"),
            ("MOONCAKE_AWS_ACCESS_KEY_ID", "mooncake-test"),
            ("MOONCAKE_AWS_SESSION_TOKEN", "fake-sts-token"),
        ):
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError):
                    validation.configure(self.cloud() | {key: value}, cloud=True)

    def test_external_endpoint_requires_cloud_mode(self):
        with self.assertRaises(ValueError):
            validation.configure(self.cloud())

    def test_cloud_preserves_addressing_mode_and_forces_s3_backend(self):
        env = validation.configure(
            self.cloud()
            | {"MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR": "bucket_storage_backend"},
            cloud=True,
        )
        self.assertEqual(env["MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING"], "true")
        self.assertEqual(env["MOONCAKE_AWS_USE_HTTPS"], "true")
        self.assertEqual(
            env["MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR"],
            "s3_object_storage_backend",
        )

    def test_direct_get_uses_full_tenant_key_without_listing(self):
        validation.ENV = validation.configure({})
        client = Mock()
        body = io.BytesIO(bytes((i * 131 + 17) % 251 for i in range(64)))
        client.get_object.return_value = {"Body": body, "ContentLength": 64}
        validation.verify_object(client, "key", 64)
        client.get_object.assert_called_once_with(
            Bucket="mooncake-dev-validation",
            Key="validation-20260908/objects/" + b"default\0key".hex(),
        )
        client.list_objects_v2.assert_not_called()
        self.assertTrue(body.closed)

    def test_truncated_and_oversized_payloads_fail(self):
        validation.ENV = validation.configure({})
        for data in (b"", b"incorrect", bytes(range(65))):
            client = Mock()
            client.get_object.return_value = {
                "Body": io.BytesIO(data),
                "ContentLength": 64,
            }
            with self.assertRaises(RuntimeError):
                validation.verify_object(client, "key", 64)


if __name__ == "__main__":
    unittest.main()
