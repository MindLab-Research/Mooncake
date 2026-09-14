#!/usr/bin/env python3
"""Isolated development validation; credentials here are for loopback MinIO only."""

import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
from urllib.parse import urlsplit

ROOT = Path(os.environ.get("MOONCAKE_VALIDATION_ROOT", "/opt/mindlab"))
EVIDENCE = Path(
    os.environ.get(
        "MOONCAKE_VALIDATION_EVIDENCE", str(ROOT / "mooncake-evidence/2026-09-08/s3")
    )
)
ENV = os.environ.copy()
DEFAULTS = {
    "MOONCAKE_AWS_ACCESS_KEY_ID": "mooncake-test",
    "MOONCAKE_AWS_SECRET_ACCESS_KEY": "mooncake-isolated-test-only",
    "MOONCAKE_AWS_BUCKET_NAME": "mooncake-dev-validation",
    "MOONCAKE_AWS_REGION": "us-east-1",
    "MOONCAKE_AWS_S3_ENDPOINT": "http://127.0.0.1:19000",
    "MOONCAKE_AWS_USE_HTTPS": "false",
    "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING": "false",
    "MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION": "when_required",
    "MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION": "when_required",
    "MOONCAKE_AWS_CONNECT_TIMEOUT_MS": "3000",
    "MOONCAKE_AWS_REQUEST_TIMEOUT_MS": "15000",
    "MOONCAKE_S3_KEY_PREFIX": "validation-20260908",
    "MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR": "s3_object_storage_backend",
    "MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES": "67108864",
    "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS": "1",
    # The caller compares total cloud-read time against this buffer lifetime.
    "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS": "60000",
    "MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION": "false",
    "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES": "1073741824",
    "MC_MS_AUTO_DISC": "0",
    "GLOG_logtostderr": "1",
}
BIN = ROOT / "mooncake-s3-build/mooncake-store/src"


def configure(source, cloud=False):
    env = source.copy()
    if not cloud:
        endpoint = urlsplit(
            env.get("MOONCAKE_AWS_S3_ENDPOINT", DEFAULTS["MOONCAKE_AWS_S3_ENDPOINT"])
        )
        if endpoint.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise ValueError("Non-loopback endpoints require --cloud")
    if cloud:
        required = (
            "MOONCAKE_AWS_ACCESS_KEY_ID",
            "MOONCAKE_AWS_SECRET_ACCESS_KEY",
            "MOONCAKE_AWS_BUCKET_NAME",
            "MOONCAKE_AWS_REGION",
            "MOONCAKE_AWS_S3_ENDPOINT",
            "MOONCAKE_S3_KEY_PREFIX",
            "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING",
        )
        missing = [name for name in required if not env.get(name)]
        if missing:
            raise ValueError("Missing cloud configuration: " + ", ".join(missing))
        endpoint = urlsplit(env["MOONCAKE_AWS_S3_ENDPOINT"])
        if (
            endpoint.scheme != "https"
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.query
            or endpoint.fragment
            or endpoint.path not in ("", "/")
        ):
            raise ValueError(
                "Cloud endpoint must be an HTTPS service URL without credentials or path"
            )
        if env.get("MOONCAKE_AWS_USE_HTTPS", "true") != "true":
            raise ValueError("Cloud mode requires HTTPS")
        if env["MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING"] not in ("true", "false"):
            raise ValueError("Addressing mode must be true or false")
        prefix = env["MOONCAKE_S3_KEY_PREFIX"]
        if not prefix.startswith("mooncake-validation-") or "/" in prefix:
            raise ValueError("Use a dedicated mooncake-validation-<run-id> prefix")
        if any(env[name] == DEFAULTS[name] for name in required[:2]):
            raise ValueError("MinIO test credentials are not valid cloud configuration")
        if env.get("AWS_SESSION_TOKEN") or env.get("MOONCAKE_AWS_SESSION_TOKEN"):
            raise ValueError(
                "This native build does not yet support STS session tokens"
            )
        env["MOONCAKE_AWS_USE_HTTPS"] = "true"
    for name, value in DEFAULTS.items():
        env.setdefault(name, value)
    env["MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR"] = "s3_object_storage_backend"
    env["LD_LIBRARY_PATH"] = (
        str(ROOT / "aws-install/lib") + ":" + env.get("LD_LIBRARY_PATH", "")
    )
    return env


def physical_key(key):
    return (
        ENV["MOONCAKE_S3_KEY_PREFIX"] + "/objects/" + ("default\0" + key).encode().hex()
    )


def wait_port(port, process=None):
    for _ in range(100):
        if process and process.poll() is not None:
            raise RuntimeError(f"process exited: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port not ready: {port}")


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=ENV["MOONCAKE_AWS_S3_ENDPOINT"],
        region_name=ENV["MOONCAKE_AWS_REGION"],
        aws_access_key_id=ENV["MOONCAKE_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=ENV["MOONCAKE_AWS_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            connect_timeout=int(ENV["MOONCAKE_AWS_CONNECT_TIMEOUT_MS"]) / 1000,
            read_timeout=int(ENV["MOONCAKE_AWS_REQUEST_TIMEOUT_MS"]) / 1000,
            retries={"mode": "standard", "total_max_attempts": 3},
            request_checksum_calculation=ENV[
                "MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION"
            ],
            response_checksum_validation=ENV[
                "MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION"
            ],
            s3={
                "addressing_style": "virtual"
                if ENV["MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING"] == "true"
                else "path"
            },
        ),
    )


def verify_object(client, key, size):
    response = client.get_object(
        Bucket=ENV["MOONCAKE_AWS_BUCKET_NAME"], Key=physical_key(key)
    )
    with response["Body"] as body:
        data = body.read(size + 1)
    expected = bytes((i * 131 + 17) % 251 for i in range(size))
    if response["ContentLength"] != size or data != expected:
        raise RuntimeError("independent bucket verification failed")
    return hashlib.sha256(data).hexdigest()


def preflight():
    # Only inspect the approved prefix. HeadBucket/ListBuckets may need broader permissions.
    client = s3_client()
    response = client.list_objects_v2(
        Bucket=ENV["MOONCAKE_AWS_BUCKET_NAME"],
        Prefix=ENV["MOONCAKE_S3_KEY_PREFIX"] + "/objects/",
        MaxKeys=1,
    )
    result = {
        "result": "PASS",
        "phase": "preflight",
        "list_permission": True,
        "has_objects": bool(response.get("Contents")),
        "note": "Read-only List probe; native Put/Get still required",
    }
    (EVIDENCE / "preflight.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


def serve():
    class Audit(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                event = json.loads(data)
                # Store only operation evidence, never headers or credentials.
                record = {
                    k: event.get(k) for k in ("time", "api", "requestID", "remotehost")
                }
                with (EVIDENCE / "minio-audit.jsonl").open("a") as output:
                    output.write(json.dumps(record) + "\n")
            except (ValueError, OSError):
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    audit = http.server.ThreadingHTTPServer(("127.0.0.1", 19003), Audit)
    threading.Thread(target=audit.serve_forever, daemon=True).start()
    env = ENV | {
        "MINIO_ROOT_USER": DEFAULTS["MOONCAKE_AWS_ACCESS_KEY_ID"],
        "MINIO_ROOT_PASSWORD": DEFAULTS["MOONCAKE_AWS_SECRET_ACCESS_KEY"],
        "MINIO_AUDIT_WEBHOOK_ENABLE_PRIMARY": "on",
        "MINIO_AUDIT_WEBHOOK_ENDPOINT_PRIMARY": "http://127.0.0.1:19003",
    }
    with (EVIDENCE / "minio.log").open("a") as log:
        process = subprocess.Popen(
            [
                str(ROOT / "minio"),
                "server",
                str(ROOT / "minio-dev-data"),
                "--address",
                "127.0.0.1:19000",
                "--console-address",
                "127.0.0.1:19001",
            ],
            env=env,
            stdout=log,
            stderr=log,
        )

        def stop(*args):
            process.terminate()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            wait_port(19000, process)
            client = s3_client()
            bucket = DEFAULTS["MOONCAKE_AWS_BUCKET_NAME"]
            if bucket not in [b["Name"] for b in client.list_buckets()["Buckets"]]:
                client.create_bucket(Bucket=bucket)
            print("MinIO ready on loopback 19000", flush=True)
            process.wait()
        finally:
            process.terminate()
            process.wait(timeout=15)
            audit.shutdown()


def run(args):
    run_id = f'{args.site}-{args.phase}-{args.key.replace("/", "_")}-{args.size}'
    directory = EVIDENCE / run_id
    directory.mkdir(exist_ok=True)
    command = [str(BIN / "s3_store_smoke"), args.phase]
    env = ENV.copy()
    if args.phase == "init-fails":
        if args.fault == "credentials":
            env["MOONCAKE_AWS_SECRET_ACCESS_KEY"] = "intentionally-wrong"
        elif args.fault == "bucket":
            env["MOONCAKE_AWS_BUCKET_NAME"] = "nonexistent-validation-bucket"
        elif args.fault == "network":
            env["MOONCAKE_AWS_S3_ENDPOINT"] = "http://127.0.0.1:19999"
        run_id += "-" + args.fault
    if args.phase in ("put", "get"):
        command += [
            args.key,
            str(args.size),
            "127.0.0.1:16300",
            "127.0.0.1:15400",
            "15300",
            "default",
        ]
    master = None
    with (directory / "master.log").open("w") as master_log:
        try:
            if args.phase in ("put", "get"):
                master = subprocess.Popen(
                    [
                        str(BIN / "mooncake_master"),
                        "--port=15400",
                        "--rpc_address=127.0.0.1",
                        "--rpc_thread_num=2",
                        "--enable_offload=true",
                        "--default_kv_lease_ttl=120000",
                        "--enable_metric_reporting=false",
                        "--metrics_host=127.0.0.1",
                        "--metrics_port=15401",
                        "--enable_http_metadata_server=false",
                    ],
                    env=env,
                    stdout=master_log,
                    stderr=master_log,
                )
                wait_port(15400, master)
            started = time.monotonic()
            result = subprocess.run(command, env=env, capture_output=True, timeout=240)
            (directory / f"{run_id}.stdout").write_bytes(result.stdout)
            (directory / f"{run_id}.stderr").write_bytes(result.stderr)
            summary = {
                "run_id": run_id,
                "returncode": result.returncode,
                "elapsed_s": time.monotonic() - started,
                "provider_mode": "cloud" if args.cloud else "isolated-minio",
                "endpoint": env["MOONCAKE_AWS_S3_ENDPOINT"],
                "bucket": env["MOONCAKE_AWS_BUCKET_NAME"],
                "prefix": env["MOONCAKE_S3_KEY_PREFIX"],
            }
            if result.returncode == 0 and args.phase in ("put", "get"):
                client = s3_client()
                summary["bucket_sha256"] = verify_object(client, args.key, args.size)
                summary["bucket_object_key"] = physical_key(args.key)
            (directory / f"{run_id}.json").write_text(json.dumps(summary, indent=2))
            print(json.dumps(summary), flush=True)
            print(result.stdout.decode(), end="", flush=True)
            if result.returncode:
                print(result.stderr.decode()[-4000:], flush=True)
                raise SystemExit(result.returncode)
        finally:
            if master:
                master.terminate()
                try:
                    master.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    master.kill()
                    master.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "phase", choices=["serve", "adapter", "init-fails", "put", "get", "preflight"]
    )
    parser.add_argument(
        "--cloud",
        action="store_true",
        help="Require explicit cloud configuration; never use MinIO credentials",
    )
    parser.add_argument("--site", default="bangkok")
    parser.add_argument("--key", default="s3-smoke-object")
    parser.add_argument("--size", type=int, default=65536)
    parser.add_argument(
        "--fault", choices=["credentials", "bucket", "network"], default="credentials"
    )
    args = parser.parse_args()
    if args.cloud and args.phase not in ("preflight", "put", "get"):
        parser.error(
            "Cloud mode supports preflight/put/get only; other phases are isolated MinIO tests"
        )
    if not 0 < args.size <= 64 * 1024 * 1024:
        parser.error("Test size must be between 1 byte and 64 MiB")
    if any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        for c in args.site + args.key
    ):
        parser.error("Site and key must use letters, digits, hyphens or underscores")
    try:
        ENV = configure(ENV, args.cloud)
    except ValueError as error:
        parser.error(str(error))
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if args.phase == "serve":
        serve()
    elif args.phase == "preflight":
        preflight()
    else:
        run(args)
