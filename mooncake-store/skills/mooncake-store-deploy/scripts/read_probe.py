#!/usr/bin/env python3
"""Read an object only through the live Mint sidecar and verify its hash."""

import argparse
import importlib
import hashlib
import json
from pathlib import Path
import sys
import time
import tomllib

import grpc

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--config", type=Path, required=True)
p.add_argument("--key", required=True)
p.add_argument("--sha256", required=True)
p.add_argument("--bytes", type=int, required=True)
p.add_argument("--result", type=Path, required=True)
a = p.parse_args()
c = tomllib.loads(a.config.read_text())
sys.path.insert(0, c["stub_path"])
pb = importlib.import_module("store_sidecar_pb2")
rpc = importlib.import_module("store_sidecar_pb2_grpc")

r = {
    "status": "FAIL",
    "key": a.key,
    "master": c.get("external_master_addr"),
    "path": "Mint sidecar GetBlob; no direct OSS client",
}
start = time.monotonic()
try:
    h = hashlib.sha256()
    size = 0
    with grpc.insecure_channel(f"127.0.0.1:{c['ports']['sidecar']}") as channel:
        for chunk in rpc.StoreSidecarServiceStub(channel).GetBlob(
            pb.GetBlobRequest(object_key=a.key), timeout=240
        ):
            h.update(chunk.data)
            size += len(chunk.data)
    r.update(bytes=size, sha256=h.hexdigest())
    if size != a.bytes or h.hexdigest() != a.sha256:
        raise ValueError("sidecar object length or SHA-256 mismatch")
    r["status"] = "PASS"
except Exception as e:
    r["error"] = str(e)
    raise
finally:
    r["seconds"] = time.monotonic() - start
    a.result.write_text(json.dumps(r, indent=2) + "\n")
    print(json.dumps(r), flush=True)
