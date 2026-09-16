#!/usr/bin/env python3
"""Inspect an offline journal or repair damaged records from an independent mirror.

Never edits the input. A repair requires a complete, separately retained mirror
with the SAME number of records; all intact input records must match it exactly.
An older backup, missing record, conflicting fence, or damaged mirror is refused.
The tool cannot reconstruct lost fences; without a matching mirror, stop and
reconcile authoritative records rather than dropping or inventing tombstones.
"""

import argparse
import contextlib
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import stat
import tempfile
import zlib

MAX_RECORD = 4 * 1024 * 2 + 128


def parse_record(line):
    if len(line) > MAX_RECORD or not line.endswith(b"\n"):
        raise ValueError("oversized or incomplete record")
    fields = line[:-1].split(b" ")
    if not fields or fields[0] not in (b"v2", b"v3"):
        raise ValueError("unsupported record version")
    expected = 8 if fields[0] == b"v3" else 7
    if len(fields) != expected or fields[4] not in (b"0", b"1"):
        raise ValueError("invalid record fields")
    decoded = []
    for field in [fields[1], fields[2], fields[3], fields[5]]:
        if not field or len(field) > 2048 or len(field) % 2:
            raise ValueError("invalid hex field length")
        value = bytes.fromhex(field.decode("ascii"))
        if value.hex().encode() != field:
            raise ValueError("noncanonical hex field")
        decoded.append(value)
    grace = None
    if fields[0] == b"v3":
        grace = int(fields[6])
        if not 0 <= grace <= 86400000 or str(grace).encode() != fields[6]:
            raise ValueError("invalid reader grace")
    checksum = str(zlib.crc32(b" ".join(fields[:-1]))).encode()
    if checksum != fields[-1]:
        raise ValueError("checksum mismatch")
    return (*decoded, fields[4] == b"1", grace)


def admit(state, record):
    tenant, key, operation, namespace, completed, grace = record
    identity = (tenant, key)
    old = state.get(identity)
    if old is None:
        if completed:
            raise ValueError("completion without admission")
    elif old[:2] != (operation, namespace) or old[3] != grace:
        raise ValueError("conflicting operation, namespace or grace")
    elif old[2] and not completed:
        raise ValueError("completed fence regressed")
    state[identity] = (operation, namespace, completed, grace)


@contextlib.contextmanager
def locked(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("journal is not a regular file")
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield stream


def lines(stream):
    while line := stream.readline(MAX_RECORD + 1):
        if len(line) > MAX_RECORD:
            raise ValueError(
                "record exceeds recovery limit; manual forensic recovery required"
            )
        yield line


def inspect(path):
    state, damaged = {}, []
    digest = hashlib.sha256()
    count = offset = 0
    with locked(path) as stream:
        for count, line in enumerate(lines(stream), 1):
            digest.update(line)
            try:
                admit(state, parse_record(line))
            except (ValueError, UnicodeError) as error:
                damaged.append(
                    {
                        "line": count,
                        "offset": offset,
                        "sha256": hashlib.sha256(line).hexdigest(),
                        "reason": str(error),
                    }
                )
            offset += len(line)
    return {
        "records": count,
        "bytes": offset,
        "sha256": digest.hexdigest(),
        "damaged": damaged,
    }


def repair(path, mirror, output):
    output = Path(output)
    if output.exists():
        raise ValueError("output already exists; never overwrite a journal")
    changed, state = [], {}
    with locked(path) as original, locked(mirror) as reference:
        original_hash, mirror_hash = hashlib.sha256(), hashlib.sha256()
        for number, (line, good) in enumerate(
            itertools.zip_longest(lines(original), lines(reference)), 1
        ):
            if line is None or good is None:
                raise ValueError(
                    "mirror record count differs; missing fences cannot be reconstructed"
                )
            original_hash.update(line)
            mirror_hash.update(good)
            admit(state, parse_record(good))  # Entire mirror must be canonical.
            try:
                parse_record(line)
            except (ValueError, UnicodeError):
                changed.append(number)
            else:
                if line != good:
                    raise ValueError(
                        "intact record differs from mirror; refusing rollback"
                    )
        if not changed:
            raise ValueError("no damaged records to repair")
        reference.seek(0)
        fd, temporary = tempfile.mkstemp(prefix=".journal-repair-", dir=output.parent)
        try:
            with os.fdopen(fd, "wb") as restored:
                while block := reference.read(1024 * 1024):
                    restored.write(block)
                restored.flush()
                os.fsync(restored.fileno())
            # Atomic publication with no overwrite, even if output appeared
            # between the initial check and this point. Input remains locked.
            os.link(temporary, output)
            directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            os.unlink(temporary)
    return {
        "repaired_lines": changed,
        "input_sha256": original_hash.hexdigest(),
        "output_sha256": mirror_hash.hexdigest(),
        "output": str(output),
        "installed": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal")
    parser.add_argument(
        "--mirror", help="independently verified complete mirror; never an older backup"
    )
    parser.add_argument(
        "--output", help="new file; inputs and active Master are never changed"
    )
    args = parser.parse_args()
    if bool(args.mirror) != bool(args.output):
        parser.error("repair requires both --mirror and --output")
    try:
        result = (
            repair(args.journal, args.mirror, args.output)
            if args.mirror
            else inspect(args.journal)
        )
    except (OSError, ValueError) as error:
        parser.exit(1, f"Recovery refused: {error}\n")
    print(json.dumps(result, indent=2))
    return 1 if result.get("damaged") else 0


if __name__ == "__main__":
    raise SystemExit(main())
