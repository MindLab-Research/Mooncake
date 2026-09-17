import importlib.util
from pathlib import Path
import tempfile
import unittest
import zlib

SPEC = importlib.util.spec_from_file_location(
    "repair", Path(__file__).parents[1] / "tools/repair_delete_journal.py"
)
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def record(key=b"key", done=False):
    body = (
        b"v3 74656e616e74 "
        + key.hex().encode()
        + b" 6f70 "
        + (b"1" if done else b"0")
        + b" 73636f7065 900000"
    )
    return body + b" " + str(zlib.crc32(body)).encode() + b"\n"


class JournalRepairTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.original = self.root / "journal"
        self.mirror = self.root / "mirror"
        self.output = self.root / "repaired"
        self.good = record() + record(done=True) + record(b"next")
        self.mirror.write_bytes(self.good)
        self.bad = (
            record()
            + record(done=True).replace(b"73636f7065", b"73636f7066")
            + record(b"next")
        )
        self.original.write_bytes(self.bad)

    def test_repair_preserves_all_fences_and_original(self):
        self.assertTrue(repair.inspect(self.original)["damaged"])
        result = repair.repair(self.original, self.mirror, self.output)
        self.assertEqual(result["repaired_lines"], [2])
        self.assertEqual(self.original.read_bytes(), self.bad)
        self.assertEqual(self.output.read_bytes(), self.good)
        self.assertFalse(repair.inspect(self.output)["damaged"])
        self.assertFalse(result["installed"])

    def test_older_or_truncated_mirror_refused(self):
        self.mirror.write_bytes(record())
        with self.assertRaises(ValueError):
            repair.repair(self.original, self.mirror, self.output)
        self.assertFalse(self.output.exists())

    def test_intact_record_conflict_refused(self):
        self.mirror.write_bytes(record() + record(done=True) + record(b"other"))
        with self.assertRaises(ValueError):
            repair.repair(self.original, self.mirror, self.output)

    def test_corrupt_mirror_refused(self):
        self.mirror.write_bytes(self.bad)
        with self.assertRaises(ValueError):
            repair.repair(self.original, self.mirror, self.output)

    def test_live_lock_and_output_overwrite_refused(self):
        with repair.locked(self.original):
            with self.assertRaises(BlockingIOError):
                repair.repair(self.original, self.mirror, self.output)
        self.output.write_bytes(b"keep")
        with self.assertRaises(ValueError):
            repair.repair(self.original, self.mirror, self.output)
        self.assertEqual(self.output.read_bytes(), b"keep")

    def test_conflicting_completed_phase_refused(self):
        self.mirror.write_bytes(record() + record(done=True) + record())
        with self.assertRaises(ValueError):
            repair.repair(self.original, self.mirror, self.output)


if __name__ == "__main__":
    unittest.main()
