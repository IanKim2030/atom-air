"""Regression tests for learned-remote carrier storage and bundle generation."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pydantic import ValidationError

from cloud import cloud_server as server


class IRFrequencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.previous_db = server.DB_PATH
        server.DB_PATH = Path(self.tmp.name) / "cloud.db"
        server.init_db()
        self.model = server.db_create_ac_model("WINIA", "CI test", None)

    def tearDown(self) -> None:
        server.DB_PATH = self.previous_db
        self.tmp.cleanup()

    def save(self, slot: str, freq_khz: int) -> None:
        raw = [3400, 1700] + [450, 450] * 9
        server.db_save_ir_code(
            self.model["model_id"], slot, freq_khz, raw,
            "ci", "S001", 1,
        )

    def test_bundle_preserves_each_slots_carrier(self) -> None:
        self.save("power_on", 36)
        self.save("temp_up", 56)

        bundle = server.db_ir_bundle(self.model["model_id"])

        self.assertIsNotNone(bundle)
        self.assertEqual(bundle["slots"]["power_on"]["freq_khz"], 36)
        self.assertEqual(bundle["slots"]["temp_up"]["freq_khz"], 56)

    def test_learning_request_defaults_to_38khz(self) -> None:
        request = server.LearnStartRequest(
            dev_id=1, model_id=self.model["model_id"], slot="power_on",
        )
        self.assertEqual(request.freq_khz, 38)

    def test_learning_request_accepts_only_supported_range(self) -> None:
        for freq in (30, 36, 38, 40, 56, 60):
            request = server.LearnStartRequest(
                dev_id=1, model_id=self.model["model_id"],
                slot="power_on", freq_khz=freq,
            )
            self.assertEqual(request.freq_khz, freq)

        for freq in (29, 61):
            with self.assertRaises(ValidationError):
                server.LearnStartRequest(
                    dev_id=1, model_id=self.model["model_id"],
                    slot="power_on", freq_khz=freq,
                )


if __name__ == "__main__":
    unittest.main()
