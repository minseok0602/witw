import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "render_config", Path(__file__).resolve().parents[1] / "alertmanager" / "render_config.py")
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


class AlertConfigTest(unittest.TestCase):
    def test_empty_destination_does_not_send_any_external_notifications(self):
        self.assertNotIn("webhook_configs", config.build_config("")["receivers"][0])

    def test_destination_is_preserved_without_yaml_interpolation(self):
        url = 'https://alerts.example.invalid/hook?token=a:b&label="pipeline"'
        receiver = config.build_config(url)["receivers"][0]
        self.assertEqual(receiver["webhook_configs"][0]["url"], url)
        self.assertTrue(receiver["webhook_configs"][0]["send_resolved"])

    def test_rejects_non_http_destination(self):
        for url in ("file:///tmp/alert", "not-a-url", "https://"):
            with self.assertRaises(ValueError):
                config.build_config(url)
