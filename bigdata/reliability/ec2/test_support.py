"""Unit checks for paid-resource bounds and cross-host wiring; no AWS mutations."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import launch
import render
import docker_router


def example():
    return {"project": "witw-unit-only", "run_id": "witw-unit-only", "profile": "unit-only",
            "db_password": "unit-lab-not-a-real-secret", "status": "planned", "region": "ap-northeast-2",
            "instance_type": "m7i.xlarge", "estimate": launch.estimate(8), "resources": {},
            "nodes": {role: {"private_ip": f"10.203.0.{index}", "instance_id": f"unit-{role}"}
                      for index, role in enumerate(("infra", "compute", "app"), 10)}}


class SupportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.inventory = example()
        render.render(self.inventory, self.directory)

    def tearDown(self):
        self.temporary.cleanup()

    def role(self, role):
        return json.loads((self.directory / f"compose.{role}.json").read_text())

    def test_every_service_runs_on_exactly_one_host(self):
        seen = []
        for role in ("infra", "compute", "app"):
            services = self.role(role)["services"]
            self.assertTrue(services)
            self.assertTrue(all(render.OWNERS[name] == role for name in services))
            seen.extend(services)
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(set(seen), set(render.OWNERS))

    def test_kafka_metadata_cannot_bypass_isolated_proxy(self):
        env = self.role("infra")["services"]["kafka"]["environment"]
        self.assertEqual(env["KAFKA_ADVERTISED_LISTENERS"],
            "INTERNAL://10.203.0.10:29092,EXTERNAL://10.203.0.10:39992,ISOLATED://10.203.0.11:39092")
        collector = self.role("compute")["services"]["collector-long"]
        self.assertEqual(collector["environment"]["KAFKA_BOOTSTRAP_SERVERS"], "toxiproxy:39092")

    def test_cross_host_database_port_and_private_binds(self):
        for role in ("infra", "compute", "app"):
            data = self.role(role)
            self.assertFalse(data["networks"]["lab"]["internal"])
            for name, service in data["services"].items():
                for port in service["ports"]:
                    self.assertTrue(port.startswith((self.inventory["nodes"][role]["private_ip"] + ":", "127.0.0.1:")))
                if name.startswith("backend-") or name == "spark":
                    self.assertIn("10.203.0.10:39932", service["environment"]["POSTGRESQL_URL"])

    def test_no_operator_secrets_in_remote_inventory(self):
        data = json.loads((self.directory / "inventory.json").read_text())
        self.assertNotIn("db_password", data)
        self.assertNotIn("aws_access_key_id", json.dumps(data))
        self.assertEqual(data["local_role"], "compute")

    def test_generated_shell_environment_uses_linux_newlines(self):
        self.assertNotIn(b"\r", (self.directory / "runtime.env").read_bytes())
        self.assertNotIn(b"\r", (self.directory / "compose.env").read_bytes())

    def test_state_writers_use_the_operator_uid(self):
        for role in ("compute", "app"):
            for name, service in self.role(role)["services"].items():
                if name in ("collector-long", "collector-case", "nlp-replay", "receiver"):
                    self.assertEqual(service["user"], "1000:1000")

    def test_mounts_resolve_to_dedicated_remote_workspace(self):
        for role in ("infra", "compute", "app"):
            for service in self.role(role)["services"].values():
                for mount in service.get("volumes", []):
                    if mount.startswith("/"):
                        self.assertTrue(mount.startswith(render.REMOTE_ROOT + "/"))

    def test_quota_failure_precedes_all_resource_creation(self):
        state = copy.deepcopy(self.inventory)
        state["nodes"] = {}
        called = []
        def fake_aws(state, service, operation, *args):
            called.append(operation)
            self.assertIn(operation, ("get-caller-identity", "get-service-quota"))
            return {"Account": "unit", "Arn": "unit"} if service == "sts" else {"Quota": {"Value": 5}}
        with patch.object(launch, "aws", side_effect=fake_aws):
            with self.assertRaisesRegex(RuntimeError, "12 vCPU"):
                launch.provision(self.directory / "state.json", state)
        self.assertEqual(called, ["get-caller-identity", "get-service-quota"])

    def test_docker_routing_separates_hosts_and_does_not_forward_aws_credentials(self):
        calls = []
        with patch.dict(os.environ, {"LAB_EC2_INVENTORY": str(self.directory / "inventory.json"),
                                      "AWS_SECRET_ACCESS_KEY": "must-not-forward", "LAB_RUN": "unit-run"}):
            with patch.object(docker_router, "execute", side_effect=lambda role, args, **kw: calls.append((role, args, kw)) or ""):
                docker_router.compose(["-p", "witw-unit-only", "-f", "/ignored/base.yml", "up", "-d", "kafka", "spark", "backend-after"])
        self.assertEqual([call[0] for call in calls], ["infra", "app", "compute"])
        self.assertTrue(all("must-not-forward" not in str(call) for call in calls))

    def test_budget_and_watchdog(self):
        self.assertEqual(launch.estimate(8)["estimated_krw"], 10512)
        self.assertLess(launch.estimate(8)["estimated_krw"], 30000)
        timer = launch.user_data("2026-11-24 00:00:00 UTC")
        self.assertIn("OnCalendar=2026-11-24 00:00:00 UTC", timer)
        self.assertIn("Persistent=true", timer)
        self.assertNotIn("OnBootSec", timer)

    @unittest.skipIf(os.name == "nt", "Windows does not preserve Linux mode bits")
    def test_prometheus_mount_is_readable_by_unprivileged_container(self):
        self.assertEqual((self.directory / "prometheus.yml").stat().st_mode & 0o777, 0o644)
        self.assertEqual((self.directory / "runtime.env").stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
