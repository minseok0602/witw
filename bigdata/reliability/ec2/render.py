"""Render one Compose file per host from the tested single-host topology."""
import copy
import json
from pathlib import Path
import shlex
import sys

import yaml

LAB = Path(__file__).resolve().parents[1]
REMOTE_ROOT = "/opt/witw/repo"
REMOTE_LAB = REMOTE_ROOT + "/bigdata/reliability"
OWNERS = {
    "kafka": "infra", "postgres": "infra", "redis": "infra", "kafka-exporter": "infra",
    "collector-long": "compute", "collector-case": "compute", "nlp-replay": "compute",
    "spark": "compute", "toxiproxy": "compute", "backend-before": "app", "backend-after": "app",
    "receiver": "app", "prometheus": "app", "alertmanager": "app",
}


def bind_path(source):
    path = Path(source)
    # Inputs in the source Compose file are relative to the reliability directory.
    resolved = (LAB / path).resolve()
    relative = resolved.relative_to(LAB.parents[1]).as_posix()
    return REMOTE_ROOT + "/" + relative


def render(inventory, output):
    base = yaml.safe_load((LAB / "compose.yml").read_text(encoding="utf-8"))
    nodes = inventory["nodes"]
    addresses = {role: node["private_ip"] for role, node in nodes.items()}
    infra, compute, app = (addresses[role] for role in ("infra", "compute", "app"))
    aliases = {name: addresses[role] for name, role in OWNERS.items()
               if name not in ("backend-before", "backend-after")}
    ports = {
        "kafka": [(29092, 29092), (39992, 9092), (39092, 39092)],
        "postgres": [(39932, 5432)], "redis": [(6379, 6379)], "kafka-exporter": [(9308, 9308)],
        "toxiproxy": [(38474, 8474), (39092, 39092)], "spark": [(9102, 9102)],
        "collector-long": [(9103, 9103)], "receiver": [(39904, 8080)],
        "prometheus": [(39900, 9090)], "alertmanager": [(39901, 9093)],
        "backend-before": [(9091, 9091)], "backend-after": [(9092, 9091)],
    }
    output.mkdir(parents=True, exist_ok=True)
    for role in ("infra", "compute", "app"):
        services = {}
        for name, original in base["services"].items():
            if OWNERS[name] != role:
                continue
            service = copy.deepcopy(original)
            service["restart"] = "always"
            service["networks"] = ["lab"]
            # Keep names local when a real Docker service is present on this host.
            service["extra_hosts"] = {alias: address for alias, address in aliases.items()
                                       if OWNERS[alias] != role}
            service["ports"] = [f"{addresses[role]}:{host}:{target}" for host, target in ports.get(name, [])]
            if name == "toxiproxy":
                service["ports"].append("127.0.0.1:38474:8474")
            volumes = []
            for mount in service.get("volumes", []):
                source, target, *flags = mount.split(":")
                if source.startswith("."):
                    source = bind_path(source)
                if name == "prometheus" and target == "/etc/prometheus/prometheus.yml":
                    source = REMOTE_LAB + "/.generated/ec2/prometheus.yml"
                volumes.append(":".join([source, target, *flags]))
            service["volumes"] = volumes
            env = service.get("environment", {})
            for key in ("POSTGRES_PASSWORD", "POSTGRESQL_PASSWORD"):
                if key in env:
                    env[key] = "${LAB_DB_PASSWORD:?lab password required}"
            if name == "kafka":
                env["KAFKA_ADVERTISED_LISTENERS"] = (
                    f"INTERNAL://{infra}:29092,EXTERNAL://{infra}:39992,ISOLATED://{compute}:39092")
            if name.startswith("backend-") or name == "spark":
                variant = name.removeprefix("backend-") if name.startswith("backend-") else "after"
                env["POSTGRESQL_URL"] = f"jdbc:postgresql://{infra}:39932/witw_{variant}"
            if name == "nlp-replay":
                env["KAFKA_CONSUMER_GROUP"] = "nlp-worker-v1"
            if name == "collector-long":
                env["LAB_DB_ROOT"] = "/persist"
                service["volumes"].append("collector-storage:/persist")
            if name in ("collector-case", "collector-long", "nlp-replay", "receiver"):
                service["user"] = "1000:1000"
                service["healthcheck"] = {"disable": True}
            if name == "kafka-exporter":
                service["command"][-1] = "--group.filter=^(nlp-worker-v1|db-writer-live-.*)$"
            services[name] = service
        volume_names = {mount.split(":", 1)[0] for service in services.values()
                        for mount in service.get("volumes", []) if not mount.startswith("/")}
        data = {"name": inventory["project"], "services": services,
                "networks": {"lab": {"internal": False}},
                "volumes": {name: {"labels": {"codex.task": inventory["run_id"]}} for name in volume_names}}
        (output / f"compose.{role}.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
    prometheus = yaml.safe_load((LAB / "prometheus.yml").read_text(encoding="utf-8"))
    prometheus["global"]["external_labels"]["cluster"] = inventory["project"]
    (output / "prometheus.yml").write_text(yaml.safe_dump(prometheus, sort_keys=False), encoding="utf-8")
    environment = {
        "LAB_PROJECT": inventory["project"], "LAB_KAFKA_BOOTSTRAP": f"{infra}:39992",
        "LAB_DB_HOST": infra, "LAB_DB_PORT": "39932", "LAB_DB_PASSWORD": inventory["db_password"],
        "LAB_RECEIVER_URL": f"http://{app}:39904", "LAB_PROXY_URL": "http://127.0.0.1:38474",
        "LAB_PROMETHEUS_URL": f"http://{app}:39900", "LAB_NLP_GROUP": "nlp-worker-v1",
        "LAB_EC2_INVENTORY": REMOTE_LAB + "/.generated/ec2/inventory.json",
    }
    (output / "runtime.env").write_text("\n".join(f"export {key}={shlex.quote(value)}" for key, value in environment.items()) + "\n", encoding="utf-8", newline="\n")
    (output / "compose.env").write_text("LAB_DB_PASSWORD=" + inventory["db_password"] + "\n", encoding="utf-8", newline="\n")
    public_inventory = copy.deepcopy(inventory)
    public_inventory.pop("db_password", None)
    public_inventory["local_role"] = "compute"
    public_inventory["ssh_key"] = "/home/ubuntu/.ssh/witw-lab"
    (output / "inventory.json").write_text(json.dumps(public_inventory, indent=2), encoding="utf-8")
    for path in output.iterdir():
        if path.is_file():
            # Prometheus runs as nobody and must read its non-secret bind mount.
            path.chmod(0o644 if path.name == "prometheus.yml" else 0o600)


if __name__ == "__main__":
    render(json.loads(Path(sys.argv[1]).read_text()), Path(sys.argv[2]))
