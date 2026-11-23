"""Create, deploy, collect and remove a dedicated three-instance experiment.

AWS CLI credentials stay on the operator's machine. Only experiment code,
synthetic fixtures and a new lab-only SSH key are sent to the instances.
"""
import argparse
from datetime import datetime, timedelta, timezone
import getpass
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
import uuid

from render import LAB, REMOTE_ROOT, REMOTE_LAB, OWNERS, render

ROOT = LAB.parents[1]
RUNS = LAB / ".generated/ec2-runs"
IMAGES = {
    "infra": ["apache/kafka:4.3.1", "postgres:17-alpine", "redis:7-alpine", "danielqsj/kafka-exporter:v1.9.0"],
    "compute": ["codex-pipeline-collector:verify", "witw-reliability-spark:verify", "ghcr.io/shopify/toxiproxy:2.12.0"],
    "app": ["witw-reliability-backend:before", "witw-reliability-backend:after", "codex-pipeline-collector:verify",
            "news-kafka-hdfs-writer-test:latest", "prom/prometheus:v3.13.1", "prom/alertmanager:v0.28.1"],
}


def run(args, capture=True, **kwargs):
    kwargs.setdefault("encoding", "utf-8")
    kwargs.setdefault("errors", "replace")
    result = subprocess.run(list(map(str, args)), check=True, text=True,
                            stdout=subprocess.PIPE if capture else None, **kwargs)
    return result.stdout if capture else ""


def save(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temporary.replace(path)


def aws(state, service, operation, *args):
    output = run(["aws", "--profile", state["profile"], "--region", state["region"],
                  service, operation, *args, "--output", "json", "--no-cli-pager"], stderr=subprocess.PIPE,
                  encoding="utf-8", env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    return json.loads(output) if output.strip() else {}


def protected_directory(path):
    path.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        user = os.environ.get("USERDOMAIN", ".") + "\\" + getpass.getuser()
        run(["icacls", str(path), "/inheritance:r", "/grant:r", user + ":(OI)(CI)F"])
    else:
        path.chmod(0o700)


INSTANCE_TYPES = {"m7i.xlarge": {"vcpus": 4, "hourly_usd": 0.2478},
                  "m7i.2xlarge": {"vcpus": 8, "hourly_usd": 0.4956}}


def estimate(hours, instance_type="m7i.xlarge"):
    hourly = 3 * (INSTANCE_TYPES[instance_type]["hourly_usd"] + 100 * 0.0912 / 720 + 0.005)
    return {"hours": hours, "instance_count": 3, "instance_type": instance_type, "root_gb_each": 100,
            "usd": round(hourly * hours, 4), "assumed_usd_krw": 1500, "assumed_vat": 0.1,
            "estimated_krw": round(hourly * hours * 1500 * 1.1), "budget_krw": 30000,
            "pricing_source": "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/ap-northeast-2/index.csv"}


def new_plan(profile):
    run_id = "witw-rel-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
    directory = RUNS / run_id
    protected_directory(directory)
    state = {"run_id": run_id, "project": run_id, "profile": profile, "region": "ap-northeast-2",
             "status": "planned", "estimate": estimate(8), "nodes": {}, "resources": {},
             "root_gb": 100, "instance_type": "m7i.xlarge", "db_password": secrets.token_urlsafe(24)}
    save(directory / "state.json", state)
    (RUNS / "latest.txt").write_text(str(directory / "state.json"), encoding="utf-8")
    print(json.dumps({"state_file": str(directory / "state.json"), "estimate": state["estimate"],
                      "status": "planned; no AWS resources created"}, indent=2))
    return directory / "state.json", state


def tag_spec(state, kind):
    return json.dumps([{"ResourceType": kind, "Tags": [
        {"Key": "LabRun", "Value": state["run_id"]}, {"Key": "Purpose", "Value": "pipeline-reliability-experiment"},
        {"Key": "Name", "Value": state["run_id"]}]}])


def user_data(deadline):
    return f"""#!/bin/bash
set -euxo pipefail
install -d /etc/systemd/system
cat >/etc/systemd/system/witw-lab-expire.service <<'SERVICE'
[Unit]
Description=Terminate the temporary WITW experiment at its absolute deadline
[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl poweroff
SERVICE
cat >/etc/systemd/system/witw-lab-expire.timer <<'TIMER'
[Unit]
Description=Absolute experiment expiry (survives reboot)
[Timer]
OnCalendar={deadline}
Persistent=true
[Install]
WantedBy=timers.target
TIMER
systemctl daemon-reload
systemctl enable --now witw-lab-expire.timer
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io docker-compose-v2 python3-venv chrony
systemctl enable --now docker chrony
usermod -aG docker ubuntu
install -d -o ubuntu -g ubuntu /opt/witw/repo /opt/witw/bin
python3 -m venv /opt/witw/venv
chown -R ubuntu:ubuntu /opt/witw
"""


def authenticate(path, state):
    identity = aws(state, "sts", "get-caller-identity")
    if state.get("account") and identity["Account"] != state["account"]:
        raise RuntimeError("AWS account changed; refusing to access this experiment")
    state["account"] = identity["Account"]
    state["identity_arn"] = identity["Arn"]
    save(path, state)
    return identity


def provision(path, state):
    if state["status"] != "planned":
        raise RuntimeError("This plan already has provisioning state; inspect/status/destroy it before creating another")
    authenticate(path, state)
    quota = aws(state, "service-quotas", "get-service-quota", "--service-code", "ec2", "--quota-code", "L-1216C47A")
    state["standard_vcpu_quota"] = quota["Quota"]["Value"]
    save(path, state)
    required_vcpus = 3 * INSTANCE_TYPES[state["instance_type"]]["vcpus"]
    if state["standard_vcpu_quota"] < required_vcpus:
        raise RuntimeError(f"Seoul EC2 quota is {state['standard_vcpu_quota']} vCPU; {required_vcpus} vCPU approval is required before any paid resources are created")
    if state["estimate"]["estimated_krw"] > 30000:
        raise RuntimeError("Estimated timed experiment exceeds the agreed budget")
    # Check an available Ubuntu image and instance offering before creating anything.
    images = aws(state, "ec2", "describe-images", "--owners", "099720109477", "--filters",
        "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*",
        "Name=state,Values=available", "Name=architecture,Values=x86_64")
    image = max(images["Images"], key=lambda item: item["CreationDate"])
    offerings = aws(state, "ec2", "describe-instance-type-offerings", "--location-type", "availability-zone",
                    "--filters", "Name=instance-type,Values=" + state["instance_type"])
    zone = sorted(item["Location"] for item in offerings["InstanceTypeOfferings"])[0]
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as response:
        operator_ip = str(ipaddress.IPv4Address(response.read().decode().strip()))
    state.update({"ami": image["ImageId"], "availability_zone": zone, "operator_cidr": operator_ip + "/32",
                  "status": "provisioning"})
    save(path, state)
    resources = state["resources"]
    resources["vpc"] = aws(state, "ec2", "create-vpc", "--cidr-block", "10.203.0.0/24",
        "--tag-specifications", tag_spec(state, "vpc"))["Vpc"]["VpcId"]
    save(path, state)
    aws(state, "ec2", "modify-vpc-attribute", "--vpc-id", resources["vpc"], "--enable-dns-support", "{\"Value\":true}")
    resources["subnet"] = aws(state, "ec2", "create-subnet", "--vpc-id", resources["vpc"], "--cidr-block", "10.203.0.0/24",
        "--availability-zone", zone, "--tag-specifications", tag_spec(state, "subnet"))["Subnet"]["SubnetId"]
    save(path, state)
    resources["igw"] = aws(state, "ec2", "create-internet-gateway", "--tag-specifications",
        tag_spec(state, "internet-gateway"))["InternetGateway"]["InternetGatewayId"]
    save(path, state)
    aws(state, "ec2", "attach-internet-gateway", "--vpc-id", resources["vpc"], "--internet-gateway-id", resources["igw"])
    resources["route_table"] = aws(state, "ec2", "create-route-table", "--vpc-id", resources["vpc"],
        "--tag-specifications", tag_spec(state, "route-table"))["RouteTable"]["RouteTableId"]
    save(path, state)
    aws(state, "ec2", "create-route", "--route-table-id", resources["route_table"], "--destination-cidr-block", "0.0.0.0/0",
        "--gateway-id", resources["igw"])
    resources["association"] = aws(state, "ec2", "associate-route-table", "--route-table-id", resources["route_table"],
                                  "--subnet-id", resources["subnet"])["AssociationId"]
    save(path, state)
    resources["security_group"] = aws(state, "ec2", "create-security-group", "--vpc-id", resources["vpc"],
        "--group-name", state["run_id"], "--description", "Isolated temporary pipeline experiment",
        "--tag-specifications", tag_spec(state, "security-group"))["GroupId"]
    save(path, state)
    aws(state, "ec2", "authorize-security-group-ingress", "--group-id", resources["security_group"], "--ip-permissions",
        json.dumps([{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": state["operator_cidr"]}]},
                    {"IpProtocol": "tcp", "FromPort": 1, "ToPort": 65535,
                     "UserIdGroupPairs": [{"GroupId": resources["security_group"]}]}]))
    key = path.parent / "lab-key"
    run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key])
    resources["key_name"] = state["run_id"]
    aws(state, "ec2", "import-key-pair", "--key-name", resources["key_name"], "--public-key-material",
        "fileb://" + str(key.with_suffix(".pub")), "--tag-specifications", tag_spec(state, "key-pair"))
    state["ssh_key"] = str(key)
    state["known_hosts"] = str(path.parent / "known_hosts")
    state["expires_at_utc"] = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S UTC")
    bootstrap = path.parent / "user-data.sh"
    bootstrap.write_text(user_data(state["expires_at_utc"]), encoding="utf-8", newline="\n")
    save(path, state)
    for role in ("infra", "compute", "app"):
        result = aws(state, "ec2", "run-instances", "--image-id", state["ami"], "--instance-type", state["instance_type"],
            "--count", "1", "--key-name", resources["key_name"], "--instance-initiated-shutdown-behavior", "terminate",
            "--metadata-options", "HttpTokens=required,HttpEndpoint=enabled", "--network-interfaces",
            json.dumps([{"DeviceIndex": 0, "SubnetId": resources["subnet"], "Groups": [resources["security_group"]],
                         "AssociatePublicIpAddress": True}]), "--block-device-mappings",
            json.dumps([{"DeviceName": image.get("RootDeviceName", "/dev/sda1"),
                         "Ebs": {"VolumeSize": 100, "VolumeType": "gp3", "DeleteOnTermination": True, "Encrypted": True}}]),
            "--tag-specifications", tag_spec(state, "instance"), "--user-data", "file://" + str(bootstrap),
            "--client-token", state["run_id"] + "-" + role)
        instance = result["Instances"][0]
        state["nodes"][role] = {"instance_id": instance["InstanceId"], "private_ip": instance["PrivateIpAddress"]}
        save(path, state)
        aws(state, "ec2", "create-tags", "--resources", instance["InstanceId"], "--tags", "Key=LabRole,Value=" + role)
    refresh(path, state)
    state["status"] = "created"
    save(path, state)
    print("Created three tagged instances; absolute automatic termination:", state["expires_at_utc"])


def refresh(path, state):
    if not state["nodes"]:
        return
    result = aws(state, "ec2", "describe-instances", "--instance-ids", *[node["instance_id"] for node in state["nodes"].values()])
    by_id = {item["InstanceId"]: item for reservation in result["Reservations"] for item in reservation["Instances"]}
    for node in state["nodes"].values():
        current = by_id[node["instance_id"]]
        tags = {item["Key"]: item["Value"] for item in current.get("Tags", [])}
        if tags.get("LabRun") != state["run_id"]:
            raise RuntimeError("Instance ownership tag mismatch; refusing to operate")
        node["state"] = current["State"]["Name"]
        node["public_ip"] = current.get("PublicIpAddress")
    save(path, state)


def ssh_options(state):
    return ["-i", state["ssh_key"], "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new", "-o", "UserKnownHostsFile=" + state["known_hosts"]]


def ssh(state, role, command, capture=True):
    return run(["ssh", *ssh_options(state), "ubuntu@" + state["nodes"][role]["public_ip"], command], capture=capture)


def upload(state, role, source, target):
    run(["scp", *ssh_options(state), str(source), "ubuntu@" + state["nodes"][role]["public_ip"] + ":" + target])


def package(path, state):
    manifest = json.loads((LAB / ".generated/source-manifest.json").read_text())
    for filename, hashes in manifest["files"].items():
        if hashlib.sha256((ROOT / filename).read_bytes()).hexdigest() != hashes["after_sha256"]:
            raise RuntimeError("Business source changed since image validation; rebuild and retest: " + filename)
    archive = path.parent / "source.tar.gz"
    paths = set()
    for directory in (LAB, LAB / "ec2", ROOT / "bigdata/collector", ROOT / "bigdata/spark/apps", ROOT / "bigdata/nlp-worker"):
        paths.update(directory.glob("*.py"))
    paths.update((ROOT / "bigdata/nlp-worker/storage").glob("*.py"))
    paths.update((ROOT / "bigdata/monitoring/grafana").rglob("*.json"))
    paths.update((ROOT / "bigdata/monitoring/grafana").rglob("*.yml"))
    paths.update([LAB / "EdgeProbe.java", LAB / "compose.yml", LAB / "prometheus.yml", LAB / "ec2/requirements.txt",
                  LAB / ".generated/collector-before.py", LAB / ".generated/source-manifest.json", LAB / ".generated/alertmanager.json",
                  ROOT / "bigdata/monitoring/prometheus/alerts.yml"])
    with tarfile.open(archive, "w:gz", compresslevel=1) as stream:
        for source in sorted(paths):
            stream.add(source, arcname=source.relative_to(ROOT).as_posix())
    state["package_sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    for role, images in IMAGES.items():
        target = path.parent / (role + "-images.tar.gz")
        if target.exists():
            continue
        print("Exporting tested images for", role, flush=True)
        process = subprocess.Popen(["docker", "save", *images], stdout=subprocess.PIPE)
        with gzip.open(target, "wb", compresslevel=1) as stream:
            shutil.copyfileobj(process.stdout, stream, 1024 * 1024)
        if process.wait() != 0:
            target.unlink()
            raise RuntimeError("Docker image export failed")
    save(path, state)


def deploy(path, state):
    authenticate(path, state)
    refresh(path, state)
    generated = path.parent / "rendered"
    render(state, generated)
    render_archive = path.parent / "rendered.tar.gz"
    with tarfile.open(render_archive, "w:gz") as stream:
        for source in generated.iterdir():
            stream.add(source, arcname="bigdata/reliability/.generated/ec2/" + source.name)
    deadline = time.monotonic() + 1200
    for role in ("infra", "compute", "app"):
        while True:
            try:
                ssh(state, role, "test -f /var/lib/cloud/instance/boot-finished && /usr/bin/docker info >/dev/null")
                break
            except subprocess.CalledProcessError:
                if time.monotonic() > deadline:
                    raise TimeoutError("cloud-init readiness: " + role)
                refresh(path, state)
                time.sleep(10)
        for source, destination in ((path.parent / "source.tar.gz", "/tmp/witw-source.tar.gz"),
                                    (render_archive, "/tmp/witw-rendered.tar.gz"),
                                    (path.parent / (role + "-images.tar.gz"), "/tmp/witw-images.tar.gz")):
            upload(state, role, source, destination)
        ssh(state, role, "tar -xzf /tmp/witw-source.tar.gz -C /opt/witw/repo && "
            "tar -xzf /tmp/witw-rendered.tar.gz -C /opt/witw/repo && "
            "/usr/bin/docker load -i /tmp/witw-images.tar.gz >/opt/witw/image-load.log && "
            "mkdir -p " + REMOTE_LAB + "/.generated/state && "
            "/opt/witw/venv/bin/pip install -r " + REMOTE_LAB + "/ec2/requirements.txt >/opt/witw/pip.log && "
            "find " + REMOTE_LAB + "/.generated/ec2 -maxdepth 1 -type f -exec chmod 600 {} + && "
            "chmod 644 " + REMOTE_LAB + "/.generated/ec2/prometheus.yml", capture=False)
        print("Deployed", role, flush=True)
        if role == "compute":
            volume = state["project"] + "_collector-storage"
            ssh(state, role, shlex.join(["/usr/bin/docker", "volume", "create", volume]))
            ssh(state, role, shlex.join(["/usr/bin/docker", "run", "--rm", "--network", "none", "-u", "0",
                "-v", volume + ":/persist", "--entrypoint", "python", "codex-pipeline-collector:verify", "-c",
                "import os; os.chown('/persist',1000,1000)"]))
    upload(state, "compute", Path(state["ssh_key"]), "/home/ubuntu/.ssh/witw-lab")
    ssh(state, "compute", "chmod 600 /home/ubuntu/.ssh/witw-lab")
    # Pin intra-lab SSH host keys to fingerprints observed through the operator's connection.
    known = []
    for role, node in state["nodes"].items():
        key = ssh(state, role, "cat /etc/ssh/ssh_host_ed25519_key.pub").split()[:2]
        known.append(node["private_ip"] + " " + " ".join(key))
    known_file = path.parent / "intra-known-hosts"
    known_file.write_text("\n".join(known) + "\n", encoding="utf-8")
    upload(state, "compute", known_file, "/home/ubuntu/.ssh/known_hosts")
    shim = path.parent / "docker-shim"
    shim.write_text("#!/bin/sh\nexec /opt/witw/venv/bin/python " + REMOTE_LAB + "/ec2/docker_router.py \"$@\"\n", encoding="utf-8", newline="\n")
    upload(state, "compute", shim, "/opt/witw/bin/docker")
    ssh(state, "compute", "chmod 755 /opt/witw/bin/docker")
    state["status"] = "deployed"
    save(path, state)


def remote_prefix():
    return "cd " + REMOTE_ROOT + " && . /opt/witw/venv/bin/activate && . " + REMOTE_LAB + "/.generated/ec2/runtime.env && export PATH=/opt/witw/bin:$PATH && "


def start(path, state):
    body = remote_prefix() + "exec flock -n /opt/witw/suite.lock python " + REMOTE_LAB + "/ec2/suite.py"
    ssh(state, "compute", "nohup sh -c " + shlex.quote(body) +
        " > /opt/witw/suite.log 2>&1 < /dev/null &", capture=False)
    state["status"] = "experiment-running"
    save(path, state)


def fetch(path, state, metadata_only=False):
    refresh(path, state)
    target = path.parent / ("metadata.tar.gz" if metadata_only else "evidence.tar.gz")
    expression = "find bigdata/reliability/results bigdata/reliability/.generated/ec2/suite-status.json -type f" + (" -name '*.json' ! -path '*/raw/*'" if metadata_only else "")
    command = "cd " + REMOTE_ROOT + " && (" + expression + " -print0 | tar --null -T - -czf -)"
    temporary = target.with_suffix(".partial")
    with temporary.open("wb") as stream:
        subprocess.run(["ssh", *ssh_options(state), "ubuntu@" + state["nodes"]["compute"]["public_ip"], command], stdout=stream, check=True)
    temporary.replace(target)
    if not metadata_only:
        observation = path.parent / "observability.tar.gz"
        temporary = observation.with_suffix(".partial")
        directory = REMOTE_LAB + "/.generated/ec2/observability"
        command = "if test -d " + directory + "; then tar -czf - -C " + directory + " .; fi"
        with temporary.open("wb") as stream:
            subprocess.run(["ssh", *ssh_options(state), "ubuntu@" + state["nodes"]["app"]["public_ip"], command], stdout=stream, check=True)
        if temporary.stat().st_size:
            temporary.replace(observation)
        else:
            temporary.unlink()
    log = ssh(state, "compute", "tail -n 25 /opt/witw/suite.log")
    (path.parent / "suite-tail.log").write_text(log, encoding="utf-8")
    print(log)
    print("Saved", target)


def host_cycles(path, state):
    """The external operator stays alive even when the compute/controller EC2 stops."""
    authenticate(path, state)
    refresh(path, state)
    progress_file = path.parent / "host-cycles.json"
    trials = json.loads(progress_file.read_text()) if progress_file.exists() else []
    completed = {(row["role"], row["repetition"]) for row in trials if row.get("status") == "passed"}
    for repetition in range(1, 4):
        for role in ("infra", "app", "compute"):
            if (role, repetition) in completed:
                continue
            run_id = f"host-{role}-r{repetition}-" + datetime.now(timezone.utc).strftime("%H%M%S")
            case = {"run": run_id, "role": role, "repetition": repetition, "status": "preparing",
                    "instance_id": state["nodes"][role]["instance_id"]}
            trials.append(case)
            progress_file.write_text(json.dumps(trials, indent=2))
            ssh(state, "compute", remote_prefix() + "python " + REMOTE_LAB + "/ec2/host_case.py prepare " + run_id, capture=False)
            started = time.monotonic()
            refresh(path, state)  # Recheck ownership tags immediately before stopping.
            identity = case["instance_id"]
            aws(state, "ec2", "stop-instances", "--instance-ids", identity)
            aws(state, "ec2", "wait", "instance-stopped", "--instance-ids", identity)
            case["status"] = "instance-stopped"
            progress_file.write_text(json.dumps(trials, indent=2))
            if role != "compute":
                ssh(state, "compute", "touch " + REMOTE_LAB + "/.generated/state/" + run_id + "/resume")
                time.sleep(30)
            aws(state, "ec2", "start-instances", "--instance-ids", identity)
            aws(state, "ec2", "wait", "instance-running", "--instance-ids", identity)
            deadline = time.monotonic() + 420
            while True:
                refresh(path, state)
                try:
                    ssh(state, role, "/usr/bin/docker info >/dev/null")
                    break
                except subprocess.CalledProcessError:
                    if time.monotonic() > deadline:
                        raise TimeoutError("Restarted instance is not ready: " + role)
                    time.sleep(5)
            ssh(state, "compute", remote_prefix() + "python " + REMOTE_LAB + "/ec2/host_case.py verify " + run_id, capture=False)
            case.update({"status": "passed", "external_fault_to_verified_seconds": time.monotonic() - started,
                         "measurement_clock": "operator monotonic clock survives every EC2 reboot"})
            progress_file.write_text(json.dumps(trials, indent=2))
            fetch(path, state, metadata_only=True)
    state["status"] = "all-experiment-phases-complete"
    save(path, state)


def wait_suite(path, state):
    deadline = datetime.strptime(state["expires_at_utc"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    while datetime.now(timezone.utc) < deadline - timedelta(minutes=45):
        try:
            response = ssh(state, "compute", "cat " + REMOTE_LAB + "/.generated/ec2/suite-status.json")
            progress = json.loads(response)
            save(path.parent / "suite-status.json", progress)
            print(json.dumps(progress), flush=True)
            fetch(path, state, metadata_only=True)
            if progress["status"] == "failed":
                raise RuntimeError("Remote suite failed; evidence retained, inspect before continuing")
            if progress["status"] == "container-trials-complete-host-trials-pending":
                return
        except (json.JSONDecodeError, subprocess.CalledProcessError):
            pass
        time.sleep(45)
    raise TimeoutError("Approaching absolute expiry; fetch evidence before proceeding")


def destroy(path, state):
    authenticate(path, state)
    refresh(path, state)
    identities = [node["instance_id"] for node in state["nodes"].values() if node.get("state") != "terminated"]
    if identities:
        aws(state, "ec2", "terminate-instances", "--instance-ids", *identities)
        aws(state, "ec2", "wait", "instance-terminated", "--instance-ids", *identities)
    resources = state["resources"]
    # The VPC ID was created by this run. Refuse to delete it without its owner tag.
    if resources.get("vpc"):
        vpc = aws(state, "ec2", "describe-vpcs", "--vpc-ids", resources["vpc"])["Vpcs"][0]
        if {item["Key"]: item["Value"] for item in vpc.get("Tags", [])}.get("LabRun") != state["run_id"]:
            raise RuntimeError("VPC ownership mismatch")
    operations = []
    if resources.get("association"):
        operations.append(("disassociate-route-table", ["--association-id", resources["association"]]))
    if resources.get("security_group"):
        operations.append(("delete-security-group", ["--group-id", resources["security_group"]]))
    if resources.get("subnet"):
        operations.append(("delete-subnet", ["--subnet-id", resources["subnet"]]))
    if resources.get("route_table"):
        operations.append(("delete-route-table", ["--route-table-id", resources["route_table"]]))
    if resources.get("igw"):
        operations += [("detach-internet-gateway", ["--internet-gateway-id", resources["igw"], "--vpc-id", resources["vpc"]]),
                       ("delete-internet-gateway", ["--internet-gateway-id", resources["igw"]])]
    if resources.get("key_name"):
        operations.append(("delete-key-pair", ["--key-name", resources["key_name"]]))
    if resources.get("vpc"):
        operations.append(("delete-vpc", ["--vpc-id", resources["vpc"]]))
    for operation, args in operations:
        aws(state, "ec2", operation, *args)
    state["status"] = "terminated-and-network-removed"
    save(path, state)
    print(state["status"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["plan", "provision", "package", "deploy", "start", "status", "fetch", "destroy", "host-cycles", "wait-suite"])
    parser.add_argument("--profile", default="default")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--metadata-only", action="store_true")
    args = parser.parse_args()
    if args.operation == "plan":
        new_plan(args.profile)
        return
    path = args.state or Path((RUNS / "latest.txt").read_text(encoding="utf-8"))
    state = json.loads(path.read_text(encoding="utf-8"))
    if args.operation == "status":
        authenticate(path, state)
        refresh(path, state)
        print(json.dumps({"run_id": state["run_id"], "status": state["status"], "nodes": state["nodes"],
                          "expires_at_utc": state.get("expires_at_utc")}, indent=2))
    elif args.operation == "fetch":
        fetch(path, state, args.metadata_only)
    elif args.operation == "host-cycles":
        host_cycles(path, state)
    elif args.operation == "wait-suite":
        wait_suite(path, state)
    else:
        globals()[args.operation](path, state)


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, RuntimeError, TimeoutError, ValueError) as error:
        print((getattr(error, "stderr", None) or str(error))[:1500], file=sys.stderr)
        sys.exit(1)
