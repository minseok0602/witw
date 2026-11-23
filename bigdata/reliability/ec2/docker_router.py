"""Route existing lab Docker commands to their owning EC2 host over SSH.

Only the tagged experiment project is accepted. This wrapper is placed before
/usr/bin/docker on the compute host; local development never installs it.
"""
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from render import OWNERS, REMOTE_LAB


def inventory():
    return json.loads(Path(os.environ["LAB_EC2_INVENTORY"]).read_text())


def execute(role, arguments, capture=False, compose_env=None):
    lab = inventory()
    prefix = ["env", *[f"{key}={value}" for key, value in (compose_env or {}).items()], "/usr/bin/docker"]
    if role == lab["local_role"]:
        args = prefix + list(arguments)
    else:
        args = ["ssh", "-i", lab["ssh_key"], "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=8", "ubuntu@" + lab["nodes"][role]["private_ip"],
                shlex.join(prefix + list(arguments))]
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE if capture else None, check=True)
    return result.stdout if capture else ""


def routes():
    path = Path(os.environ["LAB_EC2_INVENTORY"]).with_name("container-routes.json")
    return path, json.loads(path.read_text()) if path.exists() else {}


def remember(role, identities):
    path, data = routes()
    for identity in identities:
        data[identity] = role
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data))
    temporary.replace(path)


def owner(identity):
    _, data = routes()
    matches = {role for key, role in data.items() if key.startswith(identity) or identity.startswith(key)}
    if len(matches) != 1:
        raise ValueError("Unknown/ambiguous lab container; obtain its ID with lab compose ps first")
    return matches.pop()


def compose(arguments):
    lab = inventory()
    actions = {"up", "start", "stop", "restart", "ps", "logs", "config"}
    action_index = next((index for index, arg in enumerate(arguments) if arg in actions), None)
    if action_index is None:
        raise ValueError("Unsupported lab compose operation")
    before, operation = arguments[:action_index], arguments[action_index]
    if "-p" not in before or before[before.index("-p") + 1] != lab["project"]:
        raise ValueError("Docker router accepts only its own experiment project")
    tail = arguments[action_index + 1:]
    services = [arg for arg in tail if arg in OWNERS]
    if not services:
        services = list(OWNERS)
    options = [arg for arg in tail if arg not in OWNERS]
    combined = []
    environment = {key: os.environ[key] for key in
                   ("LAB_RUN", "LAB_VARIANT", "LAB_COUNT", "LAB_FAILPOINT") if key in os.environ}
    for role in ("infra", "app", "compute"):
        selected = [service for service in services if OWNERS[service] == role]
        if not selected:
            continue
        args = ["compose", "--project-directory", REMOTE_LAB,
                "--env-file", REMOTE_LAB + "/.generated/ec2/compose.env",
                "-p", lab["project"], "-f", REMOTE_LAB + f"/.generated/ec2/compose.{role}.json",
                operation, *options]
        if operation != "config":
            args += selected
        output = execute(role, args, capture=True, compose_env=environment)
        if operation == "ps" and ("-q" in options or "--quiet" in options):
            remember(role, [line for line in output.splitlines() if re.fullmatch(r"[a-f0-9]{12,64}", line)])
        combined.append(output)
    return "".join(combined)


def main(arguments):
    if not arguments:
        raise ValueError("A lab Docker operation is required")
    lab = inventory()
    operation = arguments[0]
    if operation == "compose":
        print(compose(arguments[1:]), end="")
    elif operation == "ps":
        if "label=com.docker.compose.project=" + lab["project"] not in arguments:
            raise ValueError("docker ps requires the lab project filter")
        for role in ("infra", "compute", "app"):
            output = execute(role, arguments, capture=True)
            remember(role, [line for line in output.splitlines() if re.fullmatch(r"[a-f0-9]{12,64}", line)])
            print(output, end="")
    elif operation == "inspect":
        identities = arguments[1:]
        combined = []
        for identity in identities:
            combined.extend(json.loads(execute(owner(identity), ["inspect", identity], capture=True)))
        print(json.dumps(combined))
    elif operation == "image" and arguments[1] == "inspect":
        for role in ("compute", "app", "infra"):
            try:
                print(execute(role, arguments, capture=True), end="")
                break
            except subprocess.CalledProcessError:
                pass
        else:
            raise ValueError("Image is absent from all lab hosts")
    elif operation == "run":
        if "--name" not in arguments or not arguments[arguments.index("--name") + 1].startswith(lab["project"] + "-probe-"):
            raise ValueError("Only the lab JDI probe may use remote docker run")
        execute("app", arguments)
    elif operation in ("kill", "start", "restart", "pause", "unpause", "exec"):
        identity = next((arg for arg in arguments[1:] if re.fullmatch(r"[a-f0-9]{12,64}", arg)), None)
        if not identity:
            raise ValueError("Expected an experiment container ID")
        execute(owner(identity), arguments)
    elif operation == "info":
        print(execute("compute", arguments, capture=True), end="")
    else:
        raise ValueError("Unsupported lab Docker operation: " + operation)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except (ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
