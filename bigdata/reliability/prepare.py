"""Prepare reproducible, lab-only baseline overlays and backend images."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

LAB = Path(__file__).resolve().parent
ROOT = LAB.parents[1]
GEN = LAB / ".generated"
BUILD_IMAGE = os.getenv("LAB_BUILD_IMAGE", "news-kafka-hdfs-writer-test:latest")
BASELINE_REV = os.getenv("LAB_BASELINE_REV", "HEAD")


def run(args):
    print("RUN", " ".join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), cwd=ROOT, check=True)


def prepare():
    GEN.mkdir(exist_ok=True)
    (GEN / "state").mkdir(exist_ok=True)
    before = GEN / "backend-before-src"
    shutil.copytree(ROOT / "backend", before, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("build", ".gradle", ".env*", "test"))
    files = ["backend/src/main/java/com/ssafy/witw/infra/kafka/dbwriter/DbWriterKafkaConsumer.java",
             "backend/src/main/java/com/ssafy/witw/infra/kafka/dbwriter/DbWriterService.java",
             "bigdata/collector/collection_kafka_producer_v2.py"]
    manifest = {"baseline_commit": subprocess.check_output(["git", "rev-parse", BASELINE_REV], cwd=ROOT,
                                                           text=True).strip(), "files": {}}
    for filename in files:
        source = subprocess.check_output(["git", "show", manifest["baseline_commit"] + ":" + filename], cwd=ROOT)
        destination = (GEN / "collector-before.py" if filename.startswith("bigdata")
                       else before / Path(filename).relative_to("backend"))
        destination.write_bytes(source)
        manifest["files"][filename] = {"before_sha256": hashlib.sha256(source).hexdigest(),
                                        "after_sha256": hashlib.sha256((ROOT / filename).read_bytes()).hexdigest()}
    (GEN / "source-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("alert_config", ROOT / "bigdata/monitoring/alertmanager/render_config.py")
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    (GEN / "alertmanager.json").write_text(json.dumps(config.build_config("http://receiver:8080/alerts")),
                                           encoding="utf-8")


def build():
    cache = Path.home() / ".gradle/caches/modules-2"
    for variant, work in [("after", ROOT / "backend"), ("before", GEN / "backend-before-src")]:
        relative = work.relative_to(ROOT).as_posix()
        run(["docker", "run", "--rm", "--name", f"witw-reliability-build-{variant}",
             "--mount", f"type=bind,source={ROOT},target=/workspace",
             "--mount", f"type=bind,source={cache},target=/dependency-cache/modules-2,readonly",
             "--workdir", "/workspace/" + relative,
             "--env", "GRADLE_USER_HOME=/workspace/.codex-tmp/gradle-pipeline",
             "--env", "GRADLE_RO_DEP_CACHE=/dependency-cache", "--entrypoint", "gradle",
             BUILD_IMAGE, "bootJar", "--no-daemon", "--max-workers=2"])
        jar = next(path for path in (work / "build/libs").glob("*.jar") if not path.name.endswith("-plain.jar"))
        shutil.copy2(jar, GEN / f"backend-{variant}.jar")
        run(["docker", "build", "-f", LAB / "Backend.Dockerfile", "--build-arg", f"VARIANT={variant}",
             "-t", f"witw-reliability-backend:{variant}", LAB])


if __name__ == "__main__":
    prepare()
    if "--build" in sys.argv:
        build()
