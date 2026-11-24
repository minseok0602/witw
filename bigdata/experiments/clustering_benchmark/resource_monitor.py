"""Sample container cgroup v2, covering both JVM and Python, outside timed code."""
import argparse
import csv
import signal
import subprocess
import time
from pathlib import Path


def sample():
    root = Path('/sys/fs/cgroup')
    cpu = dict(line.split() for line in (root / 'cpu.stat').read_text().splitlines())
    mem = dict(line.split() for line in (root / 'memory.stat').read_text().splitlines())
    return dict(epoch_s=time.time(), cpu_usage_s=int(cpu['usage_usec']) / 1e6,
                memory_bytes=int((root / 'memory.current').read_text()),
                anon_bytes=int(mem['anon']), throttled_s=int(cpu.get('throttled_usec', 0)) / 1e6)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True)
    p.add_argument('command', nargs=argparse.REMAINDER)
    a = p.parse_args()
    path = Path(a.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    child = subprocess.Popen(a.command[1:] if a.command[0] == '--' else a.command)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda signum, frame: child.send_signal(signum))
    with path.open('w', newline='') as f:
        first = sample()
        writer = csv.DictWriter(f, fieldnames=list(first))
        writer.writeheader()
        writer.writerow(first)
        while child.poll() is None:
            time.sleep(.25)
            writer.writerow(sample())
            f.flush()
    raise SystemExit(child.returncode)


if __name__ == '__main__':
    main()
