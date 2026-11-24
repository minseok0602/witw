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
    interfaces=[p/'statistics' for p in Path('/sys/class/net').iterdir()
                if p.name!='lo' and not p.name.startswith(('veth','docker','br-'))]
    rx=sum(int((p/'rx_bytes').read_text()) for p in interfaces)
    tx=sum(int((p/'tx_bytes').read_text()) for p in interfaces)
    events=dict(line.split() for line in (root/'memory.events').read_text().splitlines())
    return dict(oom_events=int(events.get('oom',0)),oom_kills=int(events.get('oom_kill',0)),net_rx_bytes=rx,net_tx_bytes=tx,epoch_s=time.time(), cpu_usage_s=int(cpu['usage_usec']) / 1e6,
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
    exists=path.exists() and path.stat().st_size>0
    with path.open('a' if exists else 'w', newline='') as f:
        first = sample()
        writer = csv.DictWriter(f, fieldnames=list(first))
        if not exists:
            writer.writeheader()
        writer.writerow(first)
        while child.poll() is None:
            time.sleep(.25)
            writer.writerow(sample())
            f.flush()
    raise SystemExit(child.returncode)


if __name__ == '__main__':
    main()
