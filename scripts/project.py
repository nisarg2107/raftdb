#!/usr/bin/env python3
"""Common project tasks: build, run, seed, demo, test and benchmark.

Uses the same commands as the README. No task runs when this module is imported.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
SEED_FILE = ROOT / 'examples' / 'seed.json'
ADDRESSES = ','.join(f'raftdb-node{n}:50051' for n in range(1, 6))


def run(command, *, capture=False, timeout=None):
    result = subprocess.run(
        list(map(str, command)), cwd=ROOT, check=True, text=True,
        encoding='utf-8', stdout=subprocess.PIPE if capture else None,
        timeout=timeout,
    )
    return result.stdout.strip() if capture else None


def cluster_cli(*arguments, capture=False):
    # Run the existing client inside a voter: seeding needs no host Go install.
    return run(['docker', 'compose', 'exec', '-T', 'raftdb-node1', 'raftctl',
                '-addresses', ADDRESSES, '-timeout', '20s', *arguments], capture=capture, timeout=30)


def build():
    (ROOT / 'bin').mkdir(exist_ok=True)
    suffix = '.exe' if os.name == 'nt' else ''
    for name in ('raftdb', 'raftctl', 'raftprobe'):
        run(['go', 'build', '-buildvcs=false', '-o', ROOT / 'bin' / (name + suffix), './cmd/' + name])


def up(no_build=False):
    run(['docker', 'compose', 'up', '--no-build' if no_build else '--build', '-d', '--wait'])


def load_seed(path):
    entries = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(entries, list) or not 1 <= len(entries) <= 128:
        raise ValueError('Seed must contain 1..128 key/value objects')
    seen, size = set(), 0
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get('key'), str) or not isinstance(entry.get('value'), str):
            raise ValueError('Every seed entry needs string key and value fields')
        key, value = entry['key'], entry['value']
        if not 1 <= len(key.encode('utf-8')) <= 1024 or len(value.encode('utf-8')) > 256 * 1024:
            raise ValueError('Seed key/value exceeds database limits')
        if key in seen:
            raise ValueError('Seed keys must be unique')
        seen.add(key)
        size += len(key.encode('utf-8')) + len(value.encode('utf-8'))
    if size > 256 * 1024:
        raise ValueError('Seed request exceeds 256 KiB')
    return entries


def seed(path=SEED_FILE):
    entries = load_seed(path)  # Validate the entire file before submitting writes.
    pairs = [part for entry in entries for part in (entry['key'], entry['value'])]
    cluster_cli('batch-put', *pairs)
    response = json.loads(cluster_cli('batch-get', *(e['key'] for e in entries), capture=True))
    results = response.get('results', [])
    if len(results) != len(entries) or any(
        not result.get('found') or result.get('value', '') != entry['value']
        for entry, result in zip(entries, results)
    ):
        raise RuntimeError('Seed read-back did not match the file')
    for entry in entries:
        print(entry['key'] + ' = ' + entry['value'], flush=True)
    return entries


def demo(duration, interval, no_setup=False):
    if duration <= 0 or interval <= 0:
        raise ValueError('Demo duration and interval must be positive')
    if not no_setup:
        up()
    entries = seed()
    expected = {e['key']: e['value'] for e in entries}
    if 'demo:counter' not in expected:
        raise ValueError('The demo seed must include demo:counter')
    print('Open http://localhost:3000/d/raftdb/raftdb-consensus-and-requests', flush=True)
    print('Prometheus: http://localhost:9090/query; Ctrl+C stops demo traffic.', flush=True)
    print('This modest activity demo is separate from official-ghz benchmarks.', flush=True)
    end, tick = time.monotonic() + duration, 0
    while time.monotonic() < end:
        tick += 1
        expected['demo:counter'] = str(tick)
        cluster_cli('batch-put', 'demo:counter', str(tick))
        response = json.loads(cluster_cli('batch-get', *expected, capture=True))
        values = response.get('results', [])
        if len(values) != len(expected) or any(
            not value.get('found') or value.get('value', '') != expected[key]
            for key, value in zip(expected, values)
        ):
            raise RuntimeError('Demo read-back mismatch')
        if tick == 1 or tick % 10 == 0:
            print(f'Cycle {tick}: committed counter={tick}; read {len(values)} keys', flush=True)
        time.sleep(min(interval, max(0, end - time.monotonic())))
    print('Demo finished. Historical graphs remain; live rates return to zero.', flush=True)


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    tasks = cli.add_subparsers(dest='task', required=True)
    tasks.add_parser('build', help='build native Go tools into bin/')
    tasks.add_parser('up', help='start the existing five-voter cluster').add_argument('--no-build', action='store_true')
    tasks.add_parser('down', help='stop the cluster, retaining data volumes')
    tasks.add_parser('status', help='show all five nodes')
    tasks.add_parser('seed', help='load and verify the example data').add_argument('--file', type=Path, default=SEED_FILE)
    command = tasks.add_parser('demo', help='seed real data and generate bounded dashboard activity')
    command.add_argument('--duration', type=float, default=120, help='seconds (default: 120)')
    command.add_argument('--interval', type=float, default=.25, help='pause between read/write cycles')
    command.add_argument('--no-setup', action='store_true', help='use the already running cluster')
    command = tasks.add_parser('test', help='run Go/Python checks')
    command.add_argument('--container', action='store_true', help='use the full Linux race/vet test image')
    command = tasks.add_parser('benchmark', help='official ghz; three 60-second trials per workload')
    command.add_argument('--output', type=Path)
    command.add_argument('--repeats', type=int, default=3)
    command.add_argument('--duration', type=int, default=60)
    command.add_argument('--no-setup', action='store_true', help='use existing native tools, images and ready cluster')
    tasks.add_parser('summary', help='show trial rates and arithmetic means').add_argument('output', type=Path)
    return cli


def main(argv=None):
    args = parser().parse_args(argv)
    if args.task == 'build':
        build()
    elif args.task == 'up':
        up(args.no_build)
    elif args.task == 'down':
        run(['docker', 'compose', 'down'])
    elif args.task == 'status':
        cluster_cli('status')
    elif args.task == 'seed':
        seed(args.file)
    elif args.task == 'demo':
        demo(args.duration, args.interval, args.no_setup)
    elif args.task == 'test':
        if args.container:
            run(['docker', 'build', '--target', 'test', '-t', 'raftdb:test', '.'])
        else:
            run(['go', 'test', '-timeout', '120s', './...'])
            run(['go', 'vet', './...'])
        run([sys.executable, '-m', 'unittest', 'discover', '-s', 'scripts', '-p', '*test*.py'])
    elif args.task == 'benchmark':
        if args.repeats < 1 or args.duration < 1:
            raise ValueError('Benchmark repeats and duration must be positive')
        if not args.no_setup:
            build()
            up()
            run(['docker', 'build', '-f', 'benchmarks/Dockerfile', '-t', 'raftdb-ghz:0.120.0', '.'])
        output = args.output or ROOT / 'benchmarks/results' / datetime.now(timezone.utc).strftime('run-%Y%m%dT%H%M%SZ')
        run([sys.executable, 'scripts/bench.py', 'run', '--output', output,
             '--repeats', args.repeats, '--duration', args.duration])
    elif args.task == 'summary':
        run([sys.executable, 'scripts/bench.py', 'summarize', args.output])
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print('\nStopped. The database and monitoring remain running.')
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print('project:', error, file=sys.stderr)
        raise SystemExit(1)
