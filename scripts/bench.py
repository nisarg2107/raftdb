#!/usr/bin/env python3
"""Supported benchmark entry point. Importing this module has no side effects."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from benchlib.results import aggregate
from benchlib.runner import Runner
from benchlib.runtime import ROOT


def screen(runner, pipelines):
    """Select candidates, then restore the deployment; never edit .env."""
    runtime = runner.runtime
    original = runtime.current_pipeline()
    candidates = {'read': [], 'write': []}
    try:
        for size in (16, 32, 64):
            for kind in candidates:
                rows = runner.case(f'screen-{kind}-{size}', kind, size, duration=20)
                if rows:
                    row = rows[0]
                    candidates[kind].append({
                        'size': size, 'pipeline': original,
                        'rate': row['successful_ops_per_second'], 'p99': row['latency_ms']['99'],
                    })
        if not all(candidates.values()):
            raise RuntimeError('No zero-error candidate for a workload; rejected reports retained')
        best = {kind: max(rows, key=lambda r: (r['rate'], -r['p99']))
                for kind, rows in candidates.items()}
        for pipeline in pipelines:
            if pipeline == original:
                continue
            runtime.pipeline(pipeline)
            rows = runner.case(f'screen-write-pipeline{pipeline}', 'write', best['write']['size'], duration=20)
            if rows:
                row = rows[0]
                if (row['successful_ops_per_second'], -row['latency_ms']['99']) > (best['write']['rate'], -best['write']['p99']):
                    best['write'] = {'size': best['write']['size'], 'pipeline': pipeline,
                                     'rate': row['successful_ops_per_second'], 'p99': row['latency_ms']['99']}
        (runner.output / 'selection.json').write_text(json.dumps(best, indent=2), encoding='utf-8')
        return best
    finally:
        runtime.pipeline(original)


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest='command', required=True)
    for name in ('run', 'screen'):
        command = commands.add_parser(name)
        command.add_argument('--output', '--root', type=Path, required=True)
        command.add_argument('--warmup', type=int, default=10, help='seconds')
        command.add_argument('--resume', action='store_true')
    run = commands.choices['run']
    run.add_argument('--workload', choices=('read', 'write', 'both'), default='both')
    run.add_argument('--read-batch-size', type=int, default=32, help='1 selects unary Get')
    run.add_argument('--write-batch-size', type=int, default=64, help='1 selects unary Put')
    run.add_argument('--duration', type=int, default=60, help='seconds per trial')
    run.add_argument('--repeats', type=int, default=1)
    commands.choices['screen'].add_argument('--pipelines', nargs='+', type=int, choices=range(1, 33), default=[2, 8])
    commands.add_parser('summarize').add_argument('output', type=Path)
    matrix = commands.add_parser('matrix', help='legacy varied-payload/unary matrix (Bash required)')
    matrix.add_argument('--workload', choices=('read', 'write'), default='read')
    return cli


def main(argv=None):
    args = parser().parse_args(argv)
    os.chdir(ROOT)
    if args.command == 'summarize':
        print(json.dumps(aggregate(args.output), indent=2))
        return 0
    if args.command == 'matrix':
        # Keep the existing varied-payload profiles reproducible during migration.
        env = dict(os.environ, WORKLOAD=args.workload)
        suffix = '.exe' if os.name == 'nt' else ''
        env.setdefault('CTL', './bin/raftctl' + suffix)
        env.setdefault('PROBE', './bin/raftprobe' + suffix)
        env.setdefault('GHZ_RUNNER', 'docker')
        env.setdefault('PYTHON', sys.executable.replace('\\', '/'))
        bash = env.get('BASH', 'bash')
        if os.name == 'nt' and 'BASH' not in env:
            git = shutil.which('git')
            candidate = Path(git).resolve().parents[1] / 'bin/bash.exe' if git else None
            if candidate is None or not candidate.is_file():
                raise SystemExit('The Windows matrix requires Git Bash; set BASH to its bash.exe path, or run from Linux/WSL with Linux tools.')
            bash = str(candidate)
        subprocess.run([bash, 'scripts/benchmark.sh'], env=env, check=True)
        return 0
    runner = Runner(args.output, warmup=args.warmup, resume=args.resume)
    if args.command == 'screen':
        print(json.dumps(screen(runner, args.pipelines), indent=2))
    else:
        for kind, size in [('read', args.read_batch_size), ('write', args.write_batch_size)]:
            if args.workload in ('both', kind):
                label = ('batch-' if size > 1 else 'unary-') + kind
                rows = runner.case(label, kind, size, repeats=args.repeats, duration=args.duration)
                if len(rows) != args.repeats:
                    print(json.dumps(aggregate(runner.output), indent=2))
                    return 1
    print(json.dumps(aggregate(runner.output), indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
