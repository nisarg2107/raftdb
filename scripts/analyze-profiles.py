"""Produce readable CPU/trace reports without shell binary redirection."""
import json
from pathlib import Path
import subprocess
import sys


def main():
    root = Path(sys.argv[1]).resolve()
    binary = root / 'raftdb-linux'
    for phase in ['read', 'write']:
        folder = root / phase
        for role in ['leader', 'follower']:
            cpu = folder / (role + '.cpu.pprof')
            if cpu.exists():
                for mode, flags in [('flat', []), ('cumulative', ['-cum'])]:
                    result = subprocess.run(['go', 'tool', 'pprof', '-top', '-nodecount=18', *flags, str(binary), str(cpu)], capture_output=True)
                    (folder / (role + '-cpu-' + mode + '.txt')).write_bytes(result.stdout + result.stderr)
                    if result.returncode:
                        raise RuntimeError('CPU profile analysis failed')
            trace = folder / (role + '.trace')
            if trace.exists():
                for kind in ['syscall', 'sync', 'sched', 'net']:
                    target = folder / (role + '-' + kind + '.pprof')
                    with target.open('wb') as stream:
                        subprocess.run(['go', 'tool', 'trace', '-pprof=' + kind, str(trace)], stdout=stream, check=True)
                    result = subprocess.run(['go', 'tool', 'pprof', '-top', '-nodecount=12', str(target)], capture_output=True)
                    (folder / (role + '-' + kind + '-top.txt')).write_bytes(result.stdout + result.stderr)
                    if result.returncode:
                        raise RuntimeError('Trace profile analysis failed')
        stats_path = folder / 'container-stats.jsonl'
        if stats_path.exists():
            stats = []
            for line in stats_path.read_text().splitlines():
                entry = json.loads(line)
                for row in entry['stats'].splitlines():
                    item = json.loads(row)
                    stats.append({'time': entry['time'], **item})
            (folder / 'container-stats.json').write_text(json.dumps(stats, indent=2), encoding='utf-8')
        print(phase, 'reports saved in', folder)


if __name__ == '__main__':
    main()
