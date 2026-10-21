"""Run bounded unary/batch workloads and retain every accepted/rejected attempt."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.request

from reports import load_report, summarize
from .observations import capture_metrics
from .runtime import Runtime


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2), encoding='utf-8')


def signature(status):
    return {s['id']: (s['term'], s['leader_id']) for s in map(json.loads, status.splitlines())}


def operation_value(metrics, method):
    prefix = 'raftdb_operations_total{method="' + method + '"} '
    return next((float(line.split()[-1]) for line in metrics.splitlines() if line.startswith(prefix)), None)


class Runner:
    def __init__(self, output, *, runtime=None, warmup=10, resume=False):
        self.runtime = runtime or Runtime()
        self.output = Path(output).resolve()
        self.runtime.container_path(self.output)  # Validate before any Docker or file operation.
        self.warmup = warmup
        self.resume = resume

    def restore(self):
        self.runtime.ctl('seed', '64')

    def metadata(self, folder):
        for n in range(1, 6):
            cid = self.runtime.run(['docker', 'compose', 'ps', '-q', 'raftdb-node' + str(n)])
            data = self.runtime.run(['docker', 'inspect', cid])
            (folder / ('node' + str(n) + '-inspect.json')).write_text(data, encoding='utf-8')
        data = self.runtime.run(['docker', 'info', '--format', '{{json .}}'])
        (folder / 'docker-info.json').write_text(data, encoding='utf-8')
        root = self.runtime.root
        paths = [*root.joinpath('api').glob('*'), *root.joinpath('cmd').rglob('*.go'),
                 *root.joinpath('internal').rglob('*.go'), root / 'go.mod', root / 'go.sum']
        hashes = [hashlib.sha256(p.read_bytes()).hexdigest() + ' ' + p.relative_to(root).as_posix()
                  for p in sorted(paths) if p.is_file()]
        (folder / 'source-sha256.txt').write_text('\n'.join(hashes), encoding='utf-8')

    def check_prefix(self, count, value):
        deadline = time.monotonic() + 10
        while True:
            try:
                self.runtime.ctl('check-prefix', str(count), value)
                return
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.2)

    def case(self, label, kind, size, *, repeats=1, duration=60):
        if kind not in ('read', 'write') or not 1 <= size <= 128:
            raise ValueError('Expected read/write and batch size 1..128')
        if repeats < 1 or duration < 1 or self.warmup < 1:
            raise ValueError('Durations and repeats must be positive')
        folder = self.output / label
        folder.mkdir(parents=True, exist_ok=True)
        results = []
        # Establish the complete fixture even on a newly started demo deployment.
        self.runtime.ctl('seed', '10000', timeout=180)
        print(self.runtime.ready(), flush=True)
        print(self.runtime.validate(), flush=True)
        # A resumed case retains the environment/source provenance of its trials.
        if not self.resume or not (folder / 'source-sha256.txt').exists():
            self.metadata(folder)
        try:
            for number in range(1, repeats + 1):
                concurrency, connections = (1000, 100) if size == 1 else (1024 // size, 4)
                name = ('batch-' if size > 1 else '') + f'{kind}-n{size}-c{concurrency}-r{number}'
                stem = folder / name
                summary = Path(str(stem) + '-summary.json')
                rejected = Path(str(stem) + '-rejected.txt')
                if self.resume and summary.exists() and not rejected.exists():
                    results.append(json.loads(summary.read_text()))
                    print('Retaining validated trial:', name, flush=True)
                    continue
                if self.resume and rejected.exists():
                    print('Retaining rejected attempt:', name, flush=True)
                    continue
                result = self.trial(stem, kind, size, duration, connections, concurrency)
                if result:
                    results.append(result)
        finally:
            self.restore()
        print(self.runtime.validate(), flush=True)
        return results

    def ghz_command(self, config):
        return [
            'docker', 'run', '--rm', '--network', self.runtime.config['name'] + '_cluster',
            '--cpus', '4', '--memory', '2g', '-e', 'GOMAXPROCS=4', '-e', 'GOMEMLIMIT=1GiB',
            '--mount', 'type=bind,source=' + str(self.runtime.root) + ',target=/work',
            '-w', '/work', 'raftdb-ghz:0.120.0', '--disable-template-data',
            '--disable-template-functions', '--config', self.runtime.container_path(config),
            '--format', 'json',
        ]

    def trial(self, stem, kind, size, duration, connections, concurrency):
        raw = Path(str(stem) + '.json')
        rejected = Path(str(stem) + '-rejected.txt')
        if raw.exists() or rejected.exists():
            raise FileExistsError('Refusing to overwrite an existing trial: ' + str(stem))
        batch = size > 1
        method = ('BatchGet' if kind == 'read' else 'BatchPut') if batch else ('Get' if kind == 'read' else 'Put')
        keys = ['bench-' + str(i) for i in range(size)]
        if batch:
            data = {'keys': keys} if kind == 'read' else {'entries': [{'key': k, 'value': 'v' * 32} for k in keys]}
        else:
            data = {'key': keys[0]} if kind == 'read' else {'key': keys[0], 'value': 'v' * 32}
        config = Path(str(stem) + '-config.json')
        save(config, {
            'proto': 'api/raftdb.proto', 'insecure': True, 'connections': connections,
            'concurrency': concurrency, 'timeout': '3s', 'duration-stop': 'wait',
            'call': 'raftdb.v1.Database.' + method, 'data': data,
        })
        statuses = self.runtime.ctl('status')
        leader = next(s['id'] for s in map(json.loads, statuses.splitlines()) if s['state'] == 'Leader')
        url = 'http://127.0.0.1:' + self.runtime.port(leader, 9091) + '/metrics'
        def metrics():
            with urllib.request.urlopen(url, timeout=10) as response:
                return response.read().decode()
        command = self.ghz_command(config)
        save(Path(str(stem) + '-command.json'), command)
        def execute(seconds, output):
            subprocess.run(
                [*command, '--duration', str(seconds) + 's', '--output',
                 self.runtime.container_path(output), 'raftdb-' + leader + ':50051'],
                cwd=self.runtime.root, check=True, timeout=seconds + 60,
            )
        try:
            warmup = Path(str(stem) + '-warmup.json')
            print('Warmup', stem.name, flush=True)
            execute(self.warmup, warmup)
            summarize(load_report(warmup))
            before = self.runtime.ctl('status')
            Path(str(stem) + '-status-before.jsonl').write_text(before, encoding='utf-8')
            m_before = metrics()
            Path(str(stem) + '-metrics-before.txt').write_text(m_before, encoding='utf-8')
            print('Measuring', stem.name, str(duration) + 's', flush=True)
            execute(duration, raw)
            m_after = metrics()
            Path(str(stem) + '-metrics-after.txt').write_text(m_after, encoding='utf-8')
            after = self.runtime.ctl('status')
            Path(str(stem) + '-status-after.jsonl').write_text(after, encoding='utf-8')
            result = summarize(load_report(raw))
            if signature(before) != signature(after):
                raise ValueError('leader or term changed')
            start, end = operation_value(m_before, method), operation_value(m_after, method)
            if batch and (start is None or end is None):
                raise ValueError('Missing acknowledged logical-operation counter')
            if start is not None and end is not None:
                if end - start != result['successful_operations']:
                    raise ValueError('server acknowledged-operation counter mismatch')
                result['server_acknowledged_operations'] = end - start
            print(self.runtime.ready(), flush=True)
            self.check_prefix(size, 'existing-value' if kind == 'read' else 'v' * 32)
            if batch and kind == 'read':
                values = json.loads(self.runtime.ctl('batch-get', *keys))['results']
                if len(values) != size or any(not v.get('found') or v.get('value') != 'existing-value' for v in values):
                    raise ValueError('batch response validation failed')
            result.update(rpc=method, kind=kind, duration_requested_seconds=duration,
                          concurrency=concurrency, connections=connections, batch_size=size, raw=str(raw))
            capture_metrics(raw)
            save(Path(str(stem) + '-summary.json'), result)
            print('VALID', stem.name, round(result['successful_ops_per_second'], 1), 'ops/s', flush=True)
            return result
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
            rejected.write_text(str(error), encoding='utf-8')
            print('REJECTED', stem.name, str(error), flush=True)
            return None
