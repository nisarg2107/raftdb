"""Paths, subprocesses and access to the existing five-voter deployment."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


class Runtime:
    def __init__(self, root=ROOT):
        self.root = Path(root).resolve()
        suffix = '.exe' if os.name == 'nt' else ''
        self.ctl_path = os.environ.get('CTL', str(self.root / 'bin' / ('raftctl' + suffix)))
        self.probe_path = os.environ.get('PROBE', str(self.root / 'bin' / ('raftprobe' + suffix)))
        self._config = None

    def run(self, command, timeout=120):
        return subprocess.check_output(
            list(map(str, command)), cwd=self.root, text=True,
            encoding='utf-8', timeout=timeout,
        ).strip()

    @property
    def config(self):
        if self._config is None:
            self._config = json.loads(self.run(['docker', 'compose', 'config', '--format', 'json']))
        return self._config

    def port(self, node, target):
        ports = self.config['services']['raftdb-' + node]['ports']
        return next(str(p['published']) for p in ports if p['target'] == target)

    @property
    def addresses(self):
        return ','.join('127.0.0.1:' + self.port('node' + str(n), 50051) for n in range(1, 6))

    def ctl(self, *items, timeout=30):
        return self.run(
            [self.ctl_path, '-addresses', self.addresses, '-timeout', str(timeout) + 's', *items],
            timeout=timeout + 30,
        )

    def ready(self):
        return self.run([self.probe_path, '-addresses', self.addresses, '-mode', 'ready'])

    def validate(self):
        addresses = ','.join('raftdb-node' + str(n) + ':50051' for n in range(1, 6))
        return self.run([
            'docker', 'compose', 'exec', '-T', 'raftdb-node1', 'raftprobe',
            '-addresses', addresses, '-mode', 'validate',
        ])

    def pipeline(self, count):
        env = dict(os.environ, RAFTDB_WRITE_PIPELINE=str(count))
        subprocess.run(
            ['docker', 'compose', 'up', '--no-build', '-d', '--wait'],
            cwd=self.root, env=env, check=True, timeout=180,
        )
        print(self.ready(), flush=True)

    def current_pipeline(self):
        cid = self.run(['docker', 'compose', 'ps', '-q', 'raftdb-node1'])
        env = json.loads(self.run(['docker', 'inspect', cid]))[0]['Config']['Env']
        return int(next((v.split('=', 1)[1] for v in env if v.startswith('RAFTDB_WRITE_PIPELINE=')), '8'))

    def container_path(self, path):
        """ghz sees /work, including when the caller passes a Windows absolute path."""
        relative = Path(path).resolve().relative_to(self.root)
        return '/work/' + relative.as_posix()
