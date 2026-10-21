"""Short diagnostic loads of the current cluster; not sustained benchmark claims.

Temporarily enables the existing private pprof override and restores base Compose
in finally. Uses official ghz without changing the server binary or data volumes.
"""
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.request


ROOT = Path(__file__).resolve().parent.parent
def main():
    os.chdir(ROOT)
    OUT = ROOT / '.tools' / ('profiles-current-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    OUT.mkdir(parents=True)
    CTL = str(ROOT / 'bin' / ('raftctl.exe' if os.name == 'nt' else 'raftctl'))
    PROBE = str(ROOT / 'bin' / ('raftprobe.exe' if os.name == 'nt' else 'raftprobe'))
    BASE = ['docker', 'compose']
    PROFILE = BASE + ['-f', 'docker-compose.yml', '-f', 'docker-compose.profile.yml']


    def run(args):
        return subprocess.check_output(args, text=True, encoding='utf-8').strip()


    def fetch(url, target):
        with urllib.request.urlopen(url, timeout=30) as response:
            target.write_bytes(response.read())


    def cpu_stat(cid):
        # Docker Desktop may expose either cgroup v1 or v2. This optional diagnostic
        # must not abort the load when one filesystem layout is unavailable.
        for path in ['/sys/fs/cgroup/cpu.stat', '/sys/fs/cgroup/cpu/cpu.stat']:
            result = subprocess.run(['docker', 'exec', cid, 'cat', path], capture_output=True, text=True)
            if result.returncode == 0:
                return path + '\n' + result.stdout
        return 'CPU throttling counters unavailable on this cgroup layout.\n'


    def stats_loop(stop, target):
        with target.open('w', encoding='utf-8') as stream:
            while not stop.is_set():
                result = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{json .}}'], capture_output=True, text=True)
                stream.write(json.dumps({'time': time.time(), 'stats': result.stdout, 'error': result.stderr}) + '\n')
                stream.flush()
                stop.wait(2)


    print('Diagnostic artifacts:', OUT, flush=True)
    try:
        subprocess.run(PROFILE + ['up', '--no-build', '-d', '--wait'], check=True, stderr=subprocess.STDOUT)
        print(run([PROBE, '-mode', 'ready']), flush=True)
        run([CTL, 'put', 'bench-0', 'existing-value'])
        project = json.loads(run(BASE + ['config', '--format', 'json']))['name']
        for workload, config in [('read', 'read-hot'), ('write', 'write-32-overwrite')]:
            folder = OUT / workload
            folder.mkdir()
            before_text = run([CTL, 'status'])
            before = [json.loads(line) for line in before_text.splitlines()]
            leader = next(s['id'] for s in before if s['state'] == 'Leader')
            follower = next(s['id'] for s in before if s['state'] == 'Follower')
            (folder / 'status-before.jsonl').write_text(before_text, encoding='utf-8')
            ids = {role: run(BASE + ['ps', '-q', 'raftdb-' + node]) for role, node in [('leader', leader), ('follower', follower)]}
            endpoints = {role: 'http://127.0.0.1:' + str(6160 + int(node[-1])) + '/debug/pprof/' for role, node in [('leader', leader), ('follower', follower)]}
            if not (OUT / 'raftdb-linux').exists():
                subprocess.run(['docker', 'cp', ids['leader'] + ':/usr/local/bin/raftdb', str(OUT / 'raftdb-linux')], check=True, stderr=subprocess.STDOUT)
            for role, cid in ids.items():
                (folder / (role + '-cpu-before.txt')).write_text(cpu_stat(cid), encoding='utf-8')
            client_name = 'raftdb-profile-' + workload + '-' + str(os.getpid())
            args = ['docker', 'run', '--rm', '--name', client_name, '--network', project + '_cluster', '--cpus', '4', '--memory', '2g', '-e', 'GOMAXPROCS=4', '-e', 'GOMEMLIMIT=1GiB', '--mount', 'type=bind,source=' + str(ROOT) + ',target=/work', '-w', '/work', 'raftdb-ghz:0.120.0', '--disable-template-data', '--disable-template-functions', '--connections', '100', '--config', 'benchmarks/configs/' + config + '.json', '--concurrency', '1000', '--duration', '35s', '--format', 'json', '--output', folder.relative_to(ROOT).as_posix() + '/ghz.json', 'raftdb-' + leader + ':50051']
            (folder / 'command.json').write_text(json.dumps(args, indent=2), encoding='utf-8')
            stop = threading.Event()
            stats = threading.Thread(target=stats_loop, args=(stop, folder / 'container-stats.jsonl'))
            stats.start()
            print('Profiling', workload, 'leader', leader, 'follower', follower, flush=True)
            try:
                with (folder / 'client.log').open('w') as client_log:
                    load = subprocess.Popen(args, stdout=client_log, stderr=subprocess.STDOUT)
                    try:
                        time.sleep(5)
                        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                            futures = [pool.submit(fetch, endpoint + 'profile?seconds=15', folder / (role + '.cpu.pprof')) for role, endpoint in endpoints.items()]
                            for future in futures:
                                future.result()
                            futures = [pool.submit(fetch, endpoint + 'trace?seconds=3', folder / (role + '.trace')) for role, endpoint in endpoints.items()]
                            for future in futures:
                                future.result()
                        for role, endpoint in endpoints.items():
                            fetch(endpoint + 'goroutine?debug=2', folder / (role + '-goroutines.txt'))
                        if load.wait(timeout=30) != 0:
                            raise RuntimeError('official ghz failed; see client.log')
                    finally:
                        if load.poll() is None:
                            subprocess.run(['docker', 'stop', client_name], check=False, stderr=subprocess.STDOUT)
                            load.wait(timeout=15)
            finally:
                stop.set()
                stats.join(timeout=10)
            (folder / 'status-after.jsonl').write_text(run([CTL, 'status']), encoding='utf-8')
            for role, cid in ids.items():
                (folder / (role + '-cpu-after.txt')).write_text(cpu_stat(cid), encoding='utf-8')
            print(workload, 'profiles collected; diagnostic load is not a new sustained benchmark', flush=True)
    finally:
        print('Restoring base Compose configuration', flush=True)
        subprocess.run(BASE + ['up', '--no-build', '-d', '--wait'], check=True, stderr=subprocess.STDOUT)
        print(run([PROBE, '-mode', 'ready']), flush=True)
        run([CTL, 'put', 'bench-0', 'existing-value'])
        print('Restored normal deployment; artifacts:', OUT, flush=True)


if __name__ == '__main__':
    main()
