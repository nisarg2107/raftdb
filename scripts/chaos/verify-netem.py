"""Verify actual netem partitions/delay and always restore the single stack."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[2]
def main():
    os.chdir(ROOT)
    parser=argparse.ArgumentParser()
    parser.add_argument('--without-netem',action='store_true')
    parser.add_argument('--root',default='benchmarks/results/batch-v3/netem')
    args=parser.parse_args()
    OUT=Path(args.root)
    OUT.mkdir(parents=True,exist_ok=True)
    CTL=str(ROOT/'bin'/('raftctl.exe' if os.name=='nt' else 'raftctl'))
    PROBE=str(ROOT/'bin'/('raftprobe.exe' if os.name=='nt' else 'raftprobe'))

    def run(args): return subprocess.check_output(args,text=True).strip()
    def netem(mode): return run([sys.executable,'scripts/chaos/netem.py',mode])
    def ready(): print(run([PROBE,'-mode','ready']),flush=True)
    def ctl(*args): return run([CTL,'-timeout','20s',*args])

    def capture_metrics(label):
        for node in range(1,6):
            with urllib.request.urlopen('http://127.0.0.1:'+str(9090+node)+'/metrics',timeout=10) as response:
                (OUT/(label+'-node'+str(node)+'-metrics.txt')).write_bytes(response.read())

    try:
        files=['-f','docker-compose.yml']+([] if args.without_netem else ['-f','docker-compose.chaos.yml'])
        subprocess.run(['docker','compose',*files,'up','--no-build','-d','--wait'],check=True,stderr=subprocess.STDOUT)
        ready()
        leader=next(s['id'] for s in map(json.loads,ctl('status').splitlines()) if s['state']=='Leader')
        follower=4 if leader=='node5' else 5
        subprocess.run(['docker','compose','stop','raftdb-node'+str(follower)],check=True,stderr=subprocess.STDOUT)
        ctl('batch-put','netem-follower-check','committed','netem-follower-check-2','committed')
        subprocess.run(['docker','compose','start','raftdb-node'+str(follower)],check=True,stderr=subprocess.STDOUT)
        ready()
        observed=run([CTL,'-addresses','127.0.0.1:'+str(50050+follower),'local','netem-follower-check'])
        if observed!='committed': raise RuntimeError('Restarted follower lost the batch')
        subprocess.run(['docker','compose','stop','raftdb-node4','raftdb-node5'],check=True,stderr=subprocess.STDOUT)
        time.sleep(3)
        run([CTL,'-addresses','127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053','batch-put','netem-quorum','three-voters'])
        subprocess.run(['docker','compose','stop','raftdb-node3'],check=True,stderr=subprocess.STDOUT)
        time.sleep(3)
        (OUT/'quorum-loss-refusal.txt').write_text(run([PROBE,'-addresses','127.0.0.1:50051,127.0.0.1:50052','-mode','refuse']),encoding='utf-8')
        subprocess.run(['docker','compose','start','raftdb-node3','raftdb-node4','raftdb-node5'],check=True,stderr=subprocess.STDOUT)
        ready()
        ctl('batch-put','netem-a','committed','netem-b','committed')
        drops={}
        if args.without_netem:
            ctl('batch-put','netem-a','9','netem-b','9')
            (OUT/'netem-unavailable.txt').write_text('Custom kernel rolled back after control instability and existing-workload health failure. No successful netem partition/delay test is claimed.\n',encoding='utf-8')
        else:
            print(netem('partition'),flush=True)
            time.sleep(4)
            majority='127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053'
            minority='127.0.0.1:50054,127.0.0.1:50055'
            print(run([CTL,'-addresses',majority,'-timeout','15s','batch-put','netem-a','majority','netem-b','majority']),flush=True)
            refusal=run([PROBE,'-addresses',minority,'-mode','refuse'])
            (OUT/'minority-refusal.txt').write_text(refusal,encoding='utf-8')
            counters=json.loads(netem('check'))
            (OUT/'partition-counters.json').write_text(json.dumps(counters,indent=2),encoding='utf-8')
            drops={int(n):sum(q.get('drops',0) for q in json.loads(raw) if q.get('kind')=='netem') for n,raw in counters.items()}
            if sum(v for n,v in drops.items() if n<=3)<=0 or sum(v for n,v in drops.items() if n>3)<=0:
                raise RuntimeError('No netem-dropped traffic observed on both sides')
            print(netem('heal'),flush=True)
            ready()
            value=json.loads(ctl('batch-get','netem-a','netem-b'))
            if any(r.get('value')!='majority' for r in value['results']): raise RuntimeError('Committed partition data was lost')
            print(netem('delay'),flush=True)
            for i in range(10): ctl('batch-put','netem-a',str(i),'netem-b',str(i))
            counters=netem('check')
            (OUT/'delay-counters.json').write_text(counters,encoding='utf-8')
            # Prove filters matched Raft traffic, rather than merely installing qdiscs.
            if sum(q.get('packets',0) for raw in json.loads(counters).values() for q in json.loads(raw) if q.get('kind')=='netem')<=0:
                raise RuntimeError('No packets passed through netem delay/loss')
            print(netem('heal'),flush=True)
            ready()
            value=json.loads(ctl('batch-get','netem-a','netem-b'))
            if any(r.get('value')!='9' for r in value['results']): raise RuntimeError('Delay/loss lost committed values')
        capture_metrics('before-snapshot')
        ctl('snapshot')
        capture_metrics('after-snapshot')
        subprocess.run(['docker','compose','restart','raftdb-node1','raftdb-node2','raftdb-node3','raftdb-node4','raftdb-node5'],check=True,stderr=subprocess.STDOUT)
        ready()
        value=json.loads(ctl('batch-get','netem-a','netem-b'))
        if any(r.get('value')!='9' for r in value['results']): raise RuntimeError('Snapshot/restart lost committed values')
        print(run(['docker','compose','exec','-T','raftdb-node1','raftprobe','-addresses',','.join('raftdb-node'+str(n)+':50051' for n in range(1,6)),'-mode','validate']),flush=True)
        (OUT/'passed.json').write_text(json.dumps({'follower_restart':True,'two_stopped_majority_batch_commit':True,'three_stopped_unary_and_batch_refusal':True,'netem_partition':not args.without_netem,'minority_unary_and_batch_refusal':not args.without_netem,'majority_batch_commit':not args.without_netem,'delay_ms':None if args.without_netem else 50,'jitter_ms':None if args.without_netem else 10,'loss_percent':None if args.without_netem else 1,'snapshot_restart':True,'dataset_validation':True,'drops':drops},indent=2),encoding='utf-8')
        print('Quorum/follower/snapshot checks passed; netem skipped' if args.without_netem else 'Netem partition, delay/loss, healing and snapshot/restart checks passed',flush=True)
    finally:
        subprocess.run(['docker','compose','start','raftdb-node1','raftdb-node2','raftdb-node3','raftdb-node4','raftdb-node5'],check=False,stderr=subprocess.STDOUT)
        print(netem('heal'),flush=True)
        subprocess.run(['docker','compose','up','--no-build','-d','--wait'],check=True,stderr=subprocess.STDOUT)
        ready()


if __name__ == '__main__':
    main()
