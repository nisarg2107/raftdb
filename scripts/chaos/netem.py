"""Raft-only netem faults. Call partition, delay, check, or heal.

Only the five current deployment containers are touched. Health, gRPC and
monitoring traffic do not match these TCP/7000 peer filters.
"""
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]

def command(args,check=True):
    result=subprocess.run(args,cwd=ROOT,capture_output=True,text=True)
    if check and result.returncode: raise RuntimeError(' '.join(args)+': '+result.stderr)
    return result.stdout.strip()

def execute(node,*args,check=True):
    return command(['docker','compose','exec','-T','--user','root','raftdb-node'+str(node),*args],check)

def heal():
    for node in range(1,6):
        # Only remove the root qdisc carrying our distinctive handle.
        qdiscs=execute(node,'tc','qdisc','show','dev','eth0',check=False)
        if 'prio 7a00:' in qdiscs:
            execute(node,'tc','qdisc','del','dev','eth0','root',check=False)

def check():
    records={str(n):execute(n,'tc','-s','-j','qdisc','show','dev','eth0') for n in range(1,6)}
    print(json.dumps(records,indent=2))
    return records

def main():
    mode=sys.argv[1]
    if mode=='heal': heal()
    elif mode=='check': check()
    elif mode in ['partition','delay']:
        try:
            # Discover addresses once; do not recreate containers during the fault.
            peers={n:execute(1,'getent','ahostsv4','raftdb-node'+str(n)).split()[0] for n in range(1,6)}
            for node in range(1,6):
                qdiscs=execute(node,'tc','qdisc','show','dev','eth0')
                if 'noqueue' not in qdiscs:
                    raise RuntimeError('Unexpected existing qdisc on node'+str(node)+'; refusing replacement')
                execute(node,'tc','qdisc','add','dev','eth0','root','handle','7a00:','prio','bands','3','priomap',*(['0']*16))
                impairment=['loss','100%'] if mode=='partition' else ['delay','50ms','10ms','loss','1%']
                execute(node,'tc','qdisc','add','dev','eth0','parent','7a00:3','handle','7a30:','netem',*impairment)
                for peer,ip in peers.items():
                    if peer==node or (mode=='partition' and (peer<=3)==(node<=3)): continue
                    for port in ['dport','sport']:
                        execute(node,'tc','filter','add','dev','eth0','protocol','ip','parent','7a00:','prio','1','u32','match','ip','dst',ip+'/32','match','ip','protocol','6','0xff','match','ip',port,'7000','0xffff','flowid','7a00:3')
            print(mode+' configured for Raft TCP/7000 only')
        except BaseException:
            heal()
            raise
    else:
        raise SystemExit('usage: netem.py partition|delay|check|heal')


if __name__ == '__main__':
    main()
