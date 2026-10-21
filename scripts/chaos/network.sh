#!/usr/bin/env bash
# Requires the NET_ADMIN override. Faults affect Raft TCP only for partitions.
set -euo pipefail
cd "$(dirname "$0")/../.."
dc=(docker compose)
execroot() { "${dc[@]}" exec -T -u root "raftdb-node$1" "${@:2}"; }
if [[ ${1:-} != heal ]]; then
  trap 'result=$?; bash "$0" heal >&2 || true; exit "$result"' ERR
fi
case ${1:-} in
  partition)
    # Symmetric 3|2 split, preserving client and monitoring connectivity.
    for a in 1 2 3; do for b in 4 5; do
      for pair in "$a $b" "$b $a"; do
        read -r from to <<< "$pair"
        peer=$(execroot "$from" getent ahostsv4 "raftdb-node$to" | awk 'NR==1{print $1}')
        execroot "$from" iptables -A OUTPUT -d "$peer" -p tcp --dport 7000 -m comment --comment raftdb-chaos -j DROP
        execroot "$from" iptables -A OUTPUT -d "$peer" -p tcp --sport 7000 -m comment --comment raftdb-chaos -j DROP
      done
    done; done
    ;;
  delay)
    for n in 1 2 3 4 5; do execroot "$n" tc qdisc replace dev eth0 root netem delay "${DELAY:-50ms}" "${JITTER:-10ms}" loss "${LOSS:-1%}"; done
    ;;
  check)
    majority=0; minority=0
    for n in 1 2 3 4 5; do
      dropped=$(execroot "$n" iptables -L OUTPUT -n -v -x | awk '/raftdb-chaos/ {sum+=$1} END {print sum+0}')
      echo "node$n: $dropped Raft packets dropped"
      if [[ $n -le 3 ]]; then majority=$((majority+dropped)); else minority=$((minority+dropped)); fi
    done
    [[ $majority -gt 0 && $minority -gt 0 ]] || { echo 'Partition did not drop traffic on both sides' >&2; exit 1; }
    ;;
  heal)
    for n in 1 2 3 4 5; do
      # Delete only this tool's rules, never flush unrelated packet filters.
      execroot "$n" sh -c 'iptables-save | grep raftdb-chaos | sed "s/^-A /iptables -D /" | while IFS= read -r rule; do eval "$rule"; done' || true
      execroot "$n" tc qdisc del dev eth0 root 2>/dev/null || true
    done
    ;;
  *) echo 'usage: network.sh partition|check|delay|heal' >&2; exit 1;;
esac
