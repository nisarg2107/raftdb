"""Validate official ghz reports; summarize successful completions, never submissions."""
import argparse
import datetime
import json
import re
import sys


def load_report(path):
    """Read ghz metadata/timing without materializing millions of detail objects.

    The complete official report stays on disk. ghz can cap its detail array,
    so a shorter array must not define the end of a longer measurement.
    """
    marker = re.compile(rb',\s*"details"\s*:\s*\[')
    timestamp = re.compile(rb'"timestamp"\s*:\s*"([^"\\]+)"')
    with open(path, 'rb') as source:
        head = b''
        while True:
            block = source.read(65536)
            if not block:
                return json.loads(head)
            head += block
            match = marker.search(head)
            if match:
                report = json.loads(head[:match.start()] + b'}')
                block = head[match.end():]
                break
            if len(head) > 4 * 1024 * 1024:
                raise ValueError('Unexpectedly large ghz metadata header')
        count, latest, latest_time = 0, None, None
        carry, offset, seen_end = b'', 0, -1
        while block:
            data = carry + block
            base = offset - len(carry)
            for item in timestamp.finditer(data):
                end = base + item.end()
                if end <= seen_end:
                    continue
                seen_end = end
                value = item.group(1).decode('ascii')
                when = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
                count += 1
                if latest_time is None or when > latest_time:
                    latest, latest_time = value, when
            offset += len(block)
            carry = data[-128:]
            block = source.read(65536)
        if not re.search(rb'\]\s*\}\s*$', carry):
            raise ValueError('Incomplete official ghz report')
        report['_rpc_details_count'] = count
        report['details'] = [{'timestamp': latest}] if latest else []
        return report


def measurement_window(report):
    # ghz Date is finalization time, not start time. RPC detail timestamps are
    # completion times with subsecond precision (runner/stats_handler.go).
    timestamps = [d["timestamp"] for d in report.get("details", [])]
    if not timestamps or report.get('_rpc_details_count', len(report.get('details', []))) < report.get('count', len(report.get('details', []))):
        timestamps = [report["date"]]
    end = max(datetime.datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp()
              for t in timestamps)
    return end - report["total"] / 1e9, end


def summarize(report):
    count = report["count"]
    statuses = report.get("statusCodeDistribution", {})
    ok = statuses.get("OK", 0)
    duration = report["total"] / 1e9  # official ghz JSON encodes Go durations as nanoseconds
    if count <= 0 or duration <= 0 or ok != count or report.get("errorDistribution"):
        raise ValueError(f"invalid run: count={count}, OK={ok}, errors={report.get('errorDistribution')}")
    percentiles = {str(p["percentage"]): p["latency"] / 1e6 for p in report["latencyDistribution"]}
    batch_size = 1
    call = report.get("options", {}).get("call", "")
    if call.endswith((".BatchGet", ".BatchPut")):
        data = report["options"]["data"]
        if isinstance(data, str):
            data = json.loads(data)
        if call.endswith(".BatchGet"):
            keys = data["keys"]
        else:
            keys = [entry["key"] for entry in data["entries"]]
        batch_size = len(keys)
        if not 1 <= batch_size <= 128 or len(set(keys)) != batch_size:
            raise ValueError("batch benchmarks require 1..128 distinct keys")
    return {"count": count, "success_rate": ok / count, "successful_ops_per_second": ok * batch_size / duration,
            "successful_rpcs_per_second": ok / duration, "operations_per_rpc": batch_size,
            "successful_operations": ok * batch_size,
            "duration_seconds": duration, "latency_ms": percentiles}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("--status-before")
    parser.add_argument("--status-after")
    parser.add_argument("--baseline", help="comparable official ghz report, same workload and environment")
    parser.add_argument("--target", type=float, help="fail if successful ops/s is below this acceptance target")
    args = parser.parse_args()
    result = summarize(load_report(args.report))
    if args.status_before or args.status_after:
        def statuses(path):
            with open(path, encoding="utf-8") as source:
                return {s["id"]: (s.get("term"), s.get("leader_id")) for s in (json.loads(line) for line in source if line.strip())}
        if statuses(args.status_before) != statuses(args.status_after):
            raise ValueError("invalid run: term or leader changed")
    if args.baseline:
        baseline_report = load_report(args.baseline)
        before = summarize(baseline_report)
        current_report = load_report(args.report)
        for option in ("call", "data", "concurrency", "connections", "duration", "timeout"):
            if baseline_report["options"].get(option) != current_report["options"].get(option):
                raise ValueError(f"incomparable baseline: {option} differs")
        for p in ("95", "99"):
            b = before["latency_ms"][p]
            result[f"p{p}_improvement_percent"] = (b - result["latency_ms"][p]) / b * 100
    print(json.dumps(result, indent=2))
    if args.target and result["successful_ops_per_second"] < args.target:
        raise ValueError("measured successful throughput is below the acceptance target")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, TypeError) as error:
        sys.exit(str(error))
