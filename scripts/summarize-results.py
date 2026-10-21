"""Aggregate saved official ghz reports; this script never generates requests."""
import argparse
import json
from pathlib import Path
import re
import statistics
from reports import summarize

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    pattern = re.compile(r"(.+)-c(\d+)-r(\d+)\.json$")
    groups = {}
    for path in sorted(args.root.rglob("*.json")):
        match = pattern.fullmatch(path.name)
        if not match:
            continue
        validated_path = path.with_name(path.stem + "-summary.json")
        if not validated_path.exists():
            raise ValueError(f"Report lacks completed validation: {path}")
        json.loads(validated_path.read_text(encoding="utf-8"))
        report = json.loads(path.read_text(encoding="utf-8"))
        result = summarize(report)  # invalid/error reports fail rather than inflate QPS
        key = (path.parent.name, match[1], int(match[2]))
        groups.setdefault(key, []).append((path, report["options"], result))

    if not groups:
        raise SystemExit("No measured official ghz reports found")

    rows = []
    for (phase, workload, concurrency), trials in sorted(groups.items()):
        options = trials[0][1]
        for _, candidate, _ in trials:
            for field in ("call", "data", "concurrency", "connections", "duration", "timeout"):
                if candidate.get(field) != options.get(field):
                    raise ValueError(f"inconsistent trials: {phase}/{workload}: {field}")
        rates = [r["successful_ops_per_second"] for _, _, r in trials]
        row = {"phase": phase, "workload": workload, "concurrency": concurrency,
               "trials": len(trials), "rpc": options["call"],
               "minimum_successful_ops_per_second": min(rates),
               "median_successful_ops_per_second": statistics.median(rates),
               "maximum_successful_ops_per_second": max(rates),
               "requests": sum(r["count"] for _, _, r in trials), "error_rate": 0,
               "median_latency_ms": {
                   p: statistics.median(r["latency_ms"][p] for _, _, r in trials)
                   for p in ("50", "95", "99")},
               "raw_reports": [p.as_posix() for p, _, _ in trials]}
        rows.append(row)

    summary = {"groups": rows, "valid_measured_trials": sum(r["trials"] for r in rows)}
    eligible_reads = [r for r in rows if r["phase"].startswith("optimized")
                      and r["rpc"].endswith(".Get") and r["trials"] >= 3]
    eligible_writes = [r for r in rows if r["rpc"].endswith(".Put") and r["trials"] >= 3]
    for name, eligible, target in (("linearizable_reads", eligible_reads, 98000),
                                    ("committed_writes", eligible_writes, 41000)):
        if eligible:
            best = max(eligible, key=lambda r: r["minimum_successful_ops_per_second"])
            rate = best["minimum_successful_ops_per_second"]
            summary[name] = {"reproducible_ops_per_second": rate, "target": target,
                             "target_met": rate >= target,
                             "shortfall_percent": max(0, (target - rate) / target * 100),
                             "workload": best["workload"], "concurrency": best["concurrency"]}

    baseline = [r for r in rows if r["phase"] == "baseline"]
    optimized = [r for r in rows if r["phase"] == "optimized-comparison"]
    if len(baseline) == len(optimized) == 1:
        before, after = baseline[0], optimized[0]
        if (before["workload"], before["concurrency"]) != (after["workload"], after["concurrency"]):
            raise ValueError("tail comparison workloads differ")
        summary["tail_improvement_percent"] = {
            p: (before["median_latency_ms"][p] - after["median_latency_ms"][p])
            / before["median_latency_ms"][p] * 100 for p in ("95", "99")}

    output = args.root / "summary.json"
    output.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print("| Phase | Workload | Concurrency | Trials | Successful ops/s min / median / max | p50 / p95 / p99 ms* |")
    print("|---|---|---:|---:|---:|---:|")
    for r in rows:
        rate = [r[f"{s}_successful_ops_per_second"] for s in ("minimum", "median", "maximum")]
        latency = [r["median_latency_ms"][p] for p in ("50", "95", "99")]
        print(f'| {r["phase"]} | {r["workload"]} | {r["concurrency"]} | {r["trials"]} | '
              + " / ".join(f"{v:,.1f}" for v in rate) + " | "
              + " / ".join(f"{v:.2f}" for v in latency) + " |")
    print("\n*Median of the per-trial percentiles; not a percentile of pooled samples.")
    print(f"\nValidated summary: {output}")


if __name__ == '__main__':
    main()
