"""Aggregate validated summaries without running commands or changing settings."""
import json
from pathlib import Path
import statistics


def aggregate(output):
    summary = {}
    output = Path(output)
    for folder in sorted(output.iterdir()):
        if not folder.is_dir():
            continue
        trials = [json.loads(p.read_text()) for p in sorted(folder.glob('*-summary.json'))]
        if not trials:
            continue
        rates = [t['successful_ops_per_second'] for t in trials]
        row = {
            'valid_trials': len(trials),
            'rejected_trials': len(list(folder.glob('*-rejected.txt'))),
            'minimum_ops_per_second': min(rates),
            'average_ops_per_second': statistics.mean(rates),
            'median_ops_per_second': statistics.median(rates),
            'maximum_ops_per_second': max(rates),
            'operations_per_rpc': trials[0]['operations_per_rpc'],
            'median_rpc_per_second': statistics.median(t['successful_rpcs_per_second'] for t in trials),
            'average_rpc_per_second': statistics.mean(t['successful_rpcs_per_second'] for t in trials),
            'median_p99_ms': statistics.median(t['latency_ms']['99'] for t in trials),
            'duration_seconds': trials[0]['duration_requested_seconds'],
            'trials': [
                {'trial': number, 'operations_per_second': trial['successful_ops_per_second'],
                 'rpc_per_second': trial['successful_rpcs_per_second'],
                 'p99_ms': trial['latency_ms']['99'],
                 'requested_duration_seconds': trial['duration_requested_seconds']}
                for number, trial in enumerate(trials, 1)
            ],
        }
        if folder.name in ('batch-read', 'batch-write'):
            qualified = len(trials) == 3 and row['rejected_trials'] == 0 and all(
                t['duration_requested_seconds'] == 60 for t in trials
            )
            target = 98000 if folder.name == 'batch-read' else 48000
            row.update(qualified_sustained_result=qualified,
                       target_ops_per_second=target, target_met=qualified and min(rates) >= target)
            if folder.name == 'batch-write':
                row['resume_41000_met'] = qualified and min(rates) >= 41000
        summary[folder.name] = row
    for kind in ('read', 'write'):
        before, after = summary.get('control-' + kind), summary.get('unary-' + kind)
        if before and after and before['valid_trials'] == after['valid_trials'] == 3:
            if not before['rejected_trials'] and not after['rejected_trials']:
                after['matched_p99_improvement_percent'] = (
                    100 * (before['median_p99_ms'] - after['median_p99_ms']) / before['median_p99_ms']
                )
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    return summary
