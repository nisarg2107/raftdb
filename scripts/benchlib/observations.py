"""Export Prometheus observations for the measured ghz interval."""
import json
import os
from pathlib import Path
import urllib.parse
import urllib.request

from reports import load_report, measurement_window

QUERIES = {
    'cpu_cores': 'rate(process_cpu_seconds_total{job="raftdb"}[10s])',
    'resident_bytes': 'process_resident_memory_bytes{job="raftdb"}',
    'heap_bytes': 'go_memstats_heap_alloc_bytes{job="raftdb"}',
    'last_log_index': 'raftdb_last_log_index{job="raftdb"}',
    'read_rounds': 'raftdb_read_rounds_total{job="raftdb"}',
    'operations': 'raftdb_operations_total{job="raftdb"}',
    'storage': 'raftdb_storage_duration_seconds_sum{job="raftdb"}',
    'storage_calls': 'raftdb_storage_duration_seconds_count{job="raftdb"}',
    'snapshot_duration': 'raftdb_snapshot_duration_seconds_sum{job="raftdb"}',
    'batch_operations': 'raftdb_committed_batch_operations_sum{job="raftdb"}',
    'committed_batches': 'raftdb_committed_batch_operations_count{job="raftdb"}',
}


def capture_metrics(report_path, base=None):
    report_path = Path(report_path)
    report = load_report(report_path)
    start, end = measurement_window(report)
    capped = report.get('_rpc_details_count', 0) < report['count']
    observations = {'window': {
        'start': start, 'end': end,
        'basis': 'ghz report date minus total duration; RPC details were capped, timestamps have second precision'
        if capped else 'last RPC completion minus official ghz total duration',
    }}
    base = base or os.environ.get('PROMETHEUS_URL', 'http://127.0.0.1:9090')
    for name, query in QUERIES.items():
        params = urllib.parse.urlencode({'query': query, 'start': start, 'end': end, 'step': 2})
        with urllib.request.urlopen(base + '/api/v1/query_range?' + params, timeout=10) as response:
            observations[name] = json.load(response)
        if observations[name].get('status') != 'success':
            raise ValueError('Prometheus query failed: ' + name)
    destination = report_path.with_name(report_path.stem + '-prometheus.json')
    destination.write_text(json.dumps(observations, indent=2) + '\n', encoding='utf-8')
