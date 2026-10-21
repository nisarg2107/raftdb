"""Regression checks for portability, import safety and honest result selection."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchlib.results import aggregate
from benchlib.runner import Runner
from benchlib.runtime import ROOT, Runtime


class ToolingTests(unittest.TestCase):
    def test_trial_average_is_arithmetic_mean_not_median(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / 'batch-read'
            folder.mkdir()
            for number, rate in enumerate((100, 200, 900), 1):
                row = {'successful_ops_per_second': rate, 'operations_per_rpc': 32,
                       'successful_rpcs_per_second': rate / 32, 'latency_ms': {'99': number},
                       'duration_requested_seconds': 60}
                (folder / f'trial-r{number}-summary.json').write_text(json.dumps(row))
            result = aggregate(Path(tmp))['batch-read']
            self.assertEqual(result['average_ops_per_second'], 400)
            self.assertEqual(result['median_ops_per_second'], 200)
            self.assertEqual(result['average_rpc_per_second'], 12.5)
            self.assertEqual([t['operations_per_second'] for t in result['trials']], [100, 200, 900])

    def test_entry_points_are_safe_to_import(self):
        scripts = ROOT / 'scripts'
        names = ['bench.py', 'project.py', 'capture-metrics.py', 'analyze-profiles.py',
                 'summarize-results.py', 'profile-current.py',
                 'chaos/netem.py', 'chaos/verify-netem.py']
        with patch('subprocess.run') as run, patch('subprocess.check_output') as output, \
                patch('os.chdir') as chdir, patch.object(Path, 'mkdir') as mkdir:
            for index, name in enumerate(names):
                spec = importlib.util.spec_from_file_location('entry_' + str(index), scripts / name)
                spec.loader.exec_module(importlib.util.module_from_spec(spec))
            run.assert_not_called()
            output.assert_not_called()
            chdir.assert_not_called()
            mkdir.assert_not_called()

    def test_absolute_report_paths_are_mapped_inside_container(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = Runtime(root)
            runner = Runner(root / 'reports with spaces', runtime=runtime)
            config = root / 'reports with spaces' / 'trial-config.json'
            with patch('subprocess.check_output') as execute:
                self.assertEqual(runtime.container_path(config), '/work/reports with spaces/trial-config.json')
                with self.assertRaises(ValueError):
                    Runner(root.parent / 'outside-workspace', runtime=runtime)
                execute.assert_not_called()
            self.assertEqual(runner.warmup, 10)

    def test_rejected_or_mixed_duration_trials_cannot_establish_target(self):
        row = {'successful_ops_per_second': 100000, 'operations_per_rpc': 32,
               'successful_rpcs_per_second': 3125, 'latency_ms': {'99': 1},
               'duration_requested_seconds': 60}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / 'batch-read'
            folder.mkdir()
            for n in range(3):
                (folder / f'{n}-summary.json').write_text(json.dumps(row))
            self.assertTrue(aggregate(root)['batch-read']['target_met'])
            (folder / 'attempt-rejected.txt').write_text('leader changed')
            self.assertFalse(aggregate(root)['batch-read']['target_met'])
            (folder / 'attempt-rejected.txt').unlink()
            row['duration_requested_seconds'] = 20
            (folder / '2-summary.json').write_text(json.dumps(row))
            self.assertFalse(aggregate(root)['batch-read']['target_met'])

    def test_resume_retains_original_source_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / 'batch-read'
            folder.mkdir()
            provenance = folder / 'source-sha256.txt'
            provenance.write_text('original source hashes')
            summary = folder / 'batch-read-n32-c32-r1-summary.json'
            summary.write_text('{"successful_ops_per_second": 1000}')
            runner = Runner(root, runtime=Runtime(root), resume=True)
            with patch.object(runner, 'restore'), patch.object(runner.runtime, 'ready', return_value='ready'), \
                    patch.object(runner.runtime, 'validate', return_value='validated'), \
                    patch.object(runner.runtime, 'ctl') as ctl, \
                    patch.object(runner, 'metadata') as metadata, patch('builtins.print'):
                results = runner.case('batch-read', 'read', 32)
            ctl.assert_called_once_with('seed', '10000', timeout=180)
            metadata.assert_not_called()
            self.assertEqual(len(results), 1)
            self.assertEqual(provenance.read_text(), 'original source hashes')


if __name__ == '__main__':
    unittest.main()
