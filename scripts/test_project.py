"""Project-task checks: file validation, verified seeding and command defaults."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import project


class ProjectTests(unittest.TestCase):
    def test_invalid_seed_is_rejected_before_any_rpc(self):
        invalid = [[], [{'key': 'a', 'value': 1}],
                   [{'key': 'a', 'value': ''}, {'key': 'a', 'value': 'again'}],
                   [{'key': 'a' * 1025, 'value': 'value'}],
                   [{'key': 'a', 'value': 'v' * (256 * 1024)}]]
        with tempfile.TemporaryDirectory() as tmp, patch.object(project, 'cluster_cli') as rpc:
            path = Path(tmp) / 'seed.json'
            for entries in invalid:
                path.write_text(json.dumps(entries), encoding='utf-8')
                with self.assertRaises(ValueError):
                    project.seed(path)
            rpc.assert_not_called()

    def test_seed_is_atomic_and_values_are_read_back(self):
        entries = [{'key': 'demo:a', 'value': 'hello'}, {'key': 'demo:b', 'value': ''}]
        response = json.dumps({'results': [{'found': True, 'value': 'hello'}, {'found': True}]})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'seed.json'
            path.write_text(json.dumps(entries), encoding='utf-8')
            with patch.object(project, 'cluster_cli', side_effect=[None, response]) as rpc, patch('builtins.print'):
                self.assertEqual(project.seed(path), entries)
            self.assertEqual(rpc.call_args_list[0].args, ('batch-put', 'demo:a', 'hello', 'demo:b', ''))
            self.assertEqual(rpc.call_args_list[1].args, ('batch-get', 'demo:a', 'demo:b'))

    def test_benchmark_defaults_to_three_real_sixty_second_trials(self):
        args = project.parser().parse_args(['benchmark'])
        self.assertEqual((args.repeats, args.duration), (3, 60))
        with patch.object(project, 'run') as run:
            project.main(['benchmark', '--no-setup', '--output', 'benchmarks/results/example'])
            command = run.call_args.args[0]
            self.assertEqual(command[1:3], ['scripts/bench.py', 'run'])
            self.assertEqual(command[-4:], ['--repeats', 3, '--duration', 60])

    def test_demo_is_bounded_and_checks_real_read_responses(self):
        entries = [{'key': 'demo:counter', 'value': '0'}, {'key': 'demo:message', 'value': 'hello'}]
        response = json.dumps({'results': [{'found': True, 'value': '1'}, {'found': True, 'value': 'hello'}]})
        with patch.object(project, 'seed', return_value=entries), \
                patch.object(project, 'cluster_cli', side_effect=[None, response]) as rpc, \
                patch.object(project.time, 'monotonic', side_effect=[0, 0, 1, 2]), \
                patch.object(project.time, 'sleep'), patch('builtins.print'):
            project.demo(duration=1, interval=.25, no_setup=True)
        self.assertEqual(rpc.call_args_list[0].args, ('batch-put', 'demo:counter', '1'))
        self.assertEqual(rpc.call_args_list[1].args, ('batch-get', 'demo:counter', 'demo:message'))


if __name__ == '__main__':
    unittest.main()
