"""Prevent batch-operation accounting from relabeling RPCs or failed calls."""
import unittest
import datetime
import json
from pathlib import Path
import tempfile
from reports import load_report, measurement_window, summarize

class BatchAccounting(unittest.TestCase):
    def report(self,method,data):
        return {'count':10,'total':1000000000,'statusCodeDistribution':{'OK':10},'errorDistribution':{},'latencyDistribution':[],'options':{'call':'raftdb.v1.Database.'+method,'data':data}}
    def test_unary_and_batch_counts_are_distinct(self):
        unary=summarize(self.report('Get',{'key':'k'}))
        self.assertEqual(unary['successful_ops_per_second'],10)
        for method,data in [('BatchGet',{'keys':['k'+str(i) for i in range(16)]}),('BatchPut',{'entries':[{'key':'k'+str(i),'value':'v'} for i in range(16)]})]:
            result=summarize(self.report(method,data))
            self.assertEqual(result['successful_rpcs_per_second'],10)
            self.assertEqual(result['successful_ops_per_second'],160)
            self.assertEqual(result['successful_operations'],160)
    def test_failed_and_duplicate_key_benchmarks_are_rejected(self):
        failed=self.report('BatchGet',{'keys':['k']})
        failed['statusCodeDistribution']={'OK':9,'DeadlineExceeded':1}
        with self.assertRaises(ValueError): summarize(failed)
        with self.assertRaises(ValueError): summarize(self.report('BatchGet',{'keys':['k','k']}))

    def test_streamed_details_and_capped_measurement_window(self):
        report=self.report('Get',{'key':'k'})
        report.update({'count':2000,'date':'2026-10-19T00:01:00Z','details':[{'timestamp':'2026-10-19T00:00:59.123456789Z','padding':'x'*40} for _ in range(2000)]})
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'report.json'
            path.write_text(json.dumps(report))
            loaded=load_report(path)
            self.assertEqual(loaded['_rpc_details_count'],2000)
            self.assertEqual(len(loaded['details']),1)
            self.assertAlmostEqual(measurement_window(loaded)[1],datetime.datetime.fromisoformat('2026-10-19T00:00:59.123456+00:00').timestamp())
            report['count']=3000
            path.write_text(json.dumps(report))
            self.assertEqual(measurement_window(load_report(path))[1],datetime.datetime.fromisoformat('2026-10-19T00:01:00+00:00').timestamp())
            path.write_bytes(path.read_bytes()[:-2])
            with self.assertRaises(ValueError): load_report(path)

if __name__=='__main__': unittest.main()
