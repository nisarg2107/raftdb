import unittest
from reports import summarize, measurement_window


class ReportsTest(unittest.TestCase):
    def report(self):
        return {"count": 100, "total": 2_000_000_000,
                "statusCodeDistribution": {"OK": 100}, "errorDistribution": {},
                "latencyDistribution": [{"percentage": 99, "latency": 5_000_000}]}

    def test_successful_rate_and_duration_units(self):
        result = summarize(self.report())
        self.assertEqual(result["successful_ops_per_second"], 50)
        self.assertEqual(result["latency_ms"]["99"], 5)

    def test_errors_are_not_throughput(self):
        report = self.report()
        report["statusCodeDistribution"] = {"OK": 90, "NotFound": 10}
        with self.assertRaises(ValueError):
            summarize(report)

    def test_resource_window_precedes_completion(self):
        report = {"date": "2026-10-18T12:01:00Z", "total": 60_000_000_000,
                  "details": [{"timestamp": "2026-10-18T12:00:59.500Z"}]}
        start, end = measurement_window(report)
        self.assertEqual(end - start, 60)
        self.assertEqual(end, 1792324859.5)


if __name__ == "__main__":
    unittest.main()
