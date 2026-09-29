import copy
import unittest
import urllib.parse
import smoke

class CoverageTest(unittest.TestCase):
    def setUp(self):
        self.now = 2000
        self.rows = {}
        self.add("discovery_success_ratio", None, None, 1)
        for folder in ("1", "2"):
            self.add("folder_available_ratio", folder, None, 1)
            for dataset in ("jobs", "logs", "queues"):
                enabled = int(dataset != "queues")
                self.add("dataset_enabled_ratio", folder, dataset, enabled)
                if enabled:
                    self.add("scrape_success_ratio", folder, dataset, 1)
                    self.add("last_success_time_seconds", folder, dataset, self.now-10)

    def add(self, metric, folder, dataset, value):
        labels = {"uipath_installation":"example", "uipath_tenant_id":"tenant"}
        if folder: labels["uipath_folder_id"] = folder
        if dataset: labels["uipath_dataset"] = dataset
        self.rows.setdefault("uipath_collector_"+metric, []).append({"metric":labels,"value":[self.now,str(value)]})

    def fetch(self, base, path):
        metric = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)["query"][0].split("{", 1)[0]
        return {"status":"success","data":{"result":copy.deepcopy(self.rows.get(metric, []))}}

    def check(self, **kwargs):
        smoke.check_coverage("example", fetch=self.fetch, now=self.now, **kwargs)

    def test_complete_coverage(self):
        self.check()

    def test_failed_discovery(self):
        self.rows["uipath_collector_discovery_success_ratio"][0]["value"][1] = "0"
        with self.assertRaises(AssertionError): self.check()

    def test_disappeared_folder_with_healthy_surviving_series(self):
        self.rows["uipath_collector_folder_available_ratio"][1]["value"][1] = "0"
        with self.assertRaises(AssertionError): self.check()

    def test_missing_enabled_dataset(self):
        self.rows["uipath_collector_scrape_success_ratio"].pop()
        with self.assertRaises(AssertionError): self.check()

    def test_missing_configuration(self):
        self.rows["uipath_collector_dataset_enabled_ratio"].pop()
        with self.assertRaises(AssertionError): self.check()

    def test_stale_and_nonfinite_observations(self):
        for value in ("1000", "NaN", "inf"):
            with self.subTest(value=value):
                self.rows["uipath_collector_last_success_time_seconds"][0]["value"][1] = value
                with self.assertRaises(AssertionError): self.check()

    def test_disabled_queue_ignores_old_failures_but_cannot_pass_required_queue_check(self):
        self.add("scrape_success_ratio", "1", "queues", 0)
        self.add("last_success_time_seconds", "1", "queues", 0)
        self.check()
        with self.assertRaises(AssertionError): self.check(require_queues=True)

    def test_query_warnings_are_gaps(self):
        def fetch(base, path):
            result = self.fetch(base, path)
            result["warnings"] = ["partial response"]
            return result
        with self.assertRaises(AssertionError):
            smoke.check_coverage("example", fetch=fetch, now=self.now)

if __name__ == "__main__":
    unittest.main()
