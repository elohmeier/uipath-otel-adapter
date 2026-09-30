import unittest
from promql import with_source_metadata


class MetadataJoinTest(unittest.TestCase):
    def test_gauge_filter_moves_to_info(self):
        query = with_source_metadata('sum(uipath_jobs{uipath_installation=~"$installation",uipath_folder_id="42"})')
        self.assertIn('uipath_jobs{uipath_folder_id="42"}', query)
        self.assertIn('target_info{job=~', query)
        self.assertIn('on (job,instance) group_left(uipath_installation,uipath_tenant_id)', query)

    def test_rate_precedes_join_and_grafana_braces_survive(self):
        query = with_source_metadata('rate(uipath_jobs_completed_total{uipath_installation=~"$installation",uipath_process_name=~"${process:regex}"}[$__rate_interval])')
        self.assertIn('rate(uipath_jobs_completed_total{uipath_process_name=~"${process:regex}"}[$__rate_interval]) * on', query)
        self.assertEqual(query.count('target_info{'), 1)

    def test_unscoped_selector_is_rejected(self):
        with self.assertRaises(ValueError):
            with_source_metadata('uipath_jobs{}')
