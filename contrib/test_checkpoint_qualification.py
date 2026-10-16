"""Qualification must await physical cache reclamation as well as checkpoint cleanup."""
import unittest
from qualify_checkpoint_tart import rolling_reclaimed


class RollingReclamation(unittest.TestCase):
    def test_waits_for_cache_report_after_checkpoint_cleanup(self):
        report = {'checkpoints': {'attempt': {'live_checkpoints': 2}},
                  'blob_payload_bytes': 3 * 65536}
        status = {'workers': [{'report': report}]}
        self.assertFalse(rolling_reclaimed(status))
        report['blob_payload_bytes'] = 2 * 65536
        self.assertTrue(rolling_reclaimed(status))
        report['checkpoints']['attempt']['live_checkpoints'] = 3
        self.assertFalse(rolling_reclaimed(status))

    def test_missing_reports_never_confirm_reclamation(self):
        for status in [{'workers': []}, {'workers': [{'report': None}]},
                       {'workers': [{'report': {'checkpoints': {}, 'blob_payload_bytes': 0}}]}]:
            self.assertFalse(rolling_reclaimed(status))
