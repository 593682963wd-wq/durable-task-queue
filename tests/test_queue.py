"""作者：Wangyuan；日期：2026-10-29。职责：验证去重、并发领取和中断恢复边界。"""
import tempfile
import unittest
from pathlib import Path
from queue_core import Queue


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.sqlite'
        self.q = Queue(self.path)

    def tearDown(self):
        self.q.close()
        self.temp.cleanup()

    def test_dedup_does_not_overwrite(self):
        self.assertTrue(self.q.enqueue('demo', {'value': 1}))
        self.assertFalse(self.q.enqueue('demo', {'value': 2}))
        self.assertEqual(self.q.get('demo')['payload'], {'value': 1})

    def test_two_connections_claim_once(self):
        other = Queue(self.path)
        try:
            self.q.enqueue('demo', {})
            self.assertEqual(self.q.claim()['id'], 'demo')
            self.assertIsNone(other.claim())
        finally:
            other.close()

    def test_recovery_never_resends(self):
        self.q.enqueue('demo', {}); self.q.claim()
        self.assertEqual(self.q.recover(), 1)
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.get('demo')['status'], 'unknown')
        self.assertEqual(self.q.recover(), 0)
        with self.assertRaises(ValueError): self.q.retry('demo', '')
        self.q.retry('demo', 'synthetic result checked')
        self.assertEqual(self.q.claim()['id'], 'demo')

    def test_late_receipt_rejected(self):
        self.q.enqueue('demo', {}); self.q.claim()
        self.q.transition('demo', 'sending', 'waiting', 1)
        with self.assertRaises(ValueError): self.q.transition('demo', 'sending', 'done', 1)
        self.q.transition('demo', 'waiting', 'done', 1)
        with self.assertRaises(ValueError): self.q.retry('demo', 'do not repeat completed')

    def test_persisted_without_auto_recovery(self):
        self.q.enqueue('demo', {}); self.q.claim(); self.q.close()
        self.q = Queue(self.path)
        self.assertEqual(self.q.get('demo')['status'], 'sending')
        self.assertIsNone(self.q.claim())

    def test_invalid_payload(self):
        with self.assertRaises(ValueError): self.q.enqueue('', {})
        with self.assertRaises(ValueError): self.q.enqueue('demo', float('nan'))

    def test_old_consumer_cannot_finish_new_attempt(self):
        self.q.enqueue('demo', {})
        old = self.q.claim()
        self.q.recover(); self.q.retry('demo', 'synthetic outcome verified')
        new = self.q.claim()
        self.assertGreater(new['attempt'], old['attempt'])
        with self.assertRaises(ValueError):
            self.q.transition('demo', 'sending', 'done', old['attempt'])
        self.assertEqual(self.q.get('demo')['status'], 'sending')
        self.q.transition('demo', 'sending', 'done', new['attempt'])


if __name__ == '__main__':
    unittest.main()
