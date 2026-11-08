"""作者：Wangyuan；日期：2026-11-08。职责：复验旧队列表结构并发迁移。"""
import concurrent.futures
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from queue_core import Queue


class MigrationTests(unittest.TestCase):
    def test_old_schema_concurrent_open_keeps_all_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "queue.sqlite"
            with sqlite3.connect(path) as connection:
                connection.execute("CREATE TABLE jobs(id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,created REAL NOT NULL,updated REAL NOT NULL,note TEXT NOT NULL DEFAULT '')")
            barrier = threading.Barrier(8)
            def open_one(index):
                barrier.wait(timeout=10)
                queue = Queue(path)
                try:
                    return queue.enqueue(str(index), {"synthetic": index})
                finally:
                    queue.close()
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                self.assertEqual(list(pool.map(open_one, range(8))), [True] * 8)
            with sqlite3.connect(path) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 8)
                self.assertIn("attempt", [row[1] for row in connection.execute("PRAGMA table_info(jobs)")])
