"""作者：Wangyuan；日期：2026-10-29。职责：离线持久任务队列、去重与不确定状态恢复。"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

TRANSITIONS = {'sending': {'waiting', 'done', 'failed', 'unknown'},
               'waiting': {'done', 'failed', 'unknown'}}


class Queue:
    """独立数据库队列；没有任何发送、桌面控制或自动重试适配器。"""

    def __init__(self, path: str | Path) -> None:
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA busy_timeout=10000')
        # 检查和迁移必须属于同一写事务，避免并发消费者重复添加列。
        try:
            self.db.execute('BEGIN IMMEDIATE')
            self.db.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL,
                created REAL NOT NULL, updated REAL NOT NULL, note TEXT NOT NULL DEFAULT '',
                attempt INTEGER NOT NULL DEFAULT 0)''')
            columns = {row['name'] for row in self.db.execute('PRAGMA table_info(jobs)')}
            if columns == {'id', 'payload', 'status', 'created', 'updated', 'note'}:
                self.db.execute('ALTER TABLE jobs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0')
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            self.db.close()
            raise

    def close(self) -> None:
        """释放当前连接，不影响其他消费者。"""
        self.db.close()

    def enqueue(self, ident: str, payload: Any) -> bool:
        """相同标识只入队一次，重复入队不覆盖原始内容。"""
        if not isinstance(ident, str) or not ident.strip() or len(ident) > 200:
            raise ValueError('任务标识须为 1—200 字')
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) > 1_000_000:
            raise ValueError('任务内容超过 1 MB')
        now = time.time()
        cur = self.db.execute('INSERT OR IGNORE INTO jobs (id,payload,status,created,updated,note) VALUES (?,?,?,?,?,?)',
                              (ident, encoded, 'queued', now, now, ''))
        return bool(cur.rowcount)

    def get(self, ident: str) -> dict[str, Any]:
        """读取一个任务，未知标识明确报错。"""
        row = self.db.execute('SELECT * FROM jobs WHERE id=?', (ident,)).fetchone()
        if row is None:
            raise ValueError('任务不存在')
        result = dict(row)
        result['payload'] = json.loads(result['payload'])
        return result

    def claim(self) -> dict[str, Any] | None:
        """用写事务原子领取最早任务，多个进程不会领取同一行。"""
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY created,id LIMIT 1").fetchone()
            if row:
                self.db.execute("UPDATE jobs SET status='sending',attempt=attempt+1,updated=? WHERE id=?", (time.time(), row['id']))
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        return self.get(row['id']) if row else None

    def transition(self, ident: str, expected: str, status: str, attempt: int, note: str = '') -> None:
        """同时核对领取代次与预期状态，拒绝旧消费者回执和非法状态跳转。"""
        if type(attempt) is not int or attempt < 1:
            raise ValueError('必须提供 claim 返回的正整数领取代次')
        if status not in TRANSITIONS.get(expected, set()):
            raise ValueError('不允许的状态转换')
        cur = self.db.execute('UPDATE jobs SET status=?,note=?,updated=? WHERE id=? AND status=? AND attempt=?',
                              (status, note, time.time(), ident, expected, attempt))
        if cur.rowcount != 1:
            raise ValueError('任务不存在或状态已变化')

    def recover(self) -> int:
        """消费者已停止后显式恢复；可能已经执行的任务标为未知，不重新入队。"""
        return self.db.execute("UPDATE jobs SET status='unknown',updated=?,note='interrupted; outcome unverified' WHERE status IN ('sending','waiting')", (time.time(),)).rowcount

    def retry(self, ident: str, note: str) -> None:
        """调用者确认结果后手动重试，必须保留核对说明。"""
        if not isinstance(note, str) or not note.strip():
            raise ValueError('重试必须写明核对依据')
        cur = self.db.execute("UPDATE jobs SET status='queued',updated=?,note=? WHERE id=? AND status IN ('unknown','failed')", (time.time(), note, ident))
        if cur.rowcount != 1:
            raise ValueError('只有未知或失败任务可经核对后重试')


def main() -> None:
    """提供显式数据库路径的离线 CLI。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    sub = parser.add_subparsers(dest='action', required=True)
    p = sub.add_parser('enqueue'); p.add_argument('id'); p.add_argument('json_payload')
    sub.add_parser('claim'); sub.add_parser('recover')
    p = sub.add_parser('get'); p.add_argument('id')
    p = sub.add_parser('transition'); p.add_argument('id'); p.add_argument('expected'); p.add_argument('status'); p.add_argument('attempt', type=int)
    p = sub.add_parser('retry'); p.add_argument('id'); p.add_argument('note')
    args = parser.parse_args()
    queue = Queue(args.db)
    try:
        if args.action == 'enqueue':
            result = queue.enqueue(args.id, json.loads(args.json_payload))
        elif args.action == 'transition':
            result = queue.transition(args.id, args.expected, args.status, args.attempt)
        elif args.action == 'retry':
            result = queue.retry(args.id, args.note)
        elif args.action == 'get':
            result = queue.get(args.id)
        else:
            result = getattr(queue, args.action)()
        print(json.dumps(result, ensure_ascii=False))
    except (ValueError, sqlite3.Error) as exc:
        parser.exit(2, str(exc) + '\n')
    finally:
        queue.close()


if __name__ == '__main__':
    main()
