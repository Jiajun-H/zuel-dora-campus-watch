from pathlib import Path
import tempfile
import unittest

from campus_watch.storage import SQLiteStore


class WatchRecoveryQueueTests(unittest.TestCase):
    def test_attempted_notifications_do_not_consume_automatic_queue_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            with SQLiteStore(Path(directory) / 'watch.sqlite3', target_category=14) as store:
                with store.run_lock():
                    connection = store._connection
                    with connection:
                        connection.execute("INSERT INTO runs(id, started_at, status) VALUES (1, 1, 'ok')")
                        for post_id, attempts, started in [(1, 1, 10), (2, 0, 11), (3, 0, None)]:
                            connection.execute(
                                "INSERT INTO posts(post_id,created_at,top,source_school,first_seen_at,batch_id,status,"
                                "attempts,notification_eligible,source_category,category_evidence,send_started_at) "
                                "VALUES(?,?,0,58,1,1,'pending',?,1,14,'response_field',?)",
                                (post_id, post_id, attempts, started))
                    before = [tuple(row) for row in connection.execute(
                        'SELECT post_id,status,attempts,send_started_at,notified_at FROM posts ORDER BY post_id')]
                    self.assertEqual([p.post_id for p in store.pending_posts(1, confirmed_only=True, first_attempt_only=True)], [3])
                    self.assertEqual([p.post_id for p in store.pending_posts(3, confirmed_only=True)], [1, 2, 3])
                    after = [tuple(row) for row in connection.execute(
                        'SELECT post_id,status,attempts,send_started_at,notified_at FROM posts ORDER BY post_id')]
                    self.assertEqual(before, after)


if __name__ == '__main__':
    unittest.main()
