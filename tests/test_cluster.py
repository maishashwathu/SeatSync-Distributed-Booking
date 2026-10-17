"""Integration tests run real subprocesses, TCP RPC, and per-node SQLite files."""
import os
os.environ['TICKET_NODE_BASE'] = '18890'
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import Cluster
from common import rpc


class ClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='seatsync-tests-')
        cls.cluster = Cluster(cls.temp.name)
        try:
            for node in (1, 2, 3):
                cls.cluster.start(node)
            cls.cluster.wait_ready(expected=3)
        except Exception:
            cls.cluster.close()
            cls.temp.cleanup()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.cluster.close()
        cls.temp.cleanup()

    def book(self, seat, name='Test guest', node=1, request_id=None):
        return self.cluster.book({'seat': seat, 'name': name, 'node': node,
                                  'request_id': request_id or uuid.uuid4().hex})

    def wait_synced(self, count, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            nodes = self.cluster.statuses()
            if all(n['online'] and len(n['bookings']) == count for n in nodes):
                self.assertEqual(nodes[0]['bookings'], nodes[1]['bookings'])
                self.assertEqual(nodes[1]['bookings'], nodes[2]['bookings'])
                return
            time.sleep(.2)
        self.fail('Replicas did not synchronize')

    def test_01_initial_election(self):
        self.assertEqual(self.cluster.wait_ready()['leader'], 3)

    def test_02_race(self):
        race = self.cluster.race('A1')['race']
        self.assertEqual((race['successes'], race['conflicts'], race['errors']), (1, 11, 0))
        self.wait_synced(1)
        self.__class__.race_result = race

    def test_03_idempotency_and_validation(self):
        key = uuid.uuid4().hex
        first = self.book('A2', request_id=key)
        retry = self.book('A2', node=2, request_id=key)
        self.assertTrue(first['ok'])
        self.assertTrue(retry['duplicate'])
        self.assertEqual(first['booking']['id'], retry['booking']['id'])
        self.assertFalse(self.book('A3', request_id=key)['ok'])
        self.assertFalse(self.book('Z99')['ok'])
        self.assertFalse(self.book('A3', name='')['ok'])
        self.wait_synced(2)

    def test_04_coordinator_crash(self):
        result = self.cluster.failover()['failover']
        self.assertEqual(result['new_leader'], 2)
        self.assertEqual(result['bookings_preserved'], 2)
        booking = self.book('B1')
        self.assertTrue(booking['ok'], booking)
        self.__class__.failover_result = result

    def test_05_higher_node_recovery(self):
        self.cluster.start(3)
        self.cluster.wait_ready(expected=3)
        self.wait_synced(3)
        self.assertFalse(self.book('A1')['ok'])

    def test_06_lower_follower_recovery(self):
        self.cluster.stop(1)
        booking = self.book('B2', node=2)
        self.assertTrue(booking['ok'], booking)
        self.cluster.start(1)
        self.wait_synced(4)
        self.cluster.wait_ready(expected=3)

    def test_07_quorum_loss(self):
        self.cluster.stop(1)
        self.cluster.stop(2)
        result = self.book('C1', node=3)
        self.assertFalse(result['ok'])
        self.assertIn('quorum', result['error'].lower())
        self.assertNotIn('C1', rpc(3, '/snapshot')['bookings'])
        existing = next(iter(rpc(3, '/snapshot')['bookings'].values()))
        retry = self.book(existing['seat'], name=existing['name'], node=3,
                          request_id=existing['request_id'])
        self.assertFalse(retry['ok'], 'An idempotent retry must still require durable quorum')
        self.cluster.start(1)
        self.cluster.start(2)
        self.wait_synced(4)

    def test_08_full_restart_durability(self):
        for node in (1, 2, 3):
            self.cluster.stop(node)
        for node in (1, 2, 3):
            self.cluster.start(node)
        self.cluster.wait_ready(expected=3)
        self.wait_synced(4)
        self.assertTrue(self.book('C2')['ok'])

    def test_09_export_measured_results(self):
        output = Path(os.environ.get('TICKET_RESULTS_PATH',
                      str(Path(__file__).resolve().parents[1] / 'docs' / 'test-results.json')))
        output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps({'recorded_at': time.time(), 'race': self.race_result,
                                     'failover': self.failover_result,
                                     'scope': 'Local fixed-membership crash/recovery, no network partitions',
                                     'checks': ['highest-ID election', '12-request race', 'idempotency',
                                                'input validation', 'coordinator crash', 'higher-ID recovery',
                                                'lower-ID recovery', 'quorum rejection', 'full restart durability']}, indent=2))


if __name__ == '__main__':
    unittest.main(verbosity=2)
