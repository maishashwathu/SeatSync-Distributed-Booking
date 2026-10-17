"""One booking replica. Launch three copies via app.py, never shared memory."""
import argparse
from collections import deque
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from common import BASE, SEATS, body, reply, rpc


class Node:
    def __init__(self, identity, directory):
        self.id = identity
        self.lock = threading.RLock()
        self.election_lock = threading.Lock()
        self.leader = None
        self.role = 'starting'
        self.clock = 0
        self.events = deque(maxlen=150)
        self.messages = 0
        self.attempts = 0
        self.conflicts = 0
        self.latencies = deque(maxlen=1000)
        self.holder = None
        self.db = sqlite3.connect(Path(directory) / f'node-{identity}.sqlite3', check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
        row = self.db.execute('SELECT payload FROM state WHERE id=1').fetchone()
        self.state = json.loads(row[0]) if row else {'term': 0, 'seq': 0, 'bookings': {}}
        self.event('START', 'Replica opened its own durable database')

    def event(self, kind, detail):
        self.clock += 1
        self.events.append({'node': self.id, 'clock': self.clock, 'kind': kind,
                            'detail': detail, 'time': time.time()})

    def call(self, peer, path, data=None, timeout=1.0):
        self.messages += 1
        if data is not None:
            self.clock += 1
            data = dict(data, clock=self.clock)
        return rpc(peer, path, data, timeout=timeout)

    def persist(self, state):
        # Publish in memory only after SQLite has durably committed.
        self.db.execute('INSERT OR REPLACE INTO state VALUES (1, ?)', (json.dumps(state),))
        self.db.commit()
        self.state = state

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps(self.state))

    def status(self):
        # State objects are replaced, never modified after publication. Health
        # reporting must not wait behind a booking's network I/O and mutex.
        state = self.state
        return {'id': self.id, 'leader': self.leader, 'role': self.role,
                'term': state['term'], 'seq': state['seq'],
                'bookings': state['bookings'], 'clock': self.clock,
                'events': list(self.events.copy()), 'messages': self.messages,
                'attempts': self.attempts, 'conflicts': self.conflicts,
                'latencies': list(self.latencies.copy()), 'holder': self.holder}

    def elect_async(self):
        threading.Thread(target=self.elect, daemon=True).start()

    def elect(self):
        if not self.election_lock.acquire(blocking=False):
            return
        try:
            with self.lock:
                self.role = 'electing'
                self.leader = None
                self.event('ELECTION', 'Bully election: contacting higher numbered replicas')
            higher = []
            for peer in range(self.id + 1, 4):
                try:
                    response = self.call(peer, '/election', {'sender': self.id})
                    if response.get('ok'):
                        higher.append(peer)
                except (OSError, ValueError):
                    pass
            if higher:
                return
            # The highest reachable process recovers before becoming writable.
            states = [self.snapshot()]
            peers = []
            for peer in range(1, 4):
                if peer == self.id:
                    continue
                try:
                    states.append(self.call(peer, '/snapshot'))
                    peers.append(peer)
                except (OSError, ValueError):
                    pass
            if len(states) < 2:
                with self.lock:
                    self.role = 'no-quorum'
                    self.event('QUORUM', 'Two replicas are required before promotion')
                return
            with self.lock:
                recovered = max(states, key=lambda s: (s['term'], s['seq']))
                recovered = json.loads(json.dumps(recovered))
                recovered['term'] = max(s['term'] for s in states) + 1
                recovered['seq'] = 0
                self.persist(recovered)
                acks = 1
                for peer in peers:
                    try:
                        result = self.call(peer, '/replicate', {'state': recovered})
                        acks += bool(result.get('ok'))
                    except (OSError, ValueError):
                        pass
                if acks < 2:
                    self.role = 'no-quorum'
                    return
                self.leader = self.id
                self.role = 'leader'
                self.event('COORDINATOR', f'Node {self.id} elected after recovery from {len(states)} replicas')
                for peer in peers:
                    try:
                        self.call(peer, '/coordinator', {'leader': self.id, 'term': recovered['term']})
                    except (OSError, ValueError):
                        pass
        finally:
            self.election_lock.release()

    def monitor(self):
        time.sleep(0.7)
        while True:
            time.sleep(0.75)
            try:
                if self.leader is None:
                    self.elect()
                elif self.leader != self.id:
                    result = self.call(self.leader, '/health')
                    if result.get('role') != 'leader':
                        self.elect()
                else:
                    # A recovered higher-id process must reclaim leadership.
                    for peer in range(self.id + 1, 4):
                        try:
                            self.call(peer, '/health')
                            self.elect()
                            break
                        except OSError:
                            pass
            except (OSError, ValueError):
                with self.lock:
                    self.event('FAILURE', f'Heartbeat to coordinator {self.leader} failed')
                self.elect()

    def book(self, data):
        seat = data.get('seat')
        name = str(data.get('name', '')).strip()
        request_id = str(data.get('request_id', ''))
        if seat not in SEATS or not name or len(name) > 60 or not request_id or len(request_id) > 100:
            return {'ok': False, 'error': 'Provide a valid seat, name (1-60 characters), and request ID'}, 400
        if self.leader != self.id:
            if self.leader is None:
                return {'ok': False, 'error': 'Election in progress. Retry shortly.'}, 503
            try:
                return self.call(self.leader, '/book', data, timeout=12.0), 200
            except OSError:
                return {'ok': False, 'error': 'Coordinator unavailable. Retry with the same request ID.'}, 503
        started = time.perf_counter()
        # Centralized distributed mutual exclusion: every entry node forwards
        # to one elected coordinator. Its mutex protects the critical section.
        with self.lock:
            if self.role != 'leader' or self.leader != self.id:
                return {'ok': False, 'error': 'Leadership changed. Retry shortly.'}, 503
            self.attempts += 1
            for previous in self.state['bookings'].values():
                if previous['request_id'] == request_id:
                    if previous['seat'] != seat or previous['name'] != name:
                        return {'ok': False, 'error': 'Request ID already belongs to a different booking'}, 409
                    acks = 1
                    for peer in range(1, 4):
                        if peer == self.id:
                            continue
                        try:
                            result = self.call(peer, '/replicate', {'state': self.snapshot()})
                            acks += bool(result.get('ok'))
                        except OSError:
                            pass
                    if acks < 2:
                        return {'ok': False, 'uncertain': True,
                                'error': 'Cannot confirm without quorum. Retry with the same request ID.'}, 503
                    return {'ok': True, 'booking': previous, 'duplicate': True, 'copies': acks}, 200
            if seat in self.state['bookings']:
                self.conflicts += 1
                self.event('CONFLICT', f'{seat}: competing booking rejected')
                return {'ok': False, 'error': f'Seat {seat} is already booked', 'conflict': True}, 409
            live = []
            for peer in range(1, 4):
                if peer != self.id:
                    try:
                        health = self.call(peer, '/health')
                        if peer > self.id:
                            self.elect_async()
                            return {'ok': False, 'error': 'Higher priority replica returned. Election in progress.'}, 503
                        live.append(peer)
                    except OSError:
                        pass
            if not live:
                self.event('QUORUM', 'Booking rejected because only one replica is reachable')
                return {'ok': False, 'error': 'No quorum: at least two replicas must be online'}, 503
            self.holder = request_id
            self.event('LOCK', f'{seat}: coordinator granted exclusive access')
            try:
                # Short intentional delay makes critical-section contention measurable.
                time.sleep(0.055)
                proposed = self.snapshot()
                proposed['seq'] += 1
                booking = {'id': uuid.uuid4().hex[:10].upper(), 'seat': seat, 'name': name,
                           'request_id': request_id, 'via': data.get('via', self.id),
                           'coordinator': self.id, 'time': time.time()}
                proposed['bookings'][seat] = booking
                self.persist(proposed)
                acks = 1
                for peer in live:
                    try:
                        result = self.call(peer, '/replicate', {'state': proposed})
                        acks += bool(result.get('ok'))
                    except (OSError, ValueError):
                        pass
                if acks < 2:
                    self.event('UNCERTAIN', f'{seat}: insufficient durable acknowledgements')
                    return {'ok': False, 'uncertain': True,
                            'error': 'Outcome uncertain. Retry with the same request ID.'}, 503
                self.event('BOOKED', f'{seat} reserved for {name}; {acks}/3 durable copies')
                return {'ok': True, 'booking': booking, 'copies': acks}, 200
            finally:
                self.holder = None
                self.event('UNLOCK', f'{seat}: critical section released')
                self.latencies.append(round((time.perf_counter() - started) * 1000, 2))


def run(identity, directory):
    node = Node(identity, directory)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == '/health':
                reply(self, {'id': node.id, 'role': node.role, 'leader': node.leader})
            elif self.path == '/status':
                reply(self, node.status())
            elif self.path == '/snapshot':
                reply(self, node.snapshot())
            else:
                reply(self, {'error': 'Not found'}, 404)

        def do_POST(self):
            if self.headers.get('Origin'):
                return reply(self, {'ok': False, 'error': 'Use the dashboard gateway'}, 403)
            try:
                data = body(self)
                with node.lock:
                    node.clock = max(node.clock, int(data.get('clock', 0))) + 1
                if self.path == '/book':
                    result, status = node.book(data)
                    reply(self, result, status)
                elif self.path == '/election':
                    reply(self, {'ok': True, 'id': node.id})
                    if node.role == 'leader':
                        def catch_up():
                            with node.lock:
                                try:
                                    node.call(data['sender'], '/replicate', {'state': node.snapshot()})
                                    node.call(data['sender'], '/coordinator',
                                              {'leader': node.id, 'term': node.state['term']})
                                except OSError:
                                    pass
                        threading.Thread(target=catch_up, daemon=True).start()
                    else:
                        node.elect_async()
                elif self.path == '/coordinator':
                    with node.lock:
                        if data['term'] >= node.state['term']:
                            node.leader = data['leader']
                            node.role = 'follower'
                            node.event('FOLLOW', f'Following coordinator {node.leader}')
                    reply(self, {'ok': True})
                elif self.path == '/replicate':
                    with node.lock:
                        state = data['state']
                        if (state['term'], state['seq']) >= (node.state['term'], node.state['seq']):
                            node.persist(state)
                            node.event('REPLICATE', f'Durable snapshot {state["term"]}.{state["seq"]}')
                            reply(self, {'ok': True})
                        else:
                            reply(self, {'ok': False, 'error': 'Stale snapshot'}, 409)
                else:
                    reply(self, {'error': 'Not found'}, 404)
            except (ValueError, KeyError, TypeError):
                reply(self, {'ok': False, 'error': 'Invalid request'}, 400)

    threading.Thread(target=node.monitor, daemon=True).start()
    ThreadingHTTPServer(('127.0.0.1', BASE + identity), Handler).serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, choices=[1, 2, 3], required=True)
    parser.add_argument('--data', required=True)
    args = parser.parse_args()
    run(args.id, args.data)
