"""Local web gateway and process supervisor. Python standard library only."""
import argparse
import atexit
from concurrent.futures import ThreadPoolExecutor
import json
import mimetypes
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from common import BASE, SEATS, body, reply, rpc

ROOT = Path(__file__).resolve().parent


class Cluster:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.processes = {}
        self.logs = {}
        self.control = threading.Lock()
        self.demo_lock = threading.Lock()
        self.last_race = None
        self.last_failover = None

    def start(self, node):
        process = self.processes.get(node)
        if process and process.poll() is None:
            return
        with socket.socket() as probe:
            if probe.connect_ex(('127.0.0.1', BASE + node)) == 0:
                raise ValueError(f'Port {BASE + node} is already occupied. Close the other demo first.')
        if node in self.logs:
            self.logs[node].close()
        self.logs[node] = open(self.directory / f'node-{node}.log', 'a', encoding='utf-8')
        self.processes[node] = subprocess.Popen(
            [sys.executable, str(ROOT / 'node.py'), '--id', str(node), '--data', str(self.directory)],
            cwd=ROOT, stdout=self.logs[node], stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))

    def stop(self, node):
        process = self.processes.get(node)
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    def close(self):
        for node in list(self.processes):
            self.stop(node)
        for log in self.logs.values():
            log.close()

    def statuses(self):
        def status(node):
            try:
                return dict(rpc(node, '/status', timeout=0.6), online=True, port=BASE + node)
            except (OSError, ValueError):
                return {'id': node, 'online': False, 'role': 'offline', 'events': [], 'port': BASE + node}
        with ThreadPoolExecutor(max_workers=3) as pool:
            return list(pool.map(status, [1, 2, 3]))

    def state(self):
        nodes = self.statuses()
        online = [n for n in nodes if n['online']]
        leaders = [n for n in online if n['role'] == 'leader' and n['leader'] == n['id']]
        source = max(online, key=lambda n: (n['term'], n['seq'])) if online else {}
        bookings = source.get('bookings', {})
        events = sorted([e for n in online for e in n['events']], key=lambda e: e['time'], reverse=True)[:60]
        latencies = [v for n in online for v in n.get('latencies', [])]
        return {'nodes': nodes, 'leader': leaders[0]['id'] if len(leaders) == 1 else None,
                'seats': SEATS, 'bookings': bookings, 'events': events,
                'quorum': len(online) >= 2, 'last_race': self.last_race,
                'last_failover': self.last_failover,
                'metrics': {'messages': sum(n.get('messages', 0) for n in online),
                            'attempts': sum(n.get('attempts', 0) for n in online),
                            'conflicts': sum(n.get('conflicts', 0) for n in online),
                            'latency_ms': round(sum(latencies) / len(latencies), 1) if latencies else 0}}

    def wait_ready(self, timeout=12, expected=None):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.state()
            online = [n for n in state['nodes'] if n['online']]
            settled = all(n.get('leader') == state['leader'] and n['role'] in ('leader', 'follower') for n in online)
            if state['leader'] is not None and state['quorum'] and settled and (expected is None or state['leader'] == expected):
                return state
            time.sleep(0.15)
        raise ValueError('Cluster has not reached a writable quorum yet. Start at least two nodes and retry.')

    def book(self, data):
        node = int(data.get('node', 1))
        if node not in (1, 2, 3):
            raise ValueError('Choose node 1, 2, or 3')
        payload = dict(data, via=node)
        try:
            return rpc(node, '/book', payload, timeout=12)
        except OSError:
            return {'ok': False, 'error': f'Node {node} is offline. Choose another entry node.'}

    def race(self, seat):
        if seat not in SEATS:
            raise ValueError('Invalid seat')
        state = self.wait_ready()
        if seat in state['bookings']:
            raise ValueError('Choose an available seat for the race')
        online = [n['id'] for n in state['nodes'] if n['online']]
        barrier = threading.Barrier(12)
        def contender(index):
            barrier.wait(timeout=5)
            started = time.perf_counter()
            result = self.book({'seat': seat, 'name': f'Race guest {index + 1}',
                                'node': online[index % len(online)], 'request_id': uuid.uuid4().hex})
            return {'guest': index + 1, 'node': online[index % len(online)],
                    'latency_ms': round((time.perf_counter() - started) * 1000, 2), **result}
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(contender, range(12)))
        self.last_race = {'seat': seat, 'requests': 12,
                          'successes': sum(bool(r.get('ok')) for r in results),
                          'conflicts': sum(bool(r.get('conflict')) for r in results),
                          'errors': sum(not r.get('ok') and not r.get('conflict') for r in results),
                          'average_ms': round(sum(r['latency_ms'] for r in results) / 12, 1),
                          'results': results, 'time': time.time()}
        return {'ok': True, 'race': self.last_race}

    def failover(self):
        state = self.wait_ready()
        if sum(n['online'] for n in state['nodes']) < 3:
            raise ValueError('Start all three nodes before running the failover experiment')
        old = state['leader']
        started = time.perf_counter()
        self.stop(old)
        state = self.wait_ready()
        self.last_failover = {'old_leader': old, 'new_leader': state['leader'],
                              'duration_ms': round((time.perf_counter() - started) * 1000, 1),
                              'bookings_preserved': len(state['bookings']), 'time': time.time()}
        return {'ok': True, 'failover': self.last_failover}


def serve(port, directory):
    cluster = Cluster(directory)
    atexit.register(cluster.close)
    # Fail before spawning workers if the dashboard port is already in use.
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == '/api/state':
                return reply(self, cluster.state())
            if self.path == '/api/export':
                return reply(self, cluster.state())
            path = self.path.split('?')[0]
            allowed = {'/': 'index.html', '/app.js': 'app.js', '/style.css': 'style.css'}
            if path not in allowed:
                return reply(self, {'error': 'Not found'}, 404)
            content = (ROOT / 'web' / allowed[path]).read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', mimetypes.guess_type(allowed[path])[0] or 'text/plain')
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(content)

        def do_POST(self):
            # Reject browser cross-origin writes to the local process supervisor.
            origin = self.headers.get('Origin')
            if origin and origin not in (f'http://127.0.0.1:{port}', f'http://localhost:{port}'):
                return reply(self, {'ok': False, 'error': 'Origin rejected'}, 403)
            try:
                data = body(self)
                if self.path == '/api/book':
                    return reply(self, cluster.book(data))
                if not cluster.demo_lock.acquire(blocking=False):
                    return reply(self, {'ok': False, 'error': 'Another cluster experiment is running'}, 409)
                try:
                    with cluster.control:
                        if self.path == '/api/race':
                            result = cluster.race(data.get('seat'))
                        elif self.path == '/api/failover':
                            result = cluster.failover()
                        elif self.path in ('/api/start', '/api/stop'):
                            node = int(data['node'])
                            if node not in (1, 2, 3):
                                raise ValueError('Invalid node')
                            if self.path == '/api/start':
                                cluster.start(node)
                            else:
                                cluster.stop(node)
                            result = {'ok': True}
                        elif self.path == '/api/reset':
                            for node in (1, 2, 3):
                                cluster.stop(node)
                            # Only these application-owned database files are removed.
                            for node in (1, 2, 3):
                                for suffix in ('', '-wal', '-shm'):
                                    (cluster.directory / f'node-{node}.sqlite3{suffix}').unlink(missing_ok=True)
                            cluster.last_race = cluster.last_failover = None
                            for node in (1, 2, 3):
                                cluster.start(node)
                            cluster.wait_ready(expected=3)
                            result = {'ok': True}
                        else:
                            return reply(self, {'ok': False, 'error': 'Not found'}, 404)
                    reply(self, result)
                finally:
                    cluster.demo_lock.release()
            except (ValueError, KeyError, TypeError) as error:
                reply(self, {'ok': False, 'error': str(error)}, 400)

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    try:
        for node in (1, 2, 3):
            cluster.start(node)
        cluster.wait_ready(expected=3)
        print(f'\n  SeatSync is ready at http://127.0.0.1:{port}\n  Press Ctrl+C to stop all processes.\n', flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        cluster.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SeatSync local distributed booking demo')
    parser.add_argument('--port', type=int, default=18780)
    parser.add_argument('--data', default=str(ROOT / '.runtime'))
    args = parser.parse_args()
    serve(args.port, args.data)
