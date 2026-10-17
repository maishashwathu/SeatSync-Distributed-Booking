"""Wire protocol shared by independent node and gateway processes."""
import json
import os
import urllib.request
import urllib.error

BASE = int(os.environ.get('TICKET_NODE_BASE', '18790'))
SEATS = [f'{row}{col}' for row in 'ABCDEF' for col in range(1, 9)]


def rpc(node, path, data=None, timeout=1.0):
    payload = json.dumps(data).encode() if data is not None else None
    request = urllib.request.Request(f'http://127.0.0.1:{BASE + node}{path}',
                                     data=payload, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return json.load(error)


def reply(handler, data, status=200):
    payload = json.dumps(data).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Cache-Control', 'no-store')
    handler.send_header('Content-Length', str(len(payload)))
    handler.end_headers()
    try:
        handler.wfile.write(payload)
    except (BrokenPipeError, ConnectionResetError):
        pass


def body(handler):
    length = int(handler.headers.get('Content-Length', 0))
    if length > 65536:
        raise ValueError('Request too large')
    return json.loads(handler.rfile.read(length) or b'{}')
