"""Off-head HTTP test gates and attempt recorder; never reads task outputs.

A single support-VM process serializes attempt registration. File evidence is
written locally, but task release decisions never poll NFS metadata.
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import time

from evidence import write_json


def main(config):
    folder = Path(config['folder'])
    released, attempts, registrations = set(), {}, {}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 8192:
                    raise ValueError('Invalid request size')
                value = json.loads(self.rfile.read(size))
                index = value.get('index', 0)
                if type(index) is not int or not 0 <= index < config['tasks']:
                    raise ValueError('Invalid task index')
                with lock:
                    if self.path == '/health':
                        reply = dict(ok=True)
                    elif self.path == '/start':
                        token = value['token']
                        if (not isinstance(token, str) or len(token) != 32 or
                                any(c not in '0123456789abcdef' for c in token)):
                            raise ValueError('Invalid attempt token')
                        if token not in registrations:
                            attempt = attempts.get(index, 0)
                            entry = dict(index=index, pid=value['pid'], node=value['node'],
                                         replay=attempt > 0, attempt=attempt, unix_s=time.time())
                            write_json(folder / f'start-{index}-{token}.json', entry)
                            registrations[token] = entry
                            attempts[index] = attempt + 1
                        reply = registrations[token]
                        if reply['index'] != index:
                            raise ValueError('Attempt/index mismatch')
                    elif self.path == '/released':
                        reply = dict(released=index in released)
                    elif self.path == '/release':
                        write_json(folder / f'release-{index}.json', dict(unix_s=time.time()))
                        released.add(index)
                        reply = dict(released=True)
                    elif self.path == '/finish':
                        token = value['token']
                        entry = registrations[token]
                        if entry['index'] != index:
                            raise ValueError('Attempt/index mismatch')
                        write_json(folder / f'finish-{index}-{token}.json', dict(
                            index=index, replay=entry['replay'], unix_s=time.time()))
                        reply = dict(ok=True)
                    else:
                        raise ValueError('Unknown gate operation')
                body = json.dumps(reply).encode()
                self.send_response(200)
            except Exception as exc:
                body = json.dumps(dict(error=str(exc))).encode()
                self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    class Server(ThreadingHTTPServer):
        request_queue_size = 128  # All original tasks register together.

    Server((config['support_ip'], 8765), Handler).serve_forever()


def request(url, action, **value):
    from urllib.request import Request
    # No environment HTTP proxy for private experiment traffic.
    from urllib.request import ProxyHandler, build_opener
    req = Request(url+'/'+action, json.dumps(value).encode(),
                  headers={'Content-Type': 'application/json'}, method='POST')
    with build_opener(ProxyHandler({})).open(req, timeout=5) as response:
        return json.load(response)


if __name__ == '__main__':
    main(json.loads(Path(sys.argv[1]).read_text()))
