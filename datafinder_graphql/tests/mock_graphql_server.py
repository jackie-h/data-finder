import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

INSTRUMENTS = [
    {"sym": "AAPL", "price": 150.0},
    {"sym": "MSFT", "price": 300.0},
    {"sym": "GOOG", "price": 2800.0},
]

ACCOUNTS = [
    {"id": 1, "name": "Trading Account 1"},
    {"id": 2, "name": "Trading Account 2"},
]

_MOCK_DATA = {
    "instruments": INSTRUMENTS,
    "accounts": ACCOUNTS,
}


class _GraphQLHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query = body.get("query", "")

        response_data = {}
        for name, data in _MOCK_DATA.items():
            if name in query:
                response_data[name] = data
                break

        payload = json.dumps({"data": response_data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # type: ignore[override]
        pass


def start_mock_server(host: str = "127.0.0.1", port: int = 0) -> HTTPServer:
    """Start a mock GraphQL server in a background thread. Port 0 = OS-assigned."""
    server = HTTPServer((host, port), _GraphQLHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
