"""Tiny local stand-in for API Gateway: turns HTTP GETs into HTTP-API (v2) events,
calls reader.handler.lambda_handler and returns its response with CORS headers.

    python local_server.py [port]        # default 8000
"""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from reader.handler import lambda_handler

ROUTES = {
    ("cities",): ("GET /cities", None),
    ("locations",): ("GET /locations", None),
}
PARAM_ROUTES = {
    "cities": ("GET /cities/{city}", "city"),
    "locations": ("GET /locations/{location_id}", "location_id"),
}


def build_event(raw_path, raw_query):
    parts = [p for p in raw_path.split("/") if p]  # kept URL-encoded, like API Gateway
    route_key, path_params = f"GET {raw_path}", None
    if tuple(parts) in ROUTES:
        route_key = ROUTES[tuple(parts)][0]
    elif len(parts) == 2 and parts[0] in PARAM_ROUTES:
        route_key, name = PARAM_ROUTES[parts[0]]
        path_params = {name: parts[1]}
    query = {k: v[-1] for k, v in parse_qs(raw_query).items()} or None
    return {
        "routeKey": route_key,
        "rawPath": raw_path,
        "pathParameters": path_params,
        "queryStringParameters": query,
    }


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        url = urlparse(self.path)
        result = lambda_handler(build_event(url.path, url.query), None)
        body = result["body"].encode()
        self.send_response(result["statusCode"])
        for k, v in result["headers"].items():
            self.send_header(k, v)
        self._cors()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print(f"Local API on http://localhost:{port}  (try /cities)")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()