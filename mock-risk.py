"""Synthetic risk endpoint for the local Compose demonstration."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY = 64 * 1024


class Handler(BaseHTTPRequestHandler):
    def read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = bytearray()
            while True:
                line = self.rfile.readline(128)
                if not line.endswith(b"\r\n"):
                    raise ValueError("Invalid chunk header")
                size = int(line.split(b";", 1)[0].strip(), 16)
                if size < 0 or len(body) + size > MAX_BODY:
                    raise ValueError("Invalid chunk size")
                if size == 0:
                    return body
                body.extend(self.rfile.read(size))
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("Invalid chunk terminator")
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= MAX_BODY:
            raise ValueError("Invalid body size")
        return self.rfile.read(length)

    def do_POST(self):
        if self.path != "/evaluate":
            self.send_error(404)
            return
        try:
            amount = json.loads(self.read_body())["amountMinor"]
            if type(amount) is not int or amount <= 0:
                raise ValueError("Expected a positive integer amount")
        except (ValueError, KeyError, TypeError):
            self.send_error(400, "Invalid risk request")
            return
        result = json.dumps({"decision": "REVIEW" if amount >= 50_000 else "ALLOW"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(result)))
        self.end_headers()
        self.wfile.write(result)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
