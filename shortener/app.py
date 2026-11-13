"""HTTP API as a plain ASGI app (no framework). Run with: uvicorn shortener.app:app"""
from __future__ import annotations

import json
import os

from .service import AliasTaken, InvalidUrl, NotFound, Shortener


def json_response(status: int, payload: dict, extra_headers: list | None = None):
    body = json.dumps(payload).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    return status, headers + (extra_headers or []), body


def create_app(service: Shortener, base_url: str = "http://localhost:8000"):
    async def read_body(receive, limit: int = 16_384) -> bytes:
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if len(body) > limit:
                raise ValueError("body too large")
            if not message.get("more_body"):
                return body

    async def handle(scope, receive):
        method, path = scope["method"], scope["path"]
        if path == "/health":
            return json_response(200, {"status": "ok", "cache_hit_rate": round(service.cache.hit_rate, 4)})
        if path == "/shorten" and method == "POST":
            try:
                data = json.loads(await read_body(receive) or b"{}")
                code = service.shorten(data["url"], data.get("alias"), data.get("ttl_seconds"))
            except AliasTaken:
                return json_response(409, {"error": "alias_taken"})
            except (InvalidUrl, KeyError, ValueError, TypeError) as exc:
                return json_response(400, {"error": "invalid_request", "detail": str(exc)})
            return json_response(201, {"code": code, "short_url": f"{base_url}/{code}"})
        if path.startswith("/stats/") and method == "GET":
            try:
                return json_response(200, service.stats(path[len("/stats/"):]))
            except NotFound:
                return json_response(404, {"error": "not_found"})
        if method in ("GET", "HEAD") and path.count("/") == 1 and len(path) > 1:
            headers = dict(scope.get("headers", []))
            try:
                url = service.resolve(path[1:], headers.get(b"referer", b"").decode("latin-1"))
            except NotFound:
                return json_response(404, {"error": "not_found"})
            # 302, not 301: browsers cache 301s forever, which would hide later clicks from analytics.
            return 302, [(b"location", url.encode()), (b"cache-control", b"no-store"), (b"content-length", b"0")], b""
        return json_response(404, {"error": "not_found"})

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        status, headers, body = await handle(scope, receive)
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    return app


def _default_service() -> Shortener:
    data_dir = os.environ.get("DATA_DIR", "data")
    os.makedirs(data_dir, exist_ok=True)
    shards = int(os.environ.get("SHARDS", "4"))
    return Shortener({f"shard-{i}": os.path.join(data_dir, f"shard-{i}.db") for i in range(shards)},
                     node_id=int(os.environ.get("NODE_ID", "0")))


def __getattr__(name: str):
    # Build the default app lazily so importing this module in tests creates no files.
    if name == "app":
        globals()["app"] = create_app(_default_service(), os.environ.get("BASE_URL", "http://localhost:8000"))
        return globals()["app"]
    raise AttributeError(name)
