"""Regional live relay kernel + control plane (single machine, multi process).

Design
------
One RegionKernel per region. Multiple viewers in the same region share a
single upstream TCP connection to the source station.

Kernel state machine:
  IDLE        no upstream, no viewers
  CONNECTING  upstream start in progress (single-flight: concurrent joins
              await the same task, duplicate instances are impossible)
  PLAYING     encoded stream is flowing and fanned out to every viewer
  FAILED      upstream error; joins get an error response, retries run with
              exponential backoff
  RETAINING   last viewer left; the upstream is kept for --retain seconds,
              then torn down. A new join during the window reuses it.

Every viewer is an independent worker process with its own bounded queue.
A slow viewer drops frames at ITS queue only and can never block the others.
A new viewer receives SPS/PPS plus the next keyframe (a real decodable
boundary), never a random mid-GOP packet.
"""
import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from common import AccessUnitFramer, NAL_SPS, NAL_PPS, iter_nals


class Subscriber:
    def __init__(self, worker_id: str, region: str, slow: bool, capacity: int):
        self.worker_id = worker_id
        self.region = region
        self.slow = slow
        self.capacity = capacity
        self.queue = asyncio.Queue(maxsize=capacity)
        # Credit-based flow control. The worker sends one ACK per access unit
        # it has actually consumed; a frame may only be queued while a credit
        # is available. A slow worker therefore exhausts its credits, its
        # bounded queue fills and only that worker starts dropping frames.
        self.credits = asyncio.Semaphore(capacity)
        self.joined_at = time.time()
        self.proc = None
        self.need_keyframe = True
        self.init_sent = False
        self.dropped = 0
        self.last_report = {}

    def snapshot(self):
        r = self.last_report
        qsize = self.queue.qsize()
        return {
            "worker_id": self.worker_id,
            "slow": self.slow,
            "pid": self.proc.pid if self.proc and self.proc.returncode is None else None,
            "alive": bool(self.proc and self.proc.returncode is None),
            "joined_at": round(self.joined_at, 3),
            "queue_size": qsize,
            "queue_capacity": self.capacity,
            "queue_fill_pct": round(100 * qsize / self.capacity, 1),
            "frames_decoded": r.get("frames_decoded", 0),
            "au_received": r.get("au_received", 0),
            "bytes_received": r.get("bytes_received", 0),
            "dropped_for_this_viewer": self.dropped,
            "first_frame_ms": r.get("first_frame_ms"),
            "needs_keyframe": self.need_keyframe,
            "jpeg_b64": r.get("jpeg_b64"),
        }


class RegionKernel:
    def __init__(self, app, region: str, source_host: str, source_port: int,
                 retain_sec: float, queue_capacity: int):
        self.app = app
        self.region = region
        self.source_host = source_host
        self.source_port = source_port
        self.retain_sec = retain_sec
        self.queue_capacity = queue_capacity
        self.state = "IDLE"
        self.subscribers = {}
        self.framer = AccessUnitFramer()
        self.latest_keyframe = b""      # full keyframe access unit
        self.init_bytes = b""           # latest SPS/PPS
        self.au_sequence = 0
        self.upstream_bytes = 0
        self.upstream_started_at = None
        self.retain_deadline = None
        self.retry_count = 0
        self.last_error = ""
        self.total_upstreams = 0        # how many upstreams ever established
        self._connect_task = None      # single-flight guard
        self._reader = None
        self._writer = None
        self._retain_task = None
        self._stream_server = None
        self._stream_port = None
        self._state_changed = time.time()

    # ------------------------------------------------------------------ log
    def _set_state(self, new_state: str):
        if self.state != new_state:
            print(f"[relay:{self.region}] {self.state} -> {new_state}", flush=True)
            self.state = new_state
            self._state_changed = time.time()

    # ------------------------------------------------------------- upstream
    async def join(self, slow: bool = False) -> Subscriber:
        """Concurrent joins merge into one upstream start (single-flight)."""
        sub = Subscriber(uuid.uuid4().hex[:10], self.region, slow,
                         self.queue_capacity)
        self.subscribers[sub.worker_id] = sub

        if self._retain_task:
            self._retain_task.cancel()
            self._retain_task = None
            self.retain_deadline = None

        if self.state in ("IDLE", "FAILED"):
            try:
                await self._start_upstream()  # raises on failure
            except Exception:
                self.subscribers.pop(sub.worker_id, None)
                raise
        elif self.state == "CONNECTING":
            # Merge into the single in-flight start. If it fails the leader
            # moves us to FAILED; this waiter reports failure too.
            await self._connect_task
        elif self.state == "RETAINING":
            self._set_state("PLAYING")
        # PLAYING: keyframe delivery happens asynchronously on the stream task
        if self.state != "PLAYING":
            self.subscribers.pop(sub.worker_id, None)
            raise RuntimeError(f"upstream not playable (state={self.state}): "
                               f"{self.last_error}")
        await self._spawn_worker(sub)
        return sub

    async def _start_upstream(self):
        if self._connect_task is None or self._connect_task.done():
            self._connect_task = asyncio.create_task(self._connect())
        try:
            await self._connect_task
        finally:
            if self.state != "CONNECTING":
                self._connect_task = None

    async def _connect(self):
        self._set_state("CONNECTING")
        self.last_error = ""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.source_host, self.source_port),
                timeout=5.0)
        except (OSError, asyncio.TimeoutError) as exc:
            self.last_error = f"connect source failed: {exc!r}"
            self._set_state("FAILED")
            self._connect_task = None
            self._schedule_retry()
            raise RuntimeError(self.last_error)
        self._reader, self._writer = reader, writer
        self.total_upstreams += 1
        self.upstream_started_at = time.time()
        self.retry_count = 0
        self.framer = AccessUnitFramer()
        self.latest_keyframe = b""
        self._set_state("PLAYING")
        self._connect_task = None
        asyncio.create_task(self._pump_upstream())

    def _schedule_retry(self):
        self.retry_count += 1
        delay = min(1.0 * 2 ** min(self.retry_count - 1, 4), 15.0)
        print(f"[relay:{self.region}] retry {self.retry_count} in {delay}s",
              flush=True)
        asyncio.create_task(self._retry_later(delay))

    async def _retry_later(self, delay: float):
        await asyncio.sleep(delay)
        if not self.subscribers and self.state == "FAILED":
            # Nobody waited around: go fully idle instead of reconnecting.
            self._set_state("IDLE")
            return
        if self.state == "FAILED":
            try:
                await self._start_upstream()
            except RuntimeError:
                pass

    async def _pump_upstream(self):
        try:
            while True:
                chunk = await self._reader.read(131072)
                if not chunk:
                    raise ConnectionResetError("source closed connection")
                self.upstream_bytes += len(chunk)
                for is_key, has_init, au in self.framer.push(chunk):
                    self.au_sequence += 1
                    if has_init:
                        for nal in iter_nals(au):
                            if nal.nal_type in (NAL_SPS, NAL_PPS):
                                self.init_bytes += nal.data
                    if is_key:
                        self.latest_keyframe = au
                    await self._fanout(is_key, has_init, au)
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            print(f"[relay:{self.region}] upstream broken: {exc!r}", flush=True)
            self.last_error = repr(exc)
            self._teardown_upstream()
            if self.subscribers:
                self._set_state("FAILED")
                # Every viewer must resync at the next keyframe after recover.
                for sub in self.subscribers.values():
                    sub.need_keyframe = True
                    sub.init_sent = False
                self._schedule_retry()
            else:
                self._set_state("IDLE")

    async def _fanout(self, is_key: bool, has_init: bool, au: bytes):
        for sub in list(self.subscribers.values()):
            if sub.need_keyframe and not is_key:
                continue  # never start a viewer on a non-decodable packet
            payload_parts = []
            if sub.need_keyframe:
                # Decodable boundary: codec init then the keyframe itself.
                payload_parts.append(self.init_bytes or self.framer.init_bytes)
                payload_parts.append(self.latest_keyframe or au)
                sub.need_keyframe = False
                sub.init_sent = True
            elif has_init and is_key:
                payload_parts.append(au)
            else:
                payload_parts.append(au)
            payload = b"".join(payload_parts)
            # The bounded queue is the backpressure boundary. A full queue
            # means the worker (via its ACK credits) cannot keep up: drop for
            # THIS viewer only so it trends toward the newest picture while
            # every other subscriber keeps its full rate.
            if sub.queue.full():
                sub.dropped += 1
                continue
            sub.queue.put_nowait((self.au_sequence, payload))

    def _teardown_upstream(self):
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    # ----------------------------------------------------------- retention
    async def leave(self, worker_id: str):
        sub = self.subscribers.pop(worker_id, None)
        if sub and sub.proc and sub.proc.returncode is None:
            sub.proc.terminate()
            try:
                await asyncio.wait_for(sub.proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                sub.proc.kill()
        if not self.subscribers and self.state in ("PLAYING", "CONNECTING",
                                                   "FAILED"):
            self._set_state("RETAINING")
            self.retain_deadline = time.time() + self.retain_sec
            self._retain_task = asyncio.create_task(self._retain_wait())
        return bool(sub)

    async def _retain_wait(self):
        try:
            await asyncio.sleep(self.retain_sec)
        except asyncio.CancelledError:
            return
        if not self.subscribers:
            print(f"[relay:{self.region}] retain window expired, releasing "
                  f"upstream", flush=True)
            self._teardown_upstream()
            self.latest_keyframe = b""
            self.init_bytes = b""
            self._set_state("IDLE")
            self.retain_deadline = None
            self._retain_task = None

    # --------------------------------------------------------- worker spawn
    async def _ensure_stream_server(self):
        if self._stream_server is not None:
            return
        self._stream_server = await asyncio.start_server(
            self._handle_stream, "127.0.0.1", 0)
        self._stream_port = self._stream_server.sockets[0].getsockname()[1]

    async def _spawn_worker(self, sub: Subscriber):
        await self._ensure_stream_server()
        cmd = [sys.executable, "-u", "-I", str(HERE / "worker.py"),
               "--relay", f"127.0.0.1:{self.app.http_port}",
               "--region", self.region, "--worker-id", sub.worker_id,
               "--stream-port", str(self._stream_port)]
        if sub.slow:
            cmd.append("--slow")
        sub.proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        asyncio.create_task(self._watch_worker(sub))

    async def _watch_worker(self, sub: Subscriber):
        # Kernel-side liveness guard: a worker killed abruptly (SIGKILL, OOM)
        # cannot post its own exit, so waiting on the process lets the kernel
        # reap it and arm the retain timer anyway.
        rc = await sub.proc.wait()
        if sub.worker_id in self.subscribers:
            print(f"[relay:{self.region}] worker {sub.worker_id} exited "
                  f"rc={rc}, reaping", flush=True)
            await self.leave(sub.worker_id)

    async def _handle_stream(self, reader, writer):
        # The worker identifies itself first, then the kernel binds this
        # socket to exactly that Subscriber record.
        try:
            ident = (await asyncio.wait_for(reader.readline(), timeout=5)).decode().strip()
        except (asyncio.TimeoutError, UnicodeDecodeError):
            writer.close()
            return
        if not ident.startswith("WORKER "):
            writer.close()
            return
        worker_id = ident.split(" ", 1)[1]
        sub = self.subscribers.get(worker_id)
        if sub is None:
            writer.close()
            return
        writer.write(b"READY\n")
        await writer.drain()

        async def ack_reader():
            # The worker writes one fixed 8-byte ACK per consumed access unit.
            while True:
                try:
                    ack = await reader.readexactly(8)
                except (asyncio.IncompleteReadError, ConnectionResetError,
                        BrokenPipeError):
                    return
                if ack[:3] == b"ACK":
                    sub.credits.release()

        ack_task = asyncio.create_task(ack_reader())
        try:
            while True:
                seq, payload = await sub.queue.get()
                # Block here until the worker has ACKed a previously consumed
                # frame. A slow worker withholds ACKs, so this coroutine stops
                # draining the queue and the bounded fanout queue fills ->
                # frames are dropped only for this subscriber.
                await sub.credits.acquire()
                # Binary length-prefixed framing: 4-byte big-endian length,
                # dropped count, then the raw access unit. Avoids per-frame
                # base64 overhead on the hot path.
                header = len(payload).to_bytes(4, "big") + \
                    sub.dropped.to_bytes(4, "big")
                writer.write(header + payload)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            ack_task.cancel()
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # -------------------------------------------------------------- reports
    def report(self, payload: dict):
        sub = self.subscribers.get(payload["worker_id"])
        if sub is not None:
            # Heartbeats without a jpeg must not erase the last snapshot, so
            # merge instead of replacing.
            jpeg = payload.pop("jpeg", None)
            sub.last_report.update(payload)
            if jpeg:
                sub.last_report["jpeg_b64"] = jpeg

    def snapshot(self):
        retain_left = None
        if self.state == "RETAINING" and self.retain_deadline:
            retain_left = round(max(0, self.retain_deadline - time.time()), 2)
        return {
            "region": self.region,
            "state": self.state,
            "state_since": round(self._state_changed, 3),
            "source": f"{self.source_host}:{self.source_port}",
            "upstream_established": self.total_upstreams,
            "upstream_live": self.state in ("PLAYING", "RETAINING"),
            "upstream_bytes": self.upstream_bytes,
            "access_units": self.au_sequence,
            "retry_count": self.retry_count,
            "retain_seconds_left": retain_left,
            "last_error": self.last_error,
            "viewer_count": len(self.subscribers),
            "viewers": [s.snapshot() for s in self.subscribers.values()],
        }


class RelayApp:
    def __init__(self, config: dict, retain_sec: float, queue_capacity: int):
        self.config = config
        self.retain_sec = retain_sec
        self.queue_capacity = queue_capacity
        self.kernels = {}
        self.http_port = None
        self._http_server = None
        for region, src in config["regions"].items():
            self.kernels[region] = RegionKernel(
                self, region, src["host"], src["port"],
                retain_sec, queue_capacity)

    def kernel(self, region: str) -> RegionKernel:
        if region not in self.kernels:
            raise KeyError(region)
        return self.kernels[region]

    # ------------------------------------------------------------- HTTP API
    async def _handle_http(self, reader, writer):
        line = await reader.readline()
        if not line:
            writer.close()
            return
        try:
            method, path, _ = line.decode().split(" ", 2)
        except ValueError:
            writer.close()
            return
        length = 0
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            if h.lower().startswith(b"content-length:"):
                length = int(h.split(b":", 1)[1].strip())
        body = await reader.readexactly(length) if length else b""
        payload = json.loads(body) if body else {}
        try:
            try:
                status, resp = await self._route(method, path, payload)
            except _Raw as raw:
                writer.write(
                    (f"HTTP/1.1 {raw.status} OK\r\nContent-Type: {raw.ctype}\r\n"
                     f"Content-Length: {len(raw.body)}\r\n"
                     "Connection: close\r\n\r\n").encode() + raw.body)
                await writer.drain()
                writer.close()
                return
        except KeyError:
            status, resp = 404, {"error": "unknown region"}
        except Exception as exc:  # surface failures back to the control page
            status, resp = 500, {"error": repr(exc)}
        data = json.dumps(resp).encode()
        writer.write(
            f"HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode()
            + data)
        await writer.drain()
        writer.close()

    async def _route(self, method, path, payload):
        if method == "GET" and path == "/api/topology":
            return 200, {
                "timestamp": time.time(),
                "retain_sec": self.retain_sec,
                "queue_capacity": self.queue_capacity,
                "regions": [k.snapshot() for k in self.kernels.values()],
            }
        if method == "GET" and path == "/api/config":
            return 200, self.config
        if method == "POST" and path == "/api/subscribers":
            region = payload["region"]
            slow = bool(payload.get("slow", False))
            sub = await self.kernel(region).join(slow=slow)
            return 200, {"worker_id": sub.worker_id, "region": region,
                         "state": "PLAYING"}
        if method == "POST" and path == "/api/subscribers/stop":
            region = payload["region"]
            worker_id = payload["worker_id"]
            removed = await self.kernel(region).leave(worker_id)
            return 200 if removed else 404, {"stopped": removed}
        if method == "POST" and path == "/internal/worker-report":
            self.kernel(payload["region"]).report(payload)
            return 200, {"ok": True}
        if method == "POST" and path == "/internal/worker-exit":
            k = next((k for k in self.kernels.values()
                      if payload["worker_id"] in k.subscribers), None)
            if k:
                if payload.get("error"):
                    k.last_error = (f"worker {payload['worker_id']}: "
                                    f"{payload['error']}")
                # Reap a worker that exited on its own so its subscriber
                # record is released and the retain timer can arm.
                await k.leave(payload["worker_id"])
            return 200, {"ok": True}
        if method == "GET" and path in ("/", "/index.html"):
            page = (HERE / "web" / "index.html").read_bytes()
            raise _Raw(200, "text/html", page)
        return 404, {"error": "not found"}

    async def start_http(self, port: int):
        self.http_port = port
        self._http_server = await asyncio.start_server(
            self._handle_http, "127.0.0.1", port)

    async def serve_forever(self):
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        for k in self.kernels.values():
            for sub in list(k.subscribers.values()):
                await k.leave(sub.worker_id)


class _Raw(Exception):
    def __init__(self, status, ctype, body):
        self.status = status
        self.ctype = ctype
        self.body = body


async def amain():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(HERE / "config.json"))
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--retain", type=float, default=10.0,
                    help="seconds to keep the upstream after last viewer leaves")
    ap.add_argument("--queue-capacity", type=int, default=120)
    args = ap.parse_args()
    config = json.loads(Path(args.config).read_text())
    app = RelayApp(config, args.retain, args.queue_capacity)
    await app.start_http(args.port)
    print(f"[relay] control page: http://127.0.0.1:{args.port}/", flush=True)
    await app.serve_forever()


if __name__ == "__main__":
    asyncio.run(amain())
