"""Subscriber (viewer) worker process.

Connects to the relay's per-region stream port, receives access units that
already start on a decodable boundary, feeds them into an ffmpeg decoder and
keeps the latest decoded JPEG. Periodically posts a heartbeat with frame
metrics and (throttled) the JPEG snapshot back to the relay control plane, so
the web page can show each viewer's actual picture.
"""
import argparse
import asyncio
import base64
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

FFMPEG = str(Path(__file__).resolve().parent / "bin" / "ffmpeg")


def split_jpegs(buf: bytes):
    """Split an MJPEG byte stream into individual JPEG images."""
    frames = []
    while True:
        start = buf.find(b"\xff\xd8")
        if start < 0:
            return frames, b""
        end = buf.find(b"\xff\xd9", start + 2)
        if end < 0:
            return frames, buf[start:]
        frames.append(buf[start:end + 2])
        buf = buf[end + 2:]


class Worker:
    def __init__(self, relay_http: str, region: str, worker_id: str,
                 stream_port: int, slow: bool):
        self.relay_http = relay_http
        self.region = region
        self.worker_id = worker_id
        self.stream_port = stream_port
        self.slow = slow
        self.decoder = None
        self.latest_jpeg = b""
        self.first_frame_at = None
        self.started_at = time.time()
        self.au_received = 0
        self.bytes_received = 0
        self.frames_decoded = 0
        self.dropped_remotely = 0
        self._stop = asyncio.Event()
        self._jpeg_tail = b""

    async def post(self, path: str, payload: dict):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"http://{self.relay_http}{path}", data=data, method="POST",
            headers={"Content-Type": "application/json"})
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, urllib.request.urlopen, req)
        except Exception:
            pass

    async def run(self):
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1",
                                                           self.stream_port)
        except OSError as exc:
            await self.post("/internal/worker-exit",
                            {"worker_id": self.worker_id, "error": str(exc)})
            return
        writer.write(f"WORKER {self.worker_id}\n".encode())
        await writer.drain()
        ready = await reader.readline()
        self.decoder = await asyncio.create_subprocess_exec(
            FFMPEG, "-hide_banner", "-loglevel", "error",
            # A tiny probe window is required: this static build otherwise
            # keeps buffering a live (never-EOF) pipe and only flushes JPEGs
            # when stdin closes. 8 KiB is enough for SPS/PPS + first IDR.
            "-probesize", "8192", "-analyzeduration", "1",
            "-flags", "low_delay",
            "-f", "h264", "-framerate", "15", "-i", "pipe:0",
            # -vsync 0 stops the image muxer pacing frames by timestamps.
            # -fflags nobuffer must not be added: it zeroes pipe output here.
            "-vsync", "0", "-flush_packets", "1",
            "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "6", "pipe:1",
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL)
        await asyncio.gather(self._pump_stream(reader, writer),
                             self._read_frames(), self._heartbeat())
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        if self.decoder.returncode is None:
            self.decoder.kill()
            await self.decoder.wait()
        await self.post("/internal/worker-exit",
                        {"worker_id": self.worker_id, "error": ""})

    async def _read_exact(self, reader, n):
        data = b""
        while len(data) < n:
            part = await reader.read(n - len(data))
            if not part:
                raise ConnectionResetError("stream closed mid-frame")
            data += part
        return data

    async def _pump_stream(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter):
        # The READY handshake line was already consumed in run().
        while not self._stop.is_set():
            try:
                header = await self._read_exact(reader, 8)
            except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
                break
            length = int.from_bytes(header[:4], "big")
            dropped = int.from_bytes(header[4:8], "big")
            try:
                au = await self._read_exact(reader, length)
            except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
                break
            self.au_received += 1
            self.bytes_received += len(au)
            self.dropped_remotely = dropped
            try:
                self.decoder.stdin.write(au)
                # Deliberately slower than the 15 fps source so the relay's
                # per-viewer bounded queue fills and starts dropping only for
                # this consumer; other viewers keep their full rate.
                if self.slow:
                    await asyncio.sleep(0.5)
                await self.decoder.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                break
            # ACK only after the frame is actually consumed at our pace.
            try:
                writer.write(b"ACK" + self.worker_id[:5].encode())
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                break

    async def _read_frames(self):
        while not self._stop.is_set():
            buf = await self.decoder.stdout.read(65536)
            if not buf:
                break
            self._jpeg_tail += buf
            frames, self._jpeg_tail = split_jpegs(self._jpeg_tail)
            if frames:
                if self.first_frame_at is None:
                    self.first_frame_at = time.time()
                self.frames_decoded += len(frames)
                self.latest_jpeg = frames[-1]

    async def _heartbeat(self):
        last_jpeg_send = 0.0
        while not self._stop.is_set():
            await asyncio.sleep(0.5)
            now = time.time()
            include_jpeg = now - last_jpeg_send >= 1.0 and bool(self.latest_jpeg)
            payload = {
                "worker_id": self.worker_id,
                "region": self.region,
                "au_received": self.au_received,
                "bytes_received": self.bytes_received,
                "frames_decoded": self.frames_decoded,
                "dropped_remotely": self.dropped_remotely,
                "first_frame_ms": None if self.first_frame_at is None
                else round((self.first_frame_at - self.started_at) * 1000, 1),
                "jpeg": (base64.b64encode(self.latest_jpeg).decode()
                         if include_jpeg else None),
            }
            if include_jpeg:
                last_jpeg_send = now
            await self.post("/internal/worker-report", payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", required=True, help="relay control host:port")
    ap.add_argument("--region", required=True)
    ap.add_argument("--worker-id", required=True)
    ap.add_argument("--stream-port", type=int, required=True)
    ap.add_argument("--slow", action="store_true")
    args = ap.parse_args()

    worker = Worker(args.relay, args.region, args.worker_id,
                    args.stream_port, args.slow)

    def _term(*_):
        worker._stop.set()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
