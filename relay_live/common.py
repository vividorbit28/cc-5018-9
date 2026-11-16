"""Shared helpers: Annex-B H.264 framing and tiny JSON HTTP client.

Only the Python standard library is used.
"""
import json
import socket
import urllib.request
from dataclasses import dataclass

NAL_SLICE = 1
NAL_DPA = 2
NAL_DPB = 3
NAL_DPC = 4
NAL_IDR = 5
NAL_SEI = 6
NAL_SPS = 7
NAL_PPS = 8
NAL_AUD = 9


@dataclass
class NALUnit:
    nal_type: int
    data: bytes  # includes the start code prefix

    @property
    def is_keyframe_slice(self) -> bool:
        return self.nal_type in (NAL_IDR, NAL_DPA)

    @property
    def is_vcl(self) -> bool:
        return self.nal_type <= 5


def iter_nals(buf: bytes):
    """Split an Annex-B byte buffer into NAL units.

    Handles 00 00 01 and 00 00 00 01 start codes and emulation prevention
    bytes are left untouched (we only inspect the header byte).
    """
    n = len(buf)
    i = 0
    starts = []
    while i < n - 3:
        if buf[i] == 0 and buf[i + 1] == 0:
            if buf[i + 2] == 1:
                starts.append((i, 3))
                i += 3
                continue
            if i < n - 3 and buf[i + 2] == 0 and buf[i + 3] == 1:
                starts.append((i, 4))
                i += 4
                continue
        i += 1
    for idx, (pos, plen) in enumerate(starts):
        end = starts[idx + 1][0] if idx + 1 < len(starts) else n
        nal = buf[pos + plen:end]
        if nal:
            yield NALUnit(nal[0] & 0x1F, buf[pos:end])


class AccessUnitFramer:
    """Reassembles an H.264 byte stream into access units (one picture each).

    A new access unit begins at an AUD, or at a VCL NAL that follows another
    VCL NAL (libx264 emits one slice per picture). Parameter sets are kept
    with the following keyframe so new subscribers always receive a
    decodable boundary.
    """

    def __init__(self):
        self._units = []
        self._vcl_in_unit = False
        self._init_bytes = b""  # latest SPS/PPS seen on the stream

    def push(self, chunk: bytes):
        out = []
        for nal in iter_nals(chunk):
            if nal.nal_type in (NAL_SPS, NAL_PPS):
                self._init_bytes += nal.data
            # An access unit boundary is an AUD, or a VCL NAL when the unit
            # already buffered a VCL NAL (libx264 emits one slice per picture).
            # Parameter sets / SEI after a slice belong to the NEXT picture, so
            # they only flush once the next VCL NAL actually arrives.
            boundary = nal.nal_type == NAL_AUD or (
                nal.is_vcl and self._vcl_in_unit)
            if boundary and self._units:
                au = b"".join(u.data for u in self._units)
                key = any(u.is_keyframe_slice for u in self._units)
                has_init = any(u.nal_type in (NAL_SPS, NAL_PPS)
                               for u in self._units)
                out.append((key, has_init, au))
                self._units = []
                self._vcl_in_unit = False
            if nal.nal_type != NAL_AUD:
                self._units.append(nal)
                if nal.is_vcl:
                    self._vcl_in_unit = True
        return out

    @property
    def init_bytes(self) -> bytes:
        return self._init_bytes


def http_get(url: str, timeout: float = 5.0):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def http_post(url: str, payload: dict, timeout: float = 5.0):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
