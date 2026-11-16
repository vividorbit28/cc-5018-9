"""Lifecycle regression checks for the shared upstream.

Exercises the parts of the design that a throughput benchmark cannot see:

  1. Re-entrant join after full teardown gets a NEW generation: the stream is
     live, the viewer decodes, and no frame from the previous generation leaks.
  2. A viewer joining during the retain window reuses the SAME upstream (no
     new source connection) and still gets a decodable first frame.
  3. The last viewer leaving releases the upstream; the region reaches IDLE
     and the source reports zero connections. Re-entering then rebuilds.
  4. A slow viewer never blocks a normal one, and its queue stays bounded.

Requires the full stack running. Run: python3 -I test_lifecycle.py
"""
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import http_get, http_post

PASS, FAIL = "PASS", "FAIL"


def topo(relay):
    return http_get(f"http://{relay}/api/topology", timeout=5)[1]


def region(topo_doc, name):
    return next(r for r in topo_doc["regions"] if r["region"] == name)


def src(cport):
    return http_get(f"http://127.0.0.1:{cport}/stats", timeout=3)[1]


def join(relay, name, slow=False):
    return http_post(f"http://{relay}/api/subscribers",
                     {"region": name, "slow": slow}, timeout=15)[1]


def stop(relay, name, wid):
    return http_post(f"http://{relay}/api/subscribers/stop",
                     {"region": name, "worker_id": wid}, timeout=15)


async def wait_for(pred, timeout, step=0.25):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return last
        await asyncio.sleep(step)
    return last


async def run(relay, name, cport, retain):
    checks = []

    # -- 1. re-entry after teardown gives a new generation ----------------
    try:
        base_ok = await wait_for(
            lambda: region(topo(relay), name)["state"] == "IDLE", 15)
        j1 = join(relay, name)
        w1 = j1["worker_id"]
        playing = await wait_for(
            lambda: region(topo(relay), name)["state"] == "PLAYING", 15)
        ff = await wait_for(
            lambda: next((v["first_frame_ms"] for v in
                          region(topo(relay), name)["viewers"]
                          if v["worker_id"] == w1
                          and v["first_frame_ms"] is not None), None), 20)
        est1 = region(topo(relay), name)["upstream_established"]
        stop(relay, name, w1)
        idle = await wait_for(
            lambda: region(topo(relay), name)["state"] == "IDLE"
            and src(cport)["upstream_connections"] == 0, retain + 15)
        # Re-enter: must establish a fresh upstream and decode again.
        j2 = join(relay, name)
        w2 = j2["worker_id"]
        playing2 = await wait_for(
            lambda: region(topo(relay), name)["state"] == "PLAYING", 15)
        ff2 = await wait_for(
            lambda: next((v["first_frame_ms"] for v in
                          region(topo(relay), name)["viewers"]
                          if v["worker_id"] == w2
                          and v["first_frame_ms"] is not None), None), 20)
        est2 = region(topo(relay), name)["upstream_established"]
        ok = (base_ok and playing and ff and idle and playing2 and ff2
              and est2 == est1 + 1)
        checks.append((PASS if ok else FAIL, "回收后重入获得新代并重新出画",
                       f"est {est1}->{est2}, 首帧 gen1={ff} gen2={ff2}, "
                       f"idle_src0={bool(idle)}"))
        stop(relay, name, w2)
        await wait_for(lambda: region(topo(relay), name)["state"] == "IDLE",
                       retain + 15)
    except Exception as exc:  # noqa: BLE001
        checks.append((FAIL, "回收后重入获得新代并重新出画", repr(exc)))

    # -- 2. join during retain window reuses the same upstream ------------
    try:
        a = join(relay, name)
        await wait_for(lambda: region(topo(relay), name)["state"] == "PLAYING",
                       15)
        await wait_for(
            lambda: next(v["first_frame_ms"] for v in
                         region(topo(relay), name)["viewers"]
                         if v["worker_id"] == a["worker_id"]), 20)
        est_before = region(topo(relay), name)["upstream_established"]
        stop(relay, name, a["worker_id"])
        retaining = await wait_for(
            lambda: region(topo(relay), name)["state"] == "RETAINING", 10)
        b = join(relay, name)  # during the retain window
        reused = await wait_for(
            lambda: region(topo(relay), name)["state"] == "PLAYING", 10)
        ff = await wait_for(
            lambda: next((v["first_frame_ms"] for v in
                          region(topo(relay), name)["viewers"]
                          if v["worker_id"] == b["worker_id"]
                          and v["first_frame_ms"] is not None), None), 20)
        est_after = region(topo(relay), name)["upstream_established"]
        ok = bool(retaining and reused and ff and est_after == est_before)
        checks.append((PASS if ok else FAIL, "保留期内加入复用同一上游",
                       f"est 不变={est_after == est_before}, 首帧={ff}"))
        stop(relay, name, b["worker_id"])
        await wait_for(lambda: region(topo(relay), name)["state"] == "IDLE",
                       retain + 15)
    except Exception as exc:  # noqa: BLE001
        checks.append((FAIL, "保留期内加入复用同一上游", repr(exc)))

    # -- 3. slow viewer isolation with bounded queue ----------------------
    try:
        fast = [join(relay, name)["worker_id"] for _ in range(2)]
        slow = join(relay, name, slow=True)["worker_id"]
        await wait_for(lambda: region(topo(relay), name)["state"] == "PLAYING",
                       15)
        await wait_for(
            lambda: all(v["first_frame_ms"] is not None
                        for v in region(topo(relay), name)["viewers"]), 25)
        slow_drops = None
        deadline = time.time() + 45
        while time.time() < deadline:
            r = region(topo(relay), name)
            sv = next(v for v in r["viewers"] if v["worker_id"] == slow)
            if sv["dropped_for_this_viewer"] > 0:
                slow_drops = sv
                break
            await asyncio.sleep(1.5)
        r = region(topo(relay), name)
        fast_v = [v for v in r["viewers"] if v["worker_id"] in fast]
        fast_drops = sum(v["dropped_for_this_viewer"] for v in fast_v)
        fast_decoding = all(v["frames_decoded"] > 0 for v in fast_v)
        bounded = (slow_drops is not None
                   and slow_drops["queue_size"] <= slow_drops["queue_capacity"])
        ok = bounded and fast_drops == 0 and fast_decoding
        checks.append((PASS if ok else FAIL, "慢观众独立背压且有界",
                       f"慢端丢帧={slow_drops and slow_drops['dropped_for_this_viewer']}, "
                       f"队列={slow_drops and slow_drops['queue_size']}/"
                       f"{slow_drops and slow_drops['queue_capacity']}, "
                       f"快端丢帧={fast_drops}, 快端解码={fast_decoding}"))
        for wid in fast + [slow]:
            stop(relay, name, wid)
        await wait_for(lambda: region(topo(relay), name)["state"] == "IDLE",
                       retain + 15)
    except Exception as exc:  # noqa: BLE001
        checks.append((FAIL, "慢观众独立背压且有界", repr(exc)))

    return checks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", default="127.0.0.1:8080")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parent
                                           / "config.json"))
    ap.add_argument("--region", default=None)
    ap.add_argument("--retain", type=float, default=10.0)
    args = ap.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    name = args.region or next(iter(cfg["regions"]))
    cport = cfg["regions"][name]["control_port"]
    print(f"lifecycle region={name} relay={args.relay}\n")
    checks = asyncio.run(run(args.relay, name, cport, args.retain))
    for status, label, detail in checks:
        print(f"[{status}] {label}  {detail}")
    failed = [c for c in checks if c[0] == FAIL]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
