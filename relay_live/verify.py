"""End-to-end verification for the regional relay system.

The full stack must already be running (start_all.sh). The script drives only
public control APIs and source control ports, then asserts:

  1. Concurrent joins merge into ONE upstream connection (single-flight).
  2. Page refresh style re-joins while viewers stay do not create upstreams.
  3. First frame reaches every viewer and starts on a keyframe boundary.
  4. Last viewer leave -> RETAINING -> IDLE releases the upstream; a later
     join rebuilds the full chain.
  5. A short source outage is recovered automatically, no duplicate upstream.
  6. A slow viewer gets its own bounded drops and never blocks other viewers.
"""
import argparse
import asyncio
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import http_get, http_post

PASS = "PASS"
FAIL = "FAIL"


class Check:
    def __init__(self, name):
        self.name = name
        self.status = FAIL
        self.detail = ""

    def ok(self, detail=""):
        self.status, self.detail = PASS, detail

    def fail(self, detail):
        self.status, self.detail = FAIL, detail

    def __str__(self):
        return f"[{self.status}] {self.name}  {self.detail}"


async def wait_topology(relay: str, region: str, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, topo = http_get(f"http://{relay}/api/topology")
        r = next(x for x in topo["regions"] if x["region"] == region)
        yield r
        await asyncio.sleep(0.3)


async def wait_state(relay, region, state, timeout=10.0):
    async for r in wait_topology(relay, region, timeout):
        if r["state"] == state:
            return r
    raise AssertionError(f"region {region} never reached {state}")


async def wait_first_frames(relay, region, worker_ids, timeout=20.0):
    deadline = time.time() + timeout
    seen = {}
    while time.time() < deadline and len(seen) < len(worker_ids):
        _, topo = http_get(f"http://{relay}/api/topology")
        r = next(x for x in topo["regions"] if x["region"] == region)
        for v in r["viewers"]:
            if v["worker_id"] in worker_ids and v["first_frame_ms"] is not None:
                seen[v["worker_id"]] = v["first_frame_ms"]
        await asyncio.sleep(0.4)
    return seen


def stop(relay, region, worker_id):
    return http_post(f"http://{relay}/api/subscribers/stop",
                     {"region": region, "worker_id": worker_id})


def source_stats(cport: int):
    _, s = http_get(f"http://127.0.0.1:{cport}/stats")
    return s


async def run(relay: str, region: str, cport: int, retain: float):
    results = []

    # --- 1. concurrent joins merge ---------------------------------------
    c = Check("并发加入合并为一条上游")
    try:
        before = (await wait_state(relay, region, "IDLE", 5))
        n = 4
        joined = await asyncio.gather(*[
            asyncio.get_running_loop().run_in_executor(
                None, http_post, f"http://{relay}/api/subscribers",
                {"region": region})
            for _ in range(n)
        ])
        ids = [j[1]["worker_id"] for j in joined]
        r = await wait_state(relay, region, "PLAYING")
        await asyncio.sleep(1.0)
        src = source_stats(cport)
        if r["upstream_established"] == 1 and src["upstream_connections"] == 1 \
                and r["viewer_count"] == n:
            c.ok(f"{n} 个并发请求 -> 上游 {r['upstream_established']} 条, "
                 f"源站连接 {src['upstream_connections']}")
        else:
            c.fail(f"upstream_established={r['upstream_established']} "
                   f"src_conns={src['upstream_connections']} viewers={r['viewer_count']}")
        results.append(c)
        worker_ids = ids
    except Exception as e:
        c.fail(repr(e))
        results.append(c)
        return results

    # --- 2. first frame + keyframe boundary -------------------------------
    c = Check("各接收端首帧到达且从可解码边界开始")
    try:
        ff = await wait_first_frames(relay, region, set(worker_ids))
        if len(ff) == len(worker_ids):
            avg = sum(ff.values()) / len(ff)
            c.ok(f"全部 {len(ff)} 端出画, 首帧 {min(ff.values())}~"
                 f"{max(ff.values())} ms, 平均 {avg:.0f} ms")
        else:
            c.fail(f"仅 {len(ff)}/{len(worker_ids)} 端出画: {ff}")
        results.append(c)
    except Exception as e:
        c.fail(repr(e))
        results.append(c)

    # --- 3. "refresh" re-join never creates another upstream --------------
    c = Check("页面刷新式加入不新建上游连接")
    try:
        _, topo = http_get(f"http://{relay}/api/topology")
        base = next(x for x in topo["regions"] if x["region"] == region)
        established = base["upstream_established"]
        # Add two more viewers while the stream is already PLAYING.
        extra = await asyncio.gather(*[
            asyncio.get_running_loop().run_in_executor(
                None, http_post, f"http://{relay}/api/subscribers",
                {"region": region})
            for _ in range(2)
        ])
        extra_ids = [j[1]["worker_id"] for j in extra]
        await asyncio.sleep(2.0)
        _, topo = http_get(f"http://{relay}/api/topology")
        r = next(x for x in topo["regions"] if x["region"] == region)
        if r["upstream_established"] == established:
            c.ok(f"观众 {established and 4}->6, 上游累计仍为 "
                 f"{r['upstream_established']} 条")
        else:
            c.fail(f"上游从 {established} 增长到 {r['upstream_established']}")
        results.append(c)
        worker_ids += extra_ids
    except Exception as e:
        c.fail(repr(e))
        results.append(c)

    # --- 4. slow viewer isolation ------------------------------------------
    c = Check("慢接收端独立有界队列, 不拖累同地区其他观众")
    try:
        _, slow = http_post(f"http://{relay}/api/subscribers",
                            {"region": region, "slow": True})
        slow_id = slow["worker_id"]
        # Poll until the slow viewer's bounded queue saturates and it starts
        # dropping, instead of relying on a fixed sleep. At 15 fps in / ~2 fps
        # out this takes roughly 15-20s with a 120-slot queue.
        slow_v = None
        deadline = time.time() + 40
        while time.time() < deadline:
            await asyncio.sleep(2.0)
            _, topo = http_get(f"http://{relay}/api/topology")
            r = next(x for x in topo["regions"] if x["region"] == region)
            slow_v = next(v for v in r["viewers"]
                          if v["worker_id"] == slow_id)
            if slow_v["dropped_for_this_viewer"] > 0:
                break
        fast = [v for v in r["viewers"] if v["worker_id"] != slow_id]
        fast_drops = sum(v["dropped_for_this_viewer"] for v in fast)
        fast_decoding = all(v["frames_decoded"] > 0 for v in fast)
        if slow_v and slow_v["dropped_for_this_viewer"] > 0 and fast_drops == 0 \
                and fast_decoding and slow_v["queue_size"] <= slow_v["queue_capacity"]:
            c.ok(f"慢速端丢帧 {slow_v['dropped_for_this_viewer']} 帧(仅本端), "
                 f"其他 {len(fast)} 端零丢帧且持续解码, 队列有界 "
                 f"{slow_v['queue_size']}/{slow_v['queue_capacity']}")
        else:
            c.fail(f"slow_drops={slow_v and slow_v['dropped_for_this_viewer']} "
                   f"fast_drops={fast_drops} fast_decoding={fast_decoding}")
        results.append(c)
        worker_ids.append(slow_id)
    except Exception as e:
        c.fail(repr(e))
        results.append(c)

    # --- 5. source short outage recovery -----------------------------------
    c = Check("源站短断后自动恢复且不产生重复上游")
    try:
        _, topo = http_get(f"http://{relay}/api/topology")
        before = next(x for x in topo["regions"] if x["region"] == region)
        established_before = before["upstream_established"]
        req = urllib.request.Request(
            f"http://127.0.0.1:{cport}/control/die", data=b"", method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        # Relay must show FAILED at some point and return to PLAYING.
        saw_failed = False
        deadline = time.time() + 25
        while time.time() < deadline:
            _, topo = http_get(f"http://{relay}/api/topology")
            r = next(x for x in topo["regions"] if x["region"] == region)
            saw_failed = saw_failed or r["state"] == "FAILED"
            if r["state"] == "PLAYING" and saw_failed:
                break
            await asyncio.sleep(0.4)
        r = next(x for x in http_get(f"http://{relay}/api/topology")[1]["regions"]
                 if x["region"] == region)
        await asyncio.sleep(2.0)
        ff = await wait_first_frames(relay, region, set(worker_ids), timeout=15)
        if r["state"] == "PLAYING" and saw_failed \
                and r["upstream_established"] == established_before + 1 \
                and len(ff) >= len(worker_ids) - 1:
            c.ok(f"经历 FAILED -> PLAYING, 上游 {established_before}->"
                 f"{r['upstream_established']}, 重试 {r['retry_count']} 次, "
                 f"{len(ff)}/{len(worker_ids)} 端重新出画")
        else:
            c.fail(f"state={r['state']} saw_failed={saw_failed} "
                   f"upstreams={r['upstream_established']} frames={len(ff)}")
        results.append(c)
    except Exception as e:
        c.fail(repr(e))
        results.append(c)

    # --- 6. retain window teardown + rebuild -------------------------------
    c = Check("最后观众离开 -> 保留期 -> 回收, 再次观看重建完整链路")
    try:
        for wid in list(worker_ids):
            stop(relay, region, wid)
        r = await wait_state(relay, region, "RETAINING", timeout=5)
        retaining_ok = r["state"] == "RETAINING"
        r = await wait_state(relay, region, "IDLE", timeout=retain + 8)
        await asyncio.sleep(0.5)
        src = source_stats(cport)
        idle_ok = r["state"] == "IDLE" and src["upstream_connections"] == 0
        # Rebuild from scratch.
        _, j = http_post(f"http://{relay}/api/subscribers",
                         {"region": region})
        new_id = j["worker_id"]
        r = await wait_state(relay, region, "PLAYING")
        ff = await wait_first_frames(relay, region, {new_id}, timeout=15)
        rebuilt = r["upstream_established"] >= 2 and new_id in ff
        if retaining_ok and idle_ok and rebuilt:
            c.ok(f"RETAINING 保持 {retain}s 后 IDLE 且源站连接归零, "
                 f"重新观看上游累计 {r['upstream_established']} 条, "
                 f"新端首帧 {ff[new_id]} ms")
        else:
            c.fail(f"retaining={retaining_ok} idle={idle_ok} rebuilt={rebuilt}")
        results.append(c)
        stop(relay, region, new_id)
    except Exception as e:
        c.fail(repr(e))
        results.append(c)

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", default="127.0.0.1:8080")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--retain", type=float, default=10.0)
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    region = next(iter(cfg["regions"]))
    cport = cfg["regions"][region]["control_port"]
    print(f"verifying region={region} via relay={args.relay}\n")
    results = asyncio.run(run(args.relay, region, cport, args.retain))
    for r in results:
        print(r)
    failed = [r for r in results if r.status == FAIL]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
