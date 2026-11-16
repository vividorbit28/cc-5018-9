"""Relay load benchmark: subscribe storm + slow viewers.

Drives the public control API only. A subscription storm joins N viewers in
parallel while the first M are flagged slow, holds them for a settle window,
samples /api/topology and every source /stats endpoint, then stops all viewers
and waits for the retain window to release the upstream.

Reports per-run numbers so a change to the shared-upstream path can be compared
against the previous run instead of a guessed threshold:

  * source upstream connections (peak and final)
  * relay upstream_established / upstream_bytes / access_units
  * per-viewer queue bytes (buffer copies) and copied_bytes
  * process-level CPU seconds and peak RSS for the relay kernel
  * first frame latency and drop counts for slow vs normal viewers

Exit status is non-zero if a hard invariant is violated (duplicate upstreams
in the same region, unbounded queues, a non-slow viewer dropping frames, or the
upstream surviving teardown), so a speed comparison can never hide an error
behind a default value.

Run with the full stack up:  python3 -I bench_relay.py
"""
import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import http_get, http_post


def read_proc(pid: int):
    """utime+stime seconds and RSS bytes for a pid, or None if gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    # comm may contain spaces; fields after the closing paren are positional.
    fields = stat[stat.rindex(")") + 2:].split()
    ticks = os.sysconf("SC_CLK_TCK")
    cpu = (int(fields[11]) + int(fields[12])) / ticks
    rss = 0
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            rss = int(line.split()[1]) * 1024
    return {"cpu_s": cpu, "rss_b": rss}


def proc_table():
    """pid -> (ppid, cpu seconds) for every visible process."""
    table = {}
    ticks = os.sysconf("SC_CLK_TCK")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        fields = stat[stat.rindex(")") + 2:].split()
        if len(fields) < 13:
            continue
        table[int(entry.name)] = (
            int(fields[1]),
            (int(fields[11]) + int(fields[12])) / ticks,
        )
    return table


def subtree_cpu(table, roots):
    """Sum CPU seconds of the given pids and all their descendants."""
    children = {}
    for pid, (ppid, _cpu) in table.items():
        children.setdefault(ppid, []).append(pid)
    total = 0.0
    seen = set()
    stack = [p for p in roots if p]
    while stack:
        pid = stack.pop()
        if pid in seen or pid not in table:
            continue
        seen.add(pid)
        total += table[pid][1]
        stack.extend(children.get(pid, []))
    return total


def worker_pids(relay, region):
    t = topology(relay)
    r = region_of(t, region)
    return [v["pid"] for v in r["viewers"] if v.get("pid")]


def relay_pid():
    pidfile = Path(__file__).resolve().parent / "run" / "relay.pid"
    try:
        return int(pidfile.read_text().strip())
    except (OSError, ValueError):
        return None


def source_stats(config):
    out = {}
    for region, src in config["regions"].items():
        try:
            _, s = http_get(f"http://127.0.0.1:{src['control_port']}/stats",
                            timeout=3)
            out[region] = s
        except Exception as exc:  # noqa: BLE001 - report, never mask
            out[region] = {"error": repr(exc)}
    return out


def topology(relay):
    _, t = http_get(f"http://{relay}/api/topology", timeout=5)
    return t


def region_of(topo, region):
    return next(r for r in topo["regions"] if r["region"] == region)


async def join_one(relay, region, slow):
    _, resp = await asyncio.get_running_loop().run_in_executor(
        None, http_post, f"http://{relay}/api/subscribers",
        {"region": region, "slow": slow})
    return resp["worker_id"]


def stop_one(relay, region, worker_id):
    return http_post(f"http://{relay}/api/subscribers/stop",
                     {"region": region, "worker_id": worker_id}, timeout=10)


async def sample(relay, region, cports, duration, interval=1.0):
    """Poll topology + source stats while viewers are attached."""
    samples = []
    deadline = time.time() + duration
    while time.time() < deadline:
        try:
            t = topology(relay)
            r = region_of(t, region)
            src = source_stats({"regions": {
                k: {"control_port": v} for k, v in cports.items()}})
        except Exception as exc:  # noqa: BLE001
            samples.append({"error": repr(exc)})
            await asyncio.sleep(interval)
            continue
        viewers = r["viewers"]
        samples.append({
            "t": time.time(),
            "state": r["state"],
            "upstream_established": r["upstream_established"],
            "upstream_bytes": r["upstream_bytes"],
            "access_units": r["access_units"],
            "copy_cost": r.get("copy_cost", {}),
            "cache_bytes": r.get("cache_bytes", {}),
            "viewer_count": r["viewer_count"],
            "queued_bytes_total": r.get("queued_bytes_total", 0),
            "copied_bytes_total": r.get("copied_bytes_total", 0),
            "source_conns": {k: v.get("upstream_connections")
                             for k, v in src.items()},
            "viewers": [{
                "worker_id": v["worker_id"],
                "slow": v["slow"],
                "queue_size": v["queue_size"],
                "queue_capacity": v["queue_capacity"],
                "queued_bytes": v.get("queued_bytes", 0),
                "frames_decoded": v["frames_decoded"],
                "au_received": v["au_received"],
                "dropped": v["dropped_for_this_viewer"],
                "first_frame_ms": v["first_frame_ms"],
            } for v in viewers],
        })
        await asyncio.sleep(interval)
    return samples


def summarize(samples, baseline_rss, baseline_cpu, proc_after, region,
              start_topo):
    ok = [s for s in samples if "error" not in s]
    if not ok:
        return {"error": "no topology samples collected"}
    last = ok[-1]
    # The kernel counters are monotonically cumulative since process start.
    # Subtract the pre-run snapshot so one run's numbers are not inflated by
    # earlier runs (a stale value must never be reported as this run's cost).
    base = start_topo or {}
    base_copy = base.get("copy_cost", {}) if base else {}
    peak_conns = max(
        (s["source_conns"].get(region) or 0) for s in ok)
    peak_queue = max(s["queued_bytes_total"] for s in ok)
    peak_cache = max((s.get("cache_bytes", {}).get("queued_bytes", 0)
                      + s.get("cache_bytes", {}).get("latest_keyframe", 0)
                      + s.get("cache_bytes", {}).get("init_bytes", 0))
                     for s in ok)
    peak_est = max(s["upstream_established"] for s in ok) - \
        base.get("upstream_established", 0)
    cost = last.get("copy_cost", {})
    if not cost:
        # Refuse to report zeroed copy cost for a relay that does not expose
        # the breakdown: a missing field must fail loudly, not look like a win.
        return {"error": "relay exposed no copy_cost breakdown; wrong or stale "
                         "relay process is answering"}
    cost_delta = {k: max(0, cost.get(k, 0) - base_copy.get(k, 0))
                  for k in ("parse_copy_bytes", "boundary_copy_bytes",
                            "wire_copy_bytes", "buffer_copy_bytes",
                            "total_copy_bytes")}
    wire = max(0, last["upstream_bytes"] - base.get("upstream_bytes", 0))
    if wire == 0:
        wire = sum(max(0, s["upstream_bytes"] - p["upstream_bytes"])
                   for p, s in zip(ok, ok[1:])) or last["upstream_bytes"] or 1
    cost_delta["wire_bytes"] = wire
    slow = [v for v in last["viewers"] if v["slow"]]
    fast = [v for v in last["viewers"] if not v["slow"]]
    all_viewers = last["viewers"]
    ff = [v["first_frame_ms"] for v in all_viewers
          if v["first_frame_ms"] is not None]
    out = {
        "samples": len(ok),
        "peak_source_connections": peak_conns,
        "peak_upstream_established": peak_est,
        "peak_queued_bytes": peak_queue,
        "peak_cache_bytes": peak_cache,
        "upstream_bytes": wire,
        "access_units": max(0, last["access_units"] -
                            base.get("access_units", 0)),
        "copied_bytes_total": last["copied_bytes_total"],
        "copy_cost": cost_delta,
        "copy_amplification": round(
            cost_delta["total_copy_bytes"] / max(1, wire), 3),
        "viewer_count": last["viewer_count"],
        "slow_viewer_drops": sum(v["dropped"] for v in slow),
        "fast_viewer_drops": sum(v["dropped"] for v in fast),
        "fast_viewers_decoding": sum(1 for v in fast if v["frames_decoded"] > 0),
        "fast_viewer_count": len(fast),
        "queue_capacities": sorted({v["queue_capacity"] for v in all_viewers}),
        "max_queue_size": max((v["queue_size"] for v in all_viewers), default=0),
        "first_frame_ms": {
            "min": min(ff) if ff else None,
            "median": statistics.median(ff) if ff else None,
            "max": max(ff) if ff else None,
        },
        "peak_rss_b": None,
        "cpu_s": None,
    }
    if proc_after and baseline_rss is not None:
        out["peak_rss_b"] = proc_after["rss_b"]
        out["cpu_s"] = round(proc_after["cpu_s"] - baseline_cpu, 3)
        out["rss_delta_b"] = proc_after["rss_b"] - baseline_rss
    return out


async def run(args):
    cfg = json.loads(Path(args.config).read_text())
    region = args.region or next(iter(cfg["regions"]))
    cports = {k: v["control_port"] for k, v in cfg["regions"].items()}
    relay = args.relay

    pid = relay_pid()
    base = read_proc(pid) if pid else None
    base_decode = subtree_cpu(proc_table(), worker_pids(relay, region))

    print(f"region={region} relay={relay} storm={args.subscribers} "
          f"slow={args.slow} hold={args.hold}s", flush=True)
    # Ensure a clean slate: an earlier viewer or retain window would make the
    # upstream counts ambiguous.
    t0 = topology(relay)
    r0 = region_of(t0, region)
    start_topo = r0
    if r0["viewer_count"] or r0["state"] not in ("IDLE",):
        print(f"NOTE: region starts in {r0['state']} with "
              f"{r0['viewer_count']} viewers", flush=True)

    storm_start = time.time()
    tasks = [join_one(relay, region, slow=(i < args.slow))
             for i in range(args.subscribers)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    ids = [r for r in results if isinstance(r, str)]
    join_elapsed = time.time() - storm_start
    if errors:
        print(f"JOIN ERRORS ({len(errors)}): {errors[:3]}", flush=True)

    try:
        r = region_of(topology(relay), region)
        if r["state"] != "PLAYING":
            state = r["state"]
            for _ in range(60):
                await asyncio.sleep(0.2)
                r = region_of(topology(relay), region)
                if r["state"] == "PLAYING":
                    break
    except Exception as exc:  # noqa: BLE001
        print(f"topology after storm failed: {exc!r}", flush=True)

    samples = await sample(relay, region, cports, args.hold,
                           interval=1.0)
    # Decode cost: relay's downstream workers plus their ffmpeg children. The
    # subtree sum is taken while the storm is still attached; baseline is the
    # same subtree before the storm so earlier runs do not inflate it.
    decode_during = subtree_cpu(proc_table(), worker_pids(relay, region))

    # Tear down and confirm the teardown invariants.
    for wid in ids:
        stop_one(relay, region, wid)
    final = None
    deadline = time.time() + args.retain + 15
    while time.time() < deadline:
        await asyncio.sleep(0.4)
        try:
            final = region_of(topology(relay), region)
        except Exception:  # noqa: BLE001
            continue
        if final["state"] == "IDLE":
            break
    proc_after = read_proc(pid) if pid else None
    src_final = source_stats(cfg)

    summary = summarize(samples, base and base["rss_b"],
                        base["cpu_s"] if base else 0.0, proc_after, region,
                        start_topo)
    summary.update({
        "region": region,
        "storm": args.subscribers,
        "slow": args.slow,
        "relay_pid": pid,
        "join_seconds": round(join_elapsed, 3),
        "join_errors": len(errors),
        "decode_cpu_s": round(decode_during - base_decode, 3),
        "relay_cpu_s": summary.get("cpu_s"),
        "final_state": final and final["state"],
        "final_source_conns": {k: v.get("upstream_connections")
                               for k, v in src_final.items()},
        "retained_upstream_after_idle": bool(
            final and final["state"] == "IDLE" and
            any((v.get("upstream_connections") or 0) > 0
                for v in src_final.values())),
    })
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--relay", default="127.0.0.1:8080")
    ap.add_argument("--config", default=str(Path(__file__).resolve().parent
                                           / "config.json"))
    ap.add_argument("--region", default=None)
    ap.add_argument("--subscribers", type=int, default=16)
    ap.add_argument("--slow", type=int, default=4,
                    help="how many of the first subscribers are slow")
    ap.add_argument("--hold", type=float, default=20.0,
                    help="seconds to keep the storm attached")
    ap.add_argument("--retain", type=float, default=10.0)
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    summary = asyncio.run(run(args))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2))

    # Hard invariants. These are explicit checks, not defaults: a regression
    # in shared-upstream behaviour fails the run instead of passing silently.
    problems = []
    if summary.get("error"):
        problems.append(summary["error"])
    if not summary.get("copy_cost", {}).get("total_copy_bytes"):
        problems.append("copy cost breakdown missing or zero from relay")
    if summary.get("upstream_bytes", 0) <= 1:
        problems.append(
            f"implausible upstream_bytes={summary.get('upstream_bytes')}; "
            f"wrong relay build or pre-run snapshot mismatch")
    if summary.get("peak_source_connections", 0) > 1:
        problems.append(
            f"duplicate upstreams: peak source connections "
            f"{summary['peak_source_connections']} > 1")
    caps = summary.get("queue_capacities") or []
    if summary.get("max_queue_size", 0) > max(caps, default=0):
        problems.append(
            f"unbounded queue: size {summary['max_queue_size']} > cap {caps}")
    if summary.get("fast_viewer_drops", 0) > 0:
        problems.append(
            f"normal viewers dropped {summary['fast_viewer_drops']} frames")
    if summary.get("retained_upstream_after_idle"):
        problems.append("upstream still connected after region reached IDLE")
    if summary.get("join_errors", 0) > 0:
        problems.append(f"{summary['join_errors']} join errors")
    if problems:
        print("\nBENCH_FAIL")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    print("\nBENCH_OK")


if __name__ == "__main__":
    main()
