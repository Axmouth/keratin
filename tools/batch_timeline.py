"""Extract measured intervals. Absolute timestamps stay integers until rebasing.
Worker groups use fsync_member parent IDs. Inline groups use replicated_done.
Staging-to-durability assignment uses same-writer offset coverage and time order.
No inferred transport, coalescing, actor or client-delivery spans are emitted.
"""

import argparse
import glob
import json
import sys
from pathlib import Path


def extract(config, base, allow_open=False):
    all_cases = []
    audit = []
    if config.get("clock") != "same-host":
        raise ValueError(
            "Only same-host captures are supported. Cross-host clock alignment needs separate evidence."
        )
    nodes = config["nodes"]
    payload_log = config.get("payload_log", "messages")
    log_kinds = config.get("logs", ["messages", "events"])
    if not log_kinds or len(set(log_kinds)) != len(log_kinds) or any(
        kind not in ("messages", "events") for kind in log_kinds
    ) or payload_log not in log_kinds:
        raise ValueError("logs must contain unique known lanes including payload_log")
    combined = payload_log == "events"

    if not nodes or not config["windows"] or not config["cases"]:
        raise ValueError("nodes, windows and cases must be nonempty")
    for case in config["cases"]:
        name = case["id"]
        paths = sorted(Path(p) for p in glob.glob(str(base / case["traces"])))
        if len(paths) != len(nodes):
            raise ValueError(
                f"{name}: expected {len(nodes)} process captures, found {len(paths)}"
            )
        writers = {}
        captures = []
        for path in paths:
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            headers = [row for row in rows if row.get("type") == "process"]
            if len(headers) != 1 or headers[0].get("version") != 1:
                raise ValueError(f"{path}: expected one version-1 process header")
            process = headers[0]
            anchor = int(process["unix_ns"])
            summaries = [r for r in rows if r.get("type") == "summary"]
            if not summaries:
                raise ValueError(f"{path}: missing capture summary")
            summary = summaries[-1]
            if not summary.get("closed"):
                if not allow_open:
                    raise ValueError(
                        f"{path}: missing close marker. Use --allow-open-capture for bounded diagnostic windows."
                    )
                print(
                    f"WARNING: {path.name}: capture has no final close marker",
                    file=sys.stderr,
                )
            if summary["dropped"] != 0:
                raise ValueError(f"{path}: dropped trace events")
            if summary["emitted"] != sum(
                "stage" in r or r.get("type") == "writer" for r in rows
            ):
                raise ValueError(f"{path}: emission count mismatch")
            captures.append({"file": path.name, "summary": summary})
            for w in [
                r
                for r in rows
                if r.get("type") == "writer"
                and "/" + config["topic"] + "/" in r["path"]
            ]:
                node = int(w["path"].split("/node-")[1].split("/")[0])
                kind = w["path"].split("/data/")[1].split("/")[0]
                if kind not in log_kinds:
                    continue
                events = []
                for r in rows:
                    if "stage" in r and r["writer"] == w["writer"]:
                        events.append(
                            dict(r, s=anchor + r["start_ns"], e=anchor + r["end_ns"])
                        )
                staged = [
                    e for e in events if e["stage"] in ("stage", "replicated_stage")
                ]
                if not staged:
                    continue
                if not 0 <= node < len(nodes):
                    raise ValueError(f"{path}: unexpected node {node}")
                if (node, kind) in writers:
                    raise ValueError(
                        "Multiple active writers require explicit incarnation handling"
                    )
                if any(e["stage"].endswith("_error") for e in events):
                    raise ValueError(
                        f"{path}: failed operations require explicit recovery handling"
                    )
                writers[node, kind] = events
        first_window = min(window["first"] for window in config["windows"])
        window_start = min(
            e["s"]
            for e in writers[0, payload_log]
            if e["stage"] == "stage" and e["first"] >= first_window
        )
        for key, events in writers.items():
            controls = [e["s"] for e in events if e["stage"] == "history_control"]
            if controls:
                cutoff = max(controls)
                if cutoff >= window_start:
                    raise ValueError(
                        f"{name}: history change after selected work begins"
                    )
                # Discard bootstrap history, never associate across its boundary.
                writers[key] = [e for e in events if e["s"] > cutoff]
        groups_by_writer = {}
        spans_by_writer = {}
        missing = []
        for (node, kind), events in writers.items():
            by = {(e["id"], e["stage"]): e for e in events}
            groups = []
            byid = {}
            inline = not any(e["stage"] == "fsync_member" for e in events)
            if inline:
                for done in events:
                    if done["stage"] != "replicated_done":
                        continue
                    f = by.get((done["id"], "fsync"))
                    if f is None:
                        continue
                    group = {
                        "id": done["id"],
                        "first": done["first"],
                        "next": done["next"],
                        "s": f["s"],
                        "e": f["e"],
                        "done": done["s"],
                        "queue": None,
                    }
                    groups.append(group)
                    byid[done["id"]] = group
            else:
                previous = 0
                for f in sorted(
                    [e for e in events if e["stage"] == "fsync"], key=lambda e: e["s"]
                ):
                    members = [
                        e
                        for e in events
                        if e["stage"] == "fsync_member" and e["parent"] == f["id"]
                    ]
                    if not members:
                        continue
                    ends = [by.get((m["id"], "durable_published")) for m in members]
                    if not all(ends):
                        missing.append([node, kind, f["id"], "durable_published"])
                        continue
                    qs = [
                        by.get(
                            (m["id"], "commit_queued"), by.get((m["id"], "sync_queued"))
                        )
                        for m in members
                    ]
                    if not all(qs):
                        missing.append([node, kind, f["id"], "queued"])
                        continue
                    next_offset = max(m["next"] for m in members)
                    group = {
                        "id": f["id"],
                        "first": previous,
                        "next": next_offset,
                        "s": f["s"],
                        "e": f["e"],
                        "done": max(e["s"] for e in ends),
                        "queue": min(q["s"] for q in qs),
                    }
                    previous = max(previous, next_offset)
                    groups.append(group)
                    byid[f["id"]] = group
                    for m in members:
                        byid[m["id"]] = group
            groups.sort(key=lambda g: g["s"])
            # A replicated_done marker means staging finished in policies 1/2. It is NOT
            # used as durable completion there. The sync worker's publication is required.
            for e in events:
                if e["stage"] not in ["stage", "replicated_stage"]:
                    continue
                span = (
                    e if e["stage"] == "stage" else by.get((e["id"], "replicated_done"))
                )
                if not span or span.get("next") is None:
                    continue
                covers = [
                    g
                    for g in groups
                    if g["next"] >= span["next"]
                    and g["first"] <= span["first"]
                    and g["s"] >= e["e"]
                ]
                if covers:
                    byid[e["id"]] = covers[0]
                else:
                    missing.append([node, kind, e["id"], "stage_group"])
            spans = []

            def emit(g, phase, s, e, source, detail="", first=None, nxt=None):
                if e < s:
                    raise ValueError(f"{name}: negative {phase} interval")
                spans.append(
                    {
                        "group": g["id"],
                        "phase": phase,
                        "s": s,
                        "e": e,
                        "source": source,
                        "detail": detail,
                        "first": g["first"] if first is None else first,
                        "next": g["next"] if nxt is None else nxt,
                    }
                )

            for g in groups:
                if g["queue"] is not None:
                    emit(
                        g,
                        "Queued",
                        g["queue"],
                        g["s"],
                        "queued → fsync",
                        "Measured sync-worker queue interval",
                    )
                f = by[(g["id"], "fsync")]
                parts = []
                for part in ["log_fsync", "index_fsync"]:
                    x = by.get((g["id"], part))
                    if x:
                        parts.append(f"{part}: {(x['e']-x['s'])/1e6:.3f} ms")
                emit(g, "Fsync", g["s"], g["e"], "fsync", ", ".join(parts))
                emit(
                    g,
                    "Durable",
                    g["done"],
                    g["done"],
                    "replicated_done" if inline else "durable_published",
                )
            for e in events:
                phase = {
                    "stage": "Stage",
                    "replicated_stage": "Stage",
                    "log_write": "Write",
                }.get(e["stage"])
                if not phase:
                    continue
                g = byid.get(e["id"])
                if g is None:
                    continue
                ranges = (
                    e
                    if e.get("first") is not None
                    else by.get((e["id"], "replicated_done"), {})
                )
                emit(
                    g,
                    phase,
                    e["s"],
                    e["e"],
                    e["stage"],
                    "",
                    ranges.get("first"),
                    ranges.get("next"),
                )
            groups_by_writer[node, kind] = groups
            spans_by_writer[node, kind] = spans
        owner_stages = [e for e in writers[0, payload_log] if e["stage"] == "stage"]
        bursts = []
        for window in config["windows"]:
            for start, count in [(window["first"], window["count"])]:
                if start < 0 or count <= 0:
                    raise ValueError("Invalid offset window")
                stop = start + count
                stages = [e for e in owner_stages if start <= e["first"] < stop]
                origin = min(e["s"] for e in stages)
                done = []
                for node in range(len(nodes)):
                    covered = [
                        g
                        for g in groups_by_writer[node, payload_log]
                        if g["first"] < stop
                        and g["next"] >= stop
                        and g["done"] >= origin
                    ]
                    if not covered:
                        raise ValueError(
                            f"{name}: node {node} has no complete window at {start}"
                        )
                    done.append(min(g["done"] for g in covered))
                end = max(done) + 1_000_000
                logs = {}
                for kind in log_kinds:
                    marks = []
                    group_labels = []
                    for node in range(len(nodes)):
                        groups = [
                            g
                            for g in groups_by_writer[node, kind]
                            if g["done"] >= origin
                            and g["s"] <= end
                            and g["next"] > g["first"]
                        ]
                        labels = {
                            g["id"]: f"{'O' if node==0 else 'F'+str(node)+'.'}{i+1}"
                            for i, g in enumerate(groups)
                        }
                        for i, g in enumerate(groups):
                            group_labels.append(
                                {
                                    "node": node,
                                    "id": labels[g["id"]],
                                    "trace_id": g["id"],
                                    "first": g["first"],
                                    "next": g["next"],
                                    "durable_ms": round((g["done"] - origin) / 1e6, 4),
                                }
                            )
                        for e in spans_by_writer[node, kind]:
                            if (
                                e["group"] not in labels
                                or e["e"] < origin
                                or e["s"] > end
                            ):
                                continue
                            marks.append(
                                {
                                    "node": node,
                                    "phase": e["phase"],
                                    "batch": labels[e["group"]],
                                    "index": list(labels).index(e["group"]),
                                    "start": round((e["s"] - origin) / 1e6, 5),
                                    "end": round((e["e"] - origin) / 1e6, 5),
                                    "first": e["first"],
                                    "next": e["next"],
                                    "source": e["source"],
                                    "detail": e["detail"],
                                }
                            )
                    logs[kind] = {"marks": marks, "groups": group_labels}
                bursts.append({"first": start, "count": count, "logs": logs})
        all_cases.append(
            {
                "policy": name,
                "name": case["name"],
                "source": name,
                "nodes": nodes,
                "payloadBytes": config.get("payload_bytes"),
                "offsetLabel": "Event offsets" if combined else "Payload offsets",
                "recordLabel": "event records" if combined else "messages",
                "logLabels": {
                    kind: ("Combined log" if combined and kind == "events" else
                           "Payloads" if kind == "messages" else "Queue events")
                    for kind in log_kinds
                },
                "openCapture": any(not c["summary"].get("closed") for c in captures),
                "bursts": bursts,
            }
        )
        audit.append({"case": name, "captures": captures, "unmatched": missing})
        if missing:
            raise ValueError(
                f"{name}: {len(missing)} unlinked operations. Use raw Perfetto export for this capture."
            )
    return all_cases, audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        type=Path,
        help="Case labels, same-host traces and offset windows in payload_log",
    )
    parser.add_argument(
        "output", type=Path, help="Interactive HTML page, opened directly in a browser"
    )
    parser.add_argument(
        "--allow-open-capture",
        action="store_true",
        help="Allow missing final close marker, still require zero drops and complete selected links",
    )
    args = parser.parse_args()
    try:
        config = json.loads(args.manifest.read_text())
        cases, audit = extract(
            config, args.manifest.resolve().parent, args.allow_open_capture
        )
        data = json.dumps(cases, separators=(",", ":")).replace("<", r"\u003c")
        template = Path(__file__).with_name("batch_timeline.template.html").read_text()
        fragment = template.replace("__TRACE_DATA__", data)
        if len(fragment.encode()) >= 1_000_000:
            raise ValueError("Timeline exceeds 1 MB. Select fewer windows or cases.")
        page = (
            Path(__file__)
            .with_name("batch_timeline.page.html")
            .read_text()
            .replace("__FRAGMENT__", fragment)
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(page)
        args.output.with_suffix(".fragment.html").write_text(fragment)
        args.output.with_suffix(".audit.json").write_text(
            json.dumps(audit, indent=2) + "\n"
        )
        print(
            f'Wrote {args.output}: {len(cases)} cases, {sum(len(c["bursts"]) for c in cases)} windows'
        )
    except (ValueError, KeyError, IndexError, OSError) as error:
        parser.exit(1, f"Timeline error: {error}\n")


if __name__ == "__main__":
    main()
