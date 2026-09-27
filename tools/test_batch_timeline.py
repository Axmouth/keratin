"""Durability attribution and incomplete-capture checks for the timeline reader."""

import json
import tempfile
import unittest
from pathlib import Path

from batch_timeline import extract


def event(ident, stage, start, end=None, first=0, nxt=2, parent=None):
    return dict(
        writer=1,
        id=ident,
        stage=stage,
        start_ns=start,
        end_ns=start if end is None else end,
        first=first,
        next=nxt,
        parent=parent,
    )


class TimelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.config = dict(
            clock="same-host",
            topic="topic1",
            nodes=["owner", "follower"],
            windows=[dict(first=0, count=2)],
            cases=[dict(id="test", name="Test", traces="*.jsonl")],
        )
        self.rows = []
        for node in range(2):
            rows = [dict(type="process", version=1, unix_ns="1790000000000000000")]
            for writer, kind in enumerate(["messages", "events"], 1):
                rows.append(
                    dict(
                        type="writer",
                        writer=writer,
                        path=f"/node-{node}/data/{kind}/topic1/0000",
                    )
                )
                stages = [
                    event(10, "stage" if node == 0 else "replicated_stage", 100, 200),
                    event(10, "replicated_done", 210),
                    event(11, "sync_queued", 250),
                    event(11, "fsync_member", 260, parent=12),
                    event(12, "fsync", 300, 800),
                    event(11, "durable_published", 900),
                ]
                for e in stages:
                    e["writer"] = writer
                rows.extend(stages)
            rows.append(
                dict(
                    type="summary",
                    dropped=0,
                    closed=True,
                    emitted=sum(
                        "stage" in r or r.get("type") == "writer" for r in rows
                    ),
                )
            )
            self.rows.append(rows)

    def extract(self, allow_open=False):
        for node, rows in enumerate(self.rows):
            (self.base / f"{node}.jsonl").write_text(
                "\n".join(json.dumps(r) for r in rows)
            )
        return extract(self.config, self.base, allow_open)

    def test_stage_completion_is_not_worker_durability(self):
        cases, audit = self.extract()
        marks = cases[0]["bursts"][0]["logs"]["messages"]["marks"]
        durable = next(m for m in marks if m["node"] == 1 and m["phase"] == "Durable")
        self.assertEqual(durable["start"], 0.0008)
        self.assertEqual(durable["source"], "durable_published")
        self.assertEqual(audit[0]["unmatched"], [])

    def test_combined_log_needs_no_message_writer(self):
        self.config.update(payload_log="events", logs=["events"])
        for node, rows in enumerate(self.rows):
            self.rows[node] = [r for r in rows if r.get("writer") != 1]
            self.rows[node][-1]["emitted"] = sum(
                "stage" in r or r.get("type") == "writer" for r in self.rows[node]
            )
        cases, audit = self.extract()
        self.assertEqual(cases[0]["offsetLabel"], "Event offsets")
        self.assertEqual(cases[0]["recordLabel"], "event records")
        self.assertEqual(cases[0]["logLabels"], {"events": "Combined log"})
        marks = cases[0]["bursts"][0]["logs"]["events"]["marks"]
        self.assertTrue(any(m["node"] == 1 and m["phase"] == "Durable" for m in marks))
        self.assertEqual(audit[0]["unmatched"], [])

    def test_payload_lane_must_be_present(self):
        self.config.update(payload_log="events", logs=["messages"])
        with self.assertRaisesRegex(ValueError, "payload_log"):
            self.extract()

    def test_missing_worker_publication_is_rejected(self):
        self.rows[1] = [
            r for r in self.rows[1] if r.get("stage") != "durable_published"
        ]
        self.rows[1][-1]["emitted"] -= 2
        with self.assertRaises(ValueError):
            self.extract()

    def test_open_capture_requires_explicit_override(self):
        self.rows[0][-1]["closed"] = False
        with self.assertRaisesRegex(ValueError, "close marker"):
            self.extract()
        cases, _ = self.extract(allow_open=True)
        self.assertTrue(cases[0]["openCapture"])

    def test_drop_override_is_not_allowed(self):
        self.rows[0][-1]["dropped"] = 1
        with self.assertRaisesRegex(ValueError, "dropped"):
            self.extract(allow_open=True)

    def test_history_changes_cannot_be_offset_correlated(self):
        self.rows[0].insert(-1, event(20, "history_control", 500))
        self.rows[0][-1]["emitted"] += 1
        with self.assertRaisesRegex(ValueError, "history change"):
            self.extract()

    def test_remote_clock_is_rejected(self):
        self.config["clock"] = "multi-host"
        with self.assertRaisesRegex(ValueError, "same-host"):
            self.extract()


if __name__ == "__main__":
    unittest.main()
