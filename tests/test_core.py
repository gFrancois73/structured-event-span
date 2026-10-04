import unittest

from structured_event_span import Span, SpanRecorder


class FakeClock:
    """A controllable monotonic clock for deterministic tests."""

    def __init__(self, start=0):
        self.t = start

    def __call__(self):
        self.t += 1
        return self.t


class TestSpanLifecycle(unittest.TestCase):
    def test_basic_context_manager(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root") as s:
            pass
        self.assertEqual(s.start_ns, 1)
        self.assertEqual(s.end_ns, 2)
        self.assertEqual(s.duration_ns, 1)
        self.assertIsNone(s.parent)
        self.assertEqual(s.children, [])

    def test_nested_spans_parent_links(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root") as root:
            with rec.span("child") as child:
                pass
        self.assertIs(child.parent, root)
        self.assertEqual(root.children, [child])
        # child started at 2, ended at 3; root at 1 and 4.
        self.assertEqual(child.duration_ns, 1)
        self.assertEqual(root.duration_ns, 3)

    def test_siblings_share_parent(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root") as root:
            with rec.span("a") as a:
                pass
            with rec.span("b") as b:
                pass
        self.assertIs(a.parent, root)
        self.assertIs(b.parent, root)
        self.assertEqual(root.children, [a, b])

    def test_open_context_manager_alias(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.open("root") as s:
            self.assertIsInstance(s, Span)
        self.assertEqual(s.duration_ns, 1)

    def test_double_end_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        s = rec.span("root").start()
        s.end()
        with self.assertRaises(ValueError):
            s.end()

    def test_double_start_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        s = rec.span("root").start()
        with self.assertRaises(ValueError):
            s.start()
        s.end()

    def test_end_without_start_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        s = rec.span("root")
        with self.assertRaises(ValueError):
            s.end()

    def test_duration_before_end_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        s = rec.span("root").start()
        with self.assertRaises(ValueError):
            _ = s.duration_ns
        s.end()

    def test_ending_parent_with_open_child_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        root = rec.span("root").start()
        child = rec.span("child").start()
        with self.assertRaises(ValueError):
            root.end()
        # Clean up so the recorder isn't left in a weird state for other tests.
        child.end()
        root.end()

    def test_out_of_order_end_raises(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        a = rec.span("a").start()
        b = rec.span("b").start()
        # Ending `a` while `b` is the innermost open span is an error.
        with self.assertRaises(ValueError):
            a.end()
        b.end()
        a.end()

    def test_metadata_copied_defensively(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        meta = {"k": 1}
        with rec.span("root", metadata=meta) as s:
            pass
        meta["k"] = 999
        self.assertEqual(s.metadata, {"k": 1})

    def test_export_flat_preorder(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root"):
            with rec.span("a"):
                pass
            with rec.span("b"):
                with rec.span("b1"):
                    pass
        exported = rec.export()
        names = [e["name"] for e in exported]
        self.assertEqual(names, ["root", "a", "b", "b1"])
        for e in exported:
            self.assertIn("span_id", e)
            self.assertIn("parent_id", e)
            self.assertIn("start_ns", e)
            self.assertIn("end_ns", e)
            self.assertIn("duration_ns", e)
            self.assertIn("metadata", e)

    def test_export_parent_ids_correct(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root") as root:
            with rec.span("child") as child:
                pass
        exported = {e["span_id"]: e for e in rec.export()}
        self.assertIsNone(exported[root.span_id]["parent_id"])
        self.assertEqual(exported[child.span_id]["parent_id"], root.span_id)

    def test_multiple_roots(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("r1"):
            pass
        with rec.span("r2"):
            pass
        self.assertEqual([r.name for r in rec.roots], ["r1", "r2"])

    def test_reset_clears_state(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root"):
            pass
        rec.reset()
        self.assertEqual(rec.roots, [])
        self.assertIsNone(rec.current)

    def test_current_tracks_open_span(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        self.assertIsNone(rec.current)
        with rec.span("root") as root:
            self.assertIs(rec.current, root)
            with rec.span("child") as child:
                self.assertIs(rec.current, child)
            self.assertIs(rec.current, root)
        self.assertIsNone(rec.current)

    def test_start_ns_override(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        # Explicit start_ns is honoured and not overwritten by the clock.
        s = rec.span("root", start_ns=1000).start()
        self.assertEqual(s.start_ns, 1000)
        s.end()
        # end_ns still comes from the clock.
        self.assertEqual(s.end_ns, 1)

    def test_to_dict_shape(self):
        clock = FakeClock()
        rec = SpanRecorder(clock=clock)
        with rec.span("root", metadata={"x": 1}):
            pass
        d = rec.export()[0]
        self.assertEqual(d["name"], "root")
        self.assertEqual(d["metadata"], {"x": 1})
        self.assertEqual(d["duration_ns"], 1)


if __name__ == "__main__":
    unittest.main()
