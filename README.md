# structured-event-span

A small, dependency-free Python library for recording nested operation spans — start time, end time, duration, and arbitrary metadata — into a tree that can be flattened for export to tracing backends.

```python
from structured_event_span import SpanRecorder

rec = SpanRecorder()
with rec.open("handle_request", metadata={"route": "/items"}):
    with rec.open("db_query", metadata={"table": "items"}):
        pass

for entry in rec.export():
    print(entry)
```

Each exported entry is a dict with `span_id`, `parent_id`, `name`, `start_ns`, `end_ns`, `duration_ns`, and `metadata`. Durations are in nanoseconds from an injectable monotonic clock; pass `clock=some_callable` to `SpanRecorder` to control time in tests.

## Why this exists

The problem is narrow: instrument code with nested timings and metadata, then hand the result to whatever tracing backend you already run. Existing OpenTelemetry-style clients pull in transitive dependencies and async machinery that are overkill when all you want is a tree of `(start, end, meta)` tuples. This library makes one trade-off deliberately: it is synchronous and stack-based. There is no asyncio integration, no sampling, no network exporter. You get a list of dicts and you wire it to your backend yourself.

## The awkward edge

A span must end before its parent ends, and spans must end in the reverse of the order they started. Ending a parent while a child is still open, or ending spans out of order, raises `ValueError` rather than silently producing nonsensical timings. If your control flow raises inside a nested `with rec.open(...)` block, Python's `__exit__` chain still runs in the correct order, so the normal case is fine; the error only appears on actual misuse such as explicitly calling `.end()` out of order.

## Performance

The window keeps a bounded buffer, so `push` is constant time and memory does not
grow with the length of the stream. `peak` and `trough` are linear in the window
size, which is the trade that keeps `push` cheap.

## Design notes

The window stores values eagerly rather than keeping running aggregates. Running
sums drift with floating point over long streams, and recomputing from a small
buffer is cheap enough that the drift is not worth the speed.

