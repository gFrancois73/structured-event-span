"""Core implementation of structured event span recording.

A `SpanRecorder` owns a forest of `Span` objects representing nested operations.
Each span records start/end instants (in nanoseconds from an injectable
monotonic clock), an arbitrary string `name`, and a free-form `metadata` dict.

Design decisions that are not obvious from the API surface:

* The clock is injected via the `clock` constructor argument. This exists so
  tests can supply a deterministic fake clock; the default uses
  `time.monotonic_ns()`. We deliberately do *not* offer a wall-clock option,
  because spans are about elapsed durations between points on a single machine,
  and a wall clock can jump backwards on NTP correction.
* A span that is closed twice raises `ValueError`. Re-closing is almost always
  a bug in the caller (a double `__exit__`, a stray `finally` calling close()
  explicitly). Staying silent would hide the problem.
* Entering a span's context manager twice, nested or not, is rejected. A span
  represents one concrete operation; reusing it muddles the tree and the
  recorded timings.
* A child must end before its parent ends. Ending a parent while a child is
  still open is treated as an error: the timing data would be nonsensical
  (child end recorded after parent end).
* Export flattens the tree depth-first, pre-order. We chose this order because
  tracing backends typically reconstruct the tree from `parent_id` pointers,
  so iteration order is cosmetic but determinism is nice.
"""

from __future__ import annotations

import contextvars
import threading
import time
from typing import Any, Dict, Iterator, List, Optional


# A process-wide contextvar holding the currently open span for the logical
# execution stream. Contextvars (not thread-locals) are used so that
# asyncio tasks and thread pools each get their own notion of "current parent"
# without cross-talk. A thread that has never opened a span sees `None`.
_current_span: contextvars.ContextVar[Optional["Span"]] = contextvars.ContextVar(
    "structured_event_span_current", default=None
)


def monotonic_ns_safe() -> int:
    """Return a monotonic timestamp in nanoseconds.

    This indirection exists so callers (including the default clock wired into
    `SpanRecorder`) can be replaced wholesale from tests by passing a function
    with the same signature.
    """
    return time.monotonic_ns()


class Span:
    """One operation span. Use as a context manager or call `start`/`end`."""

    __slots__ = (
        "recorder",
        "name",
        "metadata",
        "parent",
        "children",
        "start_ns",
        "end_ns",
        "span_id",
        "_running",
        "_ended",
        "_lock",
    )

    _id_counter = 0
    _id_lock = threading.Lock()

    def __init__(
        self,
        recorder: "SpanRecorder",
        name: str,
        parent: Optional["Span"],
        metadata: Optional[Dict[str, Any]] = None,
        start_ns: Optional[int] = None,
    ) -> None:
        self.recorder = recorder
        self.name = name
        # Copy defensively: the caller might mutate their dict after handing it
        # in. Copying is shallow by design — metadata is for opaque payloads,
        # not for us to recursively protect.
        self.metadata: Dict[str, Any] = dict(metadata) if metadata else {}
        self.parent = parent
        self.children: List["Span"] = []
        self.start_ns: Optional[int] = start_ns
        self.end_ns: Optional[int] = None
        # An integer id is portable across export formats and avoids any
        # dependency on uuid for a field whose only consumer is the caller's
        # tree reconstruction.
        with Span._id_lock:
            Span._id_counter += 1
            self.span_id = Span._id_counter
        self._running = False
        self._ended = False
        self._lock = threading.Lock()

    def start(self) -> "Span":
        """Mark this span as started and attach it to the current parent."""
        with self._lock:
            if self._running:
                raise ValueError(f"span {self.span_id!r} is already running")
            if self._ended:
                raise ValueError(f"span {self.span_id!r} has already ended")
            self.start_ns = self.recorder.clock() if self.start_ns is None else self.start_ns
            self._running = True
        if self.parent is not None:
            self.parent.children.append(self)
        # Set this span as the current parent for subsequently started spans in
        # the same logical stream (thread / task).
        self.recorder._set_current(self)
        return self

    def end(self) -> None:
        """Mark this span as ended and restore the previous current parent."""
        with self._lock:
            if not self._running:
                raise ValueError(f"span {self.span_id!r} is not running")
            if self._ended:
                raise ValueError(f"span {self.span_id!r} has already ended")
            # Any child still running means the caller ended the parent before
            # the child — that's a misuse we surface loudly.
            for child in self.children:
                if child.start_ns is not None and child.end_ns is None:
                    raise ValueError(
                        f"cannot end span {self.span_id!r} "
                        f"with open child {child.span_id!r}"
                    )
            self.end_ns = self.recorder.clock()
            self._ended = True
            self._running = False
        self.recorder._restore_current(self)

    # Context-manager sugar so `with recorder.span("x"): ...` works.
    def __enter__(self) -> "Span":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        # We do not branch on exc_type: spans record timings, not success/failure.
        # If end() raises (e.g. double-exit), we let it propagate — the caller
        # already has an exception in flight only if exc is not None, and we
        # do not want to mask that with our own. However, re-entering end()
        # after an exception-driven exit would itself raise; that is the
        # documented behaviour.
        self.end()

    @property
    def duration_ns(self) -> int:
        """Elapsed time in nanoseconds, or raise if the span is not closed."""
        if self.start_ns is None:
            raise ValueError(f"span {self.span_id!r} was never started")
        if self.end_ns is None:
            raise ValueError(f"span {self.span_id!r} has not ended")
        return self.end_ns - self.start_ns

    def to_dict(self) -> Dict[str, Any]:
        """Flat dict representation suitable for tracing backends.

        Children are referenced via `parent_id` rather than nesting so the
        export shape matches what most tracing backends ingest.
        """
        return {
            "span_id": self.span_id,
            "parent_id": self.parent.span_id if self.parent is not None else None,
            "name": self.name,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "duration_ns": self.duration_ns if self._ended else None,
            "metadata": dict(self.metadata),
        }

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        state = "running" if self._running else ("ended" if self._ended else "new")
        return f"<Span id={self.span_id} name={self.name!r} state={state}>"


class SpanRecorder:
    """Owns a forest of spans and the notion of the "current" parent span."""

    def __init__(self, clock=None) -> None:
        # The clock is the single seam between this library and the outside
        # world's time. Injecting it is what makes the test suite deterministic.
        self.clock = clock if clock is not None else monotonic_ns_safe
        self.roots: List[Span] = []
        # Each `start()` pushes the previous current onto a stack so that
        # `end()` can restore it. This correctly handles reentrancy and
        # interleaved siblings within one logical stream.
        self._stack: List[Span] = []

    # The recorder owns the stack, so the stack lives on the recorder instance
    # rather than in a contextvar. A contextvar would be more correct under
    # asyncio, but it would also mean a task that opens a span and awaits could
    # see a different "current" on resume; for the narrow use case of nested
    # synchronous spans, an explicit stack is easier to reason about and test.
    def _set_current(self, span: Span) -> None:
        self._stack.append(span)
        if span.parent is None:
            self.roots.append(span)

    def _restore_current(self, span: Span) -> None:
        if not self._stack or self._stack[-1] is not span:
            raise ValueError(
                f"span {span.span_id!r} ended but was not the innermost open span"
            )
        self._stack.pop()

    @property
    def current(self) -> Optional[Span]:
        """The innermost currently open span, or None."""
        return self._stack[-1] if self._stack else None

    def span(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
        start_ns: Optional[int] = None,
    ) -> Span:
        """Create a new span parented to the current open span (or a root)."""
        return Span(self, name, self.current, metadata, start_ns)

    # A convenience contextmanager that creates and starts a span in one call.
    # This is the form most call sites want.
    from contextlib import contextmanager as _cm

    @_cm
    def open(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Span]:
        s = self.span(name, metadata)
        with s:
            yield s

    def export(self) -> List[Dict[str, Any]]:
        """Flatten all recorded spans depth-first, pre-order, as dicts."""
        out: List[Dict[str, Any]] = []
        stack: List[Span] = list(reversed(self.roots))
        while stack:
            s = stack.pop()
            out.append(s.to_dict())
            # Push children reversed so the first child is processed first.
            for c in reversed(s.children):
                stack.append(c)
        return out

    def reset(self) -> None:
        """Drop all recorded spans. Useful between test cases."""
        self.roots.clear()
        self._stack.clear()
        # If any caller still holds a Span reference, it will be detached from
        # the recorder. That is acceptable; reset is for a clean slate.
