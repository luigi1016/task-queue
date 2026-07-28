"""Shared SQL for reading queue state, organized by purpose.

- ``queries.metrics`` — queue-state queries scraped by the metrics exporter.
- ``queries.diagnostics`` — operational/debugging queries referenced from
  docs/operations.md (run them in psql, or from future tooling).

The job-lifecycle SQL (enqueue/dequeue/ack/nack) intentionally stays inline
in ``taskqueue.queue``: each statement exists to serve exactly one
function's contract and changes in lockstep with the Python around it.
"""
