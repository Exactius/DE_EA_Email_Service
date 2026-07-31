"""DAG-to-Cloud-Run trace correlation: bind incoming trace headers to every log line."""

from __future__ import annotations

import contextvars
import logging
import uuid

CORRELATION_ID_HEADER = "X-Correlation-Id"
DAG_ID_HEADER = "X-Dag-Id"
DAG_RUN_ID_HEADER = "X-Dag-Run-Id"
TASK_ID_HEADER = "X-Task-Id"

TRACE_FIELDS = ("correlation_id", "dag_id", "dag_run_id", "task_id")

trace_context: contextvars.ContextVar[dict] = contextvars.ContextVar("trace_context", default={})


def new_correlation_id() -> str:
    """Return a new unique correlation id."""
    correlation_id = uuid.uuid4().hex
    return correlation_id


def bind_trace_context(
    correlation_id: str,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
) -> None:
    """Bind trace identifiers to the current execution context."""
    context = {"correlation_id": correlation_id}
    if dag_id:
        context["dag_id"] = dag_id
    if dag_run_id:
        context["dag_run_id"] = dag_run_id
    if task_id:
        context["task_id"] = task_id
    trace_context.set(context)


def get_trace_context() -> dict:
    """Return the trace identifiers bound to the current context."""
    context = trace_context.get()
    return context


async def trace_middleware(request, call_next):
    """Bind incoming trace headers (or a new correlation id) for the request lifetime."""
    correlation_id = request.headers.get(CORRELATION_ID_HEADER) or new_correlation_id()
    bind_trace_context(
        correlation_id=correlation_id,
        dag_id=request.headers.get(DAG_ID_HEADER),
        dag_run_id=request.headers.get(DAG_RUN_ID_HEADER),
        task_id=request.headers.get(TASK_ID_HEADER),
    )
    response = await call_next(request)
    response.headers[CORRELATION_ID_HEADER] = correlation_id
    return response


class TraceContextFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """Stamp the bound trace identifiers onto every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach the current trace context fields to the log record."""
        context = get_trace_context()
        for field_name in TRACE_FIELDS:
            setattr(record, field_name, context.get(field_name))
        return True
