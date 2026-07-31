"""Structured logging setup with Cloud Run detection and DAG trace fields."""

from __future__ import annotations

import datetime
import json
import logging
import os
import sys

from src.utils.trace import TRACE_FIELDS, TraceContextFilter

IS_CLOUD_RUN = os.getenv("K_SERVICE") is not None


class JsonFormatter(logging.Formatter):
    """GCP-compatible JSON formatter for Cloud Run."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = (
            datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        log_entry = {
            "timestamp": timestamp,
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        for field_name in TRACE_FIELDS:
            field_value = getattr(record, field_name, None)
            if field_value:
                log_entry[field_name] = field_value
        return json.dumps(log_entry)


class TextFormatter(logging.Formatter):
    """Human-readable formatter for local development."""

    def format(self, record: logging.LogRecord) -> str:
        formatted = f"{self.formatTime(record)} - {record.name} - {record.levelname} - {record.getMessage()}"
        return formatted


def setup_logging() -> None:
    """Configure the root logger with a JSON (Cloud Run) or text handler + trace filter."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if IS_CLOUD_RUN else TextFormatter())
    handler.addFilter(TraceContextFilter())

    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), handlers=[handler])
