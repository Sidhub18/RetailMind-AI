"""Structured logging configuration for application and library logs."""

import logging
import sys
from collections.abc import Mapping
from typing import cast

import orjson
import structlog
from structlog.stdlib import BoundLogger, ProcessorFormatter
from structlog.typing import EventDict, Processor, WrappedLogger

from config.constants import (
    REDACTED_LOG_VALUE,
    SENSITIVE_LOG_KEY_FRAGMENTS,
    THIRD_PARTY_LOGGERS,
    LogFormat,
    LogLevel,
)
from config.settings import ApplicationSettings, LoggingSettings


def _numeric_log_level(level: LogLevel) -> int:
    """Translate a validated log level into its numeric representation."""
    return logging.getLevelNamesMapping()[level.value]


def _is_sensitive_key(key: str) -> bool:
    """Return whether a structured-log field name can contain a secret."""
    normalized_key = key.casefold()
    return any(fragment in normalized_key for fragment in SENSITIVE_LOG_KEY_FRAGMENTS)


def _redact_value(value: object, field_name: str | None = None) -> object:
    """Recursively redact values associated with sensitive field names."""
    if field_name is not None and _is_sensitive_key(field_name):
        return REDACTED_LOG_VALUE
    if isinstance(value, Mapping):
        return {str(key): _redact_value(item, str(key)) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    return value


def _redact_sensitive_fields(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Structlog processor that removes sensitive structured values."""
    return cast(EventDict, _redact_value(event_dict))


def _serialize_json(value: object, **_kwargs: object) -> str:
    """Serialize a log event as UTF-8 JSON."""
    return orjson.dumps(
        value,
        default=str,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_UTC_Z,
    ).decode("utf-8")


def _build_application_context_processor(
    settings: ApplicationSettings,
) -> Processor:
    """Build a processor that adds immutable application identity fields."""

    def add_application_context(
        _logger: WrappedLogger,
        _method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict.update(
            {
                "environment": settings.environment.value,
                "service": settings.service_name,
                "version": settings.version,
            }
        )
        return event_dict

    return add_application_context


def _build_shared_processors(
    logging_settings: LoggingSettings,
    application_settings: ApplicationSettings,
) -> list[Processor]:
    """Build processors shared by structlog and standard logging."""
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        _build_application_context_processor(application_settings),
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        _redact_sensitive_fields,
    ]
    if logging_settings.include_timestamp:
        processors.append(structlog.processors.TimeStamper(fmt="iso", utc=True))
    if logging_settings.include_callsite:
        processors.append(
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            )
        )
    return processors


def _build_renderer(settings: LoggingSettings) -> Processor:
    """Build the configured final log renderer."""
    if settings.format is LogFormat.JSON:
        return structlog.processors.JSONRenderer(serializer=_serialize_json)
    return structlog.dev.ConsoleRenderer(colors=settings.color)


def configure_logging(
    logging_settings: LoggingSettings,
    application_settings: ApplicationSettings,
) -> None:
    """Configure consistent structlog and standard-library output.

    Args:
        logging_settings: Validated logging behavior.
        application_settings: Application identity fields added to every log.
    """
    shared_processors = _build_shared_processors(
        logging_settings,
        application_settings,
    )
    renderer = _build_renderer(logging_settings)

    formatter = ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    for existing_handler in root_logger.handlers[:]:
        root_logger.removeHandler(existing_handler)
        existing_handler.close()
    root_logger.addHandler(handler)
    root_logger.setLevel(_numeric_log_level(logging_settings.level))

    third_party_level = _numeric_log_level(logging_settings.third_party_level)
    for logger_name in THIRD_PARTY_LOGGERS:
        logging.getLogger(logger_name).setLevel(third_party_level)

    logging.captureWarnings(capture=True)
    structlog.reset_defaults()
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    structlog.contextvars.clear_contextvars()


def get_logger(name: str | None = None, **context: object) -> BoundLogger:
    """Return a context-aware structured logger.

    Args:
        name: Optional logger name, normally the calling module's ``__name__``.
        **context: Stable contextual fields to bind to the logger.

    Returns:
        A standard-library-compatible structlog bound logger.
    """
    if name is None:
        return cast(BoundLogger, structlog.get_logger(**context))
    return cast(BoundLogger, structlog.get_logger(name, **context))


def bind_log_context(**context: object) -> None:
    """Bind request- or task-local fields to subsequent log events."""
    structlog.contextvars.bind_contextvars(**context)


def clear_log_context() -> None:
    """Clear request- or task-local logging context."""
    structlog.contextvars.clear_contextvars()
