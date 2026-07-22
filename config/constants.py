"""Non-secret configuration constants and constrained option types.

Runtime values remain overridable through environment variables. Constants in
this module are safe defaults and invariants, not credentials or deployment-
specific resource identifiers.
"""

from enum import StrEnum
from typing import Final


class LogFormat(StrEnum):
    """Supported structured log renderers."""

    JSON = "json"
    CONSOLE = "console"


class LogLevel(StrEnum):
    """Supported standard-library logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AWSRetryMode(StrEnum):
    """Botocore retry modes approved for application use."""

    STANDARD = "standard"
    ADAPTIVE = "adaptive"


DISTRIBUTION_NAME: Final = "retailmind-ai"
DEFAULT_APPLICATION_NAME: Final = "retailmind-ai"
LOCAL_APPLICATION_VERSION: Final = "0.0.0+local"

DEFAULT_ENV_FILE_NAME: Final = ".env"
ENV_FILE_VARIABLE: Final = "RETAILMIND_ENV_FILE"
DEFAULT_CONFIG_ENCODING: Final = "utf-8"
DEFAULT_ENVIRONMENT: Final = "development"

DEFAULT_API_HOST: Final = "127.0.0.1"
DEFAULT_API_PORT: Final = 8000
DEFAULT_API_PREFIX: Final = "/api/v1"

DEFAULT_LOG_LEVEL: Final = LogLevel.INFO
DEFAULT_THIRD_PARTY_LOG_LEVEL: Final = LogLevel.WARNING
DEFAULT_LOG_FORMAT: Final = LogFormat.JSON

DEFAULT_AWS_RETRY_MODE: Final = AWSRetryMode.STANDARD
DEFAULT_AWS_MAX_ATTEMPTS: Final = 5
DEFAULT_AWS_CONNECT_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_AWS_READ_TIMEOUT_SECONDS: Final = 60.0
DEFAULT_AWS_MAX_POOL_CONNECTIONS: Final = 25

DEFAULT_RETRY_MAX_ATTEMPTS: Final = 5
DEFAULT_RETRY_INITIAL_DELAY_SECONDS: Final = 0.5
DEFAULT_RETRY_MAX_DELAY_SECONDS: Final = 30.0
DEFAULT_RETRY_BACKOFF_MULTIPLIER: Final = 2.0
DEFAULT_RETRY_JITTER_SECONDS: Final = 1.0

DEFAULT_CONFIG_ROOT_NAME: Final = "config"
DEFAULT_DATA_ROOT_NAME: Final = "data"
DEFAULT_LOG_ROOT_NAME: Final = "logs"
DEFAULT_MODEL_ROOT_NAME: Final = "models"
DEFAULT_PROMPT_ROOT_NAME: Final = "prompts"
DEFAULT_REPORT_ROOT_NAME: Final = "reports"

REDACTED_LOG_VALUE: Final = "[REDACTED]"
SENSITIVE_LOG_KEY_FRAGMENTS: Final[frozenset[str]] = frozenset(
    {
        "access_key",
        "authorization",
        "cookie",
        "credential",
        "database_url",
        "password",
        "private_key",
        "secret",
        "session_token",
        "token",
    }
)

THIRD_PARTY_LOGGERS: Final[tuple[str, ...]] = (
    "boto3",
    "botocore",
    "s3transfer",
    "urllib3",
)
