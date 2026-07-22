"""Typed configuration models for the RetailMind AI runtime.

The models validate configuration only. They do not create infrastructure,
connect to AWS, open databases, or execute application behavior.
"""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic.functional_validators import BeforeValidator

from config.constants import (
    DEFAULT_API_HOST,
    DEFAULT_API_PORT,
    DEFAULT_API_PREFIX,
    DEFAULT_APPLICATION_NAME,
    DEFAULT_AWS_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_AWS_MAX_ATTEMPTS,
    DEFAULT_AWS_MAX_POOL_CONNECTIONS,
    DEFAULT_AWS_READ_TIMEOUT_SECONDS,
    DEFAULT_AWS_RETRY_MODE,
    DEFAULT_CONFIG_ROOT_NAME,
    DEFAULT_DATA_ROOT_NAME,
    DEFAULT_ENVIRONMENT,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_ROOT_NAME,
    DEFAULT_MODEL_ROOT_NAME,
    DEFAULT_PROMPT_ROOT_NAME,
    DEFAULT_REPORT_ROOT_NAME,
    DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
    DEFAULT_RETRY_JITTER_SECONDS,
    DEFAULT_RETRY_MAX_ATTEMPTS,
    DEFAULT_RETRY_MAX_DELAY_SECONDS,
    DEFAULT_THIRD_PARTY_LOG_LEVEL,
    DISTRIBUTION_NAME,
    LOCAL_APPLICATION_VERSION,
    AWSRetryMode,
    LogFormat,
    LogLevel,
)
from config.env import (
    DeploymentEnvironment,
    EnvironmentSettings,
    empty_string_to_none,
    resolve_env_file,
)
from config.paths import PROJECT_ROOT, PathValue, resolve_project_path

type OptionalString = Annotated[str | None, BeforeValidator(empty_string_to_none)]
type OptionalHttpUrl = Annotated[
    HttpUrl | None,
    BeforeValidator(empty_string_to_none),
]
type OptionalSecret = Annotated[
    SecretStr | None,
    BeforeValidator(empty_string_to_none),
]


def _installed_application_version() -> str:
    """Return installed package metadata or a deterministic local version."""
    try:
        return version(DISTRIBUTION_NAME)
    except PackageNotFoundError:
        return LOCAL_APPLICATION_VERSION


class ApplicationSettings(EnvironmentSettings):
    """Process-level application settings."""

    name: str = Field(
        default=DEFAULT_APPLICATION_NAME,
        min_length=1,
        validation_alias="RETAILMIND_NAME",
    )
    version: str = Field(
        default_factory=_installed_application_version,
        min_length=1,
        validation_alias="RETAILMIND_VERSION",
    )
    environment: DeploymentEnvironment = Field(
        default=DeploymentEnvironment(DEFAULT_ENVIRONMENT),
        validation_alias="RETAILMIND_ENV",
    )
    debug: bool = Field(
        default=False,
        validation_alias="RETAILMIND_DEBUG",
    )
    api_host: str = Field(
        default=DEFAULT_API_HOST,
        min_length=1,
        validation_alias="API_HOST",
    )
    api_port: int = Field(
        default=DEFAULT_API_PORT,
        ge=1,
        le=65535,
        validation_alias="API_PORT",
    )
    api_prefix: str = Field(
        default=DEFAULT_API_PREFIX,
        min_length=1,
        validation_alias="API_PREFIX",
    )
    service_name: str = Field(
        default=DEFAULT_APPLICATION_NAME,
        min_length=1,
        validation_alias="OTEL_SERVICE_NAME",
    )
    database_url: OptionalSecret = Field(
        default=None,
        validation_alias="DATABASE_URL",
    )

    @field_validator("api_prefix")
    @classmethod
    def validate_api_prefix(cls, value: str) -> str:
        """Require an absolute API path without a trailing slash."""
        if not value.startswith("/"):
            message = "API_PREFIX must start with '/'"
            raise ValueError(message)
        if value != "/" and value.endswith("/"):
            message = "API_PREFIX must not end with '/'"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_production_debug(self) -> Self:
        """Prevent debug mode from running in production."""
        if self.environment.is_production and self.debug:
            message = "RETAILMIND_DEBUG must be false in production"
            raise ValueError(message)
        return self


class LoggingSettings(EnvironmentSettings):
    """Structured logging configuration."""

    level: LogLevel = Field(
        default=DEFAULT_LOG_LEVEL,
        validation_alias="LOG_LEVEL",
    )
    third_party_level: LogLevel = Field(
        default=DEFAULT_THIRD_PARTY_LOG_LEVEL,
        validation_alias="LOG_THIRD_PARTY_LEVEL",
    )
    format: LogFormat = Field(
        default=DEFAULT_LOG_FORMAT,
        validation_alias="LOG_FORMAT",
    )
    include_timestamp: bool = Field(
        default=True,
        validation_alias="LOG_INCLUDE_TIMESTAMP",
    )
    include_callsite: bool = Field(
        default=False,
        validation_alias="LOG_INCLUDE_CALLSITE",
    )
    color: bool = Field(
        default=False,
        validation_alias="LOG_COLOR",
    )


class RetrySettings(EnvironmentSettings):
    """Technology-neutral retry and exponential-backoff settings."""

    max_attempts: int = Field(
        default=DEFAULT_RETRY_MAX_ATTEMPTS,
        ge=1,
        le=20,
        validation_alias="RETRY_MAX_ATTEMPTS",
    )
    initial_delay_seconds: float = Field(
        default=DEFAULT_RETRY_INITIAL_DELAY_SECONDS,
        ge=0.0,
        le=300.0,
        validation_alias="RETRY_INITIAL_DELAY_SECONDS",
    )
    max_delay_seconds: float = Field(
        default=DEFAULT_RETRY_MAX_DELAY_SECONDS,
        ge=0.0,
        le=3600.0,
        validation_alias="RETRY_MAX_DELAY_SECONDS",
    )
    backoff_multiplier: float = Field(
        default=DEFAULT_RETRY_BACKOFF_MULTIPLIER,
        ge=1.0,
        le=10.0,
        validation_alias="RETRY_BACKOFF_MULTIPLIER",
    )
    jitter_seconds: float = Field(
        default=DEFAULT_RETRY_JITTER_SECONDS,
        ge=0.0,
        le=300.0,
        validation_alias="RETRY_JITTER_SECONDS",
    )

    @model_validator(mode="after")
    def validate_delay_range(self) -> Self:
        """Ensure the retry delay ceiling is not below the initial delay."""
        if self.max_delay_seconds < self.initial_delay_seconds:
            message = (
                "RETRY_MAX_DELAY_SECONDS must be greater than or equal to "
                "RETRY_INITIAL_DELAY_SECONDS"
            )
            raise ValueError(message)
        return self


class AWSSettings(EnvironmentSettings):
    """AWS SDK client defaults without embedded credentials."""

    region: OptionalString = Field(
        default=None,
        validation_alias="AWS_REGION",
    )
    profile: OptionalString = Field(
        default=None,
        validation_alias="AWS_PROFILE",
    )
    endpoint_url: OptionalHttpUrl = Field(
        default=None,
        validation_alias="AWS_ENDPOINT_URL",
    )
    retry_mode: AWSRetryMode = Field(
        default=DEFAULT_AWS_RETRY_MODE,
        validation_alias="AWS_RETRY_MODE",
    )
    max_attempts: int = Field(
        default=DEFAULT_AWS_MAX_ATTEMPTS,
        ge=1,
        le=20,
        validation_alias="AWS_MAX_ATTEMPTS",
    )
    connect_timeout_seconds: float = Field(
        default=DEFAULT_AWS_CONNECT_TIMEOUT_SECONDS,
        gt=0.0,
        le=300.0,
        validation_alias="AWS_CONNECT_TIMEOUT_SECONDS",
    )
    read_timeout_seconds: float = Field(
        default=DEFAULT_AWS_READ_TIMEOUT_SECONDS,
        gt=0.0,
        le=3600.0,
        validation_alias="AWS_READ_TIMEOUT_SECONDS",
    )
    max_pool_connections: int = Field(
        default=DEFAULT_AWS_MAX_POOL_CONNECTIONS,
        ge=1,
        le=1000,
        validation_alias="AWS_MAX_POOL_CONNECTIONS",
    )
    tcp_keepalive: bool = Field(
        default=True,
        validation_alias="AWS_TCP_KEEPALIVE",
    )
    use_ssl: bool = Field(
        default=True,
        validation_alias="AWS_USE_SSL",
    )


class PathSettings(EnvironmentSettings):
    """Validated runtime filesystem locations."""

    config_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_CONFIG_ROOT_NAME,
        validation_alias="CONFIG_ROOT",
    )
    data_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_DATA_ROOT_NAME,
        validation_alias="DATA_ROOT",
    )
    log_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_LOG_ROOT_NAME,
        validation_alias="LOG_ROOT",
    )
    model_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_MODEL_ROOT_NAME,
        validation_alias="MODEL_ROOT",
    )
    prompt_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_PROMPT_ROOT_NAME,
        validation_alias="PROMPT_ROOT",
    )
    report_root: Path = Field(
        default=PROJECT_ROOT / DEFAULT_REPORT_ROOT_NAME,
        validation_alias="REPORT_ROOT",
    )

    @field_validator(
        "config_root",
        "data_root",
        "log_root",
        "model_root",
        "prompt_root",
        "report_root",
        mode="before",
    )
    @classmethod
    def normalize_path(cls, value: object) -> Path:
        """Normalize absolute and project-relative configured paths."""
        if not isinstance(value, (str, Path)):
            message = "Configured paths must be strings or pathlib.Path values"
            raise TypeError(message)
        return resolve_project_path(value)


class Settings(BaseModel):
    """Complete immutable runtime configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    application: ApplicationSettings
    logging: LoggingSettings
    retry: RetrySettings
    aws: AWSSettings
    paths: PathSettings

    @model_validator(mode="after")
    def validate_managed_environment(self) -> Self:
        """Apply safety requirements for staging and production runtimes."""
        environment = self.application.environment
        if environment.is_deployed and self.aws.region is None:
            message = "AWS_REGION is required in staging and production"
            raise ValueError(message)

        if environment.is_production:
            if self.aws.profile is not None:
                message = "AWS_PROFILE is not permitted in production"
                raise ValueError(message)
            if self.aws.endpoint_url is not None:
                message = "AWS_ENDPOINT_URL is not permitted in production"
                raise ValueError(message)
            if self.logging.format is not LogFormat.JSON:
                message = "LOG_FORMAT must be json in production"
                raise ValueError(message)
            if self.logging.color:
                message = "LOG_COLOR must be false in production"
                raise ValueError(message)
        return self


def load_settings(env_file: PathValue | None = None) -> Settings:
    """Load and validate all configuration groups.

    Operating-system environment variables override values from the optional
    dotenv file according to Pydantic Settings precedence.

    Args:
        env_file: Optional local dotenv path. Production should use injected
            environment variables rather than a dotenv file.

    Returns:
        A fully validated immutable settings object.
    """
    selected_env_file = resolve_env_file(env_file)
    return Settings(
        application=ApplicationSettings(_env_file=selected_env_file),
        logging=LoggingSettings(_env_file=selected_env_file),
        retry=RetrySettings(_env_file=selected_env_file),
        aws=AWSSettings(_env_file=selected_env_file),
        paths=PathSettings(_env_file=selected_env_file),
    )
