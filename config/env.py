"""Environment-source primitives for immutable application settings."""

import os
from enum import StrEnum
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

from config.constants import (
    DEFAULT_CONFIG_ENCODING,
    ENV_FILE_VARIABLE,
)
from config.paths import DEFAULT_ENV_FILE, PathValue, resolve_project_path


class DeploymentEnvironment(StrEnum):
    """Supported deployment environments."""

    DEVELOPMENT = "development"
    TEST = "test"
    STAGING = "staging"
    PRODUCTION = "production"

    @property
    def is_production(self) -> bool:
        """Return whether production-only safety rules must apply."""
        return self is DeploymentEnvironment.PRODUCTION

    @property
    def is_deployed(self) -> bool:
        """Return whether the runtime represents a managed AWS environment."""
        return self in {
            DeploymentEnvironment.STAGING,
            DeploymentEnvironment.PRODUCTION,
        }


class EnvironmentSettings(BaseSettings):
    """Base class for environment-backed, immutable settings models."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file_encoding=DEFAULT_CONFIG_ENCODING,
        extra="ignore",
        frozen=True,
        populate_by_name=True,
        validate_default=True,
    )


def empty_string_to_none(value: object) -> object:
    """Convert blank environment-variable values to ``None``.

    Args:
        value: Raw value provided by Pydantic Settings.

    Returns:
        ``None`` for blank strings; otherwise the original value.
    """
    if isinstance(value, str) and not value.strip():
        return None
    return value


def resolve_env_file(explicit_path: PathValue | None = None) -> Path | None:
    """Resolve the dotenv file selected for local execution.

    Real environment variables retain precedence over dotenv values. When a
    caller or ``RETAILMIND_ENV_FILE`` explicitly names a missing file, loading
    fails instead of silently using an unintended configuration.

    Args:
        explicit_path: Optional dotenv path supplied by the caller.

    Returns:
        The resolved dotenv path, or ``None`` when the default file is absent.

    Raises:
        FileNotFoundError: If an explicitly configured dotenv file is missing.
    """
    configured_path = explicit_path or os.getenv(ENV_FILE_VARIABLE)
    if configured_path:
        resolved_path = resolve_project_path(configured_path)
        if not resolved_path.is_file():
            message = f"Configured environment file does not exist: {resolved_path}"
            raise FileNotFoundError(message)
        return resolved_path

    if DEFAULT_ENV_FILE.is_file():
        return DEFAULT_ENV_FILE
    return None
