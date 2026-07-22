"""Project-path discovery and normalization utilities.

This module resolves paths but never creates, deletes, or mutates filesystem
content. Runtime directories are provisioned by deployment infrastructure.
"""

from os import PathLike
from pathlib import Path
from typing import Final

from config.constants import DEFAULT_ENV_FILE_NAME

type PathValue = str | PathLike[str]

PACKAGE_ROOT: Final = Path(__file__).resolve().parent
PROJECT_ROOT: Final = PACKAGE_ROOT.parent
SOURCE_ROOT: Final = PROJECT_ROOT / "src"
DEFAULT_ENV_FILE: Final = PROJECT_ROOT / DEFAULT_ENV_FILE_NAME


def resolve_project_path(value: PathValue) -> Path:
    """Resolve a configured path without requiring it to exist.

    Relative paths are anchored to the repository root. Absolute paths are
    preserved, allowing the same configuration model to work inside a
    container or an AWS runtime.

    Args:
        value: Absolute or project-relative filesystem path.

    Returns:
        A normalized absolute path.
    """
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate.resolve(strict=False)
