"""Public configuration facade and AWS SDK configuration factory."""

from functools import lru_cache
from typing import Required, TypedDict

import boto3
from botocore.config import Config as BotocoreConfig

from config.logger import configure_logging
from config.settings import Settings, load_settings


class AWSClientArguments(TypedDict, total=False):
    """Typed common keyword arguments accepted by Boto3 clients."""

    config: Required[BotocoreConfig]
    use_ssl: Required[bool]
    endpoint_url: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide validated settings singleton."""
    return load_settings()


@lru_cache(maxsize=1)
def get_aws_client_config() -> BotocoreConfig:
    """Build immutable Botocore defaults for AWS service clients."""
    settings = get_settings()
    aws = settings.aws
    return BotocoreConfig(
        region_name=aws.region,
        retries={
            "mode": aws.retry_mode.value,
            "total_max_attempts": aws.max_attempts,
        },
        connect_timeout=aws.connect_timeout_seconds,
        read_timeout=aws.read_timeout_seconds,
        max_pool_connections=aws.max_pool_connections,
        tcp_keepalive=aws.tcp_keepalive,
        user_agent_appid=settings.application.name,
    )


def create_aws_session() -> boto3.Session:
    """Create a Boto3 session using the standard credential provider chain.

    A new session is returned on each call because Boto3 sessions should not be
    shared across threads or processes. No access keys or secret values are
    accepted by this configuration facade.

    Returns:
        A session configured with the optional AWS Region and local profile.
    """
    aws = get_settings().aws
    session_arguments: dict[str, str] = {}
    if aws.region is not None:
        session_arguments["region_name"] = aws.region
    if aws.profile is not None:
        session_arguments["profile_name"] = aws.profile
    return boto3.Session(**session_arguments)


def get_aws_client_arguments() -> AWSClientArguments:
    """Return common keyword arguments for creating AWS service clients.

    Returns:
        A new dictionary safe for expansion into ``Session.client``.
    """
    aws = get_settings().aws
    arguments = AWSClientArguments(
        config=get_aws_client_config(),
        use_ssl=aws.use_ssl,
    )
    if aws.endpoint_url is not None:
        arguments["endpoint_url"] = str(aws.endpoint_url)
    return arguments


def initialize_configuration() -> Settings:
    """Load settings and initialize process-wide structured logging."""
    settings = get_settings()
    configure_logging(settings.logging, settings.application)
    return settings


def reload_configuration() -> Settings:
    """Clear cached state and reload settings for tests or controlled startup.

    Returns:
        Newly loaded and validated settings.

    Notes:
        Production code should load configuration once at process startup.
    """
    get_aws_client_config.cache_clear()
    get_settings.cache_clear()

    return initialize_configuration()
