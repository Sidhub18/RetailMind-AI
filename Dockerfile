# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm

FROM ${PYTHON_IMAGE} AS dependency-builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

COPY requirements.txt ./requirements.txt

RUN python -m pip wheel \
    --wheel-dir=/wheels \
    --requirement=requirements.txt


FROM ${PYTHON_IMAGE} AS runtime

ARG APP_UID=10001
ARG APP_GID=10001

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONPATH=/workspace/src

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        libgomp1 \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid "${APP_GID}" retailmind \
    && useradd \
        --uid "${APP_UID}" \
        --gid "${APP_GID}" \
        --create-home \
        --shell /usr/sbin/nologin \
        retailmind

COPY requirements.txt /tmp/requirements.txt
COPY --from=dependency-builder /wheels /wheels

RUN python -m pip install \
        --no-index \
        --find-links=/wheels \
        --requirement=/tmp/requirements.txt \
    && rm -rf /tmp/requirements.txt /wheels

WORKDIR /workspace

USER retailmind

CMD ["sleep", "infinity"]
