# RetailMind AI

Enterprise Demand Forecasting and Autonomous Inventory Replenishment Platform.

## Current phase

**Environment Setup**

This phase provides dependency, Python tooling, environment-variable, and
container configuration only. It contains no application, ETL, ML, agent, or
Power BI implementation code.

## Runtime baseline

- Python 3.12 only
- Default container runtime: Python 3.12.13 on Debian Bookworm slim
- Configuration through environment variables
- Non-root container execution
- Structured JSON logging dependency
- Exact direct-dependency pins

## Prerequisites

Use one of these supported setup paths:

- Local: CPython 3.12 and a current `pip` release.
- Container: Docker Desktop or Docker Engine with Docker Compose v2.

Git is required for normal repository workflows. AWS access is not required to
validate this environment, and credentials must not be configured until an
authorized AWS integration phase.

## Files in this phase

| File | Purpose |
|---|---|
| `requirements.txt` | Single source for direct production dependencies |
| `pyproject.toml` | Project metadata and quality, test, typing, and security-tool configuration |
| `.gitignore` | Prevents local state, credentials, data, models, and generated output from entering Git |
| `.env.example` | Documents safe environment-variable names and non-secret defaults |
| `Dockerfile` | Builds the Python, Java, PySpark, analytics, and ML workspace image |
| `docker-compose.yml` | Runs a hardened local workspace without application services |
| `README.md` | Documents setup, validation, and dependency decisions |

## Dependency management design

`requirements.txt` is the authoritative list of direct production
dependencies. `pyproject.toml` reads that file dynamically, avoiding two
independently maintained production dependency lists.

Development and testing tools are optional dependency groups:

- `dev` contains formatting, linting, typing, pre-commit, and security tools.
- `test` contains unit, property, asynchronous, AWS-mocking, parallel, and
  container integration-test tools.

Direct packages are pinned exactly. Transitive packages are resolved by pip.
A separate hash-locked deployment file should be added only when a later phase
authorizes dependency-lock generation.

## Production dependencies

| Dependency | Purpose |
|---|---|
| `boto3` | AWS SDK used by outer adapters for S3, DynamoDB, SQS, SageMaker, Bedrock, Secrets Manager, and other AWS services |
| `pydantic` | Type-driven validation for configuration and boundary contracts |
| `pydantic-settings` | Loads and validates environment-based settings without hardcoded configuration |
| `PyYAML` | Parses governed YAML configuration and policy documents |
| `orjson` | Fast JSON serialization for APIs, events, logs, and analytical metadata |
| `structlog` | Structured, machine-readable logging with contextual fields |
| `tenacity` | Explicit retry, backoff, and stop policies for transient integration failures |
| `fastapi` | Typed inbound HTTP adapter framework for the future API layer |
| `httpx` | Synchronous and asynchronous HTTP client for enterprise integrations and tests |
| `uvicorn` | ASGI runtime for FastAPI; the `standard` extra includes production protocol dependencies |
| `alembic` | Version-controlled SQLAlchemy database schema migrations |
| `psycopg` | PostgreSQL driver and connection pool for Aurora PostgreSQL-compatible persistence |
| `SQLAlchemy` | Persistence adapter toolkit; domain and application layers must not import it |
| `numpy` | Numerical arrays and vectorized computation for analytics and ML |
| `pandas` | Local tabular analysis, validation, and model dataset preparation |
| `pyarrow` | Arrow and Parquet interoperability across pandas, Spark, and the lakehouse |
| `pyspark` | Distributed ETL and feature engineering aligned with EMR Serverless |
| `mlflow-skinny` | Lightweight experiment and model-tracking client without the full MLflow server stack |
| `scikit-learn` | Baselines, preprocessing, metrics, and conventional ML models |
| `scipy` | Statistical and numerical algorithms used by forecasting and evaluation |
| `statsmodels` | Statistical time-series models and diagnostic tests |
| `xgboost` | Gradient-boosted tree forecasting candidate for structured retail features |

No general-purpose agent framework is installed. AWS Bedrock is accessible
through `boto3`, and adding another abstraction before agent requirements are
implemented would increase dependency and supply-chain risk unnecessarily.

## Build dependencies

| Dependency | Purpose |
|---|---|
| `setuptools` | PEP 517 build backend and dynamic loading of production requirements |
| `wheel` | Standard Python wheel artifact support for repeatable container installation |

## Development dependencies

| Dependency | Purpose |
|---|---|
| `ruff` | PEP 8 linting, import ordering, modernization, docstring enforcement, and formatting |
| `mypy` | Strict static type checking for Python 3.12 |
| `pre-commit` | Runs repository quality controls before a commit is created |
| `bandit` | Static security analysis for Python implementation code |
| `pip-audit` | Detects known vulnerabilities in installed Python packages |

## Test dependencies

| Dependency | Purpose |
|---|---|
| `pytest` | Primary automated-test runner |
| `pytest-cov` | Branch and statement coverage reporting |
| `pytest-asyncio` | Deterministic testing of asynchronous boundaries |
| `pytest-xdist` | Parallel test execution in CI |
| `hypothesis` | Property-based tests for domain invariants and edge cases |
| `moto` | In-process AWS service mocks for fast adapter tests |
| `testcontainers` | Disposable PostgreSQL and other real service dependencies for integration tests |

## Local setup on Windows PowerShell

```powershell
Copy-Item .env.example .env
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev,test]"
python -m pip check
```

## Local setup on Linux or macOS

```bash
cp .env.example .env
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,test]"
python -m pip check
```

## Docker setup

Docker Compose starts a development workspace only. It deliberately does not
start an API, database, message broker, or application process because those
would exceed this phase.

```powershell
Copy-Item .env.example .env
docker compose config
docker compose build --pull
docker compose up --detach
docker compose exec workspace python --version
docker compose exec workspace python -m pip check
docker compose down
```

The container:

- Runs as an unprivileged user.
- Drops Linux capabilities.
- Prevents privilege escalation.
- Uses a read-only root filesystem.
- Uses a writable temporary filesystem only for `/tmp`.
- Mounts the repository at `/workspace` for development.
- Installs Java 17 solely because local PySpark requires a JVM.

The image also installs `libgomp1`, the GNU OpenMP runtime required by compiled
numerical and gradient-boosting libraries such as XGBoost. Neither operating-
system package is an application dependency.

## Environment variables

| Variable | Purpose |
|---|---|
| `PYTHON_IMAGE` | Approved Python 3.12 container base |
| `IMAGE_NAME`, `IMAGE_TAG` | Local container identity |
| `APP_UID`, `APP_GID` | Non-root container identity |
| `CONTAINER_CPU_LIMIT`, `CONTAINER_MEMORY_LIMIT` | Local resource safeguards |
| `RETAILMIND_ENV` | Active application environment |
| `LOG_LEVEL`, `LOG_FORMAT` | Structured logging behavior |
| `CONFIG_ROOT` | Configuration root path |
| `DATA_ROOT` | Local data root path |
| `MODEL_ROOT` | Local model-artifact path |
| `PROMPT_ROOT` | Prompt-asset path |
| `REPORT_ROOT` | Generated report path |
| `AWS_REGION`, `AWS_PROFILE` | AWS Region and temporary workload identity profile |
| `AWS_ENDPOINT_URL` | Optional local AWS-compatible endpoint |
| `DATABASE_URL` | PostgreSQL connection string supplied at runtime |
| `OTEL_SERVICE_NAME` | OpenTelemetry service identity |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional telemetry collector endpoint |
| `ERP_BASE_URL` | Enterprise ERP integration endpoint |
| `ERP_SECRET_ARN` | Reference to ERP credentials in AWS Secrets Manager |

Never store AWS access keys, passwords, API tokens, or production connection
strings in `.env` or Git.

## Quality commands for future implementation phases

```powershell
ruff format --check .
ruff check .
mypy src etl ml agents utils
pytest
bandit -r src etl ml agents
pip-audit -r requirements.txt
```

No implementation exists yet, so the current environment checks are:

```powershell
python --version
python -m pip check
docker compose config
```
