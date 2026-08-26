"""Confluent Cloud Flink Statements deployment adapter.

Implements the statement lifecycle:

    validate -> create-stopped -> status -> start   (plus stop / resume / manifest)

This module owns configuration loading, release validation, SQL hashing, the
REST client, lifecycle polling, and manifest writing. Presentation lives in
``adapter/cli.py``.

Credentials are read from the environment only (``FLINK_API_KEY`` /
``FLINK_API_SECRET``) and are never logged.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import requests
import yaml

DEFAULT_CONFIG_PATH = "config/demo-dev.yaml"
DEFAULT_MANIFEST_DIR = "manifests"
DEFAULT_DEMO_TOPIC_PREFIX = "flink-demo-"
DEFAULT_POLL_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 30

# Confluent Cloud starts every statement on create, so create-stopped has to
# follow up with a PATCH. A statement that is still PENDING can reject that
# update, hence the short retry.
STOP_AFTER_CREATE_ATTEMPTS = 5
STOP_AFTER_CREATE_INTERVAL_SECONDS = 3
# How long a phase listed in ``stall_phases`` may persist before polling gives
# up. Short enough to fail well inside the default poll timeout.
STALL_GRACE_SECONDS = 45

API_KEY_ENV = "FLINK_API_KEY"
API_SECRET_ENV = "FLINK_API_SECRET"

REQUIRED_FLINK_KEYS = (
    "organization_id",
    "environment_id",
    "cloud_provider",
    "cloud_region",
    "compute_pool_id",
)

RUNNING_PHASES = frozenset({"RUNNING"})
STOPPED_PHASES = frozenset({"STOPPED"})
FAILURE_PHASES = frozenset({"FAILED", "FAILING", "DELETING"})

STATEMENT_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,99}$")
INSERT_INTO_RE = re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE)
LINE_COMMENT_RE = re.compile(r"--[^\n]*")
BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)

Log = Callable[[str], None]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class AdapterError(Exception):
    """Base class for every error this adapter reports to the CLI."""


class ConfigError(AdapterError):
    """Configuration is missing, unreadable, or structurally wrong."""


class ValidationError(AdapterError):
    """A release failed pre-flight validation."""


class ApiError(AdapterError):
    """The Flink Statements API returned an error or was unreachable."""


class LifecycleError(AdapterError):
    """A statement reached a failing phase or the poll timeout expired."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Pipeline:
    """A single deployable pipeline entry from ``pipelines:`` in the config."""

    name: str
    statement_name: str
    sql_path: Path
    topics: dict[str, str]
    schemas: dict[str, str]


@dataclass(frozen=True)
class Config:
    path: Path
    root: Path
    data: dict[str, Any]

    @property
    def environment_name(self) -> str:
        return str(self.data.get("environment_name") or self.path.stem)

    @property
    def flink(self) -> dict[str, Any]:
        return dict(self.data.get("flink") or {})

    @property
    def policy(self) -> dict[str, Any]:
        return dict(self.data.get("policy") or {})

    @property
    def poll_timeout_seconds(self) -> int:
        value = self.policy.get("poll_timeout_seconds", DEFAULT_POLL_TIMEOUT_SECONDS)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return DEFAULT_POLL_TIMEOUT_SECONDS

    @property
    def demo_topic_prefix(self) -> str:
        return str(self.policy.get("demo_topic_prefix") or DEFAULT_DEMO_TOPIC_PREFIX)

    @property
    def pipeline_names(self) -> list[str]:
        return sorted((self.data.get("pipelines") or {}).keys())

    def pipeline(self, name: str) -> Pipeline:
        pipelines = self.data.get("pipelines") or {}
        entry = pipelines.get(name)
        if not isinstance(entry, dict):
            available = ", ".join(self.pipeline_names) or "<none>"
            raise ConfigError(
                f"Unknown pipeline '{name}' in {self.path}. Available: {available}"
            )
        topics = {k: str(v) for k, v in entry.items() if k.endswith("_topic")}
        schemas = {k: str(v) for k, v in entry.items() if k.endswith("_schema")}
        return Pipeline(
            name=name,
            statement_name=str(entry.get("statement_name") or ""),
            sql_path=self.resolve_path(str(entry.get("sql_file") or "")),
            topics=topics,
            schemas=schemas,
        )

    def resolve_path(self, value: str) -> Path:
        """Resolve a repo-relative path from the config.

        Tries the current working directory first, then the repository root
        inferred from the config file location, so the adapter behaves the same
        whether it is invoked from the repo root or from elsewhere.
        """
        if not value:
            return Path("")
        candidate = Path(value)
        if candidate.is_absolute():
            return candidate
        for base in (Path.cwd(), self.root, self.path.parent):
            resolved = base / candidate
            if resolved.exists():
                return resolved
        return Path.cwd() / candidate


def load_config(config_path: str | Path) -> Config:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # pragma: no cover - depends on user input
        raise ConfigError(f"Configuration file {path} is not valid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"Configuration file {path} must contain a YAML mapping")
    resolved = path.resolve()
    # config/demo-dev.yaml -> repository root is two levels up.
    root = resolved.parent.parent if resolved.parent.name == "config" else resolved.parent
    return Config(path=resolved, root=root, data=data)


def credentials_from_env() -> tuple[str | None, str | None]:
    """Return the Flink API credentials from the environment (never logged)."""
    key = os.environ.get(API_KEY_ENV) or None
    secret = os.environ.get(API_SECRET_ENV) or None
    return key, secret


# ---------------------------------------------------------------------------
# SQL hashing
# ---------------------------------------------------------------------------


def normalize_sql(sql: str) -> str:
    """Normalize SQL so the same statement always hashes to the same value."""
    lines = [line.rstrip() for line in sql.replace("\r\n", "\n").split("\n")]
    return "\n".join(line for line in lines).strip() + "\n"


def sql_hash(sql: str) -> str:
    """SHA-256 of the statement text alone.

    Comparable against the SQL returned by the API, which is what ``start``
    uses to confirm the remote statement is the one in this release.
    """
    return hashlib.sha256(normalize_sql(sql).encode("utf-8")).hexdigest()


def release_hash(config: Config, pipeline: Pipeline, sql: str) -> str:
    """SHA-256 of the SQL plus the configuration that shapes the statement."""
    flink = config.flink
    material = {
        "sql": sql_hash(sql),
        "statement_name": pipeline.statement_name,
        "environment_id": flink.get("environment_id"),
        "compute_pool_id": flink.get("compute_pool_id"),
        "catalog": flink.get("catalog"),
        "database": flink.get("database"),
        "principal_id": flink.get("principal_id"),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def read_sql(pipeline: Pipeline) -> str:
    if not pipeline.sql_path or not pipeline.sql_path.exists():
        raise ValidationError(f"SQL file not found: {pipeline.sql_path}")
    sql = pipeline.sql_path.read_text(encoding="utf-8")
    if not sql.strip():
        raise ValidationError(f"SQL file is empty: {pipeline.sql_path}")
    return sql


def strip_sql_comments(sql: str) -> str:
    return LINE_COMMENT_RE.sub(" ", BLOCK_COMMENT_RE.sub(" ", sql))


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str
    severity: str = "error"  # "error" or "warning"

    @property
    def failed(self) -> bool:
        return not self.ok and self.severity == "error"

    @property
    def warned(self) -> bool:
        return not self.ok and self.severity == "warning"


def validate(
    config_path: str | Path,
    *,
    pipelines: Sequence[str] | None = None,
    require_credentials: bool = False,
) -> list[Check]:
    """Run every pre-flight check. Makes no API calls and changes nothing."""
    checks: list[Check] = []
    path = Path(config_path)
    try:
        config = load_config(path)
    except ConfigError as exc:
        return [Check("configuration file", False, str(exc))]
    checks.append(Check("configuration file", True, str(config.path)))

    flink = config.flink
    missing = [key for key in REQUIRED_FLINK_KEYS if not flink.get(key)]
    checks.append(
        Check(
            "flink identifiers",
            not missing,
            "all required identifiers present"
            if not missing
            else f"missing flink.{', flink.'.join(missing)}",
        )
    )

    for key in ("catalog", "database"):
        checks.append(
            Check(
                f"flink {key}",
                bool(flink.get(key)),
                str(flink.get(key))
                if flink.get(key)
                else f"flink.{key} not set; statements must fully qualify table names",
                severity="warning",
            )
        )

    policy = config.policy
    for key in ("allow_topic_create", "allow_topic_delete"):
        value = policy.get(key, False)
        checks.append(
            Check(
                f"policy {key}",
                value is False,
                "disabled" if value is False else f"must be false, found {value!r}",
            )
        )
    checks.append(
        Check(
            "policy create_stopped",
            policy.get("create_stopped", True) is True,
            "enabled"
            if policy.get("create_stopped", True) is True
            else "create_stopped must be true for this boilerplate",
        )
    )

    key, secret = credentials_from_env()
    have_credentials = bool(key and secret)
    missing_env = [
        name
        for name, value in ((API_KEY_ENV, key), (API_SECRET_ENV, secret))
        if not value
    ]
    checks.append(
        Check(
            "credentials",
            have_credentials,
            "present in environment"
            if have_credentials
            else f"missing {', '.join(missing_env)}",
            severity="error" if require_credentials else "warning",
        )
    )

    names = list(pipelines) if pipelines else config.pipeline_names
    if not names:
        checks.append(Check("pipelines", False, "no pipelines defined in configuration"))
        return checks

    for name in names:
        checks.extend(_validate_pipeline(config, name))
    return checks


def _validate_pipeline(config: Config, name: str) -> list[Check]:
    checks: list[Check] = []
    try:
        pipeline = config.pipeline(name)
    except ConfigError as exc:
        return [Check(f"pipeline {name}", False, str(exc))]

    label = f"pipeline {name}"
    valid_name = bool(STATEMENT_NAME_RE.match(pipeline.statement_name))
    checks.append(
        Check(
            f"{label} statement name",
            valid_name,
            pipeline.statement_name
            if valid_name
            else f"invalid or missing statement_name {pipeline.statement_name!r} "
            "(lowercase letters, digits and hyphens)",
        )
    )

    sql = ""
    try:
        sql = read_sql(pipeline)
        checks.append(Check(f"{label} sql file", True, str(pipeline.sql_path)))
    except ValidationError as exc:
        checks.append(Check(f"{label} sql file", False, str(exc)))

    if sql:
        has_insert = bool(INSERT_INTO_RE.search(strip_sql_comments(sql)))
        checks.append(
            Check(
                f"{label} sql shape",
                has_insert,
                "contains INSERT INTO"
                if has_insert
                else "statement must contain INSERT INTO (continuous DML)",
            )
        )
        checks.append(Check(f"{label} sql hash", True, sql_hash(sql)))

    prefix = config.demo_topic_prefix
    if not pipeline.topics:
        checks.append(
            Check(f"{label} topics", False, "no *_topic entries configured")
        )
    for topic_key, topic in sorted(pipeline.topics.items()):
        dedicated = topic.startswith(prefix)
        checks.append(
            Check(
                f"{label} {topic_key}",
                dedicated,
                topic
                if dedicated
                else f"{topic!r} is not a dedicated demo topic (expected prefix {prefix!r})",
            )
        )

    for schema_key, schema in sorted(pipeline.schemas.items()):
        schema_path = config.resolve_path(schema)
        checks.append(
            Check(
                f"{label} {schema_key}",
                schema_path.exists(),
                str(schema_path) if schema_path.exists() else f"not found: {schema_path}",
                severity="warning",
            )
        )
    return checks


def raise_on_failed_checks(checks: Iterable[Check]) -> None:
    failed = [check for check in checks if check.failed]
    if failed:
        details = "\n".join(f"  - {check.name}: {check.detail}" for check in failed)
        raise ValidationError(f"Validation failed:\n{details}")


# ---------------------------------------------------------------------------
# REST client
# ---------------------------------------------------------------------------


class FlinkStatementsClient:
    """Thin client over the Confluent Cloud Flink Statements REST API."""

    def __init__(
        self,
        config: Config,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        self.config = config
        flink = config.flink
        missing = [key for key in REQUIRED_FLINK_KEYS if not flink.get(key)]
        if missing:
            raise ConfigError(
                "Configuration is missing required identifiers: "
                + ", ".join(f"flink.{key}" for key in missing)
            )
        if api_key is None or api_secret is None:
            env_key, env_secret = credentials_from_env()
            api_key = api_key or env_key
            api_secret = api_secret or env_secret
        if not api_key or not api_secret:
            raise ConfigError(
                f"{API_KEY_ENV} and {API_SECRET_ENV} must be set in the environment"
            )
        self.organization_id = str(flink["organization_id"])
        self.environment_id = str(flink["environment_id"])
        self.compute_pool_id = str(flink["compute_pool_id"])
        self.principal_id = str(flink.get("principal_id") or "") or None
        self.catalog = str(flink.get("catalog") or "") or None
        self.database = str(flink.get("database") or "") or None
        self.base_url = (
            f"https://flink.{flink['cloud_region']}.{flink['cloud_provider']}"
            f".confluent.cloud/sql/v1/organizations/{self.organization_id}"
            f"/environments/{self.environment_id}"
        )
        self.timeout = timeout
        self._session = session or requests.Session()
        self._session.auth = (api_key, api_secret)

    # -- HTTP -------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        content_type: str | None = None,
        allow_404: bool = False,
    ) -> dict[str, Any] | None:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if json_body is not None:
            headers["Content-Type"] = content_type or "application/json"
        try:
            response = self._session.request(
                method,
                url,
                json=json_body,
                headers=headers,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise ApiError(f"{method} {url} failed: {exc}") from exc

        if response.status_code == 404 and allow_404:
            return None
        if response.status_code == 401 or response.status_code == 403:
            raise ApiError(
                f"{method} {path} was rejected ({response.status_code}): "
                f"{_error_detail(response)}. "
                f"Check the {API_KEY_ENV}/{API_SECRET_ENV} credentials and their RBAC roles."
            )
        if response.status_code >= 400:
            raise ApiError(
                f"{method} {path} failed with HTTP {response.status_code}: "
                f"{_error_detail(response)}"
            )
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - unexpected API response
            raise ApiError(f"{method} {path} returned a non-JSON response") from exc

    # -- Statements -------------------------------------------------------

    def get_statement(self, name: str) -> dict[str, Any] | None:
        """Return the statement resource, or ``None`` when it does not exist."""
        return self._request("GET", f"/statements/{name}", allow_404=True)

    def create_statement(self, name: str, sql: str) -> dict[str, Any]:
        """Create the statement.

        ``spec.stopped: true`` is sent, but Confluent Cloud ignores it on
        create and starts the statement regardless. It is kept in the payload
        so this works unchanged if the API ever honours it; until then callers
        must follow up with :meth:`stop_after_create`.
        """
        spec: dict[str, Any] = {
            "statement": sql,
            "compute_pool_id": self.compute_pool_id,
            "stopped": True,
        }
        if self.principal_id:
            spec["principal"] = self.principal_id
        properties: dict[str, str] = {}
        if self.catalog:
            properties["sql.current-catalog"] = self.catalog
        if self.database:
            properties["sql.current-database"] = self.database
        if properties:
            spec["properties"] = properties
        payload = {
            "name": name,
            "organization_id": self.organization_id,
            "environment_id": self.environment_id,
            "spec": spec,
        }
        result = self._request("POST", "/statements", json_body=payload)
        return result or {}

    def set_stopped(self, name: str, stopped: bool) -> dict[str, Any]:
        """PATCH ``/spec/stopped`` (sections 9.5 and 9.6)."""
        patch = [{"op": "replace", "path": "/spec/stopped", "value": stopped}]
        try:
            result = self._request(
                "PATCH",
                f"/statements/{name}",
                json_body=patch,
                content_type="application/json-patch+json",
            )
            return result or {}
        except ApiError as exc:
            if not _is_method_unsupported(exc):
                raise
        # Some API revisions expect a full-resource update instead of a patch.
        current = self.get_statement(name)
        if current is None:
            raise ApiError(f"Statement '{name}' does not exist")
        body = {key: value for key, value in current.items() if key != "status"}
        body["spec"] = dict(current.get("spec") or {})
        body["spec"]["stopped"] = stopped
        result = self._request("PUT", f"/statements/{name}", json_body=body)
        return result or {}

    def stop_after_create(
        self,
        name: str,
        *,
        attempts: int = STOP_AFTER_CREATE_ATTEMPTS,
        interval_seconds: int = STOP_AFTER_CREATE_INTERVAL_SECONDS,
        log: Log | None = None,
    ) -> dict[str, Any]:
        """Disable processing on a statement that was just created.

        The API offers no atomic create-stopped: the statement is live from the
        moment it is created, so this is called immediately afterwards to shrink
        the window rather than remove it. A statement still in PENDING can
        reject the update, so retry before giving up.
        """
        last_error: ApiError | None = None
        for attempt in range(1, attempts + 1):
            try:
                return self.set_stopped(name, True)
            except ApiError as exc:
                last_error = exc
                if attempt == attempts:
                    break
                if log:
                    log(
                        f"  stop request rejected (attempt {attempt}/{attempts}), "
                        f"retrying in {interval_seconds}s"
                    )
                time.sleep(interval_seconds)
        raise LifecycleError(
            f"Statement '{name}' was created but could not be stopped after "
            f"{attempts} attempts: {last_error}. It may still be processing - "
            f"stop it before continuing."
        ) from last_error

    # -- Polling ----------------------------------------------------------

    def poll_until(
        self,
        name: str,
        *,
        target_phases: Iterable[str],
        timeout_seconds: int,
        failure_phases: Iterable[str] = FAILURE_PHASES,
        interval_seconds: int = POLL_INTERVAL_SECONDS,
        stall_phases: Iterable[str] = (),
        stall_grace_seconds: int = STALL_GRACE_SECONDS,
        log: Log | None = None,
    ) -> dict[str, Any]:
        """Poll the statement until a target/failure phase or the timeout.

        Always bounded by ``timeout_seconds`` - never an unbounded loop.

        ``stall_phases`` are phases that are legitimate while a requested
        transition is in flight but mean the request did not take effect if
        they persist past ``stall_grace_seconds``. Waiting for STOPPED passes
        RUNNING here: a statement briefly stays RUNNING while it winds down,
        but one that sits there is never going to stop on its own.
        """
        targets = {phase.upper() for phase in target_phases}
        failures = {phase.upper() for phase in failure_phases} - targets
        stalls = {phase.upper() for phase in stall_phases} - targets - failures
        deadline = time.monotonic() + max(1, timeout_seconds)
        stall_deadline: float | None = None
        last_phase: str | None = None
        statement: dict[str, Any] | None = None

        while True:
            statement = self.get_statement(name)
            if statement is None:
                raise ApiError(f"Statement '{name}' disappeared while polling")
            phase = statement_phase(statement)
            if phase != last_phase:
                last_phase = phase
                if log:
                    log(f"  phase: {phase}")
            if phase in targets:
                return statement
            if phase in failures:
                raise LifecycleError(
                    f"Statement '{name}' entered phase {phase}: "
                    f"{statement_detail(statement) or 'no detail reported'}"
                )
            if phase in stalls:
                if stall_deadline is None:
                    stall_deadline = time.monotonic() + max(1, stall_grace_seconds)
                elif time.monotonic() >= stall_deadline:
                    raise LifecycleError(
                        f"Statement '{name}' stayed in phase {phase} for over "
                        f"{stall_grace_seconds}s while waiting for "
                        f"{'/'.join(sorted(targets))}. The transition was accepted "
                        f"but never took effect"
                        + (
                            f": {statement_detail(statement)}"
                            if statement_detail(statement)
                            else "."
                        )
                    )
            else:
                stall_deadline = None
            if time.monotonic() >= deadline:
                raise LifecycleError(
                    f"Timed out after {timeout_seconds}s waiting for statement "
                    f"'{name}' to reach {'/'.join(sorted(targets))}; last phase was {phase}"
                )
            time.sleep(min(interval_seconds, max(1, deadline - time.monotonic())))


def _error_detail(response: requests.Response) -> str:
    """Extract a readable message from an API error body (no secrets echoed)."""
    try:
        body = response.json()
    except ValueError:
        return response.text.strip()[:500] or "<empty response body>"
    if isinstance(body, dict):
        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            parts = []
            for error in errors:
                if isinstance(error, dict):
                    parts.append(
                        str(error.get("detail") or error.get("title") or error)
                    )
                else:
                    parts.append(str(error))
            return "; ".join(parts)[:500]
        for key in ("message", "detail", "error"):
            if body.get(key):
                return str(body[key])[:500]
    return json.dumps(body)[:500]


def _is_method_unsupported(exc: ApiError) -> bool:
    return any(f"HTTP {code}" in str(exc) for code in (405, 415, 501))


def statement_phase(statement: dict[str, Any] | None) -> str:
    if not statement:
        return "UNKNOWN"
    return str((statement.get("status") or {}).get("phase") or "UNKNOWN").upper()


def statement_detail(statement: dict[str, Any] | None) -> str:
    if not statement:
        return ""
    return str((statement.get("status") or {}).get("detail") or "")


def statement_id(statement: dict[str, Any] | None) -> str:
    if not statement:
        return ""
    return str((statement.get("metadata") or {}).get("uid") or "")


def statement_sql(statement: dict[str, Any] | None) -> str:
    if not statement:
        return ""
    return str((statement.get("spec") or {}).get("statement") or "")


def is_processing(statement: dict[str, Any] | None) -> bool:
    """True when the statement is actually processing records."""
    if not statement:
        return False
    stopped = (statement.get("spec") or {}).get("stopped")
    return statement_phase(statement) in RUNNING_PHASES and stopped is not True


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def git_commit(root: Path) -> str:
    commit = os.environ.get("GITHUB_SHA")
    if commit:
        return commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def write_manifest(
    *,
    config: Config,
    pipeline: Pipeline,
    operation: str,
    statement: dict[str, Any] | None,
    sql_sha256: str,
    release_sha256: str,
    manifest_dir: str | Path = DEFAULT_MANIFEST_DIR,
    notes: str | None = None,
) -> Path:
    """Record what was deployed and what was observed (acceptance criteria)."""
    timestamp = utc_now()
    directory = Path(manifest_dir)
    if not directory.is_absolute():
        directory = (
            directory if directory.exists() else config.root / manifest_dir
        )
    directory.mkdir(parents=True, exist_ok=True)

    spec = (statement or {}).get("spec") or {}
    manifest = {
        "manifest_version": 1,
        "operation": operation,
        "timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "git_commit": git_commit(config.root),
        "config_file": str(_relative_to_root(config.path, config.root)),
        "environment_name": config.environment_name,
        "flink": {
            "organization_id": config.flink.get("organization_id"),
            "environment_id": config.flink.get("environment_id"),
            "compute_pool_id": spec.get("compute_pool_id")
            or config.flink.get("compute_pool_id"),
            "principal": spec.get("principal") or config.flink.get("principal_id"),
            "catalog": config.flink.get("catalog"),
            "database": config.flink.get("database"),
        },
        "pipeline": pipeline.name,
        "statement": {
            "name": pipeline.statement_name,
            "id": statement_id(statement),
            "phase": statement_phase(statement),
            "detail": statement_detail(statement),
            "stopped": spec.get("stopped"),
            "processing": is_processing(statement),
        },
        "sql": {
            "file": str(_relative_to_root(pipeline.sql_path, config.root)),
            "sha256": sql_sha256,
            "release_sha256": release_sha256,
        },
        "resources": dict(sorted(pipeline.topics.items())),
    }
    if notes:
        manifest["notes"] = notes

    stamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
    base = f"{stamp}-{pipeline.statement_name}-{operation}"
    path = directory / f"{base}.json"
    suffix = 1
    while path.exists():  # never overwrite an existing run record
        path = directory / f"{base}-{suffix}.json"
        suffix += 1
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path


def _relative_to_root(path: Path, root: Path) -> Path:
    try:
        return path.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return path


# ---------------------------------------------------------------------------
# Lifecycle operations
# ---------------------------------------------------------------------------


@dataclass
class OperationResult:
    operation: str
    pipeline: str
    statement_name: str
    statement: dict[str, Any] | None
    sql_sha256: str
    release_sha256: str
    manifest_path: Path | None = None
    notes: list[str] = field(default_factory=list)
    changed: bool = False

    @property
    def phase(self) -> str:
        return statement_phase(self.statement)

    @property
    def statement_id(self) -> str:
        return statement_id(self.statement)

    @property
    def processing(self) -> bool:
        return is_processing(self.statement)


@dataclass
class Release:
    """A validated pipeline release: config + SQL + hashes."""

    config: Config
    pipeline: Pipeline
    sql: str
    sql_sha256: str
    release_sha256: str


def prepare_release(
    config_path: str | Path,
    pipeline_name: str,
    *,
    log: Log | None = None,
) -> Release:
    """Load config, validate the release, and read the SQL (sections 9.2/9.3)."""
    checks = validate(config_path, pipelines=[pipeline_name], require_credentials=True)
    raise_on_failed_checks(checks)
    config = load_config(config_path)
    pipeline = config.pipeline(pipeline_name)
    sql = read_sql(pipeline)
    if log:
        log(f"Validation passed for pipeline '{pipeline_name}'")
    return Release(
        config=config,
        pipeline=pipeline,
        sql=sql,
        sql_sha256=sql_hash(sql),
        release_sha256=release_hash(config, pipeline, sql),
    )


def create_stopped(
    config_path: str | Path,
    pipeline_name: str,
    *,
    manifest_dir: str | Path = DEFAULT_MANIFEST_DIR,
    wait: bool = True,
    timeout_seconds: int | None = None,
    client: FlinkStatementsClient | None = None,
    log: Log | None = None,
) -> OperationResult:
    """Create the statement with processing disabled."""
    release = prepare_release(config_path, pipeline_name, log=log)
    config, pipeline = release.config, release.pipeline
    api = client or FlinkStatementsClient(config)
    timeout = timeout_seconds or config.poll_timeout_seconds
    notes: list[str] = []

    existing = api.get_statement(pipeline.statement_name)
    if existing is not None:
        note = (
            f"Statement '{pipeline.statement_name}' already exists; reporting it "
            "instead of creating a duplicate."
        )
        notes.append(note)
        if statement_sql(existing) and sql_hash(statement_sql(existing)) != release.sql_sha256:
            notes.append(
                "Remote SQL does not match the local release. Submitted SQL is "
                "immutable: a SQL change requires a new statement name."
            )
        if log:
            for entry in notes:
                log(entry)
        manifest_path = write_manifest(
            config=config,
            pipeline=pipeline,
            operation="create-stopped",
            statement=existing,
            sql_sha256=release.sql_sha256,
            release_sha256=release.release_sha256,
            manifest_dir=manifest_dir,
            notes=" ".join(notes),
        )
        return OperationResult(
            operation="create-stopped",
            pipeline=pipeline_name,
            statement_name=pipeline.statement_name,
            statement=existing,
            sql_sha256=release.sql_sha256,
            release_sha256=release.release_sha256,
            manifest_path=manifest_path,
            notes=notes,
            changed=False,
        )

    if log:
        log(f"Creating statement '{pipeline.statement_name}'")
    statement = api.create_statement(pipeline.statement_name, release.sql)

    # Confluent Cloud starts the statement on create regardless of
    # spec.stopped, so processing is disabled as a separate step. The statement
    # is live between the two calls and may process records in that window;
    # the API has no atomic create-stopped, so this narrows it rather than
    # closing it. Treat create-stopped as "not left running", not as a
    # guarantee that nothing was ever read.
    try:
        if log:
            log("Requesting spec.stopped=true (the API starts statements on create)")
        statement = api.stop_after_create(pipeline.statement_name, log=log) or statement

        if wait:
            if log:
                log(f"Polling for STOPPED (timeout {timeout}s)")
            statement = api.poll_until(
                pipeline.statement_name,
                target_phases=STOPPED_PHASES,
                timeout_seconds=timeout,
                stall_phases=RUNNING_PHASES,
                log=log,
            )
    except LifecycleError:
        latest = api.get_statement(pipeline.statement_name)
        write_manifest(
            config=config,
            pipeline=pipeline,
            operation="create-stopped",
            statement=latest,
            sql_sha256=release.sql_sha256,
            release_sha256=release.release_sha256,
            manifest_dir=manifest_dir,
            notes="create-stopped did not reach STOPPED",
        )
        raise

    notes.append(
        "Created and then stopped: the API starts statements on create, so the "
        "statement was briefly live before spec.stopped=true was applied."
    )

    manifest_path = write_manifest(
        config=config,
        pipeline=pipeline,
        operation="create-stopped",
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_dir=manifest_dir,
    )
    return OperationResult(
        operation="create-stopped",
        pipeline=pipeline_name,
        statement_name=pipeline.statement_name,
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_path=manifest_path,
        notes=notes,
        changed=True,
    )


def status(
    config_path: str | Path,
    pipeline_name: str,
    *,
    manifest_dir: str | Path = DEFAULT_MANIFEST_DIR,
    write_manifest_file: bool = False,
    wait_for: str | None = None,
    timeout_seconds: int | None = None,
    client: FlinkStatementsClient | None = None,
    log: Log | None = None,
) -> OperationResult:
    """Report the observed statement state."""
    release = prepare_release(config_path, pipeline_name, log=log)
    config, pipeline = release.config, release.pipeline
    api = client or FlinkStatementsClient(config)
    timeout = timeout_seconds or config.poll_timeout_seconds
    notes: list[str] = []

    if wait_for:
        statement = api.poll_until(
            pipeline.statement_name,
            target_phases=[wait_for],
            timeout_seconds=timeout,
            log=log,
        )
    else:
        statement = api.get_statement(pipeline.statement_name)

    if statement is None:
        notes.append(
            f"Statement '{pipeline.statement_name}' does not exist in environment "
            f"{config.flink.get('environment_id')}."
        )
    else:
        remote_sql = statement_sql(statement)
        if remote_sql and sql_hash(remote_sql) != release.sql_sha256:
            notes.append("Remote SQL hash differs from the local release SQL hash.")

    manifest_path = None
    if write_manifest_file:
        manifest_path = write_manifest(
            config=config,
            pipeline=pipeline,
            operation="status",
            statement=statement,
            sql_sha256=release.sql_sha256,
            release_sha256=release.release_sha256,
            manifest_dir=manifest_dir,
            notes=" ".join(notes) or None,
        )

    return OperationResult(
        operation="status",
        pipeline=pipeline_name,
        statement_name=pipeline.statement_name,
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_path=manifest_path,
        notes=notes,
        changed=False,
    )


def set_statement_stopped(
    config_path: str | Path,
    pipeline_name: str,
    *,
    operation: str,
    stopped: bool,
    manifest_dir: str | Path = DEFAULT_MANIFEST_DIR,
    wait: bool = True,
    timeout_seconds: int | None = None,
    allow_sql_mismatch: bool = False,
    enforce_sql_hash: bool = True,
    client: FlinkStatementsClient | None = None,
    log: Log | None = None,
) -> OperationResult:
    """Shared implementation of start, stop, and resume (sections 9.5/9.6)."""
    release = prepare_release(config_path, pipeline_name, log=log)
    config, pipeline = release.config, release.pipeline
    api = client or FlinkStatementsClient(config)
    timeout = timeout_seconds or config.poll_timeout_seconds
    notes: list[str] = []

    statement = api.get_statement(pipeline.statement_name)
    if statement is None:
        raise LifecycleError(
            f"Statement '{pipeline.statement_name}' does not exist. "
            "Run create-stopped first."
        )

    remote_environment = str(statement.get("environment_id") or "")
    expected_environment = str(config.flink.get("environment_id") or "")
    if remote_environment and remote_environment != expected_environment:
        raise LifecycleError(
            f"Statement '{pipeline.statement_name}' belongs to environment "
            f"{remote_environment}, not {expected_environment}"
        )

    remote_sql = statement_sql(statement)
    if remote_sql:
        remote_hash = sql_hash(remote_sql)
        if remote_hash != release.sql_sha256:
            mismatch = (
                "SQL hash mismatch: the deployed statement does not match the local "
                f"release (deployed {remote_hash[:12]}..., local "
                f"{release.sql_sha256[:12]}...)."
            )
            if not enforce_sql_hash:
                notes.append(f"{mismatch} Not enforced for {operation}.")
            elif allow_sql_mismatch:
                notes.append(f"Overridden: {mismatch}")
            else:
                raise ValidationError(
                    f"{mismatch} Submitted SQL is immutable; deploy a new statement "
                    "name or pass --allow-sql-mismatch to override."
                )
            if log:
                log(notes[-1])

    current_stopped = (statement.get("spec") or {}).get("stopped")
    changed = True
    if current_stopped == stopped:
        notes.append(
            f"Statement already has spec.stopped={str(stopped).lower()}; no change submitted."
        )
        changed = False
        if log:
            log(notes[-1])
    else:
        if log:
            log(
                f"Setting spec.stopped={str(stopped).lower()} on "
                f"'{pipeline.statement_name}'"
            )
        statement = api.set_stopped(pipeline.statement_name, stopped)

    target = STOPPED_PHASES if stopped else RUNNING_PHASES
    # The phase being transitioned away from is expected briefly; if it sticks,
    # the update was accepted but did nothing, so stop waiting on the timeout.
    stalls = RUNNING_PHASES if stopped else STOPPED_PHASES
    if wait:
        if log:
            log(f"Polling for {'/'.join(sorted(target))} (timeout {timeout}s)")
        try:
            statement = api.poll_until(
                pipeline.statement_name,
                target_phases=target,
                timeout_seconds=timeout,
                stall_phases=stalls,
                log=log,
            )
        except LifecycleError:
            latest = api.get_statement(pipeline.statement_name)
            write_manifest(
                config=config,
                pipeline=pipeline,
                operation=operation,
                statement=latest,
                sql_sha256=release.sql_sha256,
                release_sha256=release.release_sha256,
                manifest_dir=manifest_dir,
                notes=f"{operation} did not reach {'/'.join(sorted(target))}",
            )
            raise

    manifest_path = write_manifest(
        config=config,
        pipeline=pipeline,
        operation=operation,
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_dir=manifest_dir,
        notes=" ".join(notes) or None,
    )
    return OperationResult(
        operation=operation,
        pipeline=pipeline_name,
        statement_name=pipeline.statement_name,
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_path=manifest_path,
        notes=notes,
        changed=changed,
    )


def start(config_path: str | Path, pipeline_name: str, **kwargs: Any) -> OperationResult:
    return set_statement_stopped(
        config_path, pipeline_name, operation="start", stopped=False, **kwargs
    )


def stop(config_path: str | Path, pipeline_name: str, **kwargs: Any) -> OperationResult:
    kwargs.pop("allow_sql_mismatch", None)
    return set_statement_stopped(
        config_path,
        pipeline_name,
        operation="stop",
        stopped=True,
        enforce_sql_hash=False,
        **kwargs,
    )


def resume(config_path: str | Path, pipeline_name: str, **kwargs: Any) -> OperationResult:
    return set_statement_stopped(
        config_path, pipeline_name, operation="resume", stopped=False, **kwargs
    )


def manifest(
    config_path: str | Path,
    pipeline_name: str,
    *,
    manifest_dir: str | Path = DEFAULT_MANIFEST_DIR,
    client: FlinkStatementsClient | None = None,
    log: Log | None = None,
) -> OperationResult:
    """Record the current observed state without changing anything."""
    release = prepare_release(config_path, pipeline_name, log=log)
    config, pipeline = release.config, release.pipeline
    api = client or FlinkStatementsClient(config)
    statement = api.get_statement(pipeline.statement_name)
    notes: list[str] = []
    if statement is None:
        notes.append(f"Statement '{pipeline.statement_name}' does not exist.")
    manifest_path = write_manifest(
        config=config,
        pipeline=pipeline,
        operation="manifest",
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_dir=manifest_dir,
        notes=" ".join(notes) or None,
    )
    return OperationResult(
        operation="manifest",
        pipeline=pipeline_name,
        statement_name=pipeline.statement_name,
        statement=statement,
        sql_sha256=release.sql_sha256,
        release_sha256=release.release_sha256,
        manifest_path=manifest_path,
        notes=notes,
        changed=False,
    )
