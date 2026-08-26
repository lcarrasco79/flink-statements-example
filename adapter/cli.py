"""Command line entry point for the Flink statement deployment adapter.

Local smoke test:

    python -m adapter.cli validate --config config/demo-dev.yaml
    python -m adapter.cli create-stopped --pipeline basic --config config/demo-dev.yaml
    python -m adapter.cli status --pipeline basic --config config/demo-dev.yaml
    python -m adapter.cli start --pipeline basic --config config/demo-dev.yaml

Credentials come from FLINK_API_KEY / FLINK_API_SECRET and are never printed.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Sequence

try:  # executed as `python -m adapter.cli`
    from . import flink_statements as fs
except ImportError:  # executed as `python adapter/cli.py`
    import flink_statements as fs  # type: ignore[no-redef]

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_INTERRUPTED = 130

LIFECYCLE_OPERATIONS = ("create-stopped", "status", "start", "stop", "resume", "manifest")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m adapter.cli",
        description="Confluent Cloud Flink statement lifecycle adapter",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--config",
            default=fs.DEFAULT_CONFIG_PATH,
            help=f"Path to the environment configuration (default: {fs.DEFAULT_CONFIG_PATH})",
        )

    def add_pipeline(sub: argparse.ArgumentParser, *, required: bool = True) -> None:
        sub.add_argument(
            "--pipeline",
            required=required,
            default=None,
            help="Pipeline key from the configuration (for example: basic, stateful)",
        )

    def add_manifest_dir(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--manifest-dir",
            default=fs.DEFAULT_MANIFEST_DIR,
            help=f"Directory for deployment manifests (default: {fs.DEFAULT_MANIFEST_DIR})",
        )

    def add_wait(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--no-wait",
            dest="wait",
            action="store_false",
            help="Submit the change without polling for the target phase",
        )
        sub.add_argument(
            "--timeout",
            type=int,
            default=None,
            help="Poll timeout in seconds (default: policy.poll_timeout_seconds)",
        )

    validate_cmd = subparsers.add_parser(
        "validate", help="Run pre-flight checks. Makes no API calls."
    )
    add_common(validate_cmd)
    add_pipeline(validate_cmd, required=False)
    validate_cmd.add_argument(
        "--require-credentials",
        action="store_true",
        help="Fail when FLINK_API_KEY/FLINK_API_SECRET are absent (default: warn)",
    )

    create_cmd = subparsers.add_parser(
        "create-stopped",
        help=(
            "Create the statement, then immediately stop it. The API starts "
            "statements on create, so it is briefly live."
        ),
    )
    add_common(create_cmd)
    add_pipeline(create_cmd)
    add_manifest_dir(create_cmd)
    add_wait(create_cmd)

    status_cmd = subparsers.add_parser("status", help="Report the statement state")
    add_common(status_cmd)
    add_pipeline(status_cmd)
    add_manifest_dir(status_cmd)
    status_cmd.add_argument(
        "--wait-for",
        default=None,
        help="Poll until the statement reaches this phase, bounded by --timeout",
    )
    status_cmd.add_argument(
        "--timeout",
        type=int,
        default=None,
        help="Poll timeout in seconds (default: policy.poll_timeout_seconds)",
    )
    status_cmd.add_argument(
        "--write-manifest",
        action="store_true",
        help="Also record the observed state as a manifest",
    )

    start_cmd = subparsers.add_parser(
        "start", help="Start the existing statement (spec.stopped = false)"
    )
    add_common(start_cmd)
    add_pipeline(start_cmd)
    add_manifest_dir(start_cmd)
    add_wait(start_cmd)
    start_cmd.add_argument(
        "--allow-sql-mismatch",
        action="store_true",
        help="Start even when the deployed SQL hash differs from the local release",
    )

    stop_cmd = subparsers.add_parser(
        "stop", help="Stop the statement (spec.stopped = true). Never deletes it."
    )
    add_common(stop_cmd)
    add_pipeline(stop_cmd)
    add_manifest_dir(stop_cmd)
    add_wait(stop_cmd)

    resume_cmd = subparsers.add_parser(
        "resume", help="Resume the same statement (spec.stopped = false)"
    )
    add_common(resume_cmd)
    add_pipeline(resume_cmd)
    add_manifest_dir(resume_cmd)
    add_wait(resume_cmd)
    resume_cmd.add_argument(
        "--allow-sql-mismatch",
        action="store_true",
        help="Resume even when the deployed SQL hash differs from the local release",
    )

    manifest_cmd = subparsers.add_parser(
        "manifest", help="Write a manifest for the current observed state"
    )
    add_common(manifest_cmd)
    add_pipeline(manifest_cmd)
    add_manifest_dir(manifest_cmd)

    return parser


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def log(message: str) -> None:
    print(message, flush=True)


def print_rows(rows: Sequence[tuple[str, Any]]) -> None:
    for label, value in rows:
        if value in (None, ""):
            continue
        print(f"{label}: {value}")


def print_validation(checks: Sequence[fs.Check]) -> None:
    for check in checks:
        if check.ok:
            marker = "PASS"
        elif check.severity == "warning":
            marker = "WARN"
        else:
            marker = "FAIL"
        print(f"[{marker}] {check.name}: {check.detail}")
    failures = sum(1 for check in checks if check.failed)
    warnings = sum(1 for check in checks if check.warned)
    print(
        f"\nChecks: {len(checks)} | failures: {failures} | warnings: {warnings}"
    )


def print_result(result: fs.OperationResult) -> None:
    statement = result.statement
    spec = (statement or {}).get("spec") or {}
    status_block = (statement or {}).get("status") or {}

    print("")
    rows: list[tuple[str, Any]] = [
        ("Action", result.operation),
        ("Pipeline", result.pipeline),
        ("Statement", result.statement_name),
    ]
    if statement is None:
        rows.append(("Phase", "NOT FOUND"))
    else:
        rows.extend(
            [
                ("Statement ID", result.statement_id or "<unknown>"),
                ("Phase", result.phase),
                ("Detail", fs.statement_detail(statement)),
                ("Stopped flag", spec.get("stopped")),
                ("Processing started", "yes" if result.processing else "no"),
                ("Compute pool", spec.get("compute_pool_id")),
                ("Principal", spec.get("principal")),
            ]
        )
    rows.extend(
        [
            ("SQL hash", result.sql_sha256),
            ("Release hash", result.release_sha256),
        ]
    )
    if result.manifest_path:
        rows.append(("Manifest", result.manifest_path))
    print_rows(rows)

    traits = status_block.get("traits")
    if isinstance(traits, dict) and traits:
        summary = ", ".join(
            f"{key}={value}" for key, value in sorted(traits.items()) if key != "schema"
        )
        if summary:
            print(f"Traits: {summary}")
    for key in ("scaling_status", "metrics", "latest_offsets_timestamp"):
        value = status_block.get(key)
        if value:
            print(f"{key.replace('_', ' ').capitalize()}: {value}")

    for note in result.notes:
        print(f"Note: {note}")


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def cmd_validate(args: argparse.Namespace) -> int:
    pipelines = [args.pipeline] if args.pipeline else None
    checks = fs.validate(
        args.config,
        pipelines=pipelines,
        require_credentials=args.require_credentials,
    )
    print_validation(checks)
    failed = any(check.failed for check in checks)
    print("\nResult: " + ("FAILED" if failed else "OK"))
    return EXIT_ERROR if failed else EXIT_OK


def cmd_create_stopped(args: argparse.Namespace) -> int:
    result = fs.create_stopped(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        wait=args.wait,
        timeout_seconds=args.timeout,
        log=log,
    )
    print_result(result)
    if not result.changed:
        print("Result: existing statement reported, nothing was created")
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    result = fs.status(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        write_manifest_file=args.write_manifest,
        wait_for=args.wait_for,
        timeout_seconds=args.timeout,
        log=log,
    )
    print_result(result)
    return EXIT_OK if result.statement is not None else EXIT_ERROR


def cmd_start(args: argparse.Namespace) -> int:
    result = fs.start(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        wait=args.wait,
        timeout_seconds=args.timeout,
        allow_sql_mismatch=args.allow_sql_mismatch,
        log=log,
    )
    print_result(result)
    return EXIT_OK


def cmd_stop(args: argparse.Namespace) -> int:
    result = fs.stop(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        wait=args.wait,
        timeout_seconds=args.timeout,
        log=log,
    )
    print_result(result)
    return EXIT_OK


def cmd_resume(args: argparse.Namespace) -> int:
    result = fs.resume(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        wait=args.wait,
        timeout_seconds=args.timeout,
        allow_sql_mismatch=args.allow_sql_mismatch,
        log=log,
    )
    print_result(result)
    return EXIT_OK


def cmd_manifest(args: argparse.Namespace) -> int:
    result = fs.manifest(
        args.config,
        args.pipeline,
        manifest_dir=args.manifest_dir,
        log=log,
    )
    print_result(result)
    return EXIT_OK


HANDLERS = {
    "validate": cmd_validate,
    "create-stopped": cmd_create_stopped,
    "status": cmd_status,
    "start": cmd_start,
    "stop": cmd_stop,
    "resume": cmd_resume,
    "manifest": cmd_manifest,
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = HANDLERS[args.command]
    try:
        return handler(args)
    except fs.AdapterError as exc:
        print(f"\nERROR ({type(exc).__name__}): {exc}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        print("\nInterrupted", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    sys.exit(main())
