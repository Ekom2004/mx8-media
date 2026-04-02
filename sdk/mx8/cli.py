from __future__ import annotations

import argparse
import os
import sys
from dataclasses import asdict, is_dataclass
from time import sleep
from typing import Any

from .client import MX8APIError, default_client

TERMINAL_STATUSES = {"COMPLETE", "FAILED"}
ANSI_RESET = "\033[0m"
ANSI_DIM = "\033[2m"
ANSI_BOLD = "\033[1m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_BLUE = "\033[34m"
ANSI_WHITE = "\033[37m"


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 1
    try:
        return int(args.handler(args))
    except MX8APIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mx8", description="MX8 operator CLI")
    subparsers = parser.add_subparsers(dest="command")

    ops = subparsers.add_parser("ops", help="operator commands")
    ops_subparsers = ops.add_subparsers(dest="ops_command")

    watch = ops_subparsers.add_parser("watch", help="watch one job live")
    watch.add_argument("job_id", help="job id to watch")
    watch.add_argument("--poll-interval", type=float, default=2.0, help="refresh interval in seconds")
    watch.add_argument("--once", action="store_true", help="render one snapshot and exit")
    watch.set_defaults(handler=_handle_watch)

    live = ops_subparsers.add_parser("live", help="watch all active jobs live")
    live.add_argument("--poll-interval", type=float, default=3.0, help="refresh interval in seconds")
    live.add_argument("--all", action="store_true", help="include terminal jobs")
    live.add_argument("--once", action="store_true", help="render one snapshot and exit")
    live.set_defaults(handler=_handle_live)

    return parser


def _handle_watch(args: argparse.Namespace) -> int:
    client = default_client()
    while True:
        job = client.get_job(args.job_id)
        _clear_screen()
        print(render_job_detail(_job_to_dict(job)))
        if args.once or job.status in TERMINAL_STATUSES:
            return 0
        sleep(max(0.2, float(args.poll_interval)))


def _handle_live(args: argparse.Namespace) -> int:
    client = default_client()
    while True:
        jobs = [_job_to_dict(job) for job in client.list_jobs()]
        _clear_screen()
        print(render_job_table(jobs, include_terminal=bool(args.all)))
        if args.once:
            return 0
        sleep(max(0.2, float(args.poll_interval)))


def render_job_detail(job: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"{_dim('Job:')} {_strong(str(job.get('id', '-')))}")
    lines.append(f"{_dim('Status:')} {_status_color(str(job.get('status', '-')))}")
    lines.append(f"{_dim('Stage:')} {_stage_color(str(job.get('stage') or '-'))}")
    lines.append("")
    lines.append(f"{_dim('Input:')}  {_strong(str(job.get('input', '-')))}")
    lines.append(f"{_dim('Output:')} {_strong(str(job.get('output', '-')))}")
    lines.append("")

    work = list(job.get("work") or ())
    lines.append(_section("Work"))
    if work:
        for item in work:
            lines.append(f"- {_format_work_item(item)}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append(_section("Progress"))
    lines.append(f"- outputs_written: {job.get('outputs_written', 0)}")
    lines.append(f"- completed_objects: {job.get('completed_objects', 0)}")
    lines.append(f"- completed_bytes: {job.get('completed_bytes', 0)}")
    lines.append(f"- matched_assets: {job.get('matched_assets') if job.get('matched_assets') is not None else '-'}")
    lines.append(f"- matched_segments: {job.get('matched_segments') if job.get('matched_segments') is not None else '-'}")
    lines.append("")

    lines.append(_section("Workers"))
    lines.append(f"- pool: {_strong(str(job.get('worker_pool') or '-'))}")
    lines.append(f"- current: {job.get('current_workers', 0)}")
    lines.append(f"- desired: {job.get('desired_workers', 0)}")
    lines.append(f"- region: {_strong(str(job.get('region') or '-'))}")
    lines.append(f"- instance_type: {_strong(str(job.get('instance_type') or '-'))}")
    lines.append("")

    lines.append(_section("Failure"))
    failure_category = str(job.get("failure_category") or "-")
    failure_message = str(job.get("failure_message") or "-")
    lines.append(f"- category: {_failure_color(failure_category) if failure_category != '-' else failure_category}")
    lines.append(f"- message: {_failure_color(failure_message) if failure_message != '-' else failure_message}")
    lines.append("")

    events = list(job.get("events") or ())
    lines.append(_section("Events"))
    if events:
        for event in events[-10:]:
            lines.append(f"- {_format_event(event)}")
    else:
        lines.append("- none")
    return "\n".join(lines)


def render_job_table(jobs: list[dict[str, Any]], *, include_terminal: bool = False) -> str:
    rows = []
    for job in jobs:
        if not include_terminal and job.get("status") in TERMINAL_STATUSES:
            continue
        rows.append(
            [
                str(job.get("id", "-"))[:8],
                str(job.get("status", "-")),
                str(job.get("stage") or "-"),
                str(job.get("worker_pool") or "-"),
                str(job.get("outputs_written", 0)),
                f"{job.get('current_workers', 0)}/{job.get('desired_workers', 0)}",
                _last_event_type(job.get("events") or ()),
                _shorten(str(job.get("updated_at", "-")), 19),
            ]
        )

    headers = ["JOB", "STATUS", "STAGE", "POOL", "OUTPUTS", "WORKERS", "LAST_EVENT", "UPDATED"]
    if not rows:
        if include_terminal:
            return "No jobs found."
        return "No active jobs."

    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(row: list[str], *, header: bool = False) -> str:
        rendered = "  ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row))
        if header:
            return _dim(rendered)
        return rendered

    lines = [fmt(headers, header=True), _dim(fmt(["-" * width for width in widths]))]
    for row in rows:
        colored = list(row)
        colored[1] = _status_color(colored[1])
        colored[2] = _stage_color(colored[2])
        if colored[6] == "job_failed":
            colored[6] = _failure_color(colored[6])
        lines.append(fmt(colored))
    return "\n".join(lines)


def _job_to_dict(job: Any) -> dict[str, Any]:
    if is_dataclass(job):
        return asdict(job)
    if hasattr(job, "__dict__"):
        return dict(vars(job))
    raise TypeError(f"unsupported job object: {type(job)!r}")


def _format_work_item(item: dict[str, Any]) -> str:
    work_type = str(item.get("type", "-"))
    params = item.get("params") or {}
    if not params:
        return f"{work_type}()"
    pieces = ", ".join(f"{key}={value!r}" for key, value in params.items())
    return f"{work_type}({pieces})"


def _format_event(event: dict[str, Any]) -> str:
    at = str(event.get("at", "-"))
    event_type = str(event.get("type", "-"))
    message = event.get("message")
    if event_type == "job_failed" or str(event.get("failure_category") or ""):
        event_type = _failure_color(event_type)
    elif event_type in {"job_running", "worker_plan_updated"}:
        event_type = _color(event_type, ANSI_BLUE)
    elif event_type in {"job_completed", "planner_completed"}:
        event_type = _color(event_type, ANSI_GREEN)
    if message:
        return f"{_shorten(at, 19)} {event_type} {message}"
    return f"{_shorten(at, 19)} {event_type}"


def _last_event_type(events: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> str:
    if not events:
        return "-"
    return str(events[-1].get("type", "-"))


def _shorten(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _clear_screen() -> None:
    if os.environ.get("TERM"):
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()


def _section(label: str) -> str:
    return _dim(label + ":")


def _status_color(status: str) -> str:
    if status == "RUNNING":
        return _color(status, ANSI_BLUE)
    if status in {"QUEUED", "PENDING", "FINDING", "PLANNED"}:
        return _color(status, ANSI_YELLOW)
    if status == "COMPLETE":
        return _color(status, ANSI_GREEN)
    if status == "FAILED":
        return _color(status, ANSI_RED)
    return _strong(status)


def _stage_color(stage: str) -> str:
    if stage == "RUNNING":
        return _color(stage, ANSI_BLUE)
    if stage in {"SUBMITTED", "PLANNING", "PLANNED", "QUEUED"}:
        return _color(stage, ANSI_YELLOW)
    if stage == "COMPLETE":
        return _color(stage, ANSI_GREEN)
    if stage == "FAILED":
        return _color(stage, ANSI_RED)
    return _strong(stage)


def _failure_color(text: str) -> str:
    return _color(text, ANSI_RED)


def _strong(text: str) -> str:
    return _color(text, ANSI_WHITE, bold=True)


def _dim(text: str) -> str:
    return f"{ANSI_DIM}{text}{ANSI_RESET}"


def _color(text: str, color: str, *, bold: bool = False) -> str:
    prefix = color
    if bold:
        prefix = ANSI_BOLD + color
    return f"{prefix}{text}{ANSI_RESET}"


if __name__ == "__main__":
    raise SystemExit(main())
