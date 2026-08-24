#!/usr/bin/env python3
"""External restart budget for an app-server process.

The supervised process must create MEMYTHOS_READINESS_FILE only after its own
state DB, migrations, and RPC loop are ready. This supervisor never opens that
DB or interprets app-server domain state.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable


STATE_SCHEMA_VERSION = 1
BREAKER_EXIT_CODE = 75


def _empty_instance(generation: str) -> dict[str, Any]:
    return {
        "generation": generation,
        "breaker_open": False,
        "attempts": [],
        "resets": [],
        "previous_generations": [],
    }


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": STATE_SCHEMA_VERSION, "instances": {}}
        state = json.loads(self.path.read_text(encoding="utf-8"))
        if state.get("schema_version") != STATE_SCHEMA_VERSION:
            raise ValueError("unsupported restart supervisor state schema")
        if not isinstance(state.get("instances"), dict):
            raise ValueError("invalid restart supervisor instances")
        return state

    def save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def select_instance(
    state: dict[str, Any], instance_id: str, generation: str
) -> dict[str, Any]:
    instance = state["instances"].get(instance_id)
    if instance is None:
        instance = _empty_instance(generation)
        state["instances"][instance_id] = instance
    elif instance["generation"] != generation:
        previous = {
            key: value
            for key, value in instance.items()
            if key != "previous_generations"
        }
        history = list(instance.get("previous_generations", []))
        history.append(previous)
        instance = _empty_instance(generation)
        instance["previous_generations"] = history
        state["instances"][instance_id] = instance
    return instance


def crashes_in_window(instance: dict[str, Any], now: float, window: float) -> int:
    threshold = now - window
    return sum(
        1
        for attempt in instance["attempts"]
        if attempt["classification"] in {"pre_readiness_crash", "post_readiness_crash"}
        and attempt["exited_at"] >= threshold
    )


def backoff_seconds(crash_count: int, base: float, maximum: float) -> float:
    return min(maximum, base * (2 ** max(0, crash_count - 1)))


def record_attempt(
    instance: dict[str, Any],
    *,
    started_at: float,
    ready_at: float | None,
    exited_at: float,
    returncode: int,
    requested_shutdown: bool,
    stable_seconds: float,
) -> str:
    if requested_shutdown:
        classification = "requested_shutdown"
    elif returncode == 0:
        classification = "normal_shutdown"
    elif ready_at is None:
        classification = "pre_readiness_crash"
    else:
        classification = "post_readiness_crash"

    stable_readiness = ready_at is not None and exited_at - ready_at >= stable_seconds
    if stable_readiness:
        instance["attempts"] = []

    instance["attempts"].append(
        {
            "started_at": started_at,
            "ready_at": ready_at,
            "exited_at": exited_at,
            "exit_status": returncode if returncode >= 0 else None,
            "signal": -returncode if returncode < 0 else None,
            "classification": classification,
            "stable_readiness": stable_readiness,
            "backoff_seconds": 0.0,
        }
    )
    return classification


def reset_instance(
    instance: dict[str, Any], *, actor: str, reason: str, now: float
) -> None:
    instance["resets"].append(
        {
            "actor": actor,
            "reason": reason,
            "reset_at": now,
            "generation": instance["generation"],
        }
    )
    instance["attempts"] = []
    instance["breaker_open"] = False


def _diagnostic(instance_id: str, instance: dict[str, Any]) -> str:
    return json.dumps(
        {
            "instance_id": instance_id,
            "generation": instance["generation"],
            "breaker_open": instance["breaker_open"],
            "last_attempt": instance["attempts"][-1] if instance["attempts"] else None,
            "action": "run reset --actor <actor> --reason <reason>",
        },
        sort_keys=True,
    )


def supervise(
    args: argparse.Namespace,
    *,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    store = StateStore(args.state_file)
    state = store.load()
    instance = select_instance(state, args.instance_id, args.generation)
    if instance["breaker_open"]:
        print(_diagnostic(args.instance_id, instance), file=sys.stderr)
        return BREAKER_EXIT_CODE

    requested_shutdown = False
    child: subprocess.Popen[bytes] | None = None

    def request_shutdown(_signum: int, _frame: object) -> None:
        nonlocal requested_shutdown
        requested_shutdown = True
        if child is not None and child.poll() is None:
            child.terminate()

    previous_handlers = {
        signum: signal.signal(signum, request_shutdown)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        while True:
            crash_count = crashes_in_window(instance, now(), args.window_seconds)
            if crash_count >= args.max_restarts:
                instance["breaker_open"] = True
                store.save(state)
                print(_diagnostic(args.instance_id, instance), file=sys.stderr)
                return BREAKER_EXIT_CODE

            args.readiness_file.unlink(missing_ok=True)
            environment = os.environ.copy()
            environment["MEMYTHOS_READINESS_FILE"] = str(args.readiness_file)
            started_at = now()
            child = subprocess.Popen(args.command, env=environment)
            ready_at: float | None = None
            while child.poll() is None:
                if ready_at is None and args.readiness_file.exists():
                    ready_at = now()
                if ready_at is None and now() - started_at >= args.readiness_timeout:
                    child.terminate()
                sleep(args.poll_seconds)
            if ready_at is None and args.readiness_file.exists():
                ready_at = now()
            exited_at = now()
            returncode = child.returncode
            assert returncode is not None
            classification = record_attempt(
                instance,
                started_at=started_at,
                ready_at=ready_at,
                exited_at=exited_at,
                returncode=returncode,
                requested_shutdown=requested_shutdown,
                stable_seconds=args.stable_seconds,
            )
            if classification in {"requested_shutdown", "normal_shutdown"}:
                store.save(state)
                return 0
            crash_count = crashes_in_window(instance, exited_at, args.window_seconds)
            delay = backoff_seconds(crash_count, args.base_backoff, args.max_backoff)
            instance["attempts"][-1]["backoff_seconds"] = delay
            if crash_count >= args.max_restarts:
                instance["breaker_open"] = True
                store.save(state)
                print(_diagnostic(args.instance_id, instance), file=sys.stderr)
                return BREAKER_EXIT_CODE
            store.save(state)
            sleep(delay)
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--state-file", type=Path, required=True)
    common.add_argument("--instance-id", required=True)
    common.add_argument("--generation", required=True)

    run = subparsers.add_parser("run", parents=[common])
    run.add_argument("--readiness-file", type=Path, required=True)
    run.add_argument("--max-restarts", type=int, default=3)
    run.add_argument("--window-seconds", type=float, default=300.0)
    run.add_argument("--stable-seconds", type=float, default=60.0)
    run.add_argument("--readiness-timeout", type=float, default=30.0)
    run.add_argument("--base-backoff", type=float, default=1.0)
    run.add_argument("--max-backoff", type=float, default=30.0)
    run.add_argument("--poll-seconds", type=float, default=0.1)
    run.add_argument("command", nargs=argparse.REMAINDER)

    status = subparsers.add_parser("status", parents=[common])
    reset = subparsers.add_parser("reset", parents=[common])
    reset.add_argument("--actor", required=True)
    reset.add_argument("--reason", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = StateStore(args.state_file)
    state = store.load()
    instance = select_instance(state, args.instance_id, args.generation)
    if args.action == "status":
        print(_diagnostic(args.instance_id, instance))
        return 0
    if args.action == "reset":
        reset_instance(instance, actor=args.actor, reason=args.reason, now=time.time())
        store.save(state)
        print(_diagnostic(args.instance_id, instance))
        return 0
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("run requires a command after --")
    return supervise(args)


if __name__ == "__main__":
    raise SystemExit(main())
