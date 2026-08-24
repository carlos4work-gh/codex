import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


MODULE_PATH = Path(__file__).with_name("app_server_restart_supervisor.py")
SPEC = importlib.util.spec_from_file_location("app_server_restart_supervisor", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
supervisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supervisor)


class FakeClock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class RestartBudgetTest(unittest.TestCase):
    def test_pre_readiness_failures_open_at_configured_limit(self) -> None:
        instance = supervisor._empty_instance("release-a")
        clock = FakeClock()
        for _ in range(3):
            started = clock.now()
            clock.advance(1)
            supervisor.record_attempt(
                instance,
                started_at=started,
                ready_at=None,
                exited_at=clock.now(),
                returncode=17,
                requested_shutdown=False,
                stable_seconds=60,
            )
        self.assertEqual(supervisor.crashes_in_window(instance, clock.now(), 300), 3)
        self.assertEqual(supervisor.backoff_seconds(3, 1, 30), 4)

    def test_stable_readiness_closes_old_crash_window(self) -> None:
        instance = supervisor._empty_instance("release-a")
        instance["attempts"] = [
            {
                "started_at": 900,
                "ready_at": None,
                "exited_at": 901,
                "exit_status": 1,
                "signal": None,
                "classification": "pre_readiness_crash",
            }
        ]
        supervisor.record_attempt(
            instance,
            started_at=1_000,
            ready_at=1_001,
            exited_at=1_071,
            returncode=-9,
            requested_shutdown=False,
            stable_seconds=60,
        )
        self.assertEqual(len(instance["attempts"]), 1)
        self.assertEqual(instance["attempts"][0]["signal"], 9)

    def test_requested_and_normal_shutdown_do_not_consume_budget(self) -> None:
        instance = supervisor._empty_instance("release-a")
        for requested, returncode in [(True, -15), (False, 0)]:
            supervisor.record_attempt(
                instance,
                started_at=1_000,
                ready_at=1_001,
                exited_at=1_002,
                returncode=returncode,
                requested_shutdown=requested,
                stable_seconds=60,
            )
        self.assertEqual(supervisor.crashes_in_window(instance, 1_002, 300), 0)

    def test_store_survives_supervisor_restart_and_generation_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = supervisor.StateStore(Path(directory) / "restart-state.json")
            state = store.load()
            instance = supervisor.select_instance(state, "node-a", "release-a")
            instance["breaker_open"] = True
            store.save(state)

            reloaded = store.load()
            self.assertTrue(reloaded["instances"]["node-a"]["breaker_open"])
            next_generation = supervisor.select_instance(reloaded, "node-a", "release-b")
            self.assertFalse(next_generation["breaker_open"])
            self.assertEqual(len(next_generation["previous_generations"]), 1)

    def test_manual_reset_is_audited(self) -> None:
        instance = supervisor._empty_instance("release-a")
        instance["breaker_open"] = True
        instance["attempts"].append({"classification": "pre_readiness_crash"})
        supervisor.reset_instance(
            instance, actor="operator:test", reason="config repaired", now=1_234
        )
        self.assertFalse(instance["breaker_open"])
        self.assertEqual(instance["attempts"], [])
        self.assertEqual(
            instance["resets"],
            [
                {
                    "actor": "operator:test",
                    "reason": "config repaired",
                    "reset_at": 1_234,
                    "generation": "release-a",
                }
            ],
        )

    def test_state_contains_no_mailbox_or_arena_payload(self) -> None:
        instance = supervisor._empty_instance("release-a")
        encoded = json.dumps(instance)
        self.assertNotIn("mailbox", encoded)
        self.assertNotIn("arena", encoded.lower())


class RestartSupervisorProcessTest(unittest.TestCase):
    def supervisor_command(
        self,
        directory: str,
        child_code: str,
        *,
        max_restarts: int = 2,
        readiness_timeout: float = 5,
    ) -> list[str]:
        return [
            sys.executable,
            str(MODULE_PATH),
            "run",
            "--state-file",
            str(Path(directory) / "state.json"),
            "--instance-id",
            "app-server-a",
            "--generation",
            "release-a",
            "--readiness-file",
            str(Path(directory) / "ready"),
            "--max-restarts",
            str(max_restarts),
            "--readiness-timeout",
            str(readiness_timeout),
            "--base-backoff",
            "0",
            "--max-backoff",
            "0",
            "--poll-seconds",
            "0.001",
            "--",
            sys.executable,
            "-c",
            child_code,
        ]

    def run_supervisor(
        self, directory: str, child_code: str, *, max_restarts: int = 2
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.supervisor_command(directory, child_code, max_restarts=max_restarts),
            check=False,
            capture_output=True,
            text=True,
        )

    def test_real_pre_readiness_exit_opens_and_persists_breaker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_supervisor(directory, "raise SystemExit(17)")
            self.assertEqual(result.returncode, supervisor.BREAKER_EXIT_CODE)
            state = json.loads((Path(directory) / "state.json").read_text())
            instance = state["instances"]["app-server-a"]
            self.assertTrue(instance["breaker_open"])
            self.assertEqual(len(instance["attempts"]), 2)
            self.assertTrue(
                all(
                    attempt["classification"] == "pre_readiness_crash"
                    and attempt["exit_status"] == 17
                    for attempt in instance["attempts"]
                )
            )
            self.assertEqual(
                [attempt["backoff_seconds"] for attempt in instance["attempts"]],
                [0.0, 0.0],
            )

            repeated = self.run_supervisor(directory, "raise SystemExit(99)")
            self.assertEqual(repeated.returncode, supervisor.BREAKER_EXIT_CODE)
            state = json.loads((Path(directory) / "state.json").read_text())
            self.assertEqual(len(state["instances"]["app-server-a"]["attempts"]), 2)

    @unittest.skipUnless(hasattr(signal, "SIGKILL"), "needs SIGKILL")
    def test_real_post_readiness_sigkill_consumes_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            child_code = (
                "from pathlib import Path; import os, signal; "
                "Path(os.environ['MEMYTHOS_READINESS_FILE']).touch(); "
                "os.kill(os.getpid(), signal.SIGKILL)"
            )
            result = self.run_supervisor(directory, child_code)
            self.assertEqual(result.returncode, supervisor.BREAKER_EXIT_CODE)
            state = json.loads((Path(directory) / "state.json").read_text())
            attempts = state["instances"]["app-server-a"]["attempts"]
            self.assertEqual(len(attempts), 2)
            self.assertTrue(
                all(
                    attempt["classification"] == "post_readiness_crash"
                    and attempt["signal"] == signal.SIGKILL
                    for attempt in attempts
                )
            )

    def test_real_normal_shutdown_does_not_open_breaker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            child_code = (
                "from pathlib import Path; import os; "
                "Path(os.environ['MEMYTHOS_READINESS_FILE']).touch()"
            )
            result = self.run_supervisor(directory, child_code)
            self.assertEqual(result.returncode, 0)
            state = json.loads((Path(directory) / "state.json").read_text())
            instance = state["instances"]["app-server-a"]
            self.assertFalse(instance["breaker_open"])
            self.assertEqual(instance["attempts"][0]["classification"], "normal_shutdown")

    def test_real_requested_shutdown_is_forwarded_and_not_budgeted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            child_code = (
                "from pathlib import Path; import os, time; "
                "Path(os.environ['MEMYTHOS_READINESS_FILE']).touch(); time.sleep(30)"
            )
            process = subprocess.Popen(
                self.supervisor_command(directory, child_code),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            readiness = Path(directory) / "ready"
            deadline = time.monotonic() + 5
            while not readiness.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(readiness.exists())
            process.terminate()
            process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0)
            state = json.loads((Path(directory) / "state.json").read_text())
            instance = state["instances"]["app-server-a"]
            self.assertFalse(instance["breaker_open"])
            self.assertEqual(
                instance["attempts"][0]["classification"], "requested_shutdown"
            )

    def test_real_readiness_timeout_terminates_hung_child(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                self.supervisor_command(
                    directory,
                    "import time; time.sleep(30)",
                    max_restarts=1,
                    readiness_timeout=0.05,
                ),
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, supervisor.BREAKER_EXIT_CODE)
            state = json.loads((Path(directory) / "state.json").read_text())
            attempt = state["instances"]["app-server-a"]["attempts"][0]
            self.assertEqual(attempt["classification"], "pre_readiness_crash")
            self.assertEqual(attempt["signal"], signal.SIGTERM)


if __name__ == "__main__":
    unittest.main()
