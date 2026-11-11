"""`wait` across real processes: each Hub below lives in its own OS process, as in Hermes
(gateway, a Desktop `serve` process, a TUI), sharing only the mailbox and the presence files.

Run: cd ~/.hermes/plugins && python3 -m unittest intercom.tests.test_processes
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import intercom
from intercom.core import process_token, token_alive
from intercom.tests.test_core import A, B, make_db

PACKAGE_PARENT = Path(intercom.__file__).resolve().parent.parent


class Proc:
    """One peer_process and a JSON-lines conversation with it."""

    def __init__(self, home, hosted=()):
        self.p = subprocess.Popen([sys.executable, "-m", "intercom.tests.peer_process", str(home), ",".join(hosted)],
                                  cwd=PACKAGE_PARENT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.answers, self.cond, self.next_id = {}, threading.Condition(), 0
        threading.Thread(target=self._read, daemon=True).start()
        self.token = self.result(0, 20)

    def _read(self):
        for line in self.p.stdout:
            msg = json.loads(line)
            with self.cond:
                self.answers[msg["id"]] = msg["result"]
                self.cond.notify_all()

    def start(self, op, **args):
        """Send a command without waiting for its answer; returns its id."""
        self.next_id += 1
        self.p.stdin.write(json.dumps({"id": self.next_id, "op": op, "args": args}) + "\n")
        self.p.stdin.flush()
        return self.next_id

    def done(self, call_id):
        with self.cond:
            return call_id in self.answers

    def result(self, call_id, timeout=10):
        with self.cond:
            if not self.cond.wait_for(lambda: call_id in self.answers, timeout):
                raise AssertionError(f"no answer to call {call_id} within {timeout}s")
            return self.answers[call_id]

    def call(self, op, **args):
        return self.result(self.start(op, **args))

    def stop(self, kill=False):
        p = getattr(self, "p", None)
        if p is None:  # never started
            return
        if kill and p.poll() is None:
            p.kill()
        for stream in (p.stdin, p.stdout):  # EOF on stdin also ends a process that was not killed
            try:
                stream.close()
            except OSError:
                pass
        p.wait(10)


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        make_db(self.home / "state.db", time.time())

        self.addCleanup(self.tmp.cleanup)

    def proc(self, *hosted):
        p = Proc.__new__(Proc)
        self.addCleanup(p.stop, kill=True)  # registered before the handshake, so a child that fails it is reaped too
        p.__init__(self.home, hosted)
        return p

    def eventually(self, check, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(0.05)
        self.fail("condition not met in time")

    def test_blocking_wait_returns_when_a_session_in_another_process_yields(self):
        host, waiter = self.proc(B), self.proc(A)
        host.call("on_turn_start", session_id=B, user_message="deploy", platform="local")
        call = waiter.start("wait", caller_id=A, to=B, timeout_s=30)
        time.sleep(1)
        self.assertFalse(waiter.done(call), "wait returned while the target was running")
        host.call("on_turn_end", session_id=B, outcome="completed")
        out = waiter.result(call)
        self.assertEqual((out["status"], out["target_state"], out["turn"]), ("yielded", "idle", "completed"))

    def test_blocking_wait_returns_when_the_process_hosting_the_target_is_killed(self):
        host, waiter = self.proc(B), self.proc(A)
        host.call("on_turn_start", session_id=B, user_message="deploy", platform="local")
        call = waiter.start("wait", caller_id=A, to=B, timeout_s=30)
        time.sleep(0.5)
        self.assertFalse(waiter.done(call))
        host.stop(kill=True)
        out = waiter.result(call)
        self.assertEqual((out["status"], out["target_state"]), ("yielded", "unknown"))

    def test_registered_waiter_is_woken_once_in_the_process_that_hosts_it(self):
        host, waiter, bystander = self.proc(B), self.proc(A), self.proc()
        host.call("on_turn_start", session_id=B, user_message="deploy", platform="local")
        waiter.call("on_turn_start", session_id=A, user_message="add invites", platform="local")
        out = waiter.call("wait", caller_id=A, to=B, timeout_s=0, note="need the tree")
        self.assertEqual((out["status"], out["registered"]), ("not_yielded_yet", True))
        waiter.call("on_turn_end", session_id=A, outcome="completed")
        self.assertEqual([w["from"] for w in host.call("status", caller_id=B)["waiting_on_you"]], [A])
        time.sleep(1)
        self.assertEqual(waiter.call("injected"), [], "woken while the target was still running")
        host.call("on_turn_end", session_id=B, outcome="completed")
        woken = self.eventually(lambda: waiter.call("injected"))
        self.assertIn("has yielded", woken[0]["text"])
        self.assertIn("(completed)", woken[0]["text"])
        time.sleep(1)  # three pollers saw the yield: the wait row is claimed by exactly one of them
        self.assertEqual(len(waiter.call("injected")), 1)
        self.assertEqual(host.call("injected") + bystander.call("injected"), [])
        self.assertEqual(waiter.call("status", caller_id=A)["waiting_for"], [])

    def test_message_from_the_target_ends_a_wait_blocking_in_another_process(self):
        host, waiter = self.proc(B), self.proc(A)
        host.call("on_turn_start", session_id=B, user_message="deploy", platform="local")
        waiter.call("on_turn_start", session_id=A, user_message="add invites", platform="local")
        call = waiter.start("wait", caller_id=A, to=B, timeout_s=30)
        time.sleep(0.5)
        host.call("send", caller_id=B, to=A, body="DEPLOYED - the tree is yours")
        out = waiter.result(call)
        self.assertEqual((out["status"], out["from_target"], out["registered"]), ("message_arrived", True, False))


class ProcessIdentityTests(unittest.TestCase):
    def test_a_process_whose_name_is_not_valid_text_is_still_identified(self):
        if not Path("/proc/self/stat").exists():
            self.skipTest("needs /proc")
        with tempfile.TemporaryDirectory() as tmp:
            odd = os.path.join(os.fsencode(tmp), b"\xff\xfeworker")  # /proc/<pid>/stat carries the name as raw bytes
            os.symlink(sys.executable, odd)
            child = subprocess.Popen([odd, "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 10
                while b"\xff" not in Path(f"/proc/{child.pid}/stat").read_bytes() and time.monotonic() < deadline:
                    time.sleep(0.02)
                token = process_token(child.pid)
                self.assertNotEqual(token.split(":")[1], "")
                self.assertTrue(token_alive(token))
            finally:
                child.stdin.close()
                child.wait(10)
            self.assertFalse(token_alive(token))

    def test_a_live_process_whose_record_cannot_be_read_just_now_counts_as_alive(self):
        token = process_token()
        with mock.patch.object(Path, "read_bytes", side_effect=OSError(24, "Too many open files")):
            self.assertTrue(token_alive(token))

    def test_pids_that_would_signal_a_whole_group_are_never_alive(self):
        self.assertFalse(token_alive("0:"))
        self.assertFalse(token_alive("-1:"))

    def test_a_process_that_exited_but_was_not_reaped_is_not_alive(self):
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        try:
            token = process_token(child.pid)
            stat = Path(f"/proc/{child.pid}/stat")
            if not stat.exists():
                self.skipTest("needs /proc")
            deadline = time.monotonic() + 10
            while stat.read_text().rsplit(")", 1)[1].split()[0] != "Z" and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(token_alive(token), "a zombie cannot be running a turn")
        finally:
            child.wait()


if __name__ == "__main__":
    unittest.main()
