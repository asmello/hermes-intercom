"""A stand-in for one Hermes process, for tests that need real processes (test_processes.py).

    python3 -m intercom.tests.peer_process <home> [hosted,session,ids]

Builds a Hub on <home> as the plugin does (real clock, real process identity, real SQLite
mailbox and presence files, all inside <home>), runs the delivery poller, and executes one JSON command
per stdin line on its own thread: {"id": n, "op": "<Hub method> | injected", "args": {...}}.
Each answer is one JSON line on stdout: {"id": n, "result": ...}.
"""

import json
import sys
import threading
from pathlib import Path

from intercom.core import Hub

OPS = ("on_turn_start", "on_turn_end", "wait", "send", "ask", "reply", "status")


def main() -> None:
    home = Path(sys.argv[1])
    hosted = {s for s in (sys.argv[2] if len(sys.argv) > 2 else "").split(",") if s}
    injected: list[dict] = []

    def inject(text, session_key):
        # Like a real host: it only starts turns in sessions it owns.
        if not any(session_key == sid or session_key.endswith(":" + sid) for sid in hosted):
            return False
        injected.append({"key": session_key, "text": text})
        return True

    # run_dir is passed explicitly: derived from the home it would be /run/user/<uid>/hermes-intercom-<hash>,
    # outside the temp dir the test removes.
    hub = Hub(inject=inject, db_path=home / "state.db", log_path=home / "intercom" / "messages.jsonl",
              run_dir=home / "run", settings=lambda key, default: {"settle_seconds": 0.5}.get(key, default),
              poll_wait=0.05)
    out_lock = threading.Lock()

    def poller():
        tick = threading.Event()
        while not tick.wait(0.1):
            hub.poll_once()

    def run(cmd):
        try:
            result = list(injected) if cmd["op"] == "injected" else getattr(hub, cmd["op"])(**cmd.get("args", {}))
        except Exception as exc:
            result = {"error": f"{type(exc).__name__}: {exc}"}
        with out_lock:
            print(json.dumps({"id": cmd["id"], "result": result}), flush=True)

    threading.Thread(target=poller, daemon=True).start()
    print(json.dumps({"id": 0, "result": hub.token}), flush=True)
    for line in sys.stdin:
        cmd = json.loads(line)
        if cmd["op"] != "injected" and cmd["op"] not in OPS:
            raise SystemExit(f"unknown op {cmd['op']!r}")
        threading.Thread(target=run, args=(cmd,), daemon=True).start()


if __name__ == "__main__":
    main()
