"""Stale-card watchdog for the `sessions` kanban board.

Every few minutes (gateway process only), look at running cards on the board.
A card whose body names its owning session (`Session: <id>`) and has had no
event (heartbeat, comment, edit, claim) for `stale_minutes` gets that session
nudged over intercom: it wakes an idle session, or lands mid-turn in a busy
one. Sessions blocked on a prompt for Andre are left alone (the nudge would
only queue behind the prompt). Each card is nudged at most once per
`renudge_minutes`; nudge times persist across gateway restarts.

Board access goes through the `hermes kanban` CLI (a public interface), not
the kanban DB.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from .core import WATCHDOG_SENDER, Hub, IntercomError, short_id

logger = logging.getLogger("hermes_plugins.intercom.watchdog")
SESSION_RE = re.compile(r"^Session:\s*(\S+)", re.MULTILINE)

_start_lock = threading.Lock()


def hermes_bin() -> str:
    found = shutil.which("hermes")
    if found:
        return found
    for cand in (Path.home() / ".hermes/hermes-agent/.hermes/bin/hermes", Path.home() / ".local/bin/hermes"):
        if cand.exists():
            return str(cand)
    return "hermes"


def _kanban(board: str, *args: str, timeout: int = 60) -> Optional[object]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("HERMES_KANBAN_")}
    try:
        out = subprocess.run([hermes_bin(), "kanban", "--board", board, *args, "--json"],
                             capture_output=True, text=True, timeout=timeout, env=env)
    except (OSError, subprocess.TimeoutExpired):
        logger.warning("intercom watchdog: kanban CLI failed", exc_info=True)
        return None
    if out.returncode != 0:
        logger.warning("intercom watchdog: kanban %s exited %s: %s", args[:1], out.returncode, out.stderr[-300:])
        return None
    try:
        return json.loads(out.stdout)
    except ValueError:
        return None


def _kanban_comment(board: str, tid: str, text: str) -> bool:
    env = {k: v for k, v in os.environ.items() if not k.startswith("HERMES_KANBAN_")}
    try:
        out = subprocess.run([hermes_bin(), "kanban", "--board", board, "comment", tid, text],
                             capture_output=True, text=True, timeout=60, env=env)
        return out.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def last_card_activity(card: dict) -> float:
    task = card.get("task") or {}
    stamps = [task.get("started_at") or 0, task.get("created_at") or 0]
    stamps += [e.get("created_at") or 0 for e in card.get("events") or []]
    stamps += [c.get("created_at") or 0 for c in card.get("comments") or []]
    return float(max(stamps))


class Watchdog:
    def __init__(self, hub: Hub, state_path: Path, clock=time.time) -> None:
        self.hub = hub
        self.state_path = state_path
        self.clock = clock

    def _load(self) -> dict:
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save(self, state: dict) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(state))
        except OSError:
            pass

    def tick(self, list_cards=None, show_card=None, comment_card=None) -> list[dict]:
        """One scan. Returns the nudges sent (for tests and logging)."""
        board = str(self.hub.setting("watchdog.board", "sessions"))
        stale_s = float(self.hub.setting("watchdog.stale_minutes", 60)) * 60
        renudge_s = float(self.hub.setting("watchdog.renudge_minutes", 60)) * 60
        list_cards = list_cards or (lambda: _kanban(board, "list", "--status", "running"))
        show_card = show_card or (lambda tid: _kanban(board, "show", tid))
        comment_card = comment_card or (lambda tid, text: _kanban_comment(board, tid, text))
        cards = list_cards()
        if not isinstance(cards, list):
            cards = (cards or {}).get("tasks", []) if isinstance(cards, dict) else []
        now = self.clock()
        state = self._load()
        sent = []
        for summary in cards:
            tid = summary.get("id")
            if not tid:
                continue
            card = show_card(tid)
            if not isinstance(card, dict):
                continue
            body = (card.get("task") or {}).get("body") or ""
            m = SESSION_RE.search(body)
            if not m:
                continue
            idle_for = now - last_card_activity(card)
            if idle_for < stale_s or now - float(state.get(tid, 0)) < renudge_s:
                continue
            peer = self.hub.peer(m.group(1))
            if peer.state == "closed":
                if not state.get(f"{tid}:closed") and comment_card(tid, (
                        f"[intercom watchdog] the owning session {short_id(peer.session_id)} has ended "
                        f"and the card has had no update for {int(idle_for // 60)} min: it is probably "
                        "orphaned. Ask Andre before taking it over.")):
                    state[f"{tid}:closed"] = now
                continue
            if peer.state == "awaiting_input":
                continue
            title = (card.get("task") or {}).get("title") or tid
            text = (f"Your card {tid} on the `{board}` kanban board (\"{title}\") has had no heartbeat, "
                    f"comment or edit for {int(idle_for // 60)} min. Update it now: "
                    f"`hermes kanban --board {board} heartbeat {tid} --note \"...\"` if you are still on it, "
                    f"`block {tid} --kind needs_input \"...\"` if you are waiting on Andre, or "
                    f"`complete {tid} --result \"...\"` if the work is done or abandoned.")
            try:
                info = self.hub.send(WATCHDOG_SENDER, peer.session_id, text, kind="nudge")
            except IntercomError as exc:
                logger.info("intercom watchdog: cannot nudge %s for %s: %s", short_id(peer.session_id), tid, exc)
                continue
            state[tid] = now
            sent.append({"card": tid, "session": peer.session_id, **info})
        state = {k: v for k, v in state.items() if now - float(v) < 7 * 86400}
        self._save(state)
        return sent


def start(hub: Hub, home: Path) -> None:
    """Start the watchdog thread once per process, only in the gateway (one instance per machine)."""
    if os.environ.get("HERMES_KANBAN_TASK"):  # a kanban worker process, not the gateway
        return
    with _start_lock:  # process-global flag: a plugin reload re-imports this module
        if getattr(sys, "_hermes_intercom_watchdog_started", False):
            return
        sys._hermes_intercom_watchdog_started = True
    lock_path = home / "intercom" / "watchdog.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.info("intercom watchdog: another process holds %s; not starting", lock_path)
        fh.close()
        return
    dog = Watchdog(hub, home / "intercom" / "watchdog_state.json")

    def loop() -> None:
        time.sleep(60)  # let the gateway settle after start
        while True:
            try:
                dog.hub = getattr(sys, "_hermes_intercom_hub", None) or dog.hub  # survive plugin reloads
                if dog.hub.setting("watchdog.enabled", True):
                    for n in dog.tick():
                        logger.info("intercom watchdog: nudged %s about %s (%s)", short_id(n["session"]),
                                    n["card"], n.get("delivery"))
            except Exception:
                logger.warning("intercom watchdog: tick failed", exc_info=True)
            time.sleep(max(60.0, float(dog.hub.setting("watchdog.interval_seconds", 300))))

    t = threading.Thread(target=loop, name="intercom-watchdog", daemon=True)
    t._intercom_lock_fh = fh  # keep the flock alive with the thread
    t.start()
