# intercom — session-to-session messaging for local Hermes sessions

User plugin (`~/.hermes/plugins/intercom`, outside the Hermes checkout, so `hermes update` never touches it).
Sessions can live in several Hermes processes at once (the messaging gateway, a Desktop `serve` process per
SSH connection, a TUI, a CLI), and each process can only start turns in the sessions it hosts. So the plugin
needs no sockets or daemons, but keeps its shared state outside process memory:

| what | where | written |
|---|---|---|
| mailbox: messages + ask/reply waiters (SQLite) | `~/.hermes/intercom/intercom.db` | once per message |
| presence: state of each hosted session + owning process | `/run/user/<uid>/hermes-intercom-<hash>/presence/` (tmpfs; falls back to `~/.hermes/intercom/run`) | per turn / prompt |
| audit log | `~/.hermes/intercom/messages.jsonl` | per delivery event |

Presence is ephemeral on purpose: an entry whose process has died is ignored and removed, so a crash or reboot
leaves nothing stale, and per-turn bookkeeping never touches the disk where tmpfs is available. Delivery goes
through the public `ctx.inject_message` API with a `session_key`; hosts refuse sessions they don't own, which
is what makes trying from the wrong process safe.

## Tool: `intercom`
| action | does |
|---|---|
| `list` | recent interactive sessions: title, state (`running` / `idle` / `awaiting_input` + the prompt / `unknown`), cwd, branch, last activity |
| `send to= message=` | one-way message |
| `ask to= message= timeout_s=` | question; blocks up to `timeout_s` (≤360) for the answer, else `no_reply_yet` and the late answer arrives as a message |
| `reply request_id= message=` | answer a question |
| `status` | own id, open questions both ways |

`to` = full id, id prefix (≥4 chars) or words from the title.

## Delivery
- idle target in the same process → injected as a new turn (wakes it; FIFO, never interrupts)
- idle target in another process → held in the mailbox; that process's poller injects it within `poll_seconds`
- running target (any process) → appended to its next tool result; leftovers injected when the turn ends
- target blocked on clarify/approval/sudo → held, lands in that prompt's tool result when it resolves
  or times out (`agent.clarify_timeout`, default 1h); the sender is told it's blocked and on what
- target not hosted by any process that has seen it run → every live process tries to inject it once (covers
  a window that's open but hasn't run a turn since its process started); otherwise held until the session's
  next turn, in whichever process runs it. Held messages survive restarts and reboots (7-day retention).
- `ask` works across processes: the asker polls the mailbox for the reply; a reply to an asker whose process
  has died is delivered as a normal message instead.

Messages are framed `[INTERCOM — message from another agent session, NOT from <user_name>]` and told they carry
no user authority. `/intercom` and `/intercom log [N]` in any session.

## Stale-card watchdog
In one process per machine (flock; another live process takes over if it exits), every `interval_seconds`:
running cards on `board` whose body has `Session: <id>` and no event for `stale_minutes` → that session gets a
nudge (once per `renudge_minutes`). Sessions blocked on a prompt for the user are skipped; cards whose session
has ended get one "probably orphaned" comment instead. While the board doesn't exist the scan is skipped
without spawning the kanban CLI; create it with `hermes kanban boards create sessions` to use the watchdog.

## Settings (`hermes config set plugins.entries.intercom.<key> <value>`)
- `allow_gateway_injection: true` — required (lets the plugin start turns)
- `settings.user_name` — how messages refer to the human (default `the user`)
- `settings.sources` — `state.db` session sources that can be listed/targeted
  (default `[gui, local, tui, cli, desktop, acp]`; add e.g. `matrix` to reach messaging chats, whose replies
  are then posted in that chat)
- `settings.poll_seconds` (3) — how often each process checks for mail left by other processes (a token-file
  read; the mailbox is only queried when it changed)
- `settings.run_dir` — override the presence directory (all processes of one Hermes home must agree)
- `settings.watchdog.enabled` (default true), `.board` (`sessions`), `.stale_minutes` (60),
  `.renudge_minutes` (60), `.interval_seconds` (300)
- `settings.list_window_hours` (24), `settings.inject_key_order` (`[route, id]`)

## Update safety
Only documented surfaces: `register_tool/hook/command/cli_command`, `inject_message`, `get_config`, the hook
payloads in the hooks catalog, `gateway.session_context.get_session_env` / `bound_session_key`. `state.db` and the kanban CLI are read
defensively (listing degrades, nothing crashes). The seam check runs at every load, logs a warning and pops a
desktop notification (macOS, or `notify-send` on a Linux desktop) once per new problem set; run
`hermes intercom doctor` after an update to check by hand.

Tests: `cd ~/.hermes/plugins && python3 -m unittest intercom.tests.test_core`
Reload after editing: `hermes plugins disable intercom && hermes plugins enable intercom` (hooks reload live;
the tool's new code reaches new sessions; state carries over).
