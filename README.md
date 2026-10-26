# intercom — session-to-session messaging for local Hermes sessions

User plugin (`~/.hermes/plugins/intercom`, outside the Hermes checkout, so `hermes update` never touches it).
Every CLI/TUI/Desktop session runs inside the one gateway process, so the plugin needs no sockets or
daemons: it keeps a directory of sessions from hooks + `state.db`, and delivers through the public
`ctx.inject_message(session_key=...)` API.

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
- idle target → injected as a new turn (wakes it; gateway FIFO, never interrupts)
- running target → appended to its next tool result; leftovers injected when the turn ends
- target blocked on clarify/approval/sudo → held, lands in that prompt's tool result when it resolves
  or times out (`agent.clarify_timeout`, default 1h); the sender is told it's blocked and on what
- target not loaded → held until its next turn (spooled to `~/.hermes/intercom/spool.json`, survives restarts)

Messages are framed `[INTERCOM — message from another agent session, NOT from Andre]` and told they carry no
user authority. Audit log: `~/.hermes/intercom/messages.jsonl`; `/intercom` and `/intercom log [N]` in any session.

## Stale-card watchdog
In the gateway, every `interval_seconds`: running cards on `board` whose body has `Session: <id>` and no event
for `stale_minutes` → that session gets a nudge (once per `renudge_minutes`). Sessions blocked on a prompt
for Andre are skipped; cards whose session has ended get one "probably orphaned" comment instead.

## Settings (`hermes config set plugins.entries.intercom.<key> <value>`)
- `allow_gateway_injection: true` — required (lets the plugin start turns)
- `settings.watchdog.enabled` (default true), `.board` (`sessions`), `.stale_minutes` (60),
  `.renudge_minutes` (60), `.interval_seconds` (300)
- `settings.list_window_hours` (24), `settings.inject_key_order` (`[route, id]`)

## Update safety
Only documented surfaces: `register_tool/hook/command/cli_command`, `inject_message`, `get_config`, the hook
payloads in the hooks catalog, `gateway.session_context.get_session_env`. `state.db` and the kanban CLI are read
defensively (listing degrades, nothing crashes). The seam check runs at every load and pops a macOS
notification once per new problem set; run `hermes intercom doctor` after an update to check by hand.

Tests: `cd ~/.hermes/plugins && python3 -m unittest intercom.tests.test_core`
Reload after editing: `hermes plugins disable intercom && hermes plugins enable intercom` (hooks reload live;
the tool's new code reaches new sessions; state carries over).
