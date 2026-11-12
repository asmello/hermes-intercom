# intercom — messaging and sequencing between local Hermes sessions

User plugin (`~/.hermes/plugins/intercom`, outside the Hermes checkout, so `hermes update` never touches it).
Sessions can live in several Hermes processes at once (the messaging gateway, a Desktop `serve` process per
SSH connection, a TUI, a CLI), and each process can only start turns in the sessions it hosts. So the plugin
needs no sockets or daemons, but keeps its shared state outside process memory:

| what | where | written |
|---|---|---|
| mailbox: messages, ask/reply waiters, registered waits (SQLite) | `~/.hermes/intercom/intercom.db` | once per message or wait |
| presence: state of each hosted session + owning process | `/run/user/<uid>/hermes-intercom-<hash>/presence/` (tmpfs; falls back to `~/.hermes/intercom/run`) | per turn / prompt |
| audit log | `~/.hermes/intercom/messages.jsonl` | per delivery event |

Presence is ephemeral on purpose: an entry whose process has died is ignored and removed, so a crash or reboot
leaves nothing stale, and per-turn bookkeeping never touches the disk where tmpfs is available. Delivery goes
through the public `ctx.inject_message` API with a `session_key`; hosts refuse sessions they don't own, which
is what makes trying from the wrong process safe.

## Tool: `intercom`
| action | does |
|---|---|
| `list` | recent interactive sessions: id, short id, title, state (`running` / `idle` / `awaiting_input` + the prompt / `unknown`), who they wait for, cwd, branch, last activity |
| `send to= message=` | one-way message |
| `ask to= message= timeout_s=` | question; blocks up to `timeout_s` (≤360) for the answer, else `no_reply_yet` and the late answer arrives as a message |
| `reply [request_id=] message=` | answer a question; `request_id` may be left out when it can only mean one question (below) |
| `wait to= [timeout_s=] [message=] [cancel=]` | block until session `to` yields (see below) |
| `status` | own id, open questions both ways, who the caller waits for and who waits for it |

`to` = the full id, any 4+ characters of it (the short id shown by `list`, a prefix, the start time in the
middle) or words from the title, each matched from the start of a title word. The short id is the random
suffix of Hermes's `YYYYMMDD_HHMMSS_xxxxxx` ids: their first eight characters are the date, shared by every
session started that day. A `to` that fits several sessions, by id or by title, is refused with the
candidates listed. A `to` shaped like an id is matched against ids only: a title is written from what the
session read, so it can quote another session's id.

`reply` without `request_id` answers the caller's only open question if its asker is still blocked in the
call, or if it was asked in the last 15 minutes and no other session has written to the caller since.
Otherwise it is refused with the open questions listed: an old question must not collect an answer meant
for a later message. Only the session a question was put to can answer it.

A call that fails on addressing (no `to`, an ambiguous or unknown `to`, a missing or unknown `request_id`)
says what would work: the sessions the caller recently exchanged messages with, or the questions waiting
for its answer. The message text of that call is kept for five minutes, so a retry of the same action can
add the missing field and leave `message` out; the result says which text was sent. What a refused `reply`
wrote can also go out with `send` (a reply to something that was not a question is a send); nothing else
crosses from one action to another. A message longer than 8000 characters is cut, and the sender is told.

## Waiting for a session to yield
`wait` is for sequencing: the caller's next step depends on another session's work, or must come after
it. A session has **yielded** when no turn is running in it and nothing is about to start one. Two
witnesses are read: what the session's host reported through the hooks (turn start and end, prompts), and
Hermes's own turn lease, the `state.db` row it keeps from before a turn's first hook until after its last,
on every way out of the turn. A session has yielded when

- its turn ended at least `settle_seconds` ago (a queued user message or held intercom mail starts the
  next turn within that window), and it has no undelivered intercom mail;
- Hermes holds no turn lease for it (it does for a turn it has accepted but whose host has not reported it
  yet, e.g. while the context is compacted before the first model call);
- it has no background subagents running or waiting to hand their results back (Hermes re-enters the
  session with them);
- it is not itself registered as waiting for another session (it ended its turn only to be resumed).

Hermes drops some failed turns (an API error it gives up on) without the end-of-turn hook. Such a turn is
recognised by its lease being gone: the session is shown as idle again, mail held for it is delivered, and
a waiter gets `turn: unreported`.

A session no live process hosts (never ran since its process started, closed, or its process exited)
counts as yielded, and the result says which. Whatever cannot be read at that moment (a presence file,
the mailbox, `state.db`) counts as "not yielded": not knowing is never reported as a yield. Background
terminal processes with `notify` are not visible to the plugin: a session that ended its turn to wait for
one looks yielded, which is one reason the result carries the session's final message.

What the caller gets, whichever comes first:

| `status` | when | registered afterwards |
|---|---|---|
| `yielded` | the target yielded: `note` (how), `turn` (`completed` / `interrupted` / `failed` / `incomplete` = out of steps / `unreported`), `final_message` + `final_message_age` (the start of the last thing it said in that turn) | no |
| `message_arrived` | an intercom message for the caller arrived; it is appended to the result as usual | no if it is from the target (it has spoken), otherwise yes |
| `interrupted` | the user stopped the caller, or sent it a message mid-turn | no after a stop, yes after a message |
| `not_yielded_yet` | `timeout_s` passed (default 240, max 360; `0` = don't block); `pending` says what the target is doing | yes |

While **registered**, the caller is told by an intercom message when the target yields, which wakes it if
it is idle. So a long wait costs nothing: the caller ends its turn and is resumed. (Blocking again instead
would re-read the whole conversation at every timeout; the default stays under the 5-minute prompt-cache
window for that reason.) A registration ends when it fires, with `cancel=true`, when the waiter's session
is deleted, or after `wait_max_hours` (the waiter is told). A waiter that Hermes only unloads or archives
(its window was closed, its client went away) keeps its registration: the notice is held like any other
mail and read when the session is opened again. Subagents and sessions on surfaces that cannot be woken (cron runs) are
never left registered.

The target is told once, at its next tool result or turn start and never by waking it, that the caller is
waiting, with the optional `message` as the reason, so it can release what the caller needs before its
turn ends (`send` to a waiter that is blocking returns its `wait` at once). A wait that could never end is
refused: the target waits for the caller (directly or through other sessions), or is blocked asking it a
question. Of two sessions that register on each other at the same moment, one is refused.

## Delivery
- idle target in the same process → injected as a new turn (wakes it; FIFO, never interrupts)
- idle target in another process → held in the mailbox; that process's poller injects it within `poll_seconds`
- running target (any process) → appended to its next tool result; leftovers injected when the turn ends
- target blocked on clarify/approval/sudo → held, lands in that prompt's tool result when it resolves
  or times out (`agent.clarify_timeout`, default 1h); the sender is told it's blocked and on what
- target not hosted by any process that has seen it run → every live process tries to inject it once (covers
  a window that's open but hasn't run a turn since its process started, and a chat session, which only the
  messaging gateway can start); otherwise held until the session's next turn, in whichever process runs it.
  The sender's call waits up to `relay_wait_seconds` for another process to take it and says which
  happened: an `ask` whose question another process delivered keeps waiting for the answer. A stop or a
  mid-turn message from the user ends that wait early. Held messages survive restarts and reboots (7-day
  retention).
- target unloaded by Hermes (its window was closed) → held until its next turn, without waiting for a relay
  (no process starts a session its host unloaded); the sender is told
- `ask` works across processes: the asker polls the mailbox for the reply; a reply to an asker whose process
  has died is delivered as a normal message instead.
- `ask` and `wait` stop blocking when the user stops the session or sends it a message mid-turn.

Messages are framed `[INTERCOM — message from another agent session, NOT from <user_name>]` and told they carry
no user authority. Everything that came from another session is untrusted, because sessions read web pages
and mail: message bodies, titles, prompts, the final message `wait` quotes, and whatever is in the shared
mailbox, which is a file of the user's. Bodies and quoted transcripts are shown as `> ` quoted lines, so
nothing in them can close the frame or open another; wherever such text opens one of Hermes's own control
frames (`[OUT-OF-BAND USER MESSAGE`, `[System note:` and the rest of Hermes's list, read at load) or an
`[INTERCOM` frame, the opener is relabelled `[peer-quoted ...`; a title or prompt shown in quotes cannot
close them; and an id read back from the mailbox is shown as a quoted scrap unless it has the shape of one.

`/intercom` and `/intercom log [N]` in any session.

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
  are then posted in that chat). A session from a source outside the defaults is listed only once the agent
  has replied in it (or it continues a compacted session that it replaced): Hermes creates one for every
  thread a cron delivery opens, holding only the brief, and nobody's conversation is in it until the user
  answers there and the agent replies. A session hosted by a running process is always listed. Mail for a
  messaging session is only ever started with its route key, never its bare id (which the gateway accepts
  and then drops).
- `settings.poll_seconds` (3) — how often each process checks for mail left by other processes (a token-file
  read; the mailbox is only queried when it changed)
- `settings.relay_wait_seconds` (`poll_seconds` + 2, at most 30) — how long a call whose target this process
  cannot start waits for another process to start it before reporting the message as held
- `settings.run_dir` — override the presence directory (all processes of one Hermes home must agree)
- `settings.watchdog.enabled` (default true), `.board` (`sessions`), `.stale_minutes` (60),
  `.renudge_minutes` (60), `.interval_seconds` (300)
- `settings.list_window_hours` (24), `settings.inject_key_order` (`[route, id]`)
- `settings.settle_seconds` (5) — how long a session must have been idle before it counts as yielded;
  `settings.wait_max_hours` (24) — how long a registered wait is kept

## Update safety
Only documented surfaces: `register_tool/hook/command/cli_command`, `inject_message`, `get_config`, the hook
payloads in the hooks catalog, `gateway.session_context.get_session_env` / `bound_session_key`. `state.db`
(session titles; for `wait`, the transcript, the turn leases and the background-delegation table), the
kanban CLI and board reader, `tools.interrupt` (user stop / mid-turn message during a blocking call) and
Hermes's list of control-frame openers are read defensively: listing degrades (without the transcript table,
chat sessions the agent never replied in are listed too), a wait reports less or runs to its timeout,
the plugin's own copy of the list is used, nothing crashes. The seam check runs at every load, logs a
warning and pops a desktop notification (macOS, or `notify-send` on a Linux desktop) once per new problem
set; run `hermes intercom doctor` after an update to check by hand.

`ask` and `wait` block for at most 360 s (an `ask`'s relay wait counts towards its `timeout_s`), inside
Hermes's default tool deadline (420 s). A `send` or `reply` blocks for at most `relay_wait_seconds`. With a lower
deadline configured (`timeouts.tools.sequential_call`), Hermes abandons the call first: the caller gets a
tool timeout instead of the outcome, and a `wait` is then dropped rather than left registered. Keep
`timeout_s` below the configured deadline.

Tests: `cd ~/.hermes/plugins && python3 -m unittest discover -s intercom/tests -t .` (`test_core`: the logic
with a fake clock; `test_plugin`: the tool handler and hook adapters; `test_processes`: `wait` across real
OS processes sharing one mailbox)
### Updating and reloading
`hermes plugins update intercom` (on a tracking branch) pulls the new revision, drops the `__pycache__`
directories and records the revision. It does **not** reload the plugin: running processes keep the old code
until they reload it or restart. (The Update button on the Desktop/dashboard plugin page reloads when it
pulls something new; after a CLI `update` it finds nothing to pull and reloads nothing.) A caution verdict from Hermes's plugin scanner needs consent, which is only
asked on a terminal: from a session without one, read the findings and pass `--force`.

To load a new revision into the running Desktop backend and gateway without restarting anything, run under
the interpreter Hermes runs, with Hermes's checkout on `sys.path`:

```python
from hermes_cli.plugins_activation import activate_plugin_now
activate_plugin_now("intercom", in_process=False)
```

It unloads and reloads every plugin of the gateway and of one Desktop/dashboard backend (the host owner's,
else the Desktop's; reload or restart any other one separately): the handler, hooks and commands are live at
once, the tool's description waits for the next session, and the new hub takes over the running one's state. When the
files were changed without `update` (e.g. a checkout of another commit), drop the `__pycache__` directories
first. Avoid `hermes plugins disable intercom && hermes plugins enable intercom` as a reload: disabling
unloads the plugin in the gateway only (the Desktop backend keeps it until it next re-discovers plugins) and
leaves the gateway without the tool and hooks until `enable` runs; meanwhile the poller started by the first
load keeps delivering there. Disabling also
removes the `intercom` toolset from every saved `platform_toolsets` list, and enabling adds it to every saved
list, including those it was left out of. `hermes plugins enable` on its own does nothing while the plugin is
enabled.
