# intercom — messaging and sequencing between local Hermes sessions

User plugin (`~/.hermes/plugins/intercom`, outside the Hermes checkout, so `hermes update` never touches it).
Sessions can live in several Hermes processes at once (the messaging gateway, a Desktop `serve` process per
SSH connection, a TUI, a CLI), and each process can only start turns in the sessions it hosts. So the plugin
needs no sockets or daemons, but keeps its shared state outside process memory:

| what | where | written |
|---|---|---|
| mailbox: messages, ask/reply waiters, registered waits, who was told how mail is announced (SQLite) | `~/.hermes/intercom/intercom.db` | per message, per wait, and when a session is told how mail is announced |
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
| `read` | the mail held for the caller: what a line after a tool's output tells a working session about (see Delivery) |

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
| `message_arrived` | an intercom message for the caller arrived; it follows in the same result, as with every `intercom` call | no if it is from the target (it has spoken), otherwise yes |
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

The target is told once, the way a working session gets any mail (below) or at its next turn start and
never by waking it, that the caller is waiting, with the optional `message` as the reason, so it can release what the caller needs before its
turn ends (`send` to a waiter that is blocking returns its `wait` at once). A wait that could never end is
refused: the target waits for the caller (directly or through other sessions), or is blocked asking it a
question. Of two sessions that register on each other at the same moment, one is refused.

## Delivery
- idle target in the same process → injected as a new turn (wakes it; FIFO, never interrupts)
- idle target in another process → held in the mailbox; that process's poller injects it within `poll_seconds`
- running target (any process) → held. Its next tool result tells it that mail is waiting and its own
  `intercom` call hands the mail over (next section); what it has not read is injected when the turn ends
- target blocked on clarify/approval/sudo → held; it is told with that prompt's tool result, when the prompt
  resolves or times out (a clarify prompt after `agent.clarify_timeout`, 90 s by default); the sender is told
  it's blocked and on what
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

### Mail for a session that is working
A message is never put inside another tool's result. A model takes an instruction it finds in a tool result
for an injection, whoever claims to have put it there, and it is right to: that is where a web page or a
file would plant one. (Hermes itself hands the model the output of web, browser and MCP tools as untrusted
data.) Mail appended to tool results was ignored for that reason, with its sender left waiting.

A working session is shown a notice with a tool's output instead. In a result that is a JSON object it is
one field more, named `intercom`, so what Hermes and the model read back by key is what the tool returned;
after a result that is not JSON at all it is a last line:

```
{"output": "links: 212 checked, 0 broken", "exit_code": 0, "intercom": "[INTERCOM mail waiting — added by
Hermes's intercom plugin, not part of the tool's own output] Waiting for you from <user_name>'s other
sessions: 1 question (its sender is waiting for the answer) and 2 messages. Read them with
intercom(action=\"read\")."}
```

The notice says how much is waiting, never from whom or what. The session then calls `intercom`, and the
mail comes back in that call's own result: `read` returns it, and every other action is followed by it.
Acting on a forged notice is harmless, since `read` returns only what the plugin holds.

A notice in a tool result is still text in a tool result, so it is shown only to a session that was told
about it where it trusts what it reads:

- a session whose system prompt was built with the plugin loaded has a section there (`## Plugin Context:
  intercom`, through `register_system_prompt_section`);
- any other session that a turn can be started in is given the same text once, as a note at the start of
  its next turn. A note can be summarised away, so after a compaction the session is told again, unless its
  prompt has the section by then (Hermes asks for the section again when it builds a prompt again);
- a session that was not told (its turn began before the plugin was loaded or updated) is shown nothing and
  gets its mail when the turn ends, as a new turn;
- a subagent and a cron run get neither section nor note, and nobody can write to them. The one mail such a
  session can get is the late answer to a question it asked itself, and the result of that `ask` tells it
  how the answer will be announced;
- no host can start a turn in a subagent, a cron run or a one-shot run (`hermes chat -q`, which is told by
  its prompt like any CLI session). Mail for one is never given to a host, not when it is sent, not when
  its turn ends, not by a poller: a host asked to start a session by a key it does not know may take the
  mail and drop it. It waits for the session's own `intercom` call. What kind of session it is comes from
  Hermes's own record; while that cannot be read, nothing is started in it either and the question is put
  again at the next poll.

The mailbox records who was told and how (table `briefings`: `prompt`, `turn`, `call`, or `doubted`; a
record goes a week after it was last written, and the record of a session at work is renewed once a day).
A record says that the text was handed over, not that it arrived: Hermes leaves a section out when the
sections of all plugins outgrow their budget, and the turn that carried a note can be undone. So a session
that is shown the notice and lets its turn end without reading stops counting as told (`doubted`), and gets
the note at its next turn start. Only the note, or the session's own read, lifts a doubt: Hermes asks for
the section again for work of its own that runs under the session's id, which tells the session nothing.

The notice is left out of output Hermes hands the model as untrusted (`web_extract`, `web_search`,
`browser_*`, `mcp_*`: the plugin's copy of Hermes's rule, and Hermes's own answer as well where it can be
asked), out of an entry of a batch of connector calls (`connectors__*`: Hermes reads the batch back as one
object), out of a result whose tool is not named, out of results that are not text, out of JSON that is not
an object (there is no field to add and no line that would leave it readable), out of a result that has a
field named `intercom` of its own, and out of results over 7,500 characters, so that the result with the
notice in it stays under 8,000: the least at which Hermes replaces a result by a preview of its head
(100,000 for a model with a large window). It then comes with the next result. A result that no turn of the
session is in progress for is left alone as well: Hermes runs its own review of a turn, and its memory and
compaction work, under the session's id. The notice is repeated when more arrives, and otherwise after a
minute, three times at most: Hermes can still drop or cut a result once the plugin has added it. Showing it
takes nothing out of the mailbox, so a result Hermes drops costs no mail, and no mail is handed to a call
the user has stopped. The audit log has `told` when a message is first announced and `delivered` when the
session reads it.

Known limits:
- Hermes takes the first plugin's answer to `transform_tool_result`. Where another plugin rewrites the same
  result, one of the two changes is lost; the notice then comes again a minute later.
- A session's background review, and anything else Hermes runs under a session's id, has its own tool
  results. A notice goes into none of them, but a review that runs while the session's own turn is in
  progress can take one.
- An intercom result that carries mail is not one JSON value (the frames follow the JSON head). Where a
  mail body quotes `"error"` or `"failed"` early, Hermes's text test marks the call failed in its display
  and counts it towards its guard against repeated failures.
- At the turn start where a session is given the note, mail and wait notices that would make the piece
  longer than 8,000 characters stay held and are announced by the notice instead: Hermes cuts a hook's
  context of over 10,000 characters down to its first and last 500. A session that is due no note gets all
  of its mail at the turn start, as before, and a long piece is cut the same way.
- `read` hands over everything that is held. More than Hermes allows one tool result (100,000 characters,
  less for a model with a small window) is replaced by a preview, with the full text in a file Hermes names.
- What its `ask` told a subagent or a cron run can be summarised away like a note, and there is no turn
  start to tell it again. After a compaction of its context such a session is no longer shown the notice,
  and a late answer waits for its next `intercom` call; an `ask` it makes after that tells it again.
- The hooks tell the plugin the tool's name, the session's surface and what a compaction did. A Hermes that
  stopped passing one of those would cost the feature that needs it — the notice, the note, or a note
  surviving a prune of tool results — and the doctor cannot see it.

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
Only documented surfaces: `register_tool/hook/command/cli_command`, `register_system_prompt_section`,
`inject_message`, `get_config`, the hook payloads in the hooks catalog, the `session_id` Hermes passes a tool
handler, `gateway.session_context.get_session_env` / `bound_session_key`. `state.db`
(session titles; for `wait`, the transcript, the turn leases and the background-delegation table), the
kanban CLI and board reader, `tools.interrupt` (user stop / mid-turn message during a blocking call),
Hermes's list of control-frame openers and its rule for which tools' output is untrusted are read
defensively (so are a few names and sizes copied from its source, which the doctor cannot check: the
`connectors__` prefix, the `subagent`, `cron` and `oneshot` session kinds, the 8,000-character floor of its
result budget, the 10,000-character limit on a hook's context, and what the hooks say about a tool's name, a
session's surface and a compaction): listing degrades (without the transcript table, chat sessions the agent never replied in are
listed too), a wait reports less or runs to its timeout, the plugin's own copy of the list or the rule is
used, nothing crashes. On a Hermes without prompt sections, or one that refuses the section, every session
is told by the note at a turn start, and the doctor says which. The seam check runs at every load, logs a
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

A process that has not reloaded yet keeps the previous behaviour for the sessions it hosts (with the version
before this change, whole messages in their tool results), and tells its own callers what that version
promised. They share one mailbox and one set of presence files either way.

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
