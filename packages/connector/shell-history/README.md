# cognee-community-connector-shell-history

A shell history data-source connector for [cognee](https://github.com/topoteretes/cognee):
turn what you typed into memory, with secrets redacted, so you or your coding agent can
ask "how did I run the e2e tests with Postgres last time?"

It reads **PowerShell** (PSReadLine), **bash**, **zsh** and **fish** history from disk,
groups the commands into blocks, and hands them to `cognee.remember(...)` as documents
that flow through cognee's normal cognify entity extraction.

## Install

```bash
uv pip install cognee-community-connector-shell-history
# or, from this monorepo:
cd packages/connector/shell-history && uv sync
```

Requires cognee 1.6.4 or later (per-row node sets). No other dependencies.

## Usage

```python
import cognee
from cognee_community_connector_shell_history import shell_history_source

await cognee.remember(
    shell_history_source(),  # auto-detects every shell's history on this machine
    dataset_name="shell_history",
    primary_key="id",
    write_disposition="merge",  # REQUIRED: the default "replace" drops unchanged rows
    max_rows_per_table=0,  # REQUIRED for more than 50 blocks
)

await cognee.recall("How did I start the app with docker compose?", datasets=["shell_history"])

# Scope to one tool or one shell through node sets:
await cognee.recall(
    "Which migrations did I run?",
    datasets=["shell_history"],
    node_name=["shell:tool/alembic"],
)
```

Re-run it whenever you like (a scheduled task, a shell exit hook): only changed blocks are
re-ingested. See `examples/example.py`.

### Where history is found

| Shell | File |
|---|---|
| PowerShell | `%APPDATA%\Microsoft\Windows\PowerShell\PSReadLine\*_history.txt` (Windows), `~/.local/share/powershell/PSReadLine/*_history.txt` |
| bash | `~/.bash_history` |
| zsh | `$ZDOTDIR/.zsh_history` or `.zhistory` |
| fish | `~/.local/share/fish/fish_history` |

Pass `shells=["bash"]` to limit detection, or `history_files={"bash": "/path/to/file"}`
to name files yourself.

### Options

| Argument | Default | |
|---|---|---|
| `shells` | all found | Limit auto-detection |
| `history_files` | auto | `{shell: path or [paths]}` |
| `since` | — | Skip timestamped commands older than this date |
| `exclude_patterns` | — | Extra regexes; a command matching one in full is left out |
| `include_trivial` | `False` | Keep `ls`, `cd`, `clear`, `git status`, … |
| `session_gap_minutes` | 30 | Pause that starts a new block (timestamped history) |
| `keep_trimmed` | `True` | Keep commands the shell trimmed off the top of its file |
| `resource_name` | `shell_history_blocks` | Must be unique per source within a dataset |

## What gets stored, and what gets redacted

**Redaction runs on every command before anything else**: before grouping, hashing or
storing. There is no switch to turn it off. A secret is replaced by
`[REDACTED:<kind>]`, keeping the rest of the command readable:

```
curl -H "Authorization: Bearer [REDACTED:auth_header]" https://api.example.com
```

| Kind | Example |
|---|---|
| Known key formats | OpenAI, Anthropic, GitHub, GitLab, AWS, Slack, Google, Stripe, Hugging Face, Telegram, Discord, SendGrid, npm, PyPI, DigitalOcean, Notion, Linear, Groq, Replicate, Supabase, JWTs, private-key blocks |
| Auth headers | `Authorization: Bearer …`, `X-Api-Key: …`, `Private-Token: …` |
| URL credentials | `https://user:password@host` |
| Password flags | `--password x`, `--token=x`, `sshpass -p x`, `mysql -px`, `curl -u user:x` |
| Secret assignments | `export API_KEY=x`, `$env:DB_PASSWORD = "x"`, `setx MY_TOKEN "x"` |

Commands that only *mention* secrets stay as they are: `git commit -m "fix token parsing"`,
`docker login --password-stdin`, `kubectl get secrets`, `mkdir -p`.

Redaction is pattern-based. It can't recognise an arbitrary random string passed as a bare
argument, so a secret in an unknown format typed as a plain positional argument would get
through. Keep this dataset on your own machine, or review it before sharing.

Stored per block: the shell, the history file's name, the time range (when the shell
records times), the tools used, and the redacted commands. Nothing else, and nothing
leaves your machine except, as with any cognee data, the redacted text sent to your LLM
provider during cognify. `cognee.forget(dataset="shell_history")` removes it all.

## How sync and forget work

* **Blocks, not lines.** One row per command would be noise. With timestamps (zsh
  extended history, fish, bash with `HISTTIMEFORMAT`), a 30-minute pause starts a new
  block, so a block is one sitting. Without them (PowerShell, plain bash), boundaries are
  *content-defined*: a block ends after a command whose hash hits a target, within
  4–64 commands. This is the technique rsync uses, so boundaries depend only on nearby
  commands.
* **Incremental.** Appending commands changes only the newest block; every other block
  keeps its id and is not re-ingested. A re-run with no new commands emits nothing.
* **Forget-on-delete.** Delete a line from your history, clear it (`history -c`,
  `Clear-History` plus deleting the file), or add an `exclude_patterns` entry, and the
  affected blocks are removed from memory on the next sync.
* **Trimming is not deleting.** Shells drop their oldest commands once history is full
  (bash keeps 500 by default, PSReadLine 4096). Thanks to content-defined boundaries, a
  trim leaves every block below the cut unchanged, so the connector can tell it apart from
  a deletion. Blocks that vanish *only from the top* are kept in memory, and the partial
  block a trim leaves behind isn't ingested twice. Pass `keep_trimmed=False` to mirror the
  file exactly.

## Limitations

* `since` only filters commands with a timestamp. PowerShell and plain bash are read in
  full.
* PSReadLine and bash don't record the working directory, so blocks say which shell ran a
  command, not where.

## Testing

```bash
uv run pytest tests/
```

The tests cover every redaction kind and the commands that must stay readable, each shell's
format (PowerShell continuations, bash timestamps, zsh extended and "metafied" bytes, fish
escapes), block stability under appends and trims, and, through real dlt pipelines into a
temp SQLite, re-syncs, edits, clears, deleted files, trims kept versus mirrored, exclude
changes, and that no fixture secret ever reaches a stored row.
