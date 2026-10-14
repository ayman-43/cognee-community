"""Shell history connector for cognee, a ``dlt`` source that turns what you typed into memory.

Sync PowerShell, bash, zsh and fish history into cognee, with secrets redacted,
incrementally and with forget-on-delete — "how did I run this last time?"::

    import cognee
    from cognee_community_connector_shell_history import shell_history_source

    await cognee.remember(
        shell_history_source(),  # auto-detects the shells on this machine
        dataset_name="shell_history",
        primary_key="id",
        write_disposition="merge",  # REQUIRED, see below
        max_rows_per_table=0,  # REQUIRED for more than 50 blocks
    )

Design
------
* **Local only**: history files are read from disk; nothing is sent anywhere
  except, as with every cognee source, the redacted text to your LLM provider.
* **Redaction first**: every command passes through :func:`redact` before it
  is grouped, rendered or hashed. Known key formats, auth headers, URL
  credentials, password flags and secret-looking assignments become
  ``[REDACTED:<kind>]``. There is no switch to turn it off.
* **One row per block of commands**: one row per command would be noise, one
  per file is unsearchable. Commands are grouped into blocks:

  - with timestamps (zsh extended history, fish, bash ``HISTTIMEFORMAT``), a
    gap of ``session_gap_minutes`` starts a new block, so a block is a sitting;
  - without (PowerShell, plain bash), block boundaries are *content-defined*:
    a block ends after a command whose hash hits a target, within min/max
    sizes, the technique rsync and backup tools use. Boundaries depend only on
    nearby commands, so appending commands, or the shell trimming old ones off
    the top, leaves every other block (and its row id) unchanged.

* **Row identity**: a block's id hashes the shell, the history file and the
  block's raw commands, so an unchanged block keeps its id across runs and an
  edited one gets a new id (the old one is tombstoned).
* **Incremental**: the whole file is parsed each run (history files are capped
  by the shell at a few MB at most), and only blocks whose rendered text
  changed are emitted. In practice that is the newest block, plus any new ones.
* **Forget-on-delete**: a block that disappears from its file is tombstoned
  (``_deleted``), and dlt's ``hard_delete`` plus cognee's ``orphan_cleanup``
  remove it from the graph and vector stores. So are blocks whose commands are
  all excluded after ``exclude_patterns`` changes. A history file that was
  deleted or emptied forgets everything it held.
* **Trimming is not deleting**: shells drop the oldest commands once history
  reaches its size (bash ``HISTFILESIZE`` defaults to 500, PSReadLine to 4096).
  With ``keep_trimmed=True`` (default), blocks that vanish *only from the top*
  of a file, ahead of the first block still present, are kept in memory, and
  the partial block the trim left behind is recognised and not re-ingested.
  ``keep_trimmed=False`` mirrors the file exactly.
* **Node sets**: ``shell:<shell>`` and ``shell:tool/<tool>`` for each tool a
  block uses (``git``, ``docker``, ``uv``...), so recall can be scoped with
  ``node_name=["shell:tool/docker"]``.

.. important::
   ``write_disposition="merge"`` is **mandatory**. The add pipeline defaults to
   ``"replace"``, which would drop every block a run did not re-emit.

Limitations
-----------
* Redaction is pattern-based. It covers the common formats (see
  ``_SECRET_PATTERNS``) but cannot recognise an arbitrary high-entropy string
  passed as a bare positional argument.
* ``since`` only filters commands that carry a timestamp; PowerShell and plain
  bash history are read in full.
* PSReadLine and bash store no working directory, so a block says which shell
  ran the commands, not where.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import sys
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = get_logger("shell_history_connector")

SHELL_SOURCE_TAG = "shell"
DEFAULT_RESOURCE_NAME = "shell_history_blocks"
SHELLS = ("powershell", "bash", "zsh", "fish")
_SHELL_LABEL = {"powershell": "PowerShell", "bash": "bash", "zsh": "zsh", "fish": "fish"}

# Content-defined block sizes (commands): average, minimum, maximum.
_BLOCK_TARGET = 16
_BLOCK_MIN = 4
_BLOCK_MAX = 64
_DEFAULT_SESSION_GAP_MINUTES = 30

# Commands with no recall value; matched against the whole (stripped) command.
DEFAULT_EXCLUDE_PATTERNS = (
    r"(ls|ll|la|dir|l)(\s+-\w+)*",
    r"cd(\s+\S+)?",
    r"(clear|cls|pwd|exit|history|h|whoami|date|true|false)",
    r"git\s+(status|st|diff|log)",
)

_REDACTED = "[REDACTED:{}]"

# Values: a double- or single-quoted string, or a run of non-space characters.
# A value an earlier, more specific rule already replaced is left alone, so its
# marker keeps the precise kind and the count is not doubled.
_VALUE = r"""(?!["']?\[REDACTED)(?:"[^"]*"|'[^']*'|[^\s"']+)"""
_SECRET_NAME = (
    r"[A-Za-z0-9_]*(?:PASS(?:WORD|WD)?|PWD|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY"
    r"|CREDENTIALS?|AUTH(?:_?KEY)?|CLIENT_?SECRET)[A-Za-z0-9_]*"
)

# (kind, pattern, replacement). Order matters: specific formats first, so a
# token inside an assignment is reported by its format, then the generic rules.
_SECRET_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    (
        "private_key",
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)",
            re.DOTALL,
        ),
        _REDACTED.format("private_key"),
    ),
    (
        "anthropic_key",
        re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{20,}"),
        _REDACTED.format("anthropic_key"),
    ),
    (
        "openai_key",
        re.compile(r"\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_\-]{20,}"),
        _REDACTED.format("openai_key"),
    ),
    (
        "github_token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})"),
        _REDACTED.format("github_token"),
    ),
    ("gitlab_token", re.compile(r"\bglpat-[A-Za-z0-9_\-]{20,}"), _REDACTED.format("gitlab_token")),
    (
        "aws_access_key",
        re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        _REDACTED.format("aws_access_key"),
    ),
    (
        "slack_token",
        re.compile(r"\bxox[abposr]-[A-Za-z0-9\-]{10,}"),
        _REDACTED.format("slack_token"),
    ),
    (
        "google_api_key",
        re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"),
        _REDACTED.format("google_api_key"),
    ),
    (
        "stripe_key",
        re.compile(r"\b[rs]k_(?:live|test)_[0-9A-Za-z]{16,}"),
        _REDACTED.format("stripe_key"),
    ),
    *(
        (kind, re.compile(pattern), _REDACTED.format(kind))
        for kind, pattern in (
            ("telegram_bot_token", r"\b\d{8,10}:AA[A-Za-z0-9_\-]{30,}"),
            ("discord_token", r"\b[MNO][A-Za-z\d_\-]{23,25}\.[A-Za-z\d_\-]{6}\.[A-Za-z\d_\-]{27,}"),
            ("sendgrid_key", r"\bSG\.[A-Za-z0-9_\-]{16,}\.[A-Za-z0-9_\-]{16,}"),
            ("npm_token", r"\bnpm_[A-Za-z0-9]{36}\b"),
            ("pypi_token", r"\bpypi-[A-Za-z0-9_\-]{50,}"),
            ("digitalocean_token", r"\bdo[opr]_v1_[a-f0-9]{64}\b"),
            ("notion_token", r"\b(?:secret_[A-Za-z0-9]{43}|ntn_[A-Za-z0-9]{40,})"),
            ("linear_key", r"\blin_(?:api|oauth)_[A-Za-z0-9]{40}\b"),
            ("groq_key", r"\bgsk_[A-Za-z0-9]{40,}"),
            ("replicate_token", r"\br8_[A-Za-z0-9]{37}\b"),
            ("supabase_token", r"\bsbp_[a-f0-9]{40}\b"),
        )
    ),
    (
        "huggingface_token",
        re.compile(r"\bhf_[A-Za-z0-9]{30,}"),
        _REDACTED.format("huggingface_token"),
    ),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_\-]{8,}\.eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}"),
        _REDACTED.format("jwt"),
    ),
    # Authorization: Bearer xyz / X-Api-Key: xyz (curl -H, httpie, Invoke-RestMethod).
    (
        "auth_header",
        re.compile(
            r"(?i)\b((?:proxy-)?authorization|x-api-key|api-key|x-auth-token|private-token)"
            r"(\s*[:=]\s*)(?:(bearer|basic|token|digest)\s+)?([^\s\"',;}]+)"
        ),
        r"\1\2\3 " + _REDACTED.format("auth_header"),
    ),
    # https://user:password@host
    (
        "url_credentials",
        re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://[^\s/:@\[]+):(?!\[REDACTED)([^\s/@]+)@"),
        r"\1:" + _REDACTED.format("url_credentials") + "@",
    ),
    # curl -u user:password / --user user:password
    (
        "basic_auth",
        re.compile(r"(\s(?:-u|--user)\s+[\"']?[^\s:\"']+):(?!\[REDACTED)([^\s\"']+)"),
        r"\1:" + _REDACTED.format("basic_auth"),
    ),
    # --password=x, --token x, -Password x (PowerShell), sshpass -p x, mysql -px
    (
        "password_flag",
        re.compile(
            r"(?i)(\s--?(?:password|passwd|pass|token|api-?key|secret|client-secret"
            r"|access-?key|secret-?key|access-?token|auth-?token)(?:=|\s+))" + _VALUE
        ),
        r"\1" + _REDACTED.format("password_flag"),
    ),
    (
        "password_flag",
        re.compile(r"(\bsshpass\s+-p\s*)" + _VALUE),
        r"\1" + _REDACTED.format("password_flag"),
    ),
    (
        "password_flag",
        re.compile(r"(\b(?:mysql|mysqldump|mariadb)\b[^|;&]*?\s-p)(?!\s)" + _VALUE),
        r"\1" + _REDACTED.format("password_flag"),
    ),
    # export API_KEY=x / $env:API_KEY = "x" / setx API_KEY "x" / set API_KEY=x
    (
        "secret_assignment",
        re.compile(r"(?i)((?:\$env:)?\b" + _SECRET_NAME + r"\b\s*=\s*)" + _VALUE),
        r"\1" + _REDACTED.format("secret_assignment"),
    ),
    (
        "secret_assignment",
        re.compile(r"(?i)(\bsetx\s+" + _SECRET_NAME + r"\s+)" + _VALUE),
        r"\1" + _REDACTED.format("secret_assignment"),
    ),
]


def redact(command: str) -> tuple[str, Counter]:
    """Return ``command`` with secrets replaced, and a count of what was replaced."""
    found: Counter = Counter()
    for kind, pattern, replacement in _SECRET_PATTERNS:
        command, count = pattern.subn(replacement, command)
        if count:
            found[kind] += count
    return command, found


# ---------------------------------------------------------------------------
# History file parsers: bytes -> [Entry]
# ---------------------------------------------------------------------------
@dataclass
class Entry:
    command: str
    timestamp: int | None = None  # Unix seconds


def _unmetafy(data: bytes) -> bytes:
    """Undo zsh's "metafied" encoding: 0x83 followed by (byte ^ 0x20)."""
    if b"\x83" not in data:
        return data
    out = bytearray()
    iterator = iter(data)
    for byte in iterator:
        if byte == 0x83:
            out.append(next(iterator, 0x20) ^ 0x20)
        else:
            out.append(byte)
    return bytes(out)


def parse_powershell(text: str) -> list[Entry]:
    """PSReadLine: one command per line; a trailing backtick continues the command."""
    entries, pending = [], []
    for line in text.splitlines():
        if line.endswith("`"):
            pending.append(line[:-1])
            continue
        pending.append(line)
        command = "\n".join(pending).strip()
        pending = []
        if command:
            entries.append(Entry(command))
    if pending and "\n".join(pending).strip():
        entries.append(Entry("\n".join(pending).strip()))
    return entries


def parse_bash(text: str) -> list[Entry]:
    """bash: one command per line, preceded by ``#<epoch>`` when HISTTIMEFORMAT is set."""
    entries, timestamp = [], None
    for line in text.splitlines():
        if re.fullmatch(r"#\d{9,11}", line.strip()):
            timestamp = int(line.strip()[1:])
            continue
        if line.strip():
            entries.append(Entry(line.strip(), timestamp))
        timestamp = None
    return entries


_ZSH_EXTENDED = re.compile(r"^: (\d+):\d+;(.*)$", re.DOTALL)


def parse_zsh(text: str) -> list[Entry]:
    """zsh: ``: <epoch>:<duration>;<command>`` (extended) or plain lines.

    A command spanning several lines ends each but the last with a backslash.
    """
    entries, pending = [], []
    for line in text.splitlines():
        if line.endswith("\\") and not line.endswith("\\\\"):
            pending.append(line[:-1])
            continue
        pending.append(line)
        raw = "\n".join(pending)
        pending = []
        match = _ZSH_EXTENDED.match(raw)
        command, timestamp = (match.group(2), int(match.group(1))) if match else (raw, None)
        if command.strip():
            entries.append(Entry(command.strip(), timestamp))
    return entries


def parse_fish(text: str) -> list[Entry]:
    """fish: ``- cmd: <command>`` then ``  when: <epoch>``; ``\\n`` and ``\\\\`` escaped."""
    entries: list[Entry] = []
    for line in text.splitlines():
        if line.startswith("- cmd: "):
            command = re.sub(
                r"\\(.)", lambda m: "\n" if m.group(1) == "n" else m.group(1), line[7:]
            )
            entries.append(Entry(command.strip()))
        elif line.startswith("  when: ") and entries:
            with contextlib.suppress(ValueError):
                entries[-1].timestamp = int(line[8:].strip())
    return [entry for entry in entries if entry.command]


_PARSERS: dict[str, Callable[[str], list[Entry]]] = {
    "powershell": parse_powershell,
    "bash": parse_bash,
    "zsh": parse_zsh,
    "fish": parse_fish,
}


def read_history(shell: str, path: Path) -> list[Entry] | None:
    """Parse one history file; ``None`` when it does not exist (deleted or never used)."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if shell == "zsh":
        data = _unmetafy(data)
    return _PARSERS[shell](data.decode("utf-8", errors="replace"))


def default_history_files(home: Path | None = None) -> dict[str, list[Path]]:
    """History files of every supported shell found on this machine."""
    home = home or Path.home()
    candidates: dict[str, list[Path]] = {shell: [] for shell in SHELLS}
    if sys.platform == "win32":
        appdata = Path(os.environ.get("APPDATA", home / "AppData" / "Roaming"))
        psreadline = appdata / "Microsoft" / "Windows" / "PowerShell" / "PSReadLine"
    else:
        psreadline = home / ".local" / "share" / "powershell" / "PSReadLine"
    candidates["powershell"] = sorted(psreadline.glob("*_history.txt"))
    candidates["bash"] = [home / ".bash_history"]
    zdotdir = Path(os.environ.get("ZDOTDIR", home))
    candidates["zsh"] = [zdotdir / ".zsh_history", zdotdir / ".zhistory"]
    candidates["fish"] = [home / ".local" / "share" / "fish" / "fish_history"]
    return {
        shell: [path for path in paths if path.is_file()]
        for shell, paths in candidates.items()
        if any(path.is_file() for path in paths)
    }


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------
@dataclass
class Block:
    entries: list[Entry] = field(default_factory=list)

    @property
    def raw_hash(self) -> str:
        return _hash("\n\x00".join(entry.command for entry in self.entries))

    @property
    def last_hash(self) -> str:
        return _hash(self.entries[-1].command)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _is_boundary(command: str) -> bool:
    return int(_hash(command)[:8], 16) % _BLOCK_TARGET == 0


def chunk(entries: list[Entry], session_gap_seconds: int) -> list[Block]:
    """Group commands into blocks (see the module docstring)."""
    blocks: list[Block] = []
    current = Block()
    for index, entry in enumerate(entries):
        current.entries.append(entry)
        following = entries[index + 1] if index + 1 < len(entries) else None
        size = len(current.entries)
        gap = (
            following is not None
            and entry.timestamp is not None
            and following.timestamp is not None
            and following.timestamp - entry.timestamp > session_gap_seconds
        )
        if (
            following is None
            or gap
            or size >= _BLOCK_MAX
            or (size >= _BLOCK_MIN and _is_boundary(entry.command))
        ):
            blocks.append(current)
            current = Block()
    return blocks


_TOOL = re.compile(r"[a-z0-9][a-z0-9._+\-]{0,29}")
_PREFIXES = {"sudo", "doas", "time", "env", "nohup", "exec", "command", "builtin", "&", "."}


# Navigation and output builtins: present in most blocks, they say nothing about
# what the block was for.
_NOT_TOOLS = {
    "cd",
    "pushd",
    "popd",
    "set-location",
    "sl",
    "echo",
    "write-host",
    "write-output",
    "export",
    "set",
    "setx",
    "unset",
}


def _tools(command: str) -> list[str]:
    """Every program a (possibly chained) command runs, in order."""
    names = (_tool(part) for part in re.split(r"&&|\|\||[;|]", command))
    return [name for name in names if name and name not in _NOT_TOOLS]


def _tool(command: str) -> str | None:
    """The program a command runs (``sudo docker ps`` -> ``docker``)."""
    for word in command.split():
        if word in _PREFIXES or re.fullmatch(r"\$?[A-Za-z_][\w:]*=\S*", word):
            continue
        name = re.split(r"[\\/]", word.strip("\"'&"))[-1].lower()
        name = re.sub(r"\.(exe|cmd|bat|ps1|sh)$", "", name)
        return name if _TOOL.fullmatch(name) else None
    return None


def _format_time(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d %H:%M UTC")


def _render(shell: str, path: Path, commands: list[Entry]) -> tuple[str, str, list[str]]:
    """(title, content, tools) for a block's kept, redacted commands."""
    label = _SHELL_LABEL[shell]
    tools = list(dict.fromkeys(tool for entry in commands for tool in _tools(entry.command)))
    stamps = [entry.timestamp for entry in commands if entry.timestamp is not None]
    when = ""
    if stamps:
        start, end = _format_time(min(stamps)), _format_time(max(stamps))
        when = start if start == end else f"{start} to {end}"
    tool_text = ", ".join(tools[:8]) if tools else "misc"
    title = f"{label} commands ({tool_text})" + (f", {when}" if when else "")

    lines = [f"Shell: {label} (history file {path.name})"]
    if when:
        lines.append(f"Time: {when}")
    if tools:
        lines.append(f"Tools used: {', '.join(tools)}")
    lines.append(f"{len(commands)} command(s), in the order they were run:")
    fence = "```"
    body = "\n".join(entry.command for entry in commands)
    if fence in body:
        fence = "~~~~"
    content = (
        "\n".join(lines)
        + f"\n\n{fence}{'powershell' if shell == 'powershell' else 'bash'}\n{body}\n{fence}"
    )
    return title, content, tools


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
@dataclass
class _Source:
    shell: str
    path: Path

    @property
    def key(self) -> str:
        return f"{self.shell}:{self.path.resolve().as_posix().lower()}"


def _iter_rows(
    sources: list[_Source],
    state: dict,
    stats: dict[str, int],
    *,
    excludes: list[re.Pattern],
    since: int | None,
    session_gap_seconds: int,
    keep_trimmed: bool,
) -> Iterator[dict]:
    """Emit changed blocks and tombstones for every history file; update ``state``.

    ``state["files"][key]`` is the file's blocks in order, each
    ``[row id, last-command hash, rendered hash or None]``.
    """
    for key in ("files", "blocks", "emitted", "unchanged", "deleted", "kept_trimmed", "redacted"):
        stats[key] = 0
    previous_files: dict[str, list[list]] = state.get("files", {})
    files_state: dict[str, list[list]] = {}
    rows: list[dict] = []
    tombstones: list[str] = []

    # Parse every file before emitting anything: a file that cannot be read
    # raises here, before a single tombstone exists.
    parsed = []
    for source in sources:
        entries = read_history(source.shell, source.path)
        parsed.append((source, entries))

    for source, entries in parsed:
        previous = previous_files.get(source.key, [])
        if entries:
            stats["files"] += 1
        redacted_entries = []
        for entry in entries or []:
            if since is not None and entry.timestamp is not None and entry.timestamp < since:
                continue
            command, found = redact(entry.command)
            stats["redacted"] += sum(found.values())
            redacted_entries.append(Entry(command, entry.timestamp))
        blocks = chunk(redacted_entries, session_gap_seconds)
        stats["blocks"] += len(blocks)

        ids: list[str] = []
        occurrences: Counter = Counter()
        for block in blocks:
            occurrences[block.raw_hash] += 1
            seed = f"{source.key}\x00{block.raw_hash}\x00{occurrences[block.raw_hash]}"
            ids.append(f"{source.shell}-{_hash(seed)[:24]}")

        previous_ids = [item[0] for item in previous]
        current_ids = set(ids)
        kept: list[list] = []
        skip_first = False
        if keep_trimmed and blocks and previous:
            surviving = next(
                (i for i, row_id in enumerate(previous_ids) if row_id in current_ids), None
            )
            if surviving is not None and surviving > 0:
                kept = previous[:surviving]
                # A trim that cut through a block leaves its tail behind as a new
                # first block ending on the same command: that is not new history.
                if ids[0] not in previous_ids and blocks[0].last_hash == kept[-1][1]:
                    skip_first = True
        stats["kept_trimmed"] += len(kept)
        kept_ids = {item[0] for item in kept}
        previous_rendered = {item[0]: item[2] for item in previous}

        current_state = list(kept)
        for index, (row_id, block) in enumerate(zip(ids, blocks, strict=True)):
            if index == 0 and skip_first:
                continue
            commands = [
                entry
                for entry in block.entries
                if not any(pattern.fullmatch(entry.command.strip()) for pattern in excludes)
            ]
            rendered_hash = None
            if commands:
                title, content, tools = _render(source.shell, source.path, commands)
                rendered_hash = _hash(content)
                if previous_rendered.get(row_id) == rendered_hash:
                    stats["unchanged"] += 1
                else:
                    rows.append(
                        {
                            "id": row_id,
                            "title": title,
                            "content": content,
                            "_deleted": False,
                            NODE_SET_COLUMN: [f"{SHELL_SOURCE_TAG}:{source.shell}"]
                            + [f"{SHELL_SOURCE_TAG}:tool/{tool}" for tool in tools],
                        }
                    )
            current_state.append([row_id, block.last_hash, rendered_hash])
        files_state[source.key] = current_state

        alive = {item[0] for item in current_state if item[2] is not None}
        tombstones.extend(
            row_id
            for row_id, rendered in previous_rendered.items()
            if rendered is not None and row_id not in alive and row_id not in kept_ids
        )

    # A history file that is no longer configured or detected: forget it.
    for key, previous in previous_files.items():
        if key not in files_state:
            tombstones.extend(item[0] for item in previous if item[2] is not None)

    stats["emitted"] = len(rows)
    stats["deleted"] = len(tombstones)
    yield from rows
    for row_id in tombstones:
        yield {"id": row_id, "_deleted": True}

    state["files"] = files_state
    logger.info(
        "Shell history: %d file(s), %d block(s): %d emitted, %d unchanged, %d forgotten, "
        "%d kept after a trim; %d secret(s) redacted.",
        stats["files"],
        stats["blocks"],
        stats["emitted"],
        stats["unchanged"],
        stats["deleted"],
        stats["kept_trimmed"],
        stats["redacted"],
    )


def shell_history_source(
    shells: list[str] | None = None,
    history_files: dict[str, str | os.PathLike | list[str | os.PathLike]] | None = None,
    *,
    since: date | datetime | str | None = None,
    exclude_patterns: list[str] | None = None,
    include_trivial: bool = False,
    session_gap_minutes: int = _DEFAULT_SESSION_GAP_MINUTES,
    keep_trimmed: bool = True,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    check_active: Callable[[], None] | None = None,
):
    """Create a dlt resource that syncs shell history into cognee.

    Args:
        shells: Restrict auto-detection to these shells (``powershell``,
            ``bash``, ``zsh``, ``fish``). Default: every shell found.
        history_files: Explicit files instead of auto-detection, as
            ``{shell: path or [paths]}``. A configured file that does not exist
            (yet, or any more) holds no history: blocks synced from it before
            are forgotten.
        since: Skip timestamped commands older than this (``date``,
            ``datetime`` or ``YYYY-MM-DD``). Commands without a timestamp are
            always read.
        exclude_patterns: Extra regular expressions; a command matching one in
            full is left out. Changing them re-syncs the affected blocks.
        include_trivial: Keep ``ls``, ``cd``, ``clear``, ``git status``... which
            are excluded by default.
        session_gap_minutes: For timestamped history, the pause that starts a
            new block.
        keep_trimmed: Keep blocks the shell trimmed off the top of its history
            file (see the module docstring). ``False`` mirrors the file exactly.
        resource_name: Stable dlt resource name, also the staging table and the
            key of the incremental state. Every shell-history source syncing
            into one dataset needs its own name.
        check_active: Optional host authorization checkpoint during extraction.

    Returns:
        A dlt resource for ``cognee.remember(..., primary_key="id",
        write_disposition="merge", max_rows_per_table=0)``.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 2:
        raise RuntimeError(
            "The shell history connector requires a cognee build that reads per-row node "
            "sets (DOCUMENT_SYNC_VERSION >= 2, cognee 1.6.4 or later). Upgrade cognee."
        )

    for shell in list(shells or []) + list(history_files or {}):
        if shell not in SHELLS:
            raise ValueError(f"Unknown shell {shell!r}; supported: {', '.join(SHELLS)}.")
    if session_gap_minutes <= 0:
        raise ValueError("session_gap_minutes must be positive.")

    excludes = []
    for pattern in list(exclude_patterns or []) + (
        [] if include_trivial else list(DEFAULT_EXCLUDE_PATTERNS)
    ):
        try:
            excludes.append(re.compile(pattern))
        except re.error as exc:
            raise ValueError(f"Invalid exclude pattern {pattern!r}: {exc}") from exc

    since_ts = None
    if since is not None:
        if isinstance(since, str):
            since = date.fromisoformat(since)
        if not isinstance(since, datetime):
            since = datetime(since.year, since.month, since.day, tzinfo=UTC)
        since_ts = int(since.timestamp())

    def resolve_sources() -> list[_Source]:
        if history_files:
            files = {
                shell: [Path(p) for p in (paths if isinstance(paths, list) else [paths])]
                for shell, paths in history_files.items()
            }
        else:
            files = default_history_files()
            if shells:
                files = {shell: paths for shell, paths in files.items() if shell in shells}
            if not files:
                raise ValueError(
                    "No shell history found on this machine; pass history_files= explicitly."
                )
        return [_Source(shell, path) for shell, paths in files.items() for path in paths]

    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        # cognee_node_set needs no json hint here: ingest_dlt_source applies
        # NODE_SET_COLUMN_HINT to every document source, merged with this one.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def shell_history_blocks():
        yield from dlt_utils.guarded_rows(
            _iter_rows(
                resolve_sources(),
                dlt.current.resource_state(),
                stats,
                excludes=excludes,
                since=since_ts,
                session_gap_seconds=session_gap_minutes * 60,
                keep_trimmed=keep_trimmed,
            ),
            check_active,
        )

    resource = shell_history_blocks()
    # Opt into the document ingestion path: each block row (id/title/content,
    # plus cognee_node_set) becomes a text document that flows through normal
    # cognify. resolve_dlt_sources reads this marker; it never imports this
    # connector.
    setattr(resource, DOCUMENT_SOURCE_ATTR, SHELL_SOURCE_TAG)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    # Host-readable diagnostics contain counts only, never commands.
    resource.cognee_sync_stats = stats
    return resource
