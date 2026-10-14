"""Unit tests for the shell history dlt connector.

* DB-free tests: redaction (every secret kind, and commands that must stay
  readable), each shell's history format, content-defined blocks, tool names,
  auto-detection and argument validation.
* dlt-pipeline tests (temp sqlite destination): first sync, unchanged re-sync,
  appends, edits and clears (forget-on-delete), trimming kept vs mirrored,
  exclude changes, node sets, and that no fixture secret ever reaches a row.
"""

import json
from pathlib import Path

import pytest

from cognee_community_connector_shell_history import shell_history as sh
from cognee_community_connector_shell_history.shell_history import (
    Entry,
    _tool,
    _tools,
    chunk,
    default_history_files,
    parse_bash,
    parse_fish,
    parse_powershell,
    parse_zsh,
    read_history,
    redact,
    shell_history_source,
)

# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------
SECRETS = {
    "openai": "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789",
    "anthropic": "sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWx",
    "github": "ghp_" + "A1b2C3d4E5" * 4,
    "aws": "AKIAIOSFODNN7EXAMPLE",
    "slack": "xoxb-1234567890-abcdefghij",
    "jwt": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N",
    "bearer": "abc123.def456-opaque",
    "url_password": "hunter2pw",
    "mysql": "s3cretMysql",
    "flag": "flagSecret99",
    "assigned": "plainValue123",
    "setx": "epoConsumerKey77",
    "basic": "curlPass55",
    "private_key": "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC",
}


@pytest.mark.parametrize(
    ("command", "secret", "kind"),
    [
        (f"export OPENAI_API_KEY={SECRETS['openai']}", SECRETS["openai"], "openai_key"),
        (
            f'$env:ANTHROPIC_API_KEY = "{SECRETS["anthropic"]}"',
            SECRETS["anthropic"],
            "anthropic_key",
        ),
        (
            f"git clone https://{SECRETS['github']}@github.com/a/b",
            SECRETS["github"],
            "github_token",
        ),
        (f"aws configure set aws_access_key_id {SECRETS['aws']}", SECRETS["aws"], "aws_access_key"),
        (f"curl -d token={SECRETS['slack']} https://slack.com", SECRETS["slack"], "slack_token"),
        (f"http :8000 Cookie:{SECRETS['jwt']}", SECRETS["jwt"], "jwt"),
        (
            f'curl -H "Authorization: Bearer {SECRETS["bearer"]}" https://api.x.io',
            SECRETS["bearer"],
            "auth_header",
        ),
        (
            f"git remote add o https://ayman:{SECRETS['url_password']}@git.example.com/r.git",
            SECRETS["url_password"],
            "url_credentials",
        ),
        (f"mysql -u root -p{SECRETS['mysql']} app", SECRETS["mysql"], "password_flag"),
        (f"docker login --password={SECRETS['flag']} reg.io", SECRETS["flag"], "password_flag"),
        (f"psql --password {SECRETS['flag']}", SECRETS["flag"], "password_flag"),
        (f"sshpass -p {SECRETS['flag']} ssh host", SECRETS["flag"], "password_flag"),
        (f"DB_PASSWORD={SECRETS['assigned']} ./run.sh", SECRETS["assigned"], "secret_assignment"),
        (f'setx EPO_OPS_SECRET "{SECRETS["setx"]}"', SECRETS["setx"], "secret_assignment"),
        (f"curl -u admin:{SECRETS['basic']} https://h", SECRETS["basic"], "basic_auth"),
        (
            "echo '-----BEGIN PRIVATE KEY-----\n"
            + SECRETS["private_key"]
            + "\n-----END PRIVATE KEY-----'",
            SECRETS["private_key"],
            "private_key",
        ),
    ],
)
def test_redact_removes_each_kind_of_secret(command, secret, kind):
    redacted, found = redact(command)
    assert secret not in redacted
    assert f"[REDACTED:{kind}]" in redacted
    assert found[kind] >= 1


@pytest.mark.parametrize(
    "command",
    [
        'git commit -m "fix token parsing"',
        "mkdir -p build/out",
        "npm test -- --pass-with-no-tests",
        'grep -r password "src"',
        "docker login --password-stdin reg.io",
        "echo $PATH",
        "kubectl get secrets -n prod",
        "pass show email/work",
        "mysql -p app",
    ],
)
def test_redact_leaves_ordinary_commands_alone(command):
    assert redact(command) == (command, {})


def test_redact_keeps_context_around_the_secret():
    redacted, _ = redact(f'curl -H "Authorization: Bearer {SECRETS["bearer"]}" https://api.x.io')
    assert redacted == 'curl -H "Authorization: Bearer [REDACTED:auth_header]" https://api.x.io'


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def test_parse_powershell_joins_backtick_continuations():
    text = "git status\nGet-ChildItem `\n  -Recurse `\n  -Filter *.py\n\nuv sync\n"
    assert [e.command for e in parse_powershell(text)] == [
        "git status",
        "Get-ChildItem \n  -Recurse \n  -Filter *.py",
        "uv sync",
    ]


def test_parse_bash_reads_histtimeformat_stamps():
    entries = parse_bash("#1700000000\nmake build\nls\n#1700000100\nmake test\n")
    assert [(e.command, e.timestamp) for e in entries] == [
        ("make build", 1700000000),
        ("ls", None),
        ("make test", 1700000100),
    ]


def test_parse_zsh_extended_multiline_and_plain():
    text = ": 1700000000:0;docker build \\\n  -t app .\n: 1700000050:2;docker run app\nplain cmd\n"
    assert [(e.command, e.timestamp) for e in parse_zsh(text)] == [
        ("docker build \n  -t app .", 1700000000),
        ("docker run app", 1700000050),
        ("plain cmd", None),
    ]


def test_read_history_unmetafies_zsh(tmp_path):
    # zsh stores "é" (0xC3 0xA9) as 0xC3 0x83 (0xA9 ^ 0x20).
    path = tmp_path / ".zsh_history"
    path.write_bytes(b": 1700000000:0;echo caf\xc3\x83\x89\n")
    assert read_history("zsh", path)[0].command == "echo café"


def test_parse_fish_unescapes_and_reads_when():
    text = (
        "- cmd: echo a\\nb\n  when: 1700000000\n"
        "- cmd: cd \\\\tmp\n  when: 1700000005\n  paths:\n    - /tmp\n"
    )
    assert [(e.command, e.timestamp) for e in parse_fish(text)] == [
        ("echo a\nb", 1700000000),
        ("cd \\tmp", 1700000005),
    ]


def test_read_history_missing_file_is_none(tmp_path):
    assert read_history("bash", tmp_path / "nope") is None


# ---------------------------------------------------------------------------
# Blocks and tools
# ---------------------------------------------------------------------------
def _commands(start, stop):
    return [Entry(f"git commit -m 'change {i}'") for i in range(start, stop)]


def _hashes(blocks):
    return [block.raw_hash for block in blocks]


def test_blocks_respect_size_bounds():
    blocks = chunk(_commands(0, 500), 1800)
    sizes = [len(block.entries) for block in blocks]
    assert sum(sizes) == 500
    assert all(size <= sh._BLOCK_MAX for size in sizes)
    assert all(size >= sh._BLOCK_MIN for size in sizes[:-1])


def test_appending_commands_keeps_earlier_blocks():
    before = chunk(_commands(0, 300), 1800)
    after = chunk(_commands(0, 330), 1800)
    assert _hashes(after)[: len(before) - 1] == _hashes(before)[:-1]


def test_trimming_the_top_keeps_later_blocks():
    before = chunk(_commands(0, 300), 1800)
    after = chunk(_commands(37, 300), 1800)
    # Every block after the one the trim cut through is unchanged...
    ends = [sum(len(b.entries) for b in before[: i + 1]) for i in range(len(before))]
    cut = next(i for i, end in enumerate(ends) if end > 37)
    assert set(_hashes(before)[cut + 1 :]) <= set(_hashes(after))
    # ...and the cut block's remnant ends on the same command.
    assert after[0].last_hash == before[cut].last_hash


def test_timestamp_gaps_split_blocks():
    entries = [Entry("a", 0), Entry("b", 60), Entry("c", 60 + 3600), Entry("d", 60 + 3660)]
    assert [len(b.entries) for b in chunk(entries, 1800)] == [2, 2]


@pytest.mark.parametrize(
    ("command", "tool"),
    [
        ("sudo docker ps", "docker"),
        ("DEBUG=1 uv run pytest", "uv"),
        ("C:\\Tools\\git.exe status", "git"),
        ("& .\\build.ps1 -Release", "build"),
        ("$env:X = 1", None),
    ],
)
def test_tool_name(command, tool):
    assert _tool(command) == tool


def test_tools_of_a_chained_command_skip_navigation():
    assert _tools("cd app; export A=1; npm ci && git push | tee log") == ["npm", "git", "tee"]


@pytest.mark.parametrize(
    ("token", "kind"),
    [
        ("1234567890:AAH" + "x9" * 16, "telegram_bot_token"),
        ("SG." + "a" * 22 + "." + "b" * 43, "sendgrid_key"),
        ("npm_" + "A1" * 18, "npm_token"),
        ("gsk_" + "Ab3" * 15, "groq_key"),
    ],
)
def test_redact_more_token_formats(token, kind):
    redacted, found = redact(f"run --flagless {token}")
    assert token not in redacted
    assert found == {kind: 1}


# ---------------------------------------------------------------------------
# Validation and detection
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"shells": ["tcsh"]}, "Unknown shell"),
        ({"history_files": {"cmd": "x"}}, "Unknown shell"),
        ({"session_gap_minutes": 0}, "positive"),
        ({"exclude_patterns": ["("]}, "Invalid exclude pattern"),
    ],
)
def test_source_validates_arguments(kwargs, message):
    with pytest.raises(ValueError, match=message):
        shell_history_source(**kwargs)


def test_default_history_files_finds_existing_files(tmp_path, monkeypatch):
    monkeypatch.setattr(sh.sys, "platform", "linux")
    (tmp_path / ".bash_history").write_text("ls\n")
    psreadline = tmp_path / ".local" / "share" / "powershell" / "PSReadLine"
    psreadline.mkdir(parents=True)
    (psreadline / "ConsoleHost_history.txt").write_text("dir\n")

    found = default_history_files(tmp_path)

    assert found == {
        "powershell": [psreadline / "ConsoleHost_history.txt"],
        "bash": [tmp_path / ".bash_history"],
    }


def test_source_declares_document_mode_and_merge(tmp_path):
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    resource = shell_history_source(history_files={"bash": tmp_path / "h"}, resource_name="sh_a")

    assert document_source_tag(resource) == "shell"
    assert getattr(resource, PIPELINE_SCOPE_ATTR) == "sh_a"
    assert resource.write_disposition == "merge"
    assert resource.columns["_deleted"]["hard_delete"] is True


# ---------------------------------------------------------------------------
# dlt pipeline
# ---------------------------------------------------------------------------
@pytest.fixture
def dlt_mod():
    return pytest.importorskip("dlt")


class Harness:
    def __init__(self, dlt, tmp_path):
        self.dlt, self.tmp_path = dlt, tmp_path

    def run(self, **kwargs):
        source = shell_history_source(**kwargs)
        pipeline = self.dlt.pipeline(
            pipeline_name="shell_test",
            destination=self.dlt.destinations.sqlalchemy(
                f"sqlite:///{(self.tmp_path / 'shell.db').as_posix()}"
            ),
            dataset_name="shell_ds",
            pipelines_dir=str(self.tmp_path / "state"),
        )
        # The same hint ingest_dlt_source applies to every document source.
        pipeline.run(source, columns={"cognee_node_set": {"data_type": "json"}})
        self.pipeline, self.stats = pipeline, source.cognee_sync_stats
        return self

    def rows(self):
        try:
            with (
                self.pipeline.sql_client() as client,
                client.execute_query(
                    "SELECT id, title, content, cognee_node_set FROM shell_history_blocks"
                ) as cursor,
            ):
                rows = cursor.fetchall()
        except Exception:  # table not created yet: nothing was ever emitted
            return {}
        return {r[0]: {"title": r[1], "content": r[2], "node_sets": json.loads(r[3])} for r in rows}


def _write(path: Path, commands):
    path.write_text("".join(f"{c}\n" for c in commands), encoding="utf-8")


def _history(n, start=0):
    tools = ["git", "docker", "uv", "kubectl"]
    return [f"{tools[i % 4]} run step-{i}" for i in range(start, start + n)]


def test_first_sync_emits_blocks_with_node_sets(dlt_mod, tmp_path):
    path = tmp_path / "ConsoleHost_history.txt"
    _write(path, ["cd repo", "uv sync", "docker compose up -d", "ls"])

    rows = Harness(dlt_mod, tmp_path).run(history_files={"powershell": path}).rows()

    (row,) = rows.values()
    assert row["node_sets"] == ["shell:powershell", "shell:tool/uv", "shell:tool/docker"]
    assert "```powershell\nuv sync\ndocker compose up -d\n```" in row["content"]
    assert "cd repo" not in row["content"]  # trivial commands are excluded
    assert row["title"] == "PowerShell commands (uv, docker)"


def test_no_fixture_secret_reaches_a_row(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(
        path,
        [
            f"export OPENAI_API_KEY={SECRETS['openai']}",
            f'curl -H "Authorization: Bearer {SECRETS["bearer"]}" https://api.x.io',
            f"mysql -u root -p{SECRETS['mysql']} app",
            f"git clone https://{SECRETS['github']}@github.com/a/b",
            f"DB_PASSWORD={SECRETS['assigned']} ./run.sh",
        ],
    )
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})

    dumped = json.dumps(harness.rows())
    for secret in SECRETS.values():
        assert secret not in dumped
    assert harness.stats["redacted"] == 5


def test_resync_emits_only_what_changed(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, _history(200))
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})
    first = harness.rows()
    assert harness.stats["emitted"] == len(first) > 3

    harness.run(history_files={"bash": path})
    assert harness.stats["emitted"] == 0

    _write(path, _history(205))
    after = harness.run(history_files={"bash": path}).rows()
    # The last block grew (new id, old one tombstoned); everything else is untouched.
    assert harness.stats["emitted"] <= 2
    assert harness.stats["deleted"] == 1
    assert len(set(first) & set(after)) == len(first) - 1
    assert "step-204" in "".join(r["content"] for r in after.values())


def test_editing_history_forgets_the_edited_block(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    commands = _history(200)
    _write(path, commands)
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})
    before = harness.rows()

    del commands[100]  # the user removes one line from the middle
    _write(path, commands)
    after = harness.run(history_files={"bash": path}).rows()

    assert "step-100" not in "".join(r["content"] for r in after.values())
    assert harness.stats["deleted"] >= 1
    assert len(set(before) & set(after)) >= len(before) - 2  # only nearby blocks change


def test_clearing_history_forgets_everything(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, _history(100))
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})
    assert harness.rows()

    path.write_text("")  # history -c && history -w
    assert harness.run(history_files={"bash": path}).rows() == {}


def test_deleted_history_file_forgets_everything(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, _history(50))
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})

    path.unlink()
    assert harness.run(history_files={"bash": path}).rows() == {}


def test_trimmed_history_is_kept_by_default(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, _history(300))
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})
    before = harness.rows()

    _write(path, [*_history(263, start=37), "make release"])  # HISTFILESIZE trims the top
    after = harness.run(history_files={"bash": path}).rows()

    # At most the newest block changed (if the new command joined it rather
    # than starting a block); the trimmed blocks stay in memory.
    assert len(set(before) - set(after)) <= 1
    assert harness.stats["kept_trimmed"] >= 1
    contents = "".join(r["content"] for r in after.values())
    assert "step-0\n" in contents
    assert "make release" in contents
    # The tail of the block the trim cut through is not ingested a second time.
    for i in range(37, 46):
        assert contents.count(f"step-{i}\n") == 1


def test_trimmed_history_is_forgotten_when_mirroring(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, _history(300))
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path}, keep_trimmed=False)

    _write(path, _history(263, start=37))
    after = harness.run(history_files={"bash": path}, keep_trimmed=False).rows()

    assert "step-0\n" not in "".join(r["content"] for r in after.values())


def test_new_exclude_pattern_resyncs_and_forgets(dlt_mod, tmp_path):
    path = tmp_path / ".bash_history"
    _write(path, ["kubectl get pods", "kubectl logs api", "kubectl delete pod x", "uv sync"])
    harness = Harness(dlt_mod, tmp_path).run(history_files={"bash": path})
    assert "kubectl logs api" in next(iter(harness.rows().values()))["content"]

    rows = harness.run(history_files={"bash": path}, exclude_patterns=[r"kubectl .*"]).rows()
    (row,) = rows.values()
    assert "kubectl" not in row["content"]
    assert row["node_sets"] == ["shell:bash", "shell:tool/uv"]

    rows = harness.run(history_files={"bash": path}, exclude_patterns=[r".*"]).rows()
    assert rows == {}  # every command excluded: the block is forgotten


def test_since_skips_old_timestamped_commands(dlt_mod, tmp_path):
    path = tmp_path / ".zsh_history"
    old, new = 1700000000, 1780000000  # 2023-11-14 and 2026-05-28
    path.write_text(f": {old}:0;make old-thing\n: {new}:0;make new-thing\n")

    rows = Harness(dlt_mod, tmp_path).run(history_files={"zsh": path}, since="2026-01-01").rows()

    content = "".join(r["content"] for r in rows.values())
    assert "new-thing" in content
    assert "old-thing" not in content
    assert "Time: 2026-05-28" in content
