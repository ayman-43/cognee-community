"""Shell history connector demo: a memory of how you actually run things.

Sync this machine's shell history (PowerShell, bash, zsh, fish) into cognee with
secrets redacted, then ask how you did things before.

Run:

    export LLM_API_KEY="sk-..."
    uv run python examples/example.py

Run it again later: only blocks with new commands are re-ingested. Delete a line
from your history, or add an exclude pattern, and re-run to see it forgotten.
"""

import asyncio

import cognee

from cognee_community_connector_shell_history import shell_history_source

DATASET_NAME = "shell_history"


async def main() -> None:
    source = shell_history_source(
        # Leave out commands you never want in memory, on top of the defaults.
        exclude_patterns=[r"ssh .*", r"gpg .*"],
    )

    print("Syncing shell history into cognee ...")
    await cognee.remember(
        source,
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )
    # Counts only: blocks emitted/unchanged/forgotten and secrets redacted.
    print("Sync stats:", source.cognee_sync_stats)

    for question in [
        "How do I usually start this project's dev server?",
        "Which commands did I run to set up the Python environment?",
    ]:
        answer = await cognee.recall(question, datasets=[DATASET_NAME])
        print(f"\nQ: {question}\nA: {answer}")

    scoped = await cognee.recall(
        "What git operations do I do most often?",
        datasets=[DATASET_NAME],
        node_name=["shell:tool/git"],
    )
    print("\ngit only:\n", scoped)


if __name__ == "__main__":
    asyncio.run(main())
