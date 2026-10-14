"""Browser connector demo: a memory of what you've read.

Sync this machine's browser history and bookmarks (Chrome, Edge, Brave, Vivaldi,
Firefox) into cognee, then ask about pages by what they were about.

Run:

    export LLM_API_KEY="sk-..."
    uv run python examples/example.py

Run it again later: only pages that changed in a way you'd notice are
re-ingested. Delete a page from history, remove a bookmark, or add a domain to
EXCLUDE and re-run to see it forgotten.
"""

import asyncio

import cognee

from cognee_community_connector_browser import browser_source

DATASET_NAME = "browsing"
# Sites you never want in memory, on top of the built-in privacy defaults.
EXCLUDE = ["youtube.com", "netflix.com"]


async def main() -> None:
    source = browser_source(
        since="2026-07-01",  # how far back history is added
        exclude_domains=EXCLUDE,
        min_visits=2,  # skip pages opened once by accident (bookmarks always count)
        # fetch_content=True,  # opt in: also store each page's readable text
    )

    print("Syncing browser history into cognee ...")
    await cognee.remember(
        source,
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )
    # Counts only: pages emitted/unchanged/forgotten.
    print("Sync stats:", source.cognee_sync_stats)

    for question in [
        "Which documentation sites have I been reading most this month?",
        "What did I read about Python packaging?",
    ]:
        answer = await cognee.recall(question, datasets=[DATASET_NAME])
        print(f"\nQ: {question}\nA: {answer}")

    scoped = await cognee.recall(
        "Summarize what my bookmarks are about.",
        datasets=[DATASET_NAME],
        node_name=["browser:bookmarks/Bookmarks bar"],
    )
    print("\nBookmarks bar only:\n", scoped)


if __name__ == "__main__":
    asyncio.run(main())
