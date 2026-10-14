"""Patents connector demo: a competitive-intelligence memory over patent filings.

Watch two companies and one technology class on EPO Open Patent Services, sync the
matching publications into cognee, and ask questions that cut across owners,
inventors, technologies and citations.

────────────────────────────────────────────────────────────────────────────
One-time setup
────────────────────────────────────────────────────────────────────────────
1. Register (free) at https://developers.epo.org, create an app, copy its
   consumer key and secret.
2. Export them and your LLM key, then run:

       export EPO_OPS_KEY="..."
       export EPO_OPS_SECRET="..."
       export LLM_API_KEY="sk-..."
       uv run python examples/example.py

Re-run it later: only publications new since the last run are fetched. Remove a
watch from WATCHES and re-run to see its publications forgotten.
"""

import asyncio
import os

import cognee

from cognee_community_connector_patents import patents_source

DATASET_NAME = "patents"

# Each watch is an OPS CQL query and becomes node set "patents:<name>".
WATCHES = {
    "neo4j": 'pa="neo4j"',
    "tigergraph": 'pa="tigergraph"',
    "graph_indexing": 'cpc=G06F16/9024 and ta="knowledge graph"',
}


async def main() -> None:
    if not (os.environ.get("EPO_OPS_KEY") and os.environ.get("EPO_OPS_SECRET")):
        print("Set EPO_OPS_KEY and EPO_OPS_SECRET (free at https://developers.epo.org).")
        return

    source = patents_source(watches=WATCHES, since="2025-01-01")

    print("Syncing patent publications into cognee ...")
    await cognee.remember(
        source,
        dataset_name=DATASET_NAME,
        primary_key="id",
        write_disposition="merge",
        max_rows_per_table=0,
    )
    print("Sync stats:", source.cognee_sync_stats)

    questions = [
        "Which companies are filing patents on knowledge graph indexing, and what do "
        "their inventions claim to improve?",
        "Which earlier patents are cited most often across these filings?",
    ]
    for question in questions:
        answer = await cognee.recall(question, datasets=[DATASET_NAME])
        print(f"\nQ: {question}\nA: {answer}")

    # Scope a question to one watch through its node set.
    scoped = await cognee.recall(
        "Summarize what this company patented and who invented it.",
        datasets=[DATASET_NAME],
        node_name=["patents:neo4j"],
    )
    print("\nneo4j only:\n", scoped)


if __name__ == "__main__":
    asyncio.run(main())
