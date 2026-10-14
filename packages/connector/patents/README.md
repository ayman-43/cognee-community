# cognee-community-connector-patents

A patent data-source connector for [cognee](https://github.com/topoteretes/cognee):
keep patent searches in sync with memory, so you can ask "what has Acme filed on
graph indexing this year, and what prior art do those filings cite?"

It exposes a `dlt` resource you hand to `cognee.remember(...)`. Each patent publication
becomes a document (title, publication and family data, applicants, inventors, CPC/IPC
classes, abstract, cited patents and literature, optionally claims) that flows through
cognee's normal cognify entity extraction.

Data comes from **EPO Open Patent Services (OPS)**, the European Patent Office's free
API over DOCDB. It covers 100+ patent offices, including the US (USPTO), Europe (EPO), WIPO
(PCT), China, Japan and Korea. USPTO's own PatentsView API went offline on 2026-03-20 for
its move to the USPTO Open Data Portal, so OPS is the free worldwide source today.

## Why a graph

Patents are already a graph: companies own them, inventors move between companies,
families group one invention filed in several countries, CPC classes group technologies,
and every publication cites earlier ones. The connector writes a cited patent with the same
spelling as that patent's own document (`US9876543B2`), so when both are in memory cognee
links them and the citation network becomes traversable.

## Install

```bash
uv pip install cognee-community-connector-patents
# or, from this monorepo:
cd packages/connector/patents && uv sync
```

Requires cognee 1.6.4 or later (per-row node sets).

## Setup

1. Register for free at <https://developers.epo.org>, create an app, and copy its
   **consumer key** and **consumer secret**. The free tier allows 4 GB per week, far more
   than bibliographic syncs use.
2. Export them along with your LLM key:

   ```bash
   export EPO_OPS_KEY="..."
   export EPO_OPS_SECRET="..."
   export LLM_API_KEY="sk-..."
   ```

## Usage

```python
import cognee
from cognee_community_connector_patents import patents_source

source = patents_source(
    watches={
        "rivals": 'pa="acme" or pa="globex"',
        "graph_indexing": "cpc=G06F16/9024",
    },
    publication_numbers=["EP3456789A1"],  # pinned, synced regardless of watches
    since="2025-01-01",  # how far back a new watch backfills
)

await cognee.remember(
    source,
    dataset_name="patents",
    primary_key="id",
    write_disposition="merge",  # REQUIRED: the default "replace" drops unchanged rows
    max_rows_per_table=0,  # REQUIRED for more than 50 publications
)

# The whole landscape:
await cognee.recall("Which companies cite Acme's graph indexing patents?", datasets=["patents"])

# One watch only: every watch is its own node set.
await cognee.recall(
    "Summarize what Acme filed this year.",
    datasets=["patents"],
    node_name=["patents:rivals"],
)
```

Re-run the same code daily or weekly (cron, a scheduler, a CI job) to keep memory current.
See `examples/example.py` for the full flow.

### Watch queries

A watch is an OPS CQL query. Useful fields:

| Field | Meaning | Example |
|---|---|---|
| `pa` | applicant (owner) | `pa="acme corp"` |
| `in` | inventor | `in="jane smith"` |
| `cpc` | CPC class | `cpc=G06F16/9024` |
| `ipc` | IPC class | `ipc=G06N` |
| `ta` | title or abstract | `ta="knowledge graph"` |
| `ti` | title | `ti=retrieval` |
| `pn` | publication number | `pn=EP3456789` |

Combine with `and`, `or`, `not` and parentheses. Leave out `pd` (publication date): the
connector adds the date window itself and rejects a watch that sets one.

### Options

| Argument | Default | |
|---|---|---|
| `watches` | — | `{name: query}`; the name becomes node set `patents:<name>` |
| `publication_numbers` | — | Pinned numbers. Without a kind code (`EP3456789`) every kind is pinned |
| `since` | 365 days ago | Earliest publication date a new watch backfills from |
| `lookback_days` | 30 | How far before its last sync a watch searches again |
| `include_claims` | `False` | Also sync claims (EP, WO and a few offices; one request per publication) |
| `resource_name` | `patent_documents` | Must be unique per patents source within a dataset |
| `consumer_key` / `consumer_secret` | env | Fall back to `EPO_OPS_KEY` / `EPO_OPS_SECRET` |

## How sync and forget work

* **Incremental.** Each watch remembers the date it was synced through, stored in dlt's
  state next to the staging table. The next run searches only from that date minus
  `lookback_days`. The overlap exists because DOCDB loads some offices' publications days or
  weeks after their publication date; a cursor on the newest date alone would skip them for
  good. Publications already in memory with the same bibliographic data are not
  re-emitted, so the overlap only costs searches. A correction DOCDB publishes in the window
  (a renamed applicant, an added citation) is re-emitted.
* **Large result sets.** OPS returns at most 2,000 results per query. A window with more
  is split in half by date until each half fits. If a single day still has more than 2,000
  matches, the run fails and asks you to narrow the watch, rather than syncing an
  arbitrary subset.
* **Forget-on-delete.** Patents are not deleted upstream; what changes is *your scope*.
  Remove a watch, change its query, or unpin a number, and every publication that no
  longer belongs to any watch is tombstoned (`_deleted`), then removed from the graph and
  vector stores by cognee's orphan cleanup. A publication still matched by another watch
  stays, with its node sets updated.
* **Failure is safe.** Every request that can fail runs before the first row is emitted,
  and dlt commits the cursor only with a successful load. A failed run (network, quota)
  changes nothing and is retried in full next time.
* **Quota and throttling.** The connector paces itself by OPS's `X-Throttling-Control`
  header, retries 429/5xx with `Retry-After`, refreshes the 20-minute token, and stops
  with a clear error when the weekly quota is exhausted.

## What is stored

Only public, published patent data: bibliographic fields, abstracts, citations, and
claims if you ask for them. EPO sees your search queries (under your OPS account) and
nothing else. The documents land in the dataset you choose, so
`cognee.forget(dataset="patents")` removes everything in one call.

## Limitations

* Publication numbers containing letters (`USRE49000E`, design patents) can't be pinned;
  watches still find them.
* Bibliographic data is refreshed only while a publication is inside a watch's lookback
  window. Pinned publications are fetched once.
* Legal status (granted, lapsed, revoked, opposed) is not synced yet. OPS has it under
  INPADOC legal events and it is the natural next step.

## Testing

```bash
uv run pytest tests/
```

The tests need no OPS credentials. They run the real `httpx` client against a fake OPS
(`httpx.MockTransport`) that serves OPS-shaped JSON. They cover parsing, paging, window
splitting, token refresh, retries, throttling and quota handling, and, through real dlt
pipelines into a temp SQLite, the incremental cursor, late-loaded publications,
corrections, and forget-on-scope-change for watches and pins.
