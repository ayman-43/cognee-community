# cognee-community-connector-browser

A browser history and bookmarks data-source connector for
[cognee](https://github.com/topoteretes/cognee): turn what you've read into memory, so you
or your agent can ask "what was that page about Postgres vacuum tuning I read last month?"
by what the page was *about*, not its URL.

It reads the browser's own profile on disk (**Chrome, Edge, Brave, Vivaldi, Chromium,
Firefox**) and hands each page to `cognee.remember(...)` as a document that flows through
cognee's normal cognify entity extraction. The existing Raindrop.io and Karakeep connectors
cover bookmark *services*; this one covers the history and bookmarks your browser already
keeps.

## Install

```bash
uv pip install cognee-community-connector-browser
# or, from this monorepo:
cd packages/connector/browser && uv sync
```

Requires cognee 1.6.4 or later (per-row node sets). No other dependencies: the history
databases are read with Python's built-in `sqlite3`.

## Usage

```python
import cognee
from cognee_community_connector_browser import browser_source

await cognee.remember(
    browser_source(),  # auto-detects every browser profile on this machine
    dataset_name="browsing",
    primary_key="id",
    write_disposition="merge",  # REQUIRED: the default "replace" drops unchanged rows
    max_rows_per_table=0,  # REQUIRED for more than 50 pages
)

await cognee.recall("What did I read about vacuum tuning?", datasets=["browsing"])

# Scope through node sets: a browser, a site, or a bookmark folder.
await cognee.recall(
    "What are my Rust bookmarks about?",
    datasets=["browsing"],
    node_name=["browser:bookmarks/Bookmarks bar/Rust"],
)
```

Re-run it whenever you like (a scheduled task works well). It's safe while the browser is
open, and only pages that changed are re-ingested. See `examples/example.py`.

### Options

| Argument | Default | |
|---|---|---|
| `browsers` | all found | `chrome`, `edge`, `brave`, `vivaldi`, `chromium`, `firefox` |
| `profiles` | auto | `{browser: profile dir or [dirs]}`, e.g. `.../User Data/Default` |
| `since` | 90 days ago | Only history pages last visited since then are *added* |
| `include_domains` / `exclude_domains` | — | Sites (and subdomains) to keep / never sync |
| `min_visits` | 1 | Skip history pages visited fewer times (bookmarks always count) |
| `bookmarks_only` | `False` | Sync bookmarks and nothing else |
| `include_searches` | `False` | Also sync search-result pages |
| `keep_expired` / `expiry_days` | `True` / 90 | Keep pages the browser expired (see below) |
| `fetch_content` | `False` | Also fetch and store each page's readable text |
| `resource_name` | `browser_pages` | Must be unique per source within a dataset |

## What gets stored

One document per page per browser profile:

```
URL: https://docs.python.org/3/library/asyncio.html
Site: docs.python.org
Browser: Edge (profile Default)
Visited: often
First seen: 2026-08-03
Last visited: 2026-10
Bookmarked in: Bookmarks bar / Python
```

The title becomes the document's heading. Node sets: `browser:<browser>`,
`browser:site/<domain>`, and `browser:bookmarks/<folder path>` for bookmarks.

With `fetch_content=True` the page's readable text is added too: scripts, navigation and
footers are stripped, and the text is capped at 20,000 characters. This is the only option
that makes network requests, and it honours `ALLOW_HTTP_REQUESTS=false`.

## Privacy defaults

History is personal, so the defaults are strict, and everything is filtered **before** it
is stored or hashed:

* **Never synced:** non-web URLs (`chrome://`, `file://`, `about:`), `localhost` and
  private-network addresses, sign-in / OAuth / password-reset pages, webmail, banking and
  payment sites, and search-result pages (unless `include_searches=True`).
* **Redacted:** query parameters that look like credentials (`token`, `code`, `session`,
  `sig`, `key`, `state`, …) become `REDACTED`, and `user:password@` is dropped from URLs.
* **Dropped:** tracking parameters (`utm_*`, `fbclid`, `gclid`, …) and fragments, which
  also merges duplicate entries for the same page.
* **Nothing leaves your machine** except, as with any cognee data, the documents sent to
  your LLM provider during cognify. `cognee.forget(dataset="browsing")` removes it all.

On a real profile with a few hundred pages, the defaults skipped about a quarter of them:
local dev servers, sign-in pages, webmail. None of those belong in memory.

## How sync and forget work

* **Safe while the browser runs.** A running browser locks its history database, so each
  run copies it (with its `-wal` / `-journal` files) to a temp directory and reads the
  copy. The live file is never opened.
* **Low churn.** Every visit bumps the browser's counters. To avoid re-ingesting a page on
  every visit, a document shows a visit *bucket* (once, a few times, often, very often) and
  the *month* of the last visit. A page is re-emitted only when one of those, its title or
  its bookmarks change.
* **Forget-on-delete.** Delete a page from history, delete its bookmark, or add its site to
  `exclude_domains`, and it is removed from memory on the next sync.
* **Expiry is not deletion.** Browsers delete old history on their own (Chrome after about
  90 days). That shouldn't wipe your memory, so a page that vanishes after its last visit
  is older than `expiry_days` is assumed expired and kept. A page you visited recently that
  vanishes was deleted by you, and is forgotten. Pass `keep_expired=False` to mirror the
  browser exactly.
* **`since` adds, it doesn't forget.** Pages already in memory stay when they age past
  `since`.

## Limitations

* Safari is not supported (its history needs Full Disk Access on macOS).
* Chromium profiles are named by folder (`Default`, `Profile 1`), not their display names.
* With `fetch_content=True`, a page whose fetch fails is stored without text and isn't
  fetched again until it changes.

## Testing

```bash
uv run pytest tests/
```

The tests build real Chromium (`History` SQLite + `Bookmarks` JSON) and Firefox
(`places.sqlite`) profiles in a temp directory, with both time encodings. They cover URL
normalisation and redaction, the privacy and search filters, merging, bookmark folders,
reading a database another connection holds locked, and, through real dlt pipelines,
low-churn re-syncs, deletions, expiry versus deletion, domain exclusion, `since`, bookmarks
only, and opt-in page fetching.
