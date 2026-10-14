"""Patent connector for cognee, a ``dlt`` source that turns patent searches into memory.

Keep named *watches* (EPO search queries) and pinned publications in sync with
cognee, incrementally and with forget-on-scope-change — "ask the patent
landscape"::

    import cognee
    from cognee_community_connector_patents import patents_source

    await cognee.remember(
        patents_source(
            watches={
                "competitor_acme": 'pa="acme"',
                "graph_databases": "cpc=G06F16/9024",
            },
            publication_numbers=["EP3456789A1"],
        ),
        dataset_name="patents",
        primary_key="id",
        write_disposition="merge",  # REQUIRED, see below
        max_rows_per_table=0,  # REQUIRED for more than 50 patents
    )

Design
------
* **Data source**: EPO Open Patent Services (OPS) v3.2, the European Patent
  Office's free REST API over DOCDB: bibliographic data for 100+ patent
  offices (US, EP, WO, CN, JP, KR, ...), with applicants, inventors, CPC/IPC
  classes, families, abstracts and the citations examiners made. USPTO's
  PatentsView API was the other free option; it was taken offline on
  2026-03-20 for its move to the USPTO Open Data Portal, with no relaunch date.
* **Auth**: an OPS consumer key + secret (free registration at
  developers.epo.org), exchanged for a 20-minute bearer token, refreshed
  automatically.
* **Transport**: plain ``httpx`` (a core cognee dependency), no OPS SDK.
  ``http_client`` is an injection point for tests.
* **One row per publication**: the row id is the DOCDB number without
  separators (``EP3456789A1``). Rendered content uses the same spelling for the
  patents a document cites, so a citation and the cited publication's own
  document name one entity and the citation graph connects in cognee.
* **Node sets**: every row carries ``cognee_node_set = ["patents:<watch>", ...]``
  for each watch that matched it (``patents:pinned`` for pinned numbers), so a
  recall can be scoped to one watch with ``node_name=["patents:<watch>"]``.
* **Incremental**: each watch remembers the date it was synced through. A run
  searches ``(<query>) and pd within "<from> <today>"`` where ``<from>`` is that
  date minus ``lookback_days`` (default 30): DOCDB loads some offices'
  publications days or weeks after their publication date, and a cursor on
  the newest ``pd`` alone would skip them for good. Publications already
  synced with the same watches and the same bibliographic data are not
  re-emitted, so the overlap costs search requests only; a bibliographic
  correction that DOCDB publishes inside the window *is* re-emitted.
* **Large result sets**: OPS serves at most 2,000 results per query, so a
  window with more is split in half by date until each part fits. A single day
  with more than 2,000 matches aborts the run with a message to narrow the
  query, rather than syncing an arbitrary 2,000 of them.
* **Forget-on-delete**: patents are not deleted upstream; what goes away is
  the *scope*. When a watch is removed, its query changes, or a pinned number
  is dropped, publications that no longer belong to any watch are emitted as
  ``_deleted`` tombstones, and dlt's ``hard_delete`` plus cognee's
  ``orphan_cleanup`` remove them from the graph and vector stores. Publications
  still matched by another watch are re-emitted with their new node sets.
  A failed request raises before anything is emitted, and dlt only commits
  the cursor with a successful load, so a failed run is retried in full.
* **Claims**: ``include_claims=True`` adds the claims of each emitted
  publication (one extra request per publication; OPS has full text for EP,
  WO and a few other offices, others are synced without claims).

.. important::
   ``write_disposition="merge"`` is **mandatory**. The add pipeline defaults to
   ``"replace"``, which would drop every publication an incremental run did
   not re-emit.

Limitations
-----------
* Publication numbers containing letters (``USRE49000E``, design patents)
  cannot be pinned; they are still synced when a watch finds them.
* A publication's bibliographic data is refreshed only while it falls inside
  a watch's lookback window. Pinned publications are fetched once.
* Legal status (lapsed, revoked, opposed) is not synced in this version.
* Every source syncing into one dataset needs its own ``resource_name``: two
  sources sharing one share state, and each run tombstones the other's
  publications.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = get_logger("patents_connector")

OPS_BASE_URL = "https://ops.epo.org/3.2"
PATENTS_SOURCE_TAG = "patents"
DEFAULT_RESOURCE_NAME = "patent_documents"
# Watch name that pinned publication numbers are synced under.
PINNED_WATCH = "pinned"

# OPS limits: results per search request, results per query, and documents per
# bulk bibliographic retrieval.
_PAGE_SIZE = 100
_MAX_SEARCH_RESULTS = 2000
_BULK_SIZE = 100

_MAX_RETRIES = 5
_DEFAULT_BACKFILL_DAYS = 365
_DEFAULT_LOOKBACK_DAYS = 30

_WATCH_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]*$")
_PUBLICATION_NUMBER = re.compile(r"^([A-Z]{2})[\s.\-]*0*([0-9]+)[\s.\-]*([A-Z][0-9]?)?$")

_CREDENTIALS_HINT = (
    "Pass consumer_key=/consumer_secret= or set EPO_OPS_KEY and EPO_OPS_SECRET "
    "(free registration at https://developers.epo.org)."
)


class PatentsAPIError(Exception):
    """An OPS response that isn't worth retrying (or ran out of retries)."""

    def __init__(self, status_code: int, message: str, *, quota_exhausted: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.quota_exhausted = quota_exhausted


# ---------------------------------------------------------------------------
# HTTP client (thin wrapper: OAuth token, retry/backoff, OPS throttling)
# ---------------------------------------------------------------------------
class _OPSClient:
    def __init__(
        self,
        http_client: Any,
        consumer_key: str,
        consumer_secret: str,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._http = http_client
        self._credentials = (consumer_key, consumer_secret)
        self._sleep = sleep
        self._token: str | None = None

    def _authenticate(self) -> None:
        response = self._http.post(
            f"{OPS_BASE_URL}/auth/accesstoken",
            auth=self._credentials,
            data={"grant_type": "client_credentials"},
        )
        if response.status_code != 200:
            raise PatentsAPIError(
                response.status_code,
                f"EPO OPS rejected the consumer key/secret (HTTP {response.status_code}). "
                + _CREDENTIALS_HINT,
            )
        self._token = response.json()["access_token"]

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        content: str | None = None,
        extra_headers: dict | None = None,
        allow_not_found: bool = False,
    ) -> dict | None:
        """Call an OPS REST endpoint and return its JSON body.

        ``allow_not_found`` turns a 404 into ``None``: OPS answers a search with
        no hits, and a publication it does not hold, with 404.
        """
        import httpx

        token_refreshed = False
        for attempt in range(_MAX_RETRIES):
            last_attempt = attempt == _MAX_RETRIES - 1
            if self._token is None:
                self._authenticate()
            headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
            if content is not None:
                headers["Content-Type"] = "text/plain"
            headers.update(extra_headers or {})
            try:
                response = self._http.request(
                    method,
                    f"{OPS_BASE_URL}/rest-services/{path}",
                    params=params,
                    content=content,
                    headers=headers,
                )
            except httpx.TransportError as exc:
                if last_attempt:
                    raise
                delay = float(2**attempt)
                logger.warning("EPO OPS: %s, retrying in %.1fs.", exc, delay)
                self._sleep(delay)
                continue

            status = response.status_code
            if status == 200:
                self._respect_throttling(response.headers, path)
                return response.json()
            if status == 404 and allow_not_found:
                return None
            if _is_expired_token(response) and not token_refreshed:
                # Tokens live 20 minutes; a long sync outlives one.
                self._token = None
                token_refreshed = True
                continue
            rejection = response.headers.get("X-Rejection-Reason")
            if status == 403 and rejection:
                raise PatentsAPIError(
                    status,
                    f"EPO OPS quota exhausted ({rejection}). The free tier allows 4 GB per "
                    "week; the run made no changes and can be retried once the quota resets.",
                    quota_exhausted=True,
                )
            if status in (429, 500, 502, 503, 504) and not last_attempt:
                delay = _retry_after(response.headers, attempt)
                logger.warning(
                    "EPO OPS: HTTP %d on %s, retrying in %.1fs (%d/%d).",
                    status,
                    path,
                    delay,
                    attempt + 1,
                    _MAX_RETRIES,
                )
                self._sleep(delay)
                continue
            raise PatentsAPIError(
                status, f"EPO OPS {method} {path} failed with HTTP {status}: {response.text[:300]}"
            )
        raise PatentsAPIError(0, f"EPO OPS {method} {path} failed after {_MAX_RETRIES} attempts.")

    def _respect_throttling(self, headers, path: str) -> None:
        """Slow down when OPS reports the service we use as no longer green.

        OPS sends ``X-Throttling-Control: busy (search=yellow:15, ...)``, where
        the number is the requests per minute it currently allows. Pacing to
        that number avoids being pushed to black (temporarily blocked).
        """
        service = "search" if path.startswith("published-data/search") else "retrieval"
        color, per_minute = _throttle_state(headers.get("X-Throttling-Control"), service)
        if color in ("yellow", "red", "black") and per_minute:
            self._sleep(60.0 / per_minute)


def _is_expired_token(response) -> bool:
    if response.status_code == 401:
        return True
    return response.status_code == 400 and "access_token" in response.text.lower()


def _retry_after(headers, attempt: int) -> float:
    """Seconds to wait before retrying: the Retry-After header, else backoff."""
    try:
        return float(headers.get("Retry-After"))
    except (TypeError, ValueError):
        return float(2**attempt)


def _throttle_state(header: str | None, service: str) -> tuple[str | None, int | None]:
    """Return (color, requests per minute) for ``service`` from X-Throttling-Control."""
    if not header:
        return None, None
    match = re.search(rf"\b{re.escape(service)}=(\w+):(\d+)", header)
    if not match:
        return None, None
    return match.group(1).lower(), int(match.group(2))


# ---------------------------------------------------------------------------
# OPS JSON helpers. OPS JSON is a mechanical conversion of its XML: attributes
# are "@name", text is "$", and an element that occurs once is an object
# instead of a one-item list. Every reader below accepts both shapes.
# ---------------------------------------------------------------------------
def _as_list(value: Any) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, dict):
        text = str(value.get("$", ""))
    elif isinstance(value, list):
        text = " ".join(_text(item) for item in value)
    else:
        text = str(value)
    return " ".join(text.split())


def _find(value: Any, key: str) -> Iterator[Any]:
    """Yield every value stored under ``key``, at any depth (not below a match)."""
    if isinstance(value, dict):
        for item_key, item in value.items():
            if item_key == key:
                yield item
            else:
                yield from _find(item, key)
    elif isinstance(value, list):
        for item in value:
            yield from _find(item, key)


def _exchange_documents(payload: Any) -> Iterator[dict]:
    """Yield the exchange-documents (publications) in an OPS response."""
    for found in _find(payload, "exchange-document"):
        for document in _as_list(found):
            # Bulk retrieval reports a number it does not hold as a stub
            # carrying @status ("not found") instead of failing the request.
            if isinstance(document, dict) and not document.get("@status"):
                yield document


def _total_result_count(payload: Any) -> int:
    for found in _find(payload, "@total-result-count"):
        try:
            return int(found)
        except (TypeError, ValueError):
            break
    return 0


def _pick_language(items: list, language: str = "en") -> Any:
    """The item tagged ``@lang=language``, else the first one.

    Case-insensitive: OPS tags abstracts ``en`` but full text ``EN``.
    """
    for item in items:
        if isinstance(item, dict) and str(item.get("@lang", "")).lower() == language:
            return item
    return items[0] if items else None


def _document_id(reference: Any, formats=("docdb", "epodoc")) -> dict:
    """Pick a document-id from a *-reference element, by preferred format."""
    ids = [d for d in _as_list((reference or {}).get("document-id")) if isinstance(d, dict)]
    for wanted in formats:
        for document_id in ids:
            if document_id.get("@document-id-type") == wanted:
                return document_id
    return ids[0] if ids else {}


def _format_date(raw: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD; anything else is returned as given."""
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}" if re.fullmatch(r"\d{8}", raw) else raw


def _compact_number(document_id: dict) -> str:
    """country + number + kind without separators, e.g. US9876543B2.

    epodoc numbers usually carry their country already (``EP18000001``), docdb
    numbers never do; the country is only prefixed when it is missing.
    """
    country = _text(document_id.get("country"))
    number = _text(document_id.get("doc-number")).replace(" ", "")
    if country and not number.startswith(country):
        number = country + number
    return number + _text(document_id.get("kind")).replace(" ", "")


def _party_names(parties: Any, item_key: str, name_key: str) -> list[str]:
    """Applicant/inventor names, preferring the "original" spelling over epodoc."""
    by_format: dict[str, list[str]] = {}
    for party in _as_list((parties or {}).get(item_key)):
        if not isinstance(party, dict):
            continue
        name = _text((party.get(name_key) or {}).get("name")).strip(" ,;")
        if name:
            by_format.setdefault(party.get("@data-format", ""), []).append(name)
    names = by_format.get("original") or by_format.get("epodoc") or []
    if not names and by_format:
        names = next(iter(by_format.values()))
    return list(dict.fromkeys(names))


def _cpc_classes(bibliographic: dict) -> list[str]:
    classes = []
    container = bibliographic.get("patent-classifications") or {}
    for item in _as_list(container.get("patent-classification")):
        if not isinstance(item, dict):
            continue
        scheme = ((item.get("classification-scheme") or {}).get("@scheme") or "").upper()
        if scheme and not scheme.startswith("CPC"):
            continue
        head = "".join(_text(item.get(part)) for part in ("section", "class", "subclass"))
        main_group, subgroup = _text(item.get("main-group")), _text(item.get("subgroup"))
        if head and main_group:
            classes.append(f"{head}{main_group}/{subgroup}" if subgroup else head + main_group)
    return list(dict.fromkeys(classes))


def _ipc_classes(bibliographic: dict) -> list[str]:
    classes = []
    container = bibliographic.get("classifications-ipcr") or {}
    for item in _as_list(container.get("classification-ipcr")):
        # "G06F  16/901       20190101AFI20200101BHEP": class, group, then dates.
        tokens = _text((item or {}).get("text")).split()
        if len(tokens) >= 2:
            classes.append(tokens[0] + tokens[1])
    return list(dict.fromkeys(classes))


def _citations(bibliographic: dict) -> tuple[list[str], list[str]]:
    """(cited patents, cited non-patent literature), as display lines."""
    patents, literature = [], []
    references = bibliographic.get("references-cited") or {}
    for citation in _as_list(references.get("citation")):
        if not isinstance(citation, dict):
            continue
        notes = []
        category = _text(citation.get("category"))
        if category:
            notes.append(f"category {category}")
        phase = citation.get("@cited-phase")
        if phase:
            notes.append(f"cited during {phase}")
        suffix = f" ({', '.join(notes)})" if notes else ""
        if citation.get("patcit"):
            number = _compact_number(_document_id(citation["patcit"]))
            if number:
                patents.append(number + suffix)
        elif citation.get("nplcit"):
            text = _text((citation["nplcit"] or {}).get("text"))
            if text:
                literature.append(text + suffix)
    return list(dict.fromkeys(patents)), list(dict.fromkeys(literature))


def _parse_document(document: dict) -> dict | None:
    """Flatten one exchange-document into the fields the connector renders."""
    bibliographic = document.get("bibliographic-data") or {}
    publication = _document_id(bibliographic.get("publication-reference"))
    country = document.get("@country") or _text(publication.get("country"))
    number = document.get("@doc-number") or _text(publication.get("doc-number"))
    kind = document.get("@kind") or _text(publication.get("kind"))
    if not (country and number):
        return None

    application = _document_id(bibliographic.get("application-reference"), ("epodoc", "docdb"))
    priorities = []
    for claim in _as_list((bibliographic.get("priority-claims") or {}).get("priority-claim")):
        priority = _document_id(claim, ("epodoc", "original", "docdb"))
        priority_number = _compact_number({**priority, "kind": None})
        if priority_number:
            priority_date = _format_date(_text(priority.get("date")))
            priorities.append(
                f"{priority_number} ({priority_date})" if priority_date else priority_number
            )

    title = _pick_language(_as_list(bibliographic.get("invention-title")))
    abstract = _pick_language(_as_list(document.get("abstract")))
    parties = bibliographic.get("parties") or {}
    cited_patents, cited_literature = _citations(bibliographic)
    compact = f"{country}{number}{kind}"

    return {
        "id": compact,
        "docdb": f"{country}.{number}.{kind}" if kind else f"{country}.{number}",
        "url": f"https://worldwide.espacenet.com/patent/search?q=pn%3D{compact}",
        "title": _text(title),
        "published": _format_date(_text(publication.get("date"))),
        "family_id": str(document.get("@family-id") or ""),
        "application": _compact_number(application),
        "filed": _format_date(_text(application.get("date"))),
        "priorities": list(dict.fromkeys(priorities)),
        "applicants": _party_names(parties.get("applicants"), "applicant", "applicant-name"),
        "inventors": _party_names(parties.get("inventors"), "inventor", "inventor-name"),
        "cpc": _cpc_classes(bibliographic),
        "ipc": _ipc_classes(bibliographic),
        "abstract": _text((abstract or {}).get("p")) if isinstance(abstract, dict) else "",
        "cited_patents": cited_patents,
        "cited_literature": cited_literature,
    }


def _parse_claims(payload: Any) -> list[str]:
    """Claim texts from a fulltext claims response, English when available."""
    blocks = [block for found in _find(payload, "claims") for block in _as_list(found)]
    block = _pick_language([b for b in blocks if isinstance(b, dict)])
    if not block:
        return []
    texts = [_text(item) for found in _find(block, "claim-text") for item in _as_list(found)]
    return [text for text in texts if text]


def _record_hash(record: dict) -> str:
    """Stable hash of a publication's bibliographic data (detects DOCDB corrections)."""
    encoded = json.dumps(record, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


def _render(record: dict, claims: list[str]) -> str:
    """Markdown body of a publication's document (the title becomes its heading)."""
    lines = [f"Publication: {record['id']}"]
    if record["published"]:
        lines[0] += f", published {record['published']}"
    if record["family_id"]:
        lines.append(
            f"Patent family: {record['family_id']} "
            "(publications sharing a family protect the same invention)"
        )
    if record["application"]:
        filed = f", filed {record['filed']}" if record["filed"] else ""
        lines.append(f"Application: {record['application']}{filed}")
    if record["priorities"]:
        lines.append(f"Priority claims: {'; '.join(record['priorities'])}")
    if record["applicants"]:
        lines.append(f"Applicants (patent owners): {'; '.join(record['applicants'])}")
    if record["inventors"]:
        lines.append(f"Inventors: {'; '.join(record['inventors'])}")
    if record["cpc"]:
        lines.append(f"CPC classifications: {', '.join(record['cpc'])}")
    if record["ipc"]:
        lines.append(f"IPC classifications: {', '.join(record['ipc'])}")

    sections = ["\n".join(lines)]
    if record["abstract"]:
        sections.append(f"## Abstract\n\n{record['abstract']}")
    if claims:
        sections.append("## Claims\n\n" + "\n".join(claims))
    if record["cited_patents"]:
        cited = "\n".join(f"- {line}" for line in record["cited_patents"])
        sections.append(f"## Cited patents\n\n{record['id']} cites:\n{cited}")
    if record["cited_literature"]:
        cited = "\n".join(f"- {line}" for line in record["cited_literature"])
        sections.append(f"## Cited non-patent literature\n\n{cited}")
    sections.append(f"Source: {record['url']}")
    return "\n\n".join(sections)


# ---------------------------------------------------------------------------
# OPS operations
# ---------------------------------------------------------------------------
def _search(client: _OPSClient, query: str, start: date, end: date) -> Iterator[dict]:
    """Yield every publication matching ``query`` published in [start, end]."""
    cql = f'({query}) and pd within "{start:%Y%m%d} {end:%Y%m%d}"'
    path = "published-data/search/biblio"
    # Search takes its range in the X-OPS-Range header (as python-epo-ops-client
    # sends it); a Range query parameter is only read by the images service.
    first = client.request(
        "GET",
        path,
        params={"q": cql},
        extra_headers={"X-OPS-Range": f"1-{_PAGE_SIZE}"},
        allow_not_found=True,
    )
    if first is None:  # OPS answers a search without hits with 404
        return
    total = _total_result_count(first)
    if total > _MAX_SEARCH_RESULTS:
        if start >= end:
            raise PatentsAPIError(
                0,
                f"EPO OPS search {query!r} matches {total} publications on {start} alone, "
                f"more than the {_MAX_SEARCH_RESULTS} one query can return. Narrow the watch.",
            )
        middle = start + (end - start) // 2
        yield from _search(client, query, start, middle)
        yield from _search(client, query, middle + timedelta(days=1), end)
        return

    yield from _exchange_documents(first)
    begin = _PAGE_SIZE + 1
    while begin <= total:
        stop = min(begin + _PAGE_SIZE - 1, total)
        page = client.request(
            "GET", path, params={"q": cql}, extra_headers={"X-OPS-Range": f"{begin}-{stop}"}
        )
        yield from _exchange_documents(page)
        begin = stop + 1


def _fetch_publication(client: _OPSClient, number: str) -> list[dict]:
    """Bibliographic records for one pinned number (all kinds when none is given)."""
    match = _PUBLICATION_NUMBER.fullmatch(number)
    if not match:
        raise ValueError(f"Cannot pin {number!r}: use a number such as EP3456789A1.")
    country, digits, kind = match.groups()
    if kind:
        path = f"published-data/publication/docdb/{country}.{digits}.{kind}/biblio"
    else:
        path = f"published-data/publication/epodoc/{country}{digits}/biblio"
    payload = client.request("GET", path, allow_not_found=True)
    records = [r for r in map(_parse_document, _exchange_documents(payload)) if r]
    if not records:
        raise ValueError(f"EPO OPS holds no publication {number!r}; fix or remove the pin.")
    return records


def _fetch_bulk(client: _OPSClient, docdb_numbers: list[str]) -> dict[str, dict]:
    """Bibliographic records for known publications, 100 per request."""
    records: dict[str, dict] = {}
    for offset in range(0, len(docdb_numbers), _BULK_SIZE):
        batch = docdb_numbers[offset : offset + _BULK_SIZE]
        payload = client.request(
            "POST", "published-data/publication/docdb/biblio", content="\n".join(batch)
        )
        for record in filter(None, map(_parse_document, _exchange_documents(payload))):
            records[record["id"]] = record
    return records


def _fetch_claims(client: _OPSClient, docdb_number: str) -> list[str]:
    payload = client.request(
        "GET", f"published-data/publication/docdb/{docdb_number}/claims", allow_not_found=True
    )
    return _parse_claims(payload) if payload else []


def _normalize_number(number: str) -> str:
    return re.sub(r"[\s.\-]", "", number.strip().upper())


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def _iter_rows(
    client: _OPSClient,
    watches: dict[str, str],
    pinned_numbers: list[str],
    state: dict,
    stats: dict[str, int],
    *,
    since: date,
    today: date,
    lookback_days: int,
    include_claims: bool,
) -> Iterator[dict]:
    """Search every watch, then emit changed rows and tombstones; update ``state``.

    All requests that can fail happen before the first tombstone is yielded, and
    ``state`` is only written once everything has been emitted.
    """
    for key in ("searched", "emitted", "unchanged", "deleted"):
        stats[key] = 0
    previous_watches: dict[str, dict] = state.get("watches", {})
    previous_docs: dict[str, dict] = state.get("docs", {})
    previous_pins: dict[str, list[str]] = state.get("pinned", {})

    # A watch whose query changed starts over: its old matches are re-earned
    # by the new query or forgotten.
    kept = {
        name
        for name, query in watches.items()
        if previous_watches.get(name, {}).get("query") == query
    }
    membership: dict[str, set[str]] = {
        doc_id: {w for w in doc["watches"] if w in kept} for doc_id, doc in previous_docs.items()
    }
    in_hand: dict[str, dict] = {}
    docdb_of = {doc_id: doc["docdb"] for doc_id, doc in previous_docs.items()}

    pins: dict[str, list[str]] = {}
    for number in pinned_numbers:
        if number in previous_pins:
            pins[number] = previous_pins[number]
            continue
        records = _fetch_publication(client, number)
        pins[number] = [record["id"] for record in records]
        in_hand.update((record["id"], record) for record in records)
    pinned_ids = {doc_id for ids in pins.values() for doc_id in ids}
    for doc_id in pinned_ids:
        membership.setdefault(doc_id, set()).add(PINNED_WATCH)

    watch_state = {}
    for name, query in watches.items():
        if name in kept:
            synced_through = date.fromisoformat(previous_watches[name]["synced_through"])
            start = min(synced_through - timedelta(days=lookback_days), today)
        else:
            start = since
        for document in _search(client, query, start, today):
            record = _parse_document(document)
            if record is None:
                continue
            stats["searched"] += 1
            in_hand[record["id"]] = record
            membership.setdefault(record["id"], set()).add(name)
        watch_state[name] = {"query": query, "synced_through": today.isoformat()}

    for record in in_hand.values():
        docdb_of[record["id"]] = record["docdb"]

    to_emit: set[str] = set()
    for doc_id, members in membership.items():
        if not members:
            continue
        previous = previous_docs.get(doc_id)
        record = in_hand.get(doc_id)
        if (
            previous
            and set(previous["watches"]) == members
            and (record is None or _record_hash(record) == previous["hash"])
        ):
            continue
        to_emit.add(doc_id)

    # Publications whose watches changed but that no search returned this run
    # (their watch was removed, or they fell out of the lookback window).
    missing = sorted(doc_id for doc_id in to_emit if doc_id not in in_hand)
    if missing:
        in_hand.update(_fetch_bulk(client, [docdb_of[doc_id] for doc_id in missing]))
        unresolved = [doc_id for doc_id in missing if doc_id not in in_hand]
        if unresolved:
            raise PatentsAPIError(
                0,
                f"EPO OPS did not return {len(unresolved)} known publication(s), e.g. "
                f"{unresolved[0]}; the run made no changes.",
            )

    rows = []
    docs_state: dict[str, dict] = {}
    for doc_id in sorted(membership):
        members = membership[doc_id]
        if not members:
            continue
        if doc_id not in to_emit:
            stats["unchanged"] += 1
            docs_state[doc_id] = previous_docs[doc_id]
            continue
        record = in_hand[doc_id]
        claims = _fetch_claims(client, record["docdb"]) if include_claims else []
        rows.append(
            {
                "id": doc_id,
                "url": record["url"],
                "title": record["title"] or doc_id,
                "content": _render(record, claims),
                "_deleted": False,
                NODE_SET_COLUMN: [f"{PATENTS_SOURCE_TAG}:{w}" for w in sorted(members)],
            }
        )
        docs_state[doc_id] = {
            "watches": sorted(members),
            "hash": _record_hash(record),
            "docdb": record["docdb"],
        }

    stats["emitted"] = len(rows)
    yield from rows
    for doc_id in sorted(previous_docs):
        if not membership.get(doc_id):
            stats["deleted"] += 1
            yield {"id": doc_id, "_deleted": True}

    state["watches"] = watch_state
    state["docs"] = docs_state
    state["pinned"] = pins
    logger.info(
        "Patents: %d search hit(s), %d publication(s) emitted, %d unchanged, %d forgotten.",
        stats["searched"],
        stats["emitted"],
        stats["unchanged"],
        stats["deleted"],
    )


def patents_source(
    watches: dict[str, str] | None = None,
    publication_numbers: list[str] | None = None,
    *,
    consumer_key: str | None = None,
    consumer_secret: str | None = None,
    since: date | str | None = None,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    include_claims: bool = False,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    check_active: Callable[[], None] | None = None,
    http_client: Any = None,
    today: date | None = None,
):
    """Create a dlt resource that syncs patent publications into cognee.

    Args:
        watches: Named EPO OPS searches, ``{name: CQL query}``. The query uses
            OPS CQL fields, e.g. ``pa="acme"`` (applicant), ``in="jane smith"``
            (inventor), ``cpc=G06F16/9024``, ``ta="knowledge graph"`` (title or
            abstract), combined with ``and`` / ``or`` / ``not``. Do not add a
            ``pd`` (publication date) condition; the connector adds the window.
            A name is a letter or digit followed by letters, digits, ``_`` or
            ``-``; it names the watch's node set, ``patents:<name>``.
        publication_numbers: Publications to sync regardless of any watch, such
            as ``EP3456789A1`` or ``US9876543B2``. Without a kind code every
            published kind is synced (``EP3456789`` -> A1 and B1).
        consumer_key / consumer_secret: OPS credentials. Fall back to
            ``EPO_OPS_KEY`` / ``EPO_OPS_SECRET``.
        since: Earliest publication date a new watch backfills from (a
            ``date`` or ``YYYY-MM-DD``). Defaults to 365 days ago.
        lookback_days: How far before its last sync a watch searches again, for
            publications DOCDB loaded late. 0 disables the overlap.
        include_claims: Also sync each publication's claims, where OPS has them.
        resource_name: Stable dlt resource name, also the staging table and the
            key of the incremental state. Every patents source syncing into one
            dataset needs its own name.
        check_active: Optional host authorization checkpoint during extraction.
        http_client: Pre-built ``httpx.Client`` (a test-injection point); when
            omitted one is built for the sync.
        today: The end of the search window; defaults to today (for tests).

    Returns:
        A dlt resource for ``cognee.remember(..., primary_key="id",
        write_disposition="merge", max_rows_per_table=0)``.
    """
    import dlt
    import httpx

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 2:
        raise RuntimeError(
            "The patents connector requires a cognee build that reads per-row node sets "
            "(DOCUMENT_SYNC_VERSION >= 2, cognee 1.6.4 or later). Upgrade cognee."
        )

    key = consumer_key or os.getenv("EPO_OPS_KEY")
    secret = consumer_secret or os.getenv("EPO_OPS_SECRET")
    if not (key and secret):
        raise ValueError(f"EPO OPS credentials required. {_CREDENTIALS_HINT}")

    resolved_watches = dict(watches or {})
    for name, query in resolved_watches.items():
        if not isinstance(name, str) or not _WATCH_NAME.fullmatch(name):
            raise ValueError(
                f"Invalid watch name {name!r}: use letters, digits, '_' or '-', "
                "starting with a letter or digit."
            )
        if name == PINNED_WATCH:
            raise ValueError(f"{PINNED_WATCH!r} is reserved for publication_numbers.")
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"Watch {name!r} needs a non-empty search query.")
        if re.search(r"\bpd\s*(=|<|>|within)", query, re.IGNORECASE):
            raise ValueError(
                f"Watch {name!r} filters on pd (publication date); remove it, the "
                "connector adds the date window itself (see since=)."
            )
    resolved_numbers = list(dict.fromkeys(_normalize_number(n) for n in publication_numbers or []))
    for number in resolved_numbers:
        if not _PUBLICATION_NUMBER.fullmatch(number):
            raise ValueError(f"Cannot pin {number!r}: use a number such as EP3456789A1.")
    if not resolved_watches and not resolved_numbers:
        raise ValueError("patents_source requires watches= and/or publication_numbers=.")
    if lookback_days < 0:
        raise ValueError("lookback_days must be 0 or more.")

    end = today or date.today()
    if since is None:
        start = end - timedelta(days=_DEFAULT_BACKFILL_DAYS)
    else:
        start = date.fromisoformat(since) if isinstance(since, str) else since
    if start > end:
        raise ValueError(f"since ({start}) is after today ({end}).")

    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        # cognee_node_set needs no json hint here: ingest_dlt_source applies
        # NODE_SET_COLUMN_HINT to every document source, merged with this one.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def patent_documents():
        owned_client = None if http_client is not None else httpx.Client(timeout=60.0)
        try:
            client = _OPSClient(http_client or owned_client, key, secret)
            yield from dlt_utils.guarded_rows(
                _iter_rows(
                    client,
                    resolved_watches,
                    resolved_numbers,
                    dlt.current.resource_state(),
                    stats,
                    since=start,
                    today=end,
                    lookback_days=lookback_days,
                    include_claims=include_claims,
                ),
                check_active,
            )
        finally:
            if owned_client is not None:
                owned_client.close()

    resource = patent_documents()
    # Opt into the document ingestion path: each publication row (id/title/
    # content/url, plus cognee_node_set) becomes a text document that flows
    # through normal cognify. resolve_dlt_sources reads this marker; it never
    # imports this connector.
    setattr(resource, DOCUMENT_SOURCE_ATTR, PATENTS_SOURCE_TAG)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    # Host-readable diagnostics contain counts only, never patent content.
    resource.cognee_sync_stats = stats
    return resource
