"""Unit tests for the patents (EPO OPS) dlt connector.

Everything runs without OPS credentials. ``FakeOPS`` is an ``httpx.MockTransport``
handler that serves a small in-memory corpus in OPS's JSON shape (attributes as
"@name", text as "$", single elements collapsed from lists), so the real
``httpx`` client, auth flow, paging and window splitting are exercised.

* DB-free tests: parsing, rendering, CQL windows, paging, splitting, the HTTP
  client's token refresh / retry / quota handling, and argument validation.
* dlt-pipeline tests (temp sqlite destination): the acceptance criteria —
  first sync, incremental window, no re-emission of unchanged publications,
  bibliographic corrections, and forget-on-scope-change for watches and pins.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from types import SimpleNamespace

import httpx
import pytest

from cognee_community_connector_patents import patents as patents_module
from cognee_community_connector_patents.patents import (
    PATENTS_SOURCE_TAG,
    PatentsAPIError,
    _OPSClient,
    _parse_claims,
    _parse_document,
    _render,
    _search,
    _throttle_state,
    patents_source,
)

TODAY = date(2026, 10, 14)


# ---------------------------------------------------------------------------
# Fake OPS
# ---------------------------------------------------------------------------
@dataclass
class Pub:
    country: str
    number: str
    kind: str
    published: date
    title: str
    applicants: list = field(default_factory=lambda: ["Acme Corp"])
    cpc: tuple = ("G", "06", "F", "16", "9024")
    cites: list = field(default_factory=list)
    family: str = "1000"
    claims: list | None = None

    @property
    def compact(self):
        return f"{self.country}{self.number}{self.kind}"

    @property
    def docdb(self):
        return f"{self.country}.{self.number}.{self.kind}"


def _doc_id(kind_of, country, number, kind=None, when=None):
    document_id = {
        "@document-id-type": kind_of,
        "country": {"$": country},
        "doc-number": {"$": number},
    }
    if kind:
        document_id["kind"] = {"$": kind}
    if when:
        document_id["date"] = {"$": when}
    return document_id


def exchange_document(pub: Pub) -> dict:
    """One publication in OPS JSON shape."""
    published = pub.published.strftime("%Y%m%d")
    citations = [
        {
            "@cited-phase": "search",
            "patcit": {"document-id": [_doc_id("docdb", *re.match(r"(..)(\d+)(..)", c).groups())]},
            "category": {"$": "X"},
        }
        for c in pub.cites
    ]
    citations.append({"nplcit": {"text": {"$": "Smith et al., Graph stores, 2017"}}})
    applicants = []
    for name in pub.applicants:
        applicants.append(
            {"@data-format": "epodoc", "applicant-name": {"name": {"$": name.upper() + " [US]"}}}
        )
        applicants.append(
            {"@data-format": "original", "applicant-name": {"name": {"$": name + ","}}}
        )
    section, klass, subclass, main_group, subgroup = pub.cpc
    return {
        "@country": pub.country,
        "@doc-number": pub.number,
        "@kind": pub.kind,
        "@family-id": pub.family,
        "bibliographic-data": {
            "publication-reference": {
                "document-id": [
                    _doc_id("docdb", pub.country, pub.number, pub.kind, published),
                    {"@document-id-type": "epodoc", "doc-number": {"$": pub.compact}},
                ]
            },
            "invention-title": [
                {"@lang": "de", "$": "Titel auf Deutsch"},
                {"@lang": "en", "$": pub.title},
            ],
            "parties": {
                "applicants": {"applicant": applicants},
                # A single inventor: OPS collapses the one-item list to an object.
                "inventors": {
                    "inventor": {
                        "@data-format": "original",
                        "inventor-name": {"name": {"$": "Jane Smith"}},
                    }
                },
            },
            "patent-classifications": {
                "patent-classification": [
                    {
                        "classification-scheme": {"@office": "EP", "@scheme": "CPCI"},
                        "section": {"$": section},
                        "class": {"$": klass},
                        "subclass": {"$": subclass},
                        "main-group": {"$": main_group},
                        "subgroup": {"$": subgroup},
                    }
                ]
            },
            "classifications-ipcr": {
                "classification-ipcr": {"text": {"$": "G06F  16/901       20190101AFI2020BHEP"}}
            },
            "application-reference": {
                "document-id": _doc_id("epodoc", pub.country, "18000001", None, "20180102")
            },
            "priority-claims": {
                "priority-claim": {
                    "document-id": _doc_id("epodoc", "US", "201762500000", None, "20170501")
                }
            },
            "references-cited": {"citation": citations},
        },
        "abstract": {"@lang": "en", "p": {"$": f"Abstract of {pub.title}."}},
    }


def _collapse(items):
    return items[0] if len(items) == 1 else items


class FakeOPS:
    """httpx.MockTransport handler serving ``corpus`` like EPO OPS does.

    ``queries`` maps each watch query string to a predicate over ``Pub``.
    """

    def __init__(self, corpus, queries):
        self.corpus = list(corpus)
        self.queries = dict(queries)
        self.requests: list[httpx.Request] = []
        self.fail_paths: dict[str, int] = {}

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        for fragment, status in self.fail_paths.items():
            if fragment in path:
                return httpx.Response(status, text="<fault/>")
        if path.endswith("/auth/accesstoken"):
            return httpx.Response(200, json={"access_token": "tok", "expires_in": "1199"})
        assert request.headers["Authorization"] == "Bearer tok"
        if path.endswith("/published-data/search/biblio"):
            return self._search(request)
        if path.endswith("/published-data/publication/docdb/biblio"):
            wanted = request.content.decode().split("\n")
            return self._documents([p for p in self.corpus if p.docdb in wanted])
        match = re.search(r"/publication/(docdb|epodoc)/([^/]+)/(biblio|claims)$", path)
        if match:
            id_type, number, constituent = match.groups()
            key = "docdb" if id_type == "docdb" else "epodoc"
            pubs = [
                p
                for p in self.corpus
                if (p.docdb == number if key == "docdb" else f"{p.country}{p.number}" == number)
            ]
            if constituent == "claims":
                if not pubs or pubs[0].claims is None:
                    return httpx.Response(404)
                return httpx.Response(200, json=self._claims(pubs[0].claims))
            return self._documents(pubs)
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    def search_queries(self):
        return [r.url.params["q"] for r in self.requests if r.url.path.endswith("/search/biblio")]

    def _search(self, request):
        cql = request.url.params["q"]
        match = re.fullmatch(r'\((.*)\) and pd within "(\d{8}) (\d{8})"', cql)
        assert match, cql
        query, start, end = match.groups()
        start = date(int(start[:4]), int(start[4:6]), int(start[6:]))
        end = date(int(end[:4]), int(end[4:6]), int(end[6:]))
        hits = [p for p in self.corpus if self.queries[query](p) and start <= p.published <= end]
        if not hits:
            return httpx.Response(404, text="<fault>SERVER.EntityNotFound</fault>")
        first, last = (int(x) for x in request.url.params["Range"].split("-"))
        page = hits[first - 1 : last]
        body = {
            "ops:world-patent-data": {
                "ops:biblio-search": {
                    "@total-result-count": str(len(hits)),
                    "ops:search-result": {
                        "exchange-documents": _collapse(
                            [{"exchange-document": exchange_document(p)} for p in page]
                        )
                    },
                }
            }
        }
        return httpx.Response(200, json=body)

    def _documents(self, pubs):
        if not pubs:
            return httpx.Response(404)
        documents = _collapse([exchange_document(p) for p in pubs])
        body = {"ops:world-patent-data": {"exchange-documents": {"exchange-document": documents}}}
        return httpx.Response(200, json=body)

    @staticmethod
    def _claims(claims):
        return {
            "ops:world-patent-data": {
                "ftxt:fulltext-documents": {
                    "ftxt:fulltext-document": {
                        "claims": {
                            "@lang": "EN",
                            "claim": {"claim-text": [{"$": text} for text in claims]},
                        }
                    }
                }
            }
        }


def _pub(number, published, title=None, **kwargs):
    return Pub("EP", str(number), "A1", published, title or f"Invention {number}", **kwargs)


def _no_sleep(_seconds):
    pass


def _client(fake):
    return _OPSClient(fake.client(), "key", "secret", sleep=_no_sleep)


# ---------------------------------------------------------------------------
# Parsing and rendering (DB-free)
# ---------------------------------------------------------------------------
def test_parse_document_reads_every_field():
    pub = _pub(3456789, date(2026, 3, 18), "Graph store", cites=["US9876543B2"])
    record = _parse_document(exchange_document(pub))

    assert record["id"] == "EP3456789A1"
    assert record["docdb"] == "EP.3456789.A1"
    assert record["title"] == "Graph store"  # English preferred over German
    assert record["published"] == "2026-03-18"
    assert record["family_id"] == "1000"
    # The "original" spelling wins over epodoc's upper-case form; trailing commas go.
    assert record["applicants"] == ["Acme Corp"]
    assert record["inventors"] == ["Jane Smith"]  # one-item list collapsed to an object
    assert record["cpc"] == ["G06F16/9024"]
    assert record["ipc"] == ["G06F16/901"]
    assert record["application"] == "EP18000001"
    assert record["filed"] == "2018-01-02"
    assert record["priorities"] == ["US201762500000 (2017-05-01)"]
    assert record["abstract"] == "Abstract of Graph store."
    assert record["cited_patents"] == ["US9876543B2 (category X, cited during search)"]
    assert record["cited_literature"] == ["Smith et al., Graph stores, 2017"]
    assert record["url"].endswith("pn%3DEP3456789A1")


def test_parse_document_tolerates_a_bare_document():
    record = _parse_document({"@country": "WO", "@doc-number": "2026123456", "@kind": "A1"})
    assert record["id"] == "WO2026123456A1"
    assert record["title"] == ""
    assert record["applicants"] == []
    assert record["cited_patents"] == []
    assert _parse_document({"bibliographic-data": {}}) is None


def test_render_names_citations_with_the_row_id_spelling():
    pub = _pub(1, date(2026, 1, 1), cites=["US9876543B2"])
    content = _render(_parse_document(exchange_document(pub)), ["1. A method."])

    assert content.startswith("Publication: EP1A1, published 2026-01-01")
    assert "Applicants (patent owners): Acme Corp" in content
    assert "## Claims\n\n1. A method." in content
    # Same spelling as the cited publication's own row id, so cognify links them.
    assert "EP1A1 cites:\n- US9876543B2" in content
    assert "## Cited non-patent literature" in content


def test_parse_claims_prefers_english():
    payload = {
        "claims": [
            {"@lang": "DE", "claim": {"claim-text": {"$": "1. Ein Verfahren."}}},
            {"@lang": "EN", "claim": {"claim-text": [{"$": "1. A method."}, {"$": "2. It."}]}},
        ]
    }
    assert _parse_claims(payload) == ["1. A method.", "2. It."]
    assert _parse_claims({}) == []


def test_throttle_state_reads_the_service_color():
    header = "busy (images=green:200, retrieval=yellow:50, search=red:5)"
    assert _throttle_state(header, "search") == ("red", 5)
    assert _throttle_state(header, "retrieval") == ("yellow", 50)
    assert _throttle_state(None, "search") == (None, None)


# ---------------------------------------------------------------------------
# Search (DB-free)
# ---------------------------------------------------------------------------
def test_search_pages_through_results():
    corpus = [_pub(n, date(2026, 5, 1)) for n in range(1, 251)]
    fake = FakeOPS(corpus, {'pa="acme"': lambda p: True})

    found = list(_search(_client(fake), 'pa="acme"', date(2026, 1, 1), TODAY))

    assert len(found) == 250
    ranges = [r.url.params["Range"] for r in fake.requests if "search" in r.url.path]
    assert ranges == ["1-100", "101-200", "201-250"]


def test_search_splits_windows_over_the_ops_cap(monkeypatch):
    monkeypatch.setattr(patents_module, "_MAX_SEARCH_RESULTS", 3)
    corpus = [_pub(n, date(2026, 1, n)) for n in range(1, 9)]
    fake = FakeOPS(corpus, {"q": lambda p: True})

    found = list(_search(_client(fake), "q", date(2026, 1, 1), date(2026, 1, 8)))

    assert sorted(d["@doc-number"] for d in found) == [str(n) for n in range(1, 9)]


def test_search_refuses_a_single_day_over_the_cap(monkeypatch):
    monkeypatch.setattr(patents_module, "_MAX_SEARCH_RESULTS", 1)
    corpus = [_pub(n, date(2026, 1, 1)) for n in range(1, 3)]
    fake = FakeOPS(corpus, {"q": lambda p: True})

    with pytest.raises(PatentsAPIError, match="Narrow the watch"):
        list(_search(_client(fake), "q", date(2026, 1, 1), date(2026, 1, 1)))


def test_search_without_hits_is_empty():
    fake = FakeOPS([], {"q": lambda p: True})
    assert list(_search(_client(fake), "q", date(2026, 1, 1), TODAY)) == []


# ---------------------------------------------------------------------------
# HTTP client (DB-free)
# ---------------------------------------------------------------------------
def _scripted(responses):
    """A transport that replays ``responses`` (after a token grant) in order."""
    queue = list(responses)
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path.endswith("/auth/accesstoken"):
            return httpx.Response(200, json={"access_token": f"tok{len(seen)}"})
        return queue.pop(0)

    return httpx.Client(transport=httpx.MockTransport(handler)), seen


def test_client_refreshes_an_expired_token_once():
    http, seen = _scripted([httpx.Response(401), httpx.Response(200, json={"ok": 1})])
    client = _OPSClient(http, "k", "s", sleep=_no_sleep)

    assert client.request("GET", "published-data/x") == {"ok": 1}
    assert sum(r.url.path.endswith("accesstoken") for r in seen) == 2


def test_client_retries_server_errors_with_retry_after():
    sleeps = []
    http, _ = _scripted(
        [httpx.Response(503, headers={"Retry-After": "7"}), httpx.Response(200, json={})]
    )
    client = _OPSClient(http, "k", "s", sleep=sleeps.append)

    assert client.request("GET", "published-data/x") == {}
    assert sleeps == [7.0]


def test_client_paces_requests_when_throttled():
    sleeps = []
    throttled = httpx.Response(
        200, json={}, headers={"X-Throttling-Control": "busy (search=yellow:20)"}
    )
    http, _ = _scripted([throttled])
    client = _OPSClient(http, "k", "s", sleep=sleeps.append)

    client.request("GET", "published-data/search/biblio")
    assert sleeps == [3.0]  # 60 s / 20 requests per minute


def test_client_reports_an_exhausted_quota():
    http, _ = _scripted(
        [httpx.Response(403, headers={"X-Rejection-Reason": "RegisteredQuotaPerWeek"})]
    )
    client = _OPSClient(http, "k", "s", sleep=_no_sleep)

    with pytest.raises(PatentsAPIError) as raised:
        client.request("GET", "published-data/x")
    assert raised.value.quota_exhausted is True


def test_client_rejects_bad_credentials():
    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(401)))
    with pytest.raises(PatentsAPIError, match="consumer key/secret"):
        _OPSClient(http, "k", "s").request("GET", "published-data/x")


# ---------------------------------------------------------------------------
# Argument validation and wiring (DB-free)
# ---------------------------------------------------------------------------
def _source(**kwargs):
    kwargs.setdefault("consumer_key", "key")
    kwargs.setdefault("consumer_secret", "secret")
    kwargs.setdefault("today", TODAY)
    return patents_source(**kwargs)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "requires watches"),
        ({"watches": {"bad name": "q"}}, "Invalid watch name"),
        ({"watches": {"pinned": "q"}}, "reserved"),
        ({"watches": {"w": " "}}, "non-empty"),
        ({"watches": {"w": 'pa="x" and pd within "2020 2021"'}}, "remove it"),
        ({"publication_numbers": ["not a number"]}, "Cannot pin"),
        ({"watches": {"w": "q"}, "lookback_days": -1}, "lookback_days"),
        ({"watches": {"w": "q"}, "since": "2027-01-01"}, "after today"),
    ],
)
def test_patents_source_validates_arguments(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _source(**kwargs)


def test_patents_source_requires_credentials(monkeypatch):
    monkeypatch.delenv("EPO_OPS_KEY", raising=False)
    monkeypatch.delenv("EPO_OPS_SECRET", raising=False)
    with pytest.raises(ValueError, match="EPO_OPS_KEY"):
        patents_source(watches={"w": "q"})


def test_patents_source_declares_document_mode_and_merge():
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    resource = _source(watches={"w": "q"}, resource_name="patents_team_a")

    assert document_source_tag(resource) == PATENTS_SOURCE_TAG == "patents"
    assert getattr(resource, PIPELINE_SCOPE_ATTR) == "patents_team_a"
    assert resource.write_disposition == "merge"
    assert resource.columns["_deleted"]["hard_delete"] is True


def test_row_node_sets_survive_cognee_namespacing():
    # cognee prefixes a node set with "<source tag>:" unless it already has it;
    # the connector's names must come through unchanged.
    from cognee.tasks.ingestion.resolve_dlt_sources import _row_node_set

    row = SimpleNamespace(
        row_data={"id": "EP1A1", "cognee_node_set": json.dumps(["patents:rivals"])},
        table_name="patent_documents",
        primary_key_value="EP1A1",
    )
    assert _row_node_set(row, "patents") == ["patents:rivals"]


# ---------------------------------------------------------------------------
# dlt pipeline: incremental sync + forget-on-scope-change
# ---------------------------------------------------------------------------
@pytest.fixture
def dlt_mod():
    return pytest.importorskip("dlt")


class Harness:
    """Runs patents_source through one dlt pipeline into a temp sqlite file."""

    def __init__(self, dlt, tmp_path, fake):
        self.dlt, self.tmp_path, self.fake = dlt, tmp_path, fake

    def run(self, today=TODAY, **kwargs):
        kwargs.setdefault("since", "2026-01-01")
        source = _source(today=today, http_client=self.fake.client(), **kwargs)
        pipeline = self.dlt.pipeline(
            pipeline_name="patents_test",
            destination=self.dlt.destinations.sqlalchemy(
                f"sqlite:///{(self.tmp_path / 'patents.db').as_posix()}"
            ),
            dataset_name="patents_ds",
            pipelines_dir=str(self.tmp_path / "state"),
        )
        # The same hint ingest_dlt_source applies to every document source.
        pipeline.run(source, columns={"cognee_node_set": {"data_type": "json"}})
        self.pipeline, self.stats = pipeline, source.cognee_sync_stats
        return self

    def rows(self):
        with (
            self.pipeline.sql_client() as client,
            client.execute_query(
                "SELECT id, title, content, cognee_node_set FROM patent_documents"
            ) as cursor,
        ):
            return {
                row[0]: {"title": row[1], "content": row[2], "node_sets": json.loads(row[3])}
                for row in cursor.fetchall()
            }


ACME = 'pa="acme"'
GRAPH = "cpc=G06F16/9024"


def _queries():
    return {
        ACME: lambda p: "Acme Corp" in p.applicants,
        GRAPH: lambda p: p.cpc == ("G", "06", "F", "16", "9024"),
        'pa="globex"': lambda p: "Globex" in p.applicants,
    }


def test_first_sync_loads_publications_with_node_sets(dlt_mod, tmp_path):
    corpus = [
        _pub(1, date(2026, 3, 1), "Graph index", cites=["EP2A1"]),
        _pub(2, date(2026, 4, 1), "Other thing", cpc=("A", "01", "B", "1", "00")),
    ]
    harness = Harness(dlt_mod, tmp_path, FakeOPS(corpus, _queries()))

    rows = harness.run(watches={"rivals": ACME, "graphs": GRAPH}).rows()

    assert set(rows) == {"EP1A1", "EP2A1"}
    assert rows["EP1A1"]["node_sets"] == ["patents:graphs", "patents:rivals"]
    assert rows["EP2A1"]["node_sets"] == ["patents:rivals"]
    assert rows["EP1A1"]["title"] == "Graph index"
    assert "EP1A1 cites:\n- EP2A1" in rows["EP1A1"]["content"]
    assert harness.stats["emitted"] == 2


def test_resync_searches_from_the_lookback_and_skips_unchanged(dlt_mod, tmp_path):
    fake = FakeOPS([_pub(1, date(2026, 10, 1))], _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME})

    # A publication DOCDB loaded late: dated before the last sync, seen after.
    fake.corpus.append(_pub(2, date(2026, 10, 10)))
    fake.requests.clear()
    harness.run(today=TODAY + timedelta(days=7), watches={"rivals": ACME}, lookback_days=10)

    assert fake.search_queries() == [f'({ACME}) and pd within "20261004 20261021"']
    assert harness.stats["emitted"] == 1  # only the new one
    assert harness.stats["unchanged"] == 1
    assert set(harness.rows()) == {"EP1A1", "EP2A1"}


def test_bibliographic_correction_is_re_emitted(dlt_mod, tmp_path):
    fake = FakeOPS([_pub(1, date(2026, 10, 1), "Old title")], _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME})

    fake.corpus[0].title = "Corrected title"
    harness.run(watches={"rivals": ACME})

    assert harness.stats["emitted"] == 1
    assert harness.rows()["EP1A1"]["title"] == "Corrected title"


def test_removed_watch_forgets_only_its_own_publications(dlt_mod, tmp_path):
    corpus = [
        _pub(1, date(2026, 3, 1)),  # rivals + graphs
        _pub(2, date(2026, 3, 1), cpc=("A", "01", "B", "1", "00")),  # rivals only
        _pub(3, date(2026, 3, 1), applicants=["Globex"]),  # graphs only
    ]
    fake = FakeOPS(corpus, _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME, "graphs": GRAPH})

    rows = harness.run(watches={"graphs": GRAPH}).rows()

    assert set(rows) == {"EP1A1", "EP3A1"}  # EP2A1 forgotten
    assert rows["EP1A1"]["node_sets"] == ["patents:graphs"]  # re-emitted, narrower
    assert harness.stats["deleted"] == 1


def test_changed_query_re_earns_or_forgets_its_matches(dlt_mod, tmp_path):
    corpus = [_pub(1, date(2026, 3, 1)), _pub(2, date(2026, 3, 1), applicants=["Globex"])]
    fake = FakeOPS(corpus, _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME})

    fake.requests.clear()
    rows = harness.run(watches={"rivals": 'pa="globex"'}).rows()

    assert set(rows) == {"EP2A1"}
    # A changed query backfills from `since`, not from the old cursor.
    assert fake.search_queries() == ['(pa="globex") and pd within "20260101 20261014"']


def test_removed_watch_refetches_publications_outside_the_window(dlt_mod, tmp_path):
    # EP1A1 stays in "graphs" but no search returns it this run (outside the
    # lookback), so its narrower node set needs one bulk retrieval.
    fake = FakeOPS([_pub(1, date(2026, 3, 1))], _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME, "graphs": GRAPH})

    fake.requests.clear()
    rows = harness.run(watches={"graphs": GRAPH}, lookback_days=0).rows()

    assert rows["EP1A1"]["node_sets"] == ["patents:graphs"]
    assert any(r.method == "POST" for r in fake.requests)


def test_pinned_publications_are_synced_once_and_forgotten_when_unpinned(dlt_mod, tmp_path):
    corpus = [_pub(7, date(2010, 1, 1)), Pub("EP", "7", "B1", date(2012, 1, 1), "Granted")]
    fake = FakeOPS(corpus, _queries())
    harness = Harness(dlt_mod, tmp_path, fake)

    # No kind code: every published kind of EP7 is pinned.
    rows = harness.run(publication_numbers=["ep 7"]).rows()
    assert set(rows) == {"EP7A1", "EP7B1"}
    assert rows["EP7A1"]["node_sets"] == ["patents:pinned"]

    fake.requests.clear()
    harness.run(publication_numbers=["EP7"])
    assert [r for r in fake.requests if "publication" in r.url.path] == []  # fetched once

    rows = harness.run(watches={"rivals": ACME}).rows()
    assert rows == {}  # unpinned and outside the watch window: forgotten


def test_unknown_pin_fails_loudly(dlt_mod, tmp_path):
    harness = Harness(dlt_mod, tmp_path, FakeOPS([], _queries()))
    with pytest.raises(Exception, match="holds no publication"):
        harness.run(publication_numbers=["EP999A1"])


def test_failed_run_keeps_memory_and_cursor(dlt_mod, tmp_path):
    fake = FakeOPS([_pub(1, date(2026, 3, 1))], _queries())
    harness = Harness(dlt_mod, tmp_path, fake)
    harness.run(watches={"rivals": ACME, "graphs": GRAPH})

    fake.fail_paths["/search/biblio"] = 500
    with pytest.raises(Exception, match="HTTP 500"):
        harness.run(watches={"rivals": ACME})  # would forget nothing, but must not commit

    fake.fail_paths.clear()
    fake.requests.clear()
    rows = harness.run(watches={"rivals": ACME, "graphs": GRAPH}).rows()
    assert set(rows) == {"EP1A1"}
    assert harness.stats["emitted"] == 0  # state from the first run is intact


def test_include_claims_adds_claims_where_ops_has_them(dlt_mod, tmp_path):
    corpus = [
        _pub(1, date(2026, 3, 1), claims=["1. A graph index.", "2. The index of claim 1."]),
        _pub(2, date(2026, 3, 1)),  # no full text in OPS: 404 on claims
    ]
    harness = Harness(dlt_mod, tmp_path, FakeOPS(corpus, _queries()))

    rows = harness.run(watches={"rivals": ACME}, include_claims=True).rows()

    assert "## Claims\n\n1. A graph index.\n2. The index of claim 1." in rows["EP1A1"]["content"]
    assert "## Claims" not in rows["EP2A1"]["content"]
