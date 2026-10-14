"""Unit tests for the browser history dlt connector.

Fixture profiles are built in ``tmp_path``: a Chromium ``History`` SQLite file
and ``Bookmarks`` JSON, and a Firefox ``places.sqlite``, with the real tables and
time encodings (microseconds since 1601 / 1970).

* DB-free tests: URL normalisation and redaction, privacy and search filters,
  both epochs, bookmark folders, merging, profile detection, page text
  extraction, visit buckets and argument validation.
* dlt-pipeline tests (temp sqlite destination): first sync and node sets,
  low-churn re-sync, deleting history or bookmarks (forget-on-delete),
  browser expiry kept vs. deletion forgotten, excluding a domain, ``since``
  not forgetting, and opt-in page fetching.
"""

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from cognee_community_connector_browser import browser as br
from cognee_community_connector_browser.browser import (
    Profile,
    browser_source,
    default_profiles,
    html_to_text,
    normalize_url,
    read_profile,
    visit_bucket,
)

TODAY = date(2026, 10, 14)
NOW = datetime(2026, 10, 14, 12, tzinfo=UTC)


def _chromium_us(moment: datetime) -> int:
    return int((moment - datetime(1601, 1, 1, tzinfo=UTC)).total_seconds() * 1_000_000)


def _unix_us(moment: datetime) -> int:
    return int(moment.timestamp() * 1_000_000)


class Chromium:
    """A Chromium profile directory with a History DB and a Bookmarks file."""

    def __init__(self, path: Path):
        self.path = path
        path.mkdir(parents=True, exist_ok=True)
        self.pages: dict[str, tuple] = {}
        self.bookmarks: list[tuple[list[str], str, str]] = []

    def visit(self, url, title, visits=1, last=NOW, first=None, hidden=0):
        self.pages[url] = (title, visits, last, first or last, hidden)
        return self

    def forget(self, url):
        self.pages.pop(url, None)
        return self

    def bookmark(self, folders, name, url):
        self.bookmarks.append((folders, name, url))
        return self

    def write(self):
        db_path = self.path / "History"
        db_path.unlink(missing_ok=True)
        db = sqlite3.connect(db_path)
        db.executescript(
            "CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INT,"
            " typed_count INT, last_visit_time INT, hidden INT);"
            "CREATE TABLE visits (id INTEGER PRIMARY KEY, url INT, visit_time INT);"
        )
        for i, (url, (title, visits, last, first, hidden)) in enumerate(self.pages.items(), 1):
            db.execute(
                "INSERT INTO urls VALUES (?, ?, ?, ?, 0, ?, ?)",
                (i, url, title, visits, _chromium_us(last), hidden),
            )
            db.execute(
                "INSERT INTO visits (url, visit_time) VALUES (?, ?)", (i, _chromium_us(first))
            )
        db.commit()
        db.close()

        bar = {"type": "folder", "name": "Bookmarks bar", "children": []}
        for folders, name, url in self.bookmarks:
            node = bar
            for folder in folders:
                child = next(
                    (c for c in node["children"] if c["type"] == "folder" and c["name"] == folder),
                    None,
                )
                if child is None:
                    child = {"type": "folder", "name": folder, "children": []}
                    node["children"].append(child)
                node = child
            node["children"].append(
                {"type": "url", "name": name, "url": url, "date_added": str(_chromium_us(NOW))}
            )
        (self.path / "Bookmarks").write_text(
            json.dumps(
                {
                    "roots": {
                        "bookmark_bar": bar,
                        "other": {"type": "folder", "name": "Other bookmarks", "children": []},
                    }
                }
            ),
            encoding="utf-8",
        )
        return Profile("edge", self.path)


def _firefox(path: Path, history, bookmarks=()):
    path.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path / "places.sqlite")
    db.executescript(
        "CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INT,"
        " last_visit_date INT, hidden INT);"
        "CREATE TABLE moz_historyvisits (id INTEGER PRIMARY KEY, place_id INT, visit_date INT);"
        "CREATE TABLE moz_bookmarks (id INTEGER PRIMARY KEY, type INT, fk INT, parent INT,"
        " title TEXT, guid TEXT, dateAdded INT);"
        "INSERT INTO moz_bookmarks VALUES (1, 2, NULL, 0, '', 'root________', 0);"
        "INSERT INTO moz_bookmarks VALUES (3, 2, NULL, 1, 'toolbar', 'toolbar_____', 0);"
        "INSERT INTO moz_bookmarks VALUES (10, 2, NULL, 3, 'Rust', 'folderRust01', 0);"
    )
    for i, (url, title, visits, last) in enumerate(history, 1):
        db.execute(
            "INSERT INTO moz_places VALUES (?, ?, ?, ?, ?, 0)",
            (i, url, title, visits, _unix_us(last)),
        )
        db.execute(
            "INSERT INTO moz_historyvisits (place_id, visit_date) VALUES (?, ?)",
            (i, _unix_us(last)),
        )
    for j, url in enumerate(bookmarks, 100):
        place = next(i for i, h in enumerate(history, 1) if h[0] == url)
        db.execute(
            "INSERT INTO moz_bookmarks VALUES (?, 1, ?, 10, 'mark', ?, ?)",
            (j, place, f"bm{j}", _unix_us(NOW)),
        )
    db.commit()
    db.close()
    return Profile("firefox", path)


# ---------------------------------------------------------------------------
# URLs and filters (DB-free)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("HTTPS://Docs.Python.org:443/3/#section", "https://docs.python.org/3/"),
        ("https://x.io/a?utm_source=tw&fbclid=1&id=7", "https://x.io/a?id=7"),
        (
            "https://app.io/cb?access_token=abc&page=2",
            "https://app.io/cb?access_token=REDACTED&page=2",
        ),
        ("https://user:pw@host.io/p", "https://host.io/p"),
        ("http://site.io:8080", "http://site.io:8080/"),
        ("chrome://settings", None),
        ("file:///C:/secret.txt", None),
        ("about:blank", None),
    ],
)
def test_normalize_url(url, expected):
    assert normalize_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:3000/",
        "http://127.0.0.1:8000/admin",
        "http://192.168.1.1/",
        "https://accounts.google.com/v3/signin",
        "https://mail.google.com/mail/u/0/",
        "https://www.paypal.com/myaccount",
        "https://onlinebanking.example.com/home",
        "https://github.com/login",
        "https://app.io/oauth/authorize",
    ],
)
def test_sensitive_pages_are_skipped(url):
    assert br._is_sensitive(normalize_url(url))


@pytest.mark.parametrize(
    ("url", "is_search"),
    [
        ("https://www.google.com/search?q=cognee", True),
        ("https://www.google.co.in/search?q=x", True),
        ("https://duckduckgo.com/?q=x", True),
        ("https://www.bing.com/search?q=x", True),
        ("https://docs.google.com/document/d/1", False),
        ("https://github.com/search?q=x", False),
        ("https://www.google.com/maps/place/x", False),
    ],
)
def test_search_pages(url, is_search):
    assert br._is_search_page(url) is is_search


def test_ordinary_pages_are_not_sensitive():
    for url in (
        "https://docs.python.org/3/",
        "https://github.com/topoteretes/cognee",
        "https://www.authorea.com/x",
    ):
        assert not br._is_sensitive(url)


def test_visit_buckets():
    assert [visit_bucket(n) for n in (0, 1, 3, 12, 50)] == [
        "never (bookmarked only)",
        "once",
        "a few times",
        "often",
        "very often",
    ]


def test_html_to_text_keeps_content_and_drops_chrome():
    html = (
        "<html><head><style>x{}</style></head><body><nav>Menu</nav><h1>Title</h1>"
        "<p>Hello &amp; welcome</p><script>evil()</script><footer>(c)</footer></body></html>"
    )
    assert html_to_text(html) == "Title\nHello & welcome"


# ---------------------------------------------------------------------------
# Reading profiles (DB-free)
# ---------------------------------------------------------------------------
def test_read_chromium_merges_variants_and_reads_bookmarks(tmp_path):
    profile = (
        Chromium(tmp_path / "Default")
        .visit("https://docs.rs/tokio#intro", "Tokio docs", visits=3, last=NOW - timedelta(days=1))
        .visit(
            "https://docs.rs/tokio?utm_source=x",
            "Tokio",
            visits=2,
            last=NOW,
            first=NOW - timedelta(days=30),
        )
        .visit("https://tracker.io/pixel", "", hidden=1)
        .bookmark(["Rust"], "Tokio", "https://docs.rs/tokio")
        .write()
    )

    pages = read_profile(profile)

    assert set(pages) == {"https://docs.rs/tokio"}  # hidden row skipped, variants merged
    page = pages["https://docs.rs/tokio"]
    assert page.visits == 5
    assert page.title == "Tokio"  # title of the most recent visit
    assert page.first_visit.date() == (NOW - timedelta(days=30)).date()
    assert page.last_visit == NOW
    assert page.bookmark_folders == ["Bookmarks bar/Rust"]


def test_read_firefox_history_and_bookmark_folders(tmp_path):
    profile = _firefox(
        tmp_path / "abc.default-release",
        [("https://doc.rust-lang.org/book/", "The Book", 4, NOW)],
        bookmarks=["https://doc.rust-lang.org/book/"],
    )

    page = read_profile(profile)["https://doc.rust-lang.org/book/"]

    assert (page.title, page.visits, page.last_visit) == ("The Book", 4, NOW)
    assert page.bookmark_folders == ["Bookmarks Toolbar/Rust"]


def test_read_profile_works_on_a_locked_database_copy(tmp_path):
    profile = Chromium(tmp_path / "Default").visit("https://a.io/", "A").write()
    holder = sqlite3.connect(profile.path / "History")
    holder.execute("BEGIN EXCLUSIVE")  # a running browser holding a write lock
    try:
        assert "https://a.io/" in read_profile(profile)
    finally:
        holder.rollback()
        holder.close()


def test_default_profiles_finds_chromium_and_firefox(tmp_path, monkeypatch):
    monkeypatch.setattr(br.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    Chromium(tmp_path / ".config" / "google-chrome" / "Default").visit("https://a.io/", "A").write()
    (tmp_path / ".config" / "google-chrome" / "Crashpad").mkdir()  # not a profile
    _firefox(tmp_path / ".mozilla" / "firefox" / "x.default", [("https://b.io/", "B", 1, NOW)])

    found = default_profiles(tmp_path)

    assert [(p.browser, p.name) for p in found] == [("chrome", "Default"), ("firefox", "x.default")]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"browsers": ["safari"]}, "Unknown browser"),
        ({"profiles": {"opera": "x"}}, "Unknown browser"),
        ({"min_visits": -1}, "min_visits"),
        ({"expiry_days": 0}, "expiry_days"),
    ],
)
def test_source_validates_arguments(kwargs, message):
    with pytest.raises(ValueError, match=message):
        browser_source(**kwargs)


def test_fetch_content_respects_allow_http_requests(monkeypatch):
    monkeypatch.setenv("ALLOW_HTTP_REQUESTS", "false")
    with pytest.raises(ValueError, match="ALLOW_HTTP_REQUESTS"):
        browser_source(fetch_content=True)


def test_source_declares_document_mode_and_merge(tmp_path):
    from cognee.tasks.ingestion.dlt_utils import PIPELINE_SCOPE_ATTR, document_source_tag

    resource = browser_source(profiles={"edge": tmp_path}, resource_name="br_a")

    assert document_source_tag(resource) == "browser"
    assert getattr(resource, PIPELINE_SCOPE_ATTR) == "br_a"
    assert resource.write_disposition == "merge"
    assert resource.columns["_deleted"]["hard_delete"] is True


# ---------------------------------------------------------------------------
# dlt pipeline
# ---------------------------------------------------------------------------
@pytest.fixture
def dlt_mod():
    return pytest.importorskip("dlt")


class Harness:
    def __init__(self, dlt, tmp_path, profile_dir):
        self.dlt, self.tmp_path, self.profile_dir = dlt, tmp_path, profile_dir

    def run(self, today=TODAY, **kwargs):
        kwargs.setdefault("profiles", {"edge": self.profile_dir})
        source = browser_source(today=today, **kwargs)
        pipeline = self.dlt.pipeline(
            pipeline_name="browser_test",
            destination=self.dlt.destinations.sqlalchemy(
                f"sqlite:///{(self.tmp_path / 'browser.db').as_posix()}"
            ),
            dataset_name="browser_ds",
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
                    "SELECT url, title, content, cognee_node_set FROM browser_pages"
                ) as cursor,
            ):
                rows = cursor.fetchall()
        except Exception:  # table not created yet: nothing was ever emitted
            return {}
        return {r[0]: {"title": r[1], "content": r[2], "node_sets": json.loads(r[3])} for r in rows}


def test_first_sync_emits_pages_with_node_sets(dlt_mod, tmp_path):
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://docs.python.org/3/library/asyncio.html", "asyncio", visits=8)
        .visit("https://github.com/login", "Sign in")
        .visit("https://www.google.com/search?q=asyncio", "asyncio - Google Search")
        .bookmark(["Python"], "asyncio", "https://docs.python.org/3/library/asyncio.html")
    )
    chromium.write()

    rows = Harness(dlt_mod, tmp_path, chromium.path).run().rows()

    assert set(rows) == {"https://docs.python.org/3/library/asyncio.html"}
    row = rows["https://docs.python.org/3/library/asyncio.html"]
    assert row["node_sets"] == [
        "browser:edge",
        "browser:site/docs.python.org",
        "browser:bookmarks/Bookmarks bar/Python",
    ]
    assert "Visited: often" in row["content"]
    assert "Last visited: 2026-10" in row["content"]
    assert "Bookmarked in: Bookmarks bar / Python" in row["content"]


def test_more_visits_in_the_same_bucket_do_not_re_emit(dlt_mod, tmp_path):
    chromium = Chromium(tmp_path / "Default").visit("https://a.io/", "A", visits=8)
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run()

    chromium.visit("https://a.io/", "A", visits=12, last=NOW + timedelta(days=1), first=NOW)
    chromium.write()
    harness.run()
    assert harness.stats["emitted"] == 0  # still "often", still October

    chromium.visit("https://a.io/", "A", visits=40, last=NOW + timedelta(days=2), first=NOW)
    chromium.write()
    harness.run()
    assert harness.stats["emitted"] == 1
    assert "Visited: very often" in harness.rows()["https://a.io/"]["content"]


def test_deleting_a_recent_page_forgets_it(dlt_mod, tmp_path):
    chromium = (
        Chromium(tmp_path / "Default").visit("https://a.io/", "A").visit("https://b.io/", "B")
    )
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run()

    chromium.forget("https://a.io/").write()  # removed from history by the user
    rows = harness.run().rows()

    assert set(rows) == {"https://b.io/"}
    assert harness.stats["deleted"] == 1


def test_browser_expiry_is_not_deletion(dlt_mod, tmp_path):
    old = NOW - timedelta(days=120)
    chromium = Chromium(tmp_path / "Default").visit("https://old.io/", "Old", last=old)
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run(since="2026-01-01")

    chromium.forget("https://old.io/").write()  # Chrome expired it after 90 days
    rows = harness.run(since="2026-01-01").rows()
    assert set(rows) == {"https://old.io/"}
    assert harness.stats["kept_expired"] == 1

    rows = harness.run(since="2026-01-01", keep_expired=False).rows()
    assert rows == {}


def test_deleting_a_bookmark_forgets_the_page(dlt_mod, tmp_path):
    old = NOW - timedelta(days=200)
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://keep.io/", "Keep", last=old)
        .bookmark(["Reading"], "Keep", "https://keep.io/")
    )
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run()
    assert set(harness.rows()) == {"https://keep.io/"}  # bookmarked: in scope despite age

    chromium.bookmarks.clear()
    chromium.forget("https://keep.io/").write()
    assert harness.run().rows() == {}


def test_excluding_a_domain_forgets_its_pages(dlt_mod, tmp_path):
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://news.io/a", "A")
        .visit("https://docs.io/b", "B")
    )
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run()

    rows = harness.run(exclude_domains=["news.io"]).rows()

    assert set(rows) == {"https://docs.io/b"}


def test_since_limits_what_is_added_not_what_is_kept(dlt_mod, tmp_path):
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://recent.io/", "R", last=NOW - timedelta(days=10))
        .visit("https://ancient.io/", "A", last=NOW - timedelta(days=400))
    )
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path).run()
    assert set(harness.rows()) == {"https://recent.io/"}

    # Months later, recent.io has aged past `since` but is still in history.
    rows = harness.run(today=TODAY + timedelta(days=120)).rows()
    assert set(rows) == {"https://recent.io/"}


def test_bookmarks_only(dlt_mod, tmp_path):
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://a.io/", "A", visits=50)
        .bookmark([], "B", "https://b.io/")
    )
    chromium.write()

    rows = Harness(dlt_mod, tmp_path, chromium.path).run(bookmarks_only=True).rows()

    assert set(rows) == {"https://b.io/"}
    assert "Visited: never (bookmarked only)" in rows["https://b.io/"]["content"]


def test_fetch_content_adds_page_text_once(dlt_mod, tmp_path):
    fetched = []

    def handler(request):
        fetched.append(str(request.url))
        if "broken" in str(request.url):
            return httpx.Response(500)
        return httpx.Response(
            200,
            html="<html><body><h1>Vacuum tuning</h1><p>Set autovacuum_naptime.</p></body></html>",
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    chromium = (
        Chromium(tmp_path / "Default")
        .visit("https://pg.io/vacuum", "Vacuum")
        .visit("https://broken.io/", "X")
    )
    chromium.write()
    harness = Harness(dlt_mod, tmp_path, chromium.path)

    rows = harness.run(fetch_content=True, http_client=client).rows()
    assert (
        "## Page text\n\nVacuum tuning\nSet autovacuum_naptime."
        in rows["https://pg.io/vacuum"]["content"]
    )
    assert (
        "## Page text" not in rows["https://broken.io/"]["content"]
    )  # a failed fetch is not fatal

    fetched.clear()
    harness.run(fetch_content=True, http_client=client)
    assert fetched == []  # unchanged pages are not fetched again


def test_firefox_profile_syncs_end_to_end(dlt_mod, tmp_path):
    profile = _firefox(
        tmp_path / "x.default",
        [("https://doc.rust-lang.org/book/", "The Book", 4, NOW)],
        bookmarks=["https://doc.rust-lang.org/book/"],
    )

    rows = Harness(dlt_mod, tmp_path, profile.path).run(profiles={"firefox": profile.path}).rows()

    assert rows["https://doc.rust-lang.org/book/"]["node_sets"] == [
        "browser:firefox",
        "browser:site/doc.rust-lang.org",
        "browser:bookmarks/Bookmarks Toolbar/Rust",
    ]
