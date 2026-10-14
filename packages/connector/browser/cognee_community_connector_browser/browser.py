"""Browser history connector for cognee, a ``dlt`` source over your research trail.

Sync browser history and bookmarks from Chrome, Edge, Brave, Vivaldi, Chromium
and Firefox into cognee, incrementally and with forget-on-delete — "what was
that page about vacuum tuning I read last month?"::

    import cognee
    from cognee_community_connector_browser import browser_source

    await cognee.remember(
        browser_source(),  # auto-detects every browser profile on this machine
        dataset_name="browsing",
        primary_key="id",
        write_disposition="merge",  # REQUIRED, see below
        max_rows_per_table=0,  # REQUIRED for more than 50 pages
    )

Design
------
* **Local only**: the browser's own profile files are read from disk. Nothing
  is fetched unless ``fetch_content=True``.
* **Locked databases**: a running browser holds its history database locked,
  so each run copies it (with its ``-wal`` / ``-journal`` sidecars) to a temp
  directory and reads the copy. The live file is never opened.
* **One row per page per browser profile**: URLs are normalised (fragment,
  tracking parameters and default ports dropped), and the browser's rows for
  the same page are merged. Content: title, URL, site, first visit, how often
  and in which month it was last visited, the bookmark folder if bookmarked,
  and (opt-in) the page's readable text.
* **Low churn**: every visit bumps the browser's counters, so a page's content
  only shows a visit *bucket* (once, a few times, often, very often) and the
  *month* of the last visit. A page is re-emitted when something a reader
  would notice changes, not on every visit.
* **Node sets**: ``browser:<browser>``, ``browser:site/<domain>`` and, for
  bookmarks, ``browser:bookmarks/<folder path>``.
* **Scope**: ``since`` (default: 90 days back) limits which *history* pages
  are added; bookmarks are always included (they are deliberate). Domain
  filters, ``min_visits`` and ``bookmarks_only`` narrow it further.
* **Forget-on-delete**: a page is tombstoned (``_deleted``) when you delete it
  from history, delete its bookmark, or a filter now excludes it; dlt's
  ``hard_delete`` plus cognee's ``orphan_cleanup`` then remove it.
* **Expiry is not deletion**: browsers delete old history on their own
  (Chrome after about 90 days). With ``keep_expired=True`` (default), a page
  that vanishes from history while its last visit is older than
  ``expiry_days`` is assumed expired and kept; a page visited recently that
  vanishes was deleted by you and is forgotten. To drop everything, use
  ``cognee.forget(dataset=...)``.
* **Privacy defaults**: non-http(s) URLs, localhost and private addresses,
  sign-in / OAuth pages, webmail, banking and payment sites, and search-result
  pages are skipped. Query parameters that look like credentials (``token``,
  ``code``, ``session``, ``sig``...) are redacted and tracking parameters are
  dropped before anything is stored or hashed.

.. important::
   ``write_disposition="merge"`` is **mandatory**. The add pipeline defaults to
   ``"replace"``, which would drop every page a run did not re-emit.

Limitations
-----------
* Safari is not supported (its history needs Full Disk Access on macOS).
* Chromium profile *display* names are not read; profiles are named by their
  folder (``Default``, ``Profile 1``).
* With ``fetch_content=True`` a page whose fetch fails is stored without its
  text and is not fetched again until it changes.
"""

from __future__ import annotations

import contextlib
import hashlib
import ipaddress
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = get_logger("browser_connector")

BROWSER_SOURCE_TAG = "browser"
DEFAULT_RESOURCE_NAME = "browser_pages"
CHROMIUM_BROWSERS = ("chrome", "edge", "brave", "vivaldi", "chromium")
BROWSERS = (*CHROMIUM_BROWSERS, "firefox")
_LABEL = {
    "chrome": "Chrome",
    "edge": "Edge",
    "brave": "Brave",
    "vivaldi": "Vivaldi",
    "chromium": "Chromium",
    "firefox": "Firefox",
}

_DEFAULT_SINCE_DAYS = 90
_DEFAULT_EXPIRY_DAYS = 90
_CHROMIUM_EPOCH = datetime(1601, 1, 1, tzinfo=UTC)
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_FETCH_TIMEOUT = 10.0
_FETCH_MAX_BYTES = 2_000_000
_PAGE_TEXT_MAX_CHARS = 20_000

# Sites never synced by default: sign-in, webmail, banking and payments.
DEFAULT_EXCLUDE_DOMAINS = (
    "accounts.google.com",
    "login.microsoftonline.com",
    "login.live.com",
    "login.yahoo.com",
    "appleid.apple.com",
    "auth0.com",
    "okta.com",
    "mail.google.com",
    "outlook.live.com",
    "outlook.office.com",
    "outlook.office365.com",
    "mail.yahoo.com",
    "mail.proton.me",
    "paypal.com",
    "venmo.com",
    "wise.com",
    "revolut.com",
    "paytm.com",
    "phonepe.com",
    "razorpay.com",
)
# A host label containing one of these is skipped (onlinebanking.x, mybank.y).
_SENSITIVE_HOST_WORDS = ("bank", "banking", "netbanking", "wallet")
_SENSITIVE_PATH = re.compile(
    r"/(log-?in|sign-?in|sign-?up|logout|oauth2?|auth|sso|saml|callback|password|reset|2fa|mfa)"
    r"(/|$|\?|\.)",
    re.IGNORECASE,
)
# Search-result pages: (host suffix, path prefix).
_SEARCH_PAGES = (
    ("google.", "/search"),
    ("bing.com", "/search"),
    ("duckduckgo.com", "/"),
    ("search.yahoo.com", "/search"),
    ("search.brave.com", "/search"),
    ("yandex.", "/search"),
    ("baidu.com", "/s"),
    ("ecosia.org", "/search"),
)
_TRACKING_PARAM = re.compile(
    r"^(utm_\w+|fbclid|gclid|dclid|msclkid|mc_cid|mc_eid|igshid|yclid|_hsenc|_hsmi|ref_src|"
    r"si|spm|scid|trk|trkinfo|ved|ei|oq|gs_lcrp|sca_esv|sxsrf|rlz|sourceid)$",
    re.IGNORECASE,
)
_SECRET_PARAM = re.compile(
    r"(token|secret|password|passwd|pwd|auth|session|sessid|sid|code|key|sig|signature|"
    r"credential|otp|jwt|state|nonce|ticket|hash)",
    re.IGNORECASE,
)
_REDACTED = "REDACTED"


# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------
def normalize_url(url: str) -> str | None:
    """Canonical form used for identity and storage; ``None`` when not syncable.

    Lower-cases scheme and host, drops credentials, default ports, the
    fragment and tracking parameters, and redacts credential-like parameters.
    """
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower().rstrip(".")
    port = (
        parts.port
        if parts.port and (scheme, parts.port) not in (("http", 80), ("https", 443))
        else None
    )
    netloc = f"{host}:{port}" if port else host
    query = []
    for name, value in parse_qsl(parts.query, keep_blank_values=True):
        if _TRACKING_PARAM.match(name):
            continue
        query.append((name, _REDACTED if _SECRET_PARAM.search(name) and value else value))
    return urlunsplit((scheme, netloc, parts.path or "/", urlencode(query), ""))


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def site_of(url: str) -> str:
    host = _host(url)
    return host[4:] if host.startswith("www.") else host


def _domain_matches(host: str, domains) -> bool:
    return any(host == d or host.endswith("." + d) for d in domains)


def _is_private_host(host: str) -> bool:
    if host in ("localhost",) or host.endswith((".localhost", ".local", ".internal", ".lan")):
        return True
    with contextlib.suppress(ValueError):
        address = ipaddress.ip_address(host.strip("[]"))
        return address.is_private or address.is_loopback or address.is_link_local
    return False


def _is_sensitive(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if _is_private_host(host) or _domain_matches(host, DEFAULT_EXCLUDE_DOMAINS):
        return True
    if any(word in label for label in host.split(".") for word in _SENSITIVE_HOST_WORDS):
        return True
    return bool(_SENSITIVE_PATH.search(parts.path))


def _is_search_page(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    query = {name for name, _ in parse_qsl(parts.query)}
    if not query & {"q", "p", "wd", "text"}:
        return False
    for marker, path in _SEARCH_PAGES:
        # "google." matches any Google country domain; others match the domain.
        host_ok = marker in host if marker.endswith(".") else _domain_matches(host, (marker,))
        if host_ok and parts.path.startswith(path):
            return True
    return False


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Profile:
    browser: str
    path: Path  # the profile directory

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def key(self) -> str:
        return f"{self.browser}:{self.path.resolve().as_posix().lower()}"


def _chromium_roots(home: Path) -> dict[str, list[Path]]:
    if sys.platform == "win32":
        local = Path(os.environ.get("LOCALAPPDATA", home / "AppData" / "Local"))
        return {
            "chrome": [local / "Google" / "Chrome" / "User Data"],
            "edge": [local / "Microsoft" / "Edge" / "User Data"],
            "brave": [local / "BraveSoftware" / "Brave-Browser" / "User Data"],
            "vivaldi": [local / "Vivaldi" / "User Data"],
            "chromium": [local / "Chromium" / "User Data"],
        }
    if sys.platform == "darwin":
        support = home / "Library" / "Application Support"
        return {
            "chrome": [support / "Google" / "Chrome"],
            "edge": [support / "Microsoft Edge"],
            "brave": [support / "BraveSoftware" / "Brave-Browser"],
            "vivaldi": [support / "Vivaldi"],
            "chromium": [support / "Chromium"],
        }
    config = Path(os.environ.get("XDG_CONFIG_HOME", home / ".config"))
    return {
        "chrome": [config / "google-chrome"],
        "edge": [config / "microsoft-edge"],
        "brave": [config / "BraveSoftware" / "Brave-Browser"],
        "vivaldi": [config / "vivaldi"],
        "chromium": [config / "chromium"],
    }


def _firefox_roots(home: Path) -> list[Path]:
    if sys.platform == "win32":
        appdata = Path(os.environ.get("APPDATA", home / "AppData" / "Roaming"))
        return [appdata / "Mozilla" / "Firefox" / "Profiles"]
    if sys.platform == "darwin":
        return [home / "Library" / "Application Support" / "Firefox" / "Profiles"]
    return [
        home / ".mozilla" / "firefox",
        home / "snap" / "firefox" / "common" / ".mozilla" / "firefox",
    ]


def default_profiles(home: Path | None = None) -> list[Profile]:
    """Every browser profile with a history database found on this machine."""
    home = home or Path.home()
    found = []
    for browser, roots in _chromium_roots(home).items():
        for root in roots:
            if root.is_dir():
                found.extend(
                    Profile(browser, d)
                    for d in sorted(root.iterdir())
                    if d.is_dir() and (d / "History").is_file()
                )
    for root in _firefox_roots(home):
        if root.is_dir():
            found.extend(
                Profile("firefox", d)
                for d in sorted(root.iterdir())
                if d.is_dir() and (d / "places.sqlite").is_file()
            )
    return found


# ---------------------------------------------------------------------------
# Reading profiles
# ---------------------------------------------------------------------------
@dataclass
class Page:
    url: str  # normalised
    title: str = ""
    visits: int = 0
    first_visit: datetime | None = None
    last_visit: datetime | None = None
    bookmark_folders: list[str] = field(default_factory=list)

    def merge(self, title: str, visits: int, first: datetime | None, last: datetime | None):
        if last and (self.last_visit is None or last > self.last_visit):
            self.last_visit = last
            self.title = title or self.title
        elif not self.title:
            self.title = title
        if first and (self.first_visit is None or first < self.first_visit):
            self.first_visit = first
        self.visits += max(visits, 0)


def _chromium_time(value: Any) -> datetime | None:
    try:
        micros = int(value)
    except (TypeError, ValueError):
        return None
    return _CHROMIUM_EPOCH + timedelta(microseconds=micros) if micros > 0 else None


def _unix_micro_time(value: Any) -> datetime | None:
    try:
        micros = int(value)
    except (TypeError, ValueError):
        return None
    return _UNIX_EPOCH + timedelta(microseconds=micros) if micros > 0 else None


@contextlib.contextmanager
def _database_copy(path: Path):
    """Open a private, read-only copy of a (possibly locked) SQLite database."""
    with tempfile.TemporaryDirectory(prefix="cognee_browser_") as tmp:
        target = Path(tmp) / path.name
        try:
            shutil.copy2(path, target)
            for suffix in ("-wal", "-journal", "-shm"):
                sidecar = path.with_name(path.name + suffix)
                if sidecar.is_file():
                    shutil.copy2(sidecar, target.with_name(target.name + suffix))
        except PermissionError as exc:
            raise PermissionError(
                f"Cannot copy {path}: the browser holds it exclusively. Close the browser "
                "(or wait for it to release the file) and sync again. Nothing was changed."
            ) from exc
        connection = sqlite3.connect(target)
        try:
            yield connection
        finally:
            connection.close()


def _read_chromium(profile: Profile, pages: dict[str, Page]) -> None:
    history = profile.path / "History"
    if history.is_file():
        with _database_copy(history) as db:
            rows = db.execute(
                "SELECT u.url, u.title, u.visit_count, u.last_visit_time, MIN(v.visit_time) "
                "FROM urls u LEFT JOIN visits v ON v.url = u.id "
                "WHERE u.hidden = 0 GROUP BY u.id"
            ).fetchall()
        for url, title, visits, last, first in rows:
            normalized = normalize_url(url or "")
            if normalized:
                pages.setdefault(normalized, Page(normalized)).merge(
                    title or "", visits or 0, _chromium_time(first), _chromium_time(last)
                )

    bookmarks = profile.path / "Bookmarks"
    if bookmarks.is_file():
        data = json.loads(bookmarks.read_text(encoding="utf-8"))

        def walk(node: dict, trail: list[str]) -> None:
            if node.get("type") == "url":
                normalized = normalize_url(node.get("url", ""))
                if normalized:
                    page = pages.setdefault(
                        normalized, Page(normalized, title=node.get("name", ""))
                    )
                    page.title = page.title or node.get("name", "")
                    page.bookmark_folders.append("/".join(trail))
                    if page.first_visit is None:
                        page.first_visit = _chromium_time(node.get("date_added"))
                return
            for child in node.get("children", []) or []:
                walk(
                    child,
                    [*trail, child.get("name", "")] if child.get("type") == "folder" else trail,
                )

        for root in (data.get("roots") or {}).values():
            if isinstance(root, dict):
                walk(root, [root.get("name") or "Bookmarks"])


_FIREFOX_ROOTS = {
    "menu": "Bookmarks Menu",
    "toolbar": "Bookmarks Toolbar",
    "unfiled": "Other Bookmarks",
    "mobile": "Mobile Bookmarks",
}


def _read_firefox(profile: Profile, pages: dict[str, Page]) -> None:
    places = profile.path / "places.sqlite"
    if not places.is_file():
        return
    with _database_copy(places) as db:
        history = db.execute(
            "SELECT p.url, p.title, p.visit_count, p.last_visit_date, MIN(v.visit_date) "
            "FROM moz_places p LEFT JOIN moz_historyvisits v ON v.place_id = p.id "
            "WHERE p.hidden = 0 AND p.visit_count > 0 GROUP BY p.id"
        ).fetchall()
        folders = {
            row[0]: (row[1], row[2], row[3])
            for row in db.execute(
                "SELECT id, parent, title, guid FROM moz_bookmarks WHERE type = 2"
            ).fetchall()
        }
        marks = db.execute(
            "SELECT p.url, b.title, b.parent, b.dateAdded FROM moz_bookmarks b "
            "JOIN moz_places p ON p.id = b.fk WHERE b.type = 1"
        ).fetchall()
    for url, title, visits, last, first in history:
        normalized = normalize_url(url or "")
        if normalized:
            pages.setdefault(normalized, Page(normalized)).merge(
                title or "", visits or 0, _unix_micro_time(first), _unix_micro_time(last)
            )

    def trail(folder_id: int) -> list[str]:
        names, seen = [], set()
        while folder_id in folders and folder_id not in seen:
            seen.add(folder_id)
            parent, title, guid = folders[folder_id]
            root = _FIREFOX_ROOTS.get((guid or "").split("_", 1)[0].rstrip("_"))
            names.append(root or title or "")
            folder_id = parent
        return [name for name in reversed(names) if name and name != "root"]

    for url, title, parent, added in marks:
        normalized = normalize_url(url or "")
        if normalized:
            page = pages.setdefault(normalized, Page(normalized, title=title or ""))
            page.title = page.title or title or ""
            page.bookmark_folders.append("/".join(trail(parent)) or "Bookmarks")
            if page.first_visit is None:
                page.first_visit = _unix_micro_time(added)


def read_profile(profile: Profile) -> dict[str, Page]:
    """All pages (history and bookmarks) of one profile, keyed by normalised URL."""
    pages: dict[str, Page] = {}
    if profile.browser == "firefox":
        _read_firefox(profile, pages)
    else:
        _read_chromium(profile, pages)
    for page in pages.values():
        page.bookmark_folders = sorted(set(page.bookmark_folders))
    return pages


# ---------------------------------------------------------------------------
# Page text (opt-in)
# ---------------------------------------------------------------------------
class _TextExtractor(HTMLParser):
    _SKIP = frozenset(
        {"script", "style", "noscript", "nav", "footer", "header", "svg", "form", "aside"}
    )
    _BLOCK = frozenset(
        {"p", "div", "li", "br", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "pre", "section"}
    )

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP:
            self.depth += 1
        elif tag in self._BLOCK:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._SKIP and self.depth:
            self.depth -= 1

    def handle_data(self, data):
        if not self.depth:
            self.parts.append(data)


def html_to_text(html: str) -> str:
    extractor = _TextExtractor()
    with contextlib.suppress(Exception):
        extractor.feed(html)
    lines = (" ".join(line.split()) for line in "".join(extractor.parts).splitlines())
    return "\n".join(line for line in lines if line)[:_PAGE_TEXT_MAX_CHARS]


def _fetch_text(client: Any, url: str) -> str:
    """Readable text of a page, or "" when it cannot be fetched (never raises)."""
    try:
        with client.stream("GET", url, follow_redirects=True, timeout=_FETCH_TIMEOUT) as response:
            if response.status_code != 200:
                return ""
            if "html" not in response.headers.get("content-type", "text/html"):
                return ""
            body = bytearray()
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > _FETCH_MAX_BYTES:
                    break
            return html_to_text(body.decode(response.encoding or "utf-8", errors="replace"))
    except Exception as exc:
        logger.info("Browser: could not fetch page text (%s).", type(exc).__name__)
        return ""


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def visit_bucket(visits: int) -> str:
    if visits <= 0:
        return "never (bookmarked only)"
    if visits == 1:
        return "once"
    if visits <= 5:
        return "a few times"
    if visits <= 20:
        return "often"
    return "very often"


def _render(profile: Profile, page: Page, text: str) -> str:
    label = _LABEL[profile.browser]
    lines = [
        f"URL: {page.url}",
        f"Site: {site_of(page.url)}",
        f"Browser: {label} (profile {profile.name})",
        f"Visited: {visit_bucket(page.visits)}",
    ]
    if page.first_visit:
        lines.append(f"First seen: {page.first_visit:%Y-%m-%d}")
    if page.last_visit:
        lines.append(f"Last visited: {page.last_visit:%Y-%m}")
    for folder in page.bookmark_folders:
        lines.append(f"Bookmarked in: {folder.replace('/', ' / ')}")
    body = "\n".join(lines)
    return f"{body}\n\n## Page text\n\n{text}" if text else body


def _day(moment: datetime | None) -> int:
    return (moment.date() - date(1970, 1, 1)).days if moment else 0


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
@dataclass
class _Filters:
    since: datetime
    include_domains: tuple[str, ...]
    exclude_domains: tuple[str, ...]
    min_visits: int
    bookmarks_only: bool
    include_bookmarks: bool
    include_searches: bool

    def in_scope(self, page: Page, previously_synced: bool) -> bool:
        """Whether a page belongs in memory. ``since`` only limits what is added."""
        host = _host(page.url)
        if _is_sensitive(page.url):
            return False
        if self.include_domains and not _domain_matches(host, self.include_domains):
            return False
        if _domain_matches(host, self.exclude_domains):
            return False
        if not self.include_searches and _is_search_page(page.url):
            return False
        bookmarked = bool(page.bookmark_folders) and self.include_bookmarks
        if self.bookmarks_only:
            return bookmarked
        if bookmarked:
            return True
        if page.visits < self.min_visits:
            return False
        recent = page.last_visit is not None and page.last_visit >= self.since
        return recent or previously_synced


def _iter_rows(
    profiles: list[Profile],
    state: dict,
    stats: dict[str, int],
    *,
    filters: _Filters,
    keep_expired: bool,
    expiry_days: int,
    today: date,
    fetch: Callable[[str], str] | None,
) -> Iterator[dict]:
    """Emit changed pages and tombstones for every profile; update ``state``.

    ``state["pages"][row id]`` is ``[content hash, last-visit day, bookmarked]``.
    Every profile is read (copied and parsed) before anything is emitted.
    """
    for key in ("profiles", "pages", "emitted", "unchanged", "deleted", "kept_expired"):
        stats[key] = 0
    previous: dict[str, list] = state.get("pages", {})
    read = [(profile, read_profile(profile)) for profile in profiles]

    rows, seen, pages_state = [], set(), {}
    for profile, pages in read:
        stats["profiles"] += 1
        for page in pages.values():
            row_id = (
                "browser-"
                + hashlib.sha256(f"{profile.key}\x00{page.url}".encode()).hexdigest()[:24]
            )
            seen.add(row_id)
            if not filters.in_scope(page, row_id in previous):
                continue
            stats["pages"] += 1
            base = _render(profile, page, "")
            digest = hashlib.sha256(base.encode()).hexdigest()[:16]
            bookmarked = int(bool(page.bookmark_folders))
            pages_state[row_id] = [digest, _day(page.last_visit), bookmarked]
            if previous.get(row_id, [None])[0] == digest:
                stats["unchanged"] += 1
                continue
            text = fetch(page.url) if fetch else ""
            node_sets = [
                f"{BROWSER_SOURCE_TAG}:{profile.browser}",
                f"{BROWSER_SOURCE_TAG}:site/{site_of(page.url)}",
                *(f"{BROWSER_SOURCE_TAG}:bookmarks/{folder}" for folder in page.bookmark_folders),
            ]
            rows.append(
                {
                    "id": row_id,
                    "url": page.url,
                    "title": page.title or page.url,
                    "content": _render(profile, page, text),
                    "_deleted": False,
                    NODE_SET_COLUMN: node_sets,
                }
            )

    expiry_cutoff = _day(datetime.combine(today, datetime.min.time(), UTC)) - expiry_days
    tombstones = []
    for row_id, (digest, last_day, bookmarked) in previous.items():
        if row_id in pages_state:
            continue
        vanished = row_id not in seen
        if vanished and keep_expired and not bookmarked and 0 < last_day < expiry_cutoff:
            # Gone from history long after its last visit: the browser expired it.
            stats["kept_expired"] += 1
            pages_state[row_id] = [digest, last_day, bookmarked]
            continue
        tombstones.append(row_id)

    stats["emitted"], stats["deleted"] = len(rows), len(tombstones)
    yield from rows
    for row_id in tombstones:
        yield {"id": row_id, "_deleted": True}

    state["pages"] = pages_state
    logger.info(
        "Browser: %d profile(s), %d page(s) in scope: %d emitted, %d unchanged, "
        "%d forgotten, %d kept after browser expiry.",
        stats["profiles"],
        stats["pages"],
        stats["emitted"],
        stats["unchanged"],
        stats["deleted"],
        stats["kept_expired"],
    )


def browser_source(
    browsers: list[str] | None = None,
    profiles: dict[str, str | os.PathLike | list[str | os.PathLike]] | None = None,
    *,
    since: date | datetime | str | None = None,
    include_domains: list[str] | None = None,
    exclude_domains: list[str] | None = None,
    min_visits: int = 1,
    bookmarks_only: bool = False,
    include_bookmarks: bool = True,
    include_searches: bool = False,
    keep_expired: bool = True,
    expiry_days: int = _DEFAULT_EXPIRY_DAYS,
    fetch_content: bool = False,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    check_active: Callable[[], None] | None = None,
    http_client: Any = None,
    today: date | None = None,
):
    """Create a dlt resource that syncs browser history and bookmarks into cognee.

    Args:
        browsers: Restrict auto-detection to these browsers (``chrome``,
            ``edge``, ``brave``, ``vivaldi``, ``chromium``, ``firefox``).
        profiles: Explicit profile directories instead of auto-detection, as
            ``{browser: dir or [dirs]}`` (the folder holding ``History`` or
            ``places.sqlite``, e.g. ``.../User Data/Default``).
        since: Only history pages last visited on or after this date are added
            (``date``/``datetime``/``YYYY-MM-DD``; default 90 days ago). Pages
            already in memory are not forgotten when they age past it.
        include_domains: Only sync these sites (and their subdomains).
        exclude_domains: Never sync these sites; adding one forgets its pages.
        min_visits: Skip history pages visited fewer times (bookmarks always count).
        bookmarks_only: Sync bookmarks and nothing else.
        include_bookmarks: Sync bookmarks (and tag their folders).
        include_searches: Also sync search-result pages (Google, Bing...).
        keep_expired: Keep pages the browser expired from history (see above).
        expiry_days: How old a page's last visit must be for its disappearance
            to count as browser expiry rather than deletion.
        fetch_content: Also fetch each emitted page and store its readable
            text. Off by default; honours ``ALLOW_HTTP_REQUESTS=false``.
        resource_name: Stable dlt resource name, also the staging table and the
            key of the incremental state. Every browser source syncing into one
            dataset needs its own name.
        check_active: Optional host authorization checkpoint during extraction.
        http_client: Pre-built ``httpx.Client`` for page fetches (tests).
        today: Reference date for ``since`` and expiry (tests).

    Returns:
        A dlt resource for ``cognee.remember(..., primary_key="id",
        write_disposition="merge", max_rows_per_table=0)``.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 2:
        raise RuntimeError(
            "The browser connector requires a cognee build that reads per-row node sets "
            "(DOCUMENT_SYNC_VERSION >= 2, cognee 1.6.4 or later). Upgrade cognee."
        )
    for browser in list(browsers or []) + list(profiles or {}):
        if browser not in BROWSERS:
            raise ValueError(f"Unknown browser {browser!r}; supported: {', '.join(BROWSERS)}.")
    if min_visits < 0 or expiry_days < 1:
        raise ValueError("min_visits must be 0 or more and expiry_days at least 1.")
    if fetch_content and os.getenv("ALLOW_HTTP_REQUESTS", "true").lower() in ("false", "0", "no"):
        raise ValueError("fetch_content=True needs HTTP requests, but ALLOW_HTTP_REQUESTS is off.")

    reference = today or date.today()
    if since is None:
        since = reference - timedelta(days=_DEFAULT_SINCE_DAYS)
    elif isinstance(since, str):
        since = date.fromisoformat(since)
    if not isinstance(since, datetime):
        since = datetime(since.year, since.month, since.day, tzinfo=UTC)
    elif since.tzinfo is None:
        since = since.replace(tzinfo=UTC)

    filters = _Filters(
        since=since,
        include_domains=tuple(d.lower().lstrip(".") for d in include_domains or []),
        exclude_domains=tuple(d.lower().lstrip(".") for d in exclude_domains or []),
        min_visits=min_visits,
        bookmarks_only=bookmarks_only,
        include_bookmarks=include_bookmarks or bookmarks_only,
        include_searches=include_searches,
    )

    def resolve_profiles() -> list[Profile]:
        if profiles:
            return [
                Profile(browser, Path(p))
                for browser, paths in profiles.items()
                for p in (paths if isinstance(paths, list) else [paths])
            ]
        found = default_profiles()
        if browsers:
            found = [p for p in found if p.browser in browsers]
        if not found:
            raise ValueError(
                "No browser profiles found on this machine; pass profiles= explicitly."
            )
        return found

    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        # cognee_node_set needs no json hint here: ingest_dlt_source applies
        # NODE_SET_COLUMN_HINT to every document source, merged with this one.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def browser_pages():
        owned = None
        fetch = None
        if fetch_content:
            import httpx

            owned = (
                None
                if http_client is not None
                else httpx.Client(headers={"User-Agent": "cognee-browser-connector/0.1"})
            )
            client = http_client or owned
            fetch = lambda url: _fetch_text(client, url)  # noqa: E731
        try:
            yield from dlt_utils.guarded_rows(
                _iter_rows(
                    resolve_profiles(),
                    dlt.current.resource_state(),
                    stats,
                    filters=filters,
                    keep_expired=keep_expired,
                    expiry_days=expiry_days,
                    today=reference,
                    fetch=fetch,
                ),
                check_active,
            )
        finally:
            if owned is not None:
                owned.close()

    resource = browser_pages()
    # Opt into the document ingestion path: each page row (id/title/content/url,
    # plus cognee_node_set) becomes a text document that flows through normal
    # cognify. resolve_dlt_sources reads this marker; it never imports this
    # connector.
    setattr(resource, DOCUMENT_SOURCE_ATTR, BROWSER_SOURCE_TAG)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    # Host-readable diagnostics contain counts only, never URLs or titles.
    resource.cognee_sync_stats = stats
    return resource
