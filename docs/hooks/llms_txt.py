"""Emit machine-readable views of the docs for LLMs and coding agents.

Agents that fetch ``https://docs.lakekeeper.io/docs/latest/configuration/``
get a JavaScript-heavy mkdocs-material shell in which the actual content is a
minority of the bytes. This hook publishes three plain-text views alongside the
rendered site so an agent can read the docs cheaply and correctly:

``/llms.txt``
    Curated index in the `llmstxt.org <https://llmstxt.org/>`_ format — one
    line per page with a short description, so an agent can pick the right
    page before fetching anything.

``/llms-full.txt``
    Every indexed page concatenated as Markdown, for one-shot ingestion.

``<page-url>.md``
    The raw Markdown source next to each rendered page, e.g.
    ``/docs/latest/configuration.md`` beside
    ``/docs/latest/configuration/``.

The Markdown is captured before conversion, so a page built from a
``pymdownx.snippets`` include (currently only ``about/code-of-conduct.md``)
carries the unexpanded ``--8<--`` directive rather than the included text. That
is the trade for emitting real Markdown instead of HTML converted back.

Only the canonical content is emitted: the top-level pages plus the
``docs/latest/`` tree. The other version trees (``docs/nightly``,
``docs/0.13.x``, ...) are duplicates or unreleased and are skipped, matching
the ``noindex`` policy in ``site/overrides/main.html``.

Wired in via ``mkdocs.yml``:

    hooks:
      - hooks/llms_txt.py
"""

import re
from pathlib import Path

# Site sections to publish, in emitted order. Each entry is
# (llms.txt heading, predicate on the page URL). First match wins, so the
# trailing catch-all must stay last.
#
# `## Optional` is meaningful in the llms.txt format: it marks pages an agent
# may skip when working within a tight context budget.
SECTIONS = (
    ("Docs", lambda url: url.startswith("docs/latest/")),
    ("Project", lambda url: not url.startswith("about/")),
    ("Optional", lambda _url: True),
)

# Non-canonical URLs, matching the `is_canonical_page` test in
# `site/overrides/main.html`:
#   - `docs/0.13.x/...`, `docs/nightly/...` — frozen or unreleased copies of
#     the same pages `docs/latest/` already serves.
#   - `docs/*/docs/...` — every version tree is published a second time one
#     level deeper, because the tree is built from a symlink mkdocs follows
#     twice (see `site/dev/common.sh`).
_SKIP_URL = re.compile(r"^docs/(?!latest/)|^docs/latest/docs/")

# Markdown/HTML noise to strip when deriving a description from page prose.
_INLINE_LINK = re.compile(r"\[([^\]]+)\]\([^)]*\)")
_HTML_TAG = re.compile(r"<[^>]+>")
_INLINE_MARK = re.compile(r"[*_`]+")
_HEADING_ANCHOR = re.compile(r"\s*\{#[^}]*\}\s*$")

# Lines that never make a usable description: headings, admonition markers,
# fences, content tabs, tables, raw HTML and list items.
_PROSE_STOP = ("#", "!!!", "```", "===", "---", "|", "<", "- ", "* ", "> ")

# A prose line opens with a word, a number or inline code — never with a CSS
# selector, a brace or a comment marker. Several pages (the Swagger embeds)
# open with a `<style>` block whose *body* is unindented and would otherwise
# be mistaken for the page's first paragraph.
_PROSE_START = re.compile(r"^[A-Za-z0-9`\"']")
_CODE_ISH = re.compile(r"[{}]|[;,]$")

MAX_DESCRIPTION = 200

# Below this, a "paragraph" is a stray fragment — the Swagger-embed pages, for
# example, open with a bare `/`. Better no description than a misleading one.
MIN_DESCRIPTION = 25

# Collected during the build, keyed by page URL. Reset per build so `mkdocs
# serve` rebuilds don't accumulate stale entries.
_pages: dict = {}


def on_config(config, **_kwargs):
    """MkDocs hook: reset per-build state."""
    _pages.clear()
    return config


def on_page_markdown(markdown, page, config, **_kwargs):
    """MkDocs hook: collect the raw Markdown of every canonical page."""
    url = page.url or ""
    # `api/README.md` and friends exist only to give a directory a nav entry;
    # linking an empty page wastes an agent's fetch.
    if _SKIP_URL.match(url) or not markdown.strip():
        return markdown

    _pages[url] = {
        "title": _clean(page.title or ""),
        "description": page.meta.get("description") or _first_paragraph(markdown),
        "markdown": markdown,
        "dest": page.file.dest_path,
        "is_homepage": page.is_homepage,
    }
    return markdown


def on_post_build(config, **_kwargs):
    """MkDocs hook: write llms.txt, llms-full.txt and the per-page .md files."""
    site_dir = Path(config["site_dir"])
    site_url = (config.get("site_url") or "").rstrip("/") + "/"

    for url, entry in _pages.items():
        _write_raw_markdown(site_dir, site_url, url, entry)

    (site_dir / "llms.txt").write_text(_render_index(config, site_url), "utf-8")
    (site_dir / "llms-full.txt").write_text(_render_full(config, site_url), "utf-8")


def _write_raw_markdown(site_dir, site_url, url, entry):
    """Write ``<page>.md`` next to the page's rendered ``<page>/index.html``."""
    dest = Path(entry["dest"])
    if dest.stem == "index" and dest.parent != Path("."):
        # Directory URLs (the default): `docs/latest/configuration/index.html`
        # -> `docs/latest/configuration.md`, so appending `.md` to the page URL
        # resolves. Built by name rather than `with_suffix` because version
        # directories like `0.13.x` already contain a dot.
        target = site_dir / dest.parent.parent / f"{dest.parent.name}.md"
    else:
        target = site_dir / dest.with_suffix(".md")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        f"---\n"
        f"title: {entry['title']}\n"
        f"source: {site_url}{url}\n"
        f"---\n\n"
        f"{entry['markdown'].rstrip()}\n",
        "utf-8",
    )


def _render_index(config, site_url):
    """Render the curated llms.txt index."""
    out = [
        "# Lakekeeper",
        "",
        f"> {config['site_description']}",
        "",
        "Lakekeeper is an Apache Iceberg REST Catalog. It manages Iceberg tables,",
        "views and namespaces for query engines such as Spark, Trino, StarRocks,",
        "DuckDB, Athena and Flink, vending short-lived storage credentials and",
        "enforcing fine-grained access control.",
        "",
        "Notes for retrieval:",
        "",
        "- These links point at the current release (`docs/latest`). Older",
        "  releases live under `/docs/<version>/` and unreleased docs under",
        "  `/docs/nightly/`; both are intentionally omitted here.",
        f"- Append `.md` to any docs URL for its Markdown source, e.g. `{site_url}docs/latest/configuration.md`.",
        f"- `{site_url}llms-full.txt` contains every page below in one file.",
        "",
    ]

    for heading, _matches in SECTIONS:
        entries = [
            (url, entry)
            for url, entry in sorted(_pages.items())
            if _section_of(url) == heading and not entry["is_homepage"]
        ]
        if not entries:
            continue
        out.append(f"## {heading}")
        out.append("")
        for url, entry in entries:
            line = f"- [{entry['title']}]({site_url}{url})"
            if entry["description"]:
                line += f": {entry['description']}"
            out.append(line)
        out.append("")

    return "\n".join(out)


def _section_of(url):
    """Return the llms.txt section a page URL belongs to."""
    for heading, matches in SECTIONS:
        if matches(url):
            return heading
    return SECTIONS[-1][0]


def _render_full(config, site_url):
    """Render llms-full.txt: every canonical page concatenated as Markdown."""
    out = [
        "# Lakekeeper — full documentation",
        "",
        f"> {config['site_description']}",
        "",
        f"Source: {site_url} — generated at build time from the current release.",
        "",
    ]
    for url, entry in sorted(_pages.items()):
        out.append("---")
        out.append("")
        out.append(f"# {entry['title']}")
        out.append("")
        out.append(f"Source: {site_url}{url}")
        out.append("")
        out.append(entry["markdown"].strip())
        out.append("")
    return "\n".join(out)


def _clean(text):
    """Strip the HTML badges and heading anchors the docs embed in titles."""
    return _INLINE_MARK.sub(
        "", _HTML_TAG.sub("", _HEADING_ANCHOR.sub("", text))
    ).strip()


def _first_paragraph(markdown):
    """Derive a one-line description from the first prose paragraph.

    A fallback only — pages should carry an explicit `description:` in their
    front matter, which both this and the `<meta name="description">` tag use
    in preference to anything guessed here.
    """
    for raw in markdown.splitlines():
        # Indented lines are admonition bodies, list continuations or code —
        # never a good summary of the page.
        if raw.startswith((" ", "\t")):
            continue
        line = raw.strip()
        if not line or line.startswith(_PROSE_STOP):
            continue
        if not _PROSE_START.match(line) or _CODE_ISH.search(line):
            continue
        line = _clean(_INLINE_LINK.sub(r"\1", line))
        if len(line) < MIN_DESCRIPTION:
            continue
        if len(line) > MAX_DESCRIPTION:
            line = line[:MAX_DESCRIPTION].rsplit(" ", 1)[0] + "…"
        return line
    return ""
