#!/usr/bin/env python3
"""Assert the crawlability invariants of the built docs site.

These are exactly the things that broke silently once already: the site shipped
for months with no `site_url`, which meant an empty `sitemap.xml`, no canonical
tags anywhere, and Open Graph tags containing the literal string "None". None
of it was visible from the rendered pages, and nothing failed a build.

Run against a built site (`site/`), e.g. via `just linkcheck`:

    python3 dev/seo_check.py site

Checks:
  1. sitemap.xml is well-formed, non-empty and lists only canonical URLs.
  2. Every page has a `<link rel="canonical">`.
  3. Exactly the non-canonical duplicate pages carry `robots: noindex`.
  4. Open Graph tags are absolute URLs, and og:image resolves to a real file.
  5. robots.txt / llms.txt / llms-full.txt were emitted and are non-trivial.
  6. Every source docs page declares a `description` in its front matter.
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SITE_URL = "https://docs.lakekeeper.io/"

# Mirrors `is_canonical_page` in overrides/main.html and overrides/sitemap.xml.
def is_canonical(url: str) -> bool:
    return not url.startswith("docs/") or url.startswith("docs/latest/")


CANONICAL_RE = re.compile(r'<link rel="canonical" href="([^"]*)"')
ROBOTS_RE = re.compile(r'<meta name="robots" content="([^"]*)"')
OG_RE = re.compile(r'<meta property="og:(url|image)" content="([^"]*)"')

_REPO = Path(__file__).resolve().parents[2]
_SITE = _REPO / "site" / "docs"

# Editable source pages that back the published site; each needs a description.
# `site/versions/` is deliberately absent: it is the generated worktree of the
# released version trees, rebuilt by `dev/common.sh` on every build. Fixes for
# those pages belong on the `docs` branch, not here.
SOURCE_GLOBS = (
    (_REPO / "docs" / "docs", "**/*.md"),  # nightly docs -> next release
    (_SITE, "*.md"),
    (_SITE / "about", "*.md"),
)

# The home page is a bespoke landing template with no prose body; it inherits
# the site-wide description.
DESCRIPTION_EXEMPT = {"index.md", "README.md"}

errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


def page_url(site: Path, html: Path) -> str:
    """Site-relative URL of a built page, e.g. `docs/latest/concepts/`."""
    rel = html.relative_to(site).parent.as_posix()
    return "" if rel == "." else f"{rel}/"


def check_sitemap(site: Path) -> None:
    path = site / "sitemap.xml"
    if not path.is_file():
        return fail("sitemap.xml was not generated")

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        return fail(f"sitemap.xml is not well-formed: {exc}")

    # `findall` (not `iter`) — only findall supports the `{*}` namespace wildcard.
    locs = [el.text or "" for el in root.findall(".//{*}loc")]
    if not locs:
        return fail("sitemap.xml lists 0 URLs — is `site_url` set in mkdocs.yml?")

    for loc in locs:
        if not loc.startswith(SITE_URL):
            fail(f"sitemap.xml lists a non-absolute or foreign URL: {loc}")
        elif not is_canonical(loc[len(SITE_URL) :]):
            fail(f"sitemap.xml lists a noindex duplicate: {loc}")

    print(f"  sitemap.xml: {len(locs)} canonical URLs")


def check_pages(site: Path) -> None:
    pages = sorted(site.rglob("index.html"))
    if not pages:
        return fail(f"no built pages found under {site}")

    for html in pages:
        url = page_url(site, html)
        text = html.read_text("utf-8", errors="replace")
        label = f"/{url}"

        canonical = CANONICAL_RE.search(text)
        if not canonical:
            fail(f"{label}: no <link rel=\"canonical\">")
        elif not canonical.group(1).startswith(SITE_URL):
            fail(f"{label}: canonical is not absolute: {canonical.group(1)}")

        robots = ROBOTS_RE.search(text)
        noindexed = bool(robots and "noindex" in robots.group(1))
        if is_canonical(url) and noindexed:
            fail(f"{label}: canonical page is marked noindex")
        elif not is_canonical(url) and not noindexed:
            fail(f"{label}: duplicate page is missing noindex")

        for prop, value in OG_RE.findall(text):
            if not value.startswith("https://"):
                fail(f"{label}: og:{prop} is not an absolute URL: {value!r}")
            elif prop == "image":
                asset = site / value[len(SITE_URL) :]
                if not asset.is_file():
                    fail(f"{label}: og:image points at a missing file: {value}")

    indexable = sum(1 for p in pages if is_canonical(page_url(site, p)))
    print(f"  pages: {len(pages)} built, {indexable} indexable, rest noindex")


def check_agent_files(site: Path) -> None:
    for name, min_bytes in (
        ("robots.txt", 40),
        ("llms.txt", 500),
        ("llms-full.txt", 10_000),
    ):
        path = site / name
        if not path.is_file():
            fail(f"{name} was not emitted")
        elif path.stat().st_size < min_bytes:
            fail(f"{name} is suspiciously small ({path.stat().st_size} bytes)")

    robots = site / "robots.txt"
    if robots.is_file() and f"Sitemap: {SITE_URL}sitemap.xml" not in robots.read_text():
        fail("robots.txt does not advertise the sitemap")

    print("  agent files: robots.txt, llms.txt, llms-full.txt")


def check_source_descriptions() -> None:
    missing, checked = [], 0
    for root, pattern in SOURCE_GLOBS:
        if not root.is_dir():
            fail(f"source docs not found at {root}")
            continue
        for md in sorted(root.glob(pattern)):
            if md.name in DESCRIPTION_EXEMPT:
                continue
            text = md.read_text("utf-8", errors="replace")
            if not text.strip():
                continue
            checked += 1
            end = text.find("\n---\n", 3) if text.startswith("---\n") else -1
            if end == -1 or "description:" not in text[4 : end + 1]:
                missing.append(md.relative_to(_REPO).as_posix())

    if missing:
        fail(
            "pages without a `description:` in their front matter (needed for "
            "search snippets and llms.txt): " + ", ".join(missing)
        )
    else:
        print(f"  source docs: all {checked} pages declare a description")


def main(argv: list[str]) -> int:
    site = Path(argv[1] if len(argv) > 1 else "site")
    if not site.is_dir():
        print(f"error: {site} not found — build the site first (dev/build.sh)")
        return 2

    print(f"seo-check {site}:")
    check_sitemap(site)
    check_pages(site)
    check_agent_files(site)
    check_source_descriptions()

    if errors:
        print(f"\n{len(errors)} problem(s):", file=sys.stderr)
        for error in errors[:40]:
            print(f"  - {error}", file=sys.stderr)
        if len(errors) > 40:
            print(f"  ... and {len(errors) - 40} more", file=sys.stderr)
        return 1

    print("\nall crawlability checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
