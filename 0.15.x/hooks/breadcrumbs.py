"""Compute a breadcrumb trail for each page, using only URLs that exist.

Google renders a `BreadcrumbList` as a navigation path in place of the raw URL
(`Lakekeeper > Docs > Configuration` rather than
`docs.lakekeeper.io/docs/latest/configuration`), and it gives an assistant the
page's place in the hierarchy rather than a flat list of documents.

The trail cannot be derived from the URL path alone: this site has no landing
page at `/docs/`, `/docs/latest/` or `/about/`, so those segments are 404s.
Emitting them as `item` links would be broken structured data, which is worse
than emitting none. So the trail is built from the set of pages the build
actually produced, and a segment is included only when a real page backs it.

Sets `page.meta["breadcrumbs"]` to a list of `{name, url}` (site-relative URLs,
excluding the home page, which the template prepends). Rendered by
`site/overrides/main.html`.

Wired in via `mkdocs.yml`:

    hooks:
      - hooks/breadcrumbs.py
"""

# url -> title for every page in the build, filled during on_nav.
_titles: dict = {}


def on_config(config, **_kwargs):
    """MkDocs hook: reset per-build state."""
    _titles.clear()
    return config


def on_nav(nav, **_kwargs):
    """MkDocs hook: record every real page URL and its title."""
    for page in nav.pages:
        if page.url:
            _titles[page.url] = (page.title or "").strip()
    return nav


def on_page_markdown(markdown, page, **_kwargs):
    """MkDocs hook: attach the ancestor trail this page can actually claim."""
    url = page.url or ""
    parts = [p for p in url.split("/") if p]

    trail = []
    for i in range(1, len(parts)):
        candidate = "/".join(parts[:i]) + "/"
        title = _titles.get(candidate)
        if title:
            trail.append({"name": title, "url": candidate})

    page.meta["breadcrumbs"] = trail
    return markdown
