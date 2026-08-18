"""Force line breaks inside long environment-variable keys in tables.

Config tables list keys like
`LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_UNHANDLED_USERS`, which are wider than
the column can accommodate. Left alone the browser either wraps them at an
arbitrary character or blows the table past the page width.

This hook rewrites the offending inline-code spans to `<code>` carrying an
explicit `<br>` at the last `__` boundary that fits, so the break lands
somewhere meaningful. Short keys are left as ordinary Markdown code spans and
kept on one line by `.md-typeset table code { white-space: nowrap }` in
`extra.css`.

Scope is deliberately limited to table rows: prose can wrap wherever it likes,
and rewriting every code span in the docs would be a much broader change than
the problem calls for.

Two input forms are handled:

* A plain Markdown code span — ``LAKEKEEPER__...`` — which is what the sources
  use. Nothing needs to be marked up by hand.
* A legacy ``<nobr>`` wrapper. The released version trees under
  ``site/versions/`` are frozen snapshots that still carry it (several with a
  stray ``<nobr>`` where ``</nobr>`` was meant), and they are rendered by this
  same hook, so the form has to keep working.

Wired in via `mkdocs.yml`:

    hooks:
      - hooks/env_key_wrap.py
"""

import re

# Maximum chars per visible line before the hook inserts a break.
MAX_LEN = 40

# Match `<nobr>...</nobr>` pairs. The content rejects nested `<nobr>` or
# `</nobr>` to avoid spanning malformed source (some frozen version trees have
# stray `<nobr>` where `</nobr>` was meant; a permissive non-greedy match would
# consume text across them and corrupt unrelated paragraphs).
_NOBR_RE = re.compile(r"<nobr>((?:(?!</?nobr>).)*?)</nobr>", re.DOTALL)

# A Markdown inline-code span. Used only on table rows (see `_is_table_row`).
_CODE_RE = re.compile(r"`([^`\n]+)`")

# Only keys shaped like an env var are candidates — never prose in backticks.
_ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:__[A-Z0-9_]+)+$")


def _is_table_row(line: str) -> bool:
    return line.lstrip().startswith("|")


def _wrap_key(key: str) -> str:
    """Insert `<br>` at the rightmost `__` boundary before `MAX_LEN`.

    Recurses on the tail if it is still longer than `MAX_LEN`, so an
    arbitrarily long key gets broken into roughly equal-length chunks.
    Returns the original key when no `__` boundary fits below `MAX_LEN`.
    """
    if len(key) <= MAX_LEN:
        return key
    # Find the rightmost `__` whose start is at or before MAX_LEN.
    # `MAX_LEN - 2` so the `__` itself fits in the head segment.
    split = key.rfind("__", 0, MAX_LEN)
    if split == -1:
        # No `__` boundary in the allowed prefix — leave alone rather
        # than break inside a token.
        return key
    head = key[: split + 2]  # include the `__` on the head line
    tail = key[split + 2 :]
    return f"{head}<br>{_wrap_key(tail)}"


def _needs_wrap(key: str) -> bool:
    return len(key) > MAX_LEN and "__" in key


def _replace_nobr(match: "re.Match[str]") -> str:
    inner = match.group(1)
    # Strip surrounding backticks (Markdown inline-code delimiters).
    # Emit raw `<code>` so embedded `<br>` parses as HTML.
    stripped = inner.strip("`")
    if not _needs_wrap(stripped):
        return match.group(0)
    return f"<code>{_wrap_key(stripped)}</code>"


def _replace_code(match: "re.Match[str]") -> str:
    key = match.group(1)
    if not _ENV_KEY_RE.match(key) or not _needs_wrap(key):
        return match.group(0)
    return f"<code>{_wrap_key(key)}</code>"


def on_page_markdown(markdown: str, **_kwargs) -> str:
    """MkDocs hook: force a line break at `__` in long env keys in tables."""
    markdown = _NOBR_RE.sub(_replace_nobr, markdown)
    return "\n".join(
        _CODE_RE.sub(_replace_code, line) if _is_table_row(line) else line
        for line in markdown.split("\n")
    )
