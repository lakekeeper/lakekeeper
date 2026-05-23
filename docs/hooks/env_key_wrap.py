"""Insert forced line breaks into long `<nobr>` env-key spans.

MkDocs uses `<nobr>...</nobr>` to prevent unwanted wrapping of short
env-var references in table cells. Very long keys (e.g.
`LAKEKEEPER__ROLE_PROVIDER_CHAIN__LOG_UNHANDLED_USERS`) make the
column wider than the rest of the page can accommodate — this hook
detects those and replaces the wrapper with HTML that forces a line
break at the last `__` boundary before `MAX_LEN` chars.

Wired in via `mkdocs.yml`:

    hooks:
      - hooks/env_key_wrap.py
"""

import re

# Maximum chars per visible line before the hook inserts a break.
MAX_LEN = 40

# Match `<nobr>...</nobr>` pairs. The content rejects nested `<nobr>` or
# `</nobr>` to avoid spanning malformed source (the docs have several
# stray `<nobr>` where `</nobr>` was meant; a permissive non-greedy
# match would consume text across them and corrupt unrelated paragraphs).
_NOBR_RE = re.compile(r"<nobr>((?:(?!</?nobr>).)*?)</nobr>", re.DOTALL)


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


def on_page_markdown(markdown: str, **_kwargs) -> str:
    """MkDocs hook: force a line break at `__` in long `<nobr>` env keys."""

    def _replace(match: "re.Match[str]") -> str:
        inner = match.group(1)
        # Strip surrounding backticks (Markdown inline-code delimiters).
        # Emit raw `<code>` so embedded `<br>` parses as HTML.
        stripped = inner.strip("`")
        if len(stripped) <= MAX_LEN or "__" not in stripped:
            return match.group(0)
        return f"<code>{_wrap_key(stripped)}</code>"

    return _NOBR_RE.sub(_replace, markdown)
