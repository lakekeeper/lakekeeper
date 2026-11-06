"""Lay out reference tables: wrap long inline code, label cells for phones.

Reference tables carry long unbroken tokens as inline code: environment
variables (`LAKEKEEPER__OPTIMIZE__SPILL_DISK_LIMIT_BYTES`), metric and action
names, paths and URLs. A browser finds no wrap opportunity inside them, so the
column takes the width of the longest token and the description column is
squeezed to a few words per line.

This hook adds a `<wbr>` at the word boundaries of long inline code in table
cells: after each `__` of an environment variable, and in other code (or in an
environment variable segment longer than `SEGMENT_LEN`) after each `_`, `/`,
`.`, `,`, `:` and `=` run and between a lower case and an upper case letter.
A kebab-case word up to `SEGMENT_LEN` characters is kept on one line
(`lk-keep` in `extra.css`), so `enable-optimize` does not break at its hyphen.
`<wbr>` adds no character, so copying the token gives the token. Code that
already contains markup, such as a hand-written `<wbr>`, is left alone.

It also copies each column header onto the column's cells as `data-label`.
On a phone `extra.css` shows every row as a block, and the label names each
value in place of the header row.

The hook works on the rendered HTML, so the Markdown sources and the raw
Markdown that `llms_txt.py` publishes stay unchanged.

The released version trees under `site/versions/` are frozen snapshots that
wrap some keys in a legacy `<nobr>` (several with a stray `<nobr>` where
`</nobr>` was meant). They are rendered by this same hook, so the wrapper is
removed before rendering.

Wired in via `mkdocs.yml`:

    hooks:
      - hooks/env_key_wrap.py
"""

import re

# Shorter code is not broken.
MIN_LEN = 16
# An environment variable segment or a kebab-case word up to this length
# stays on one line.
SEGMENT_LEN = 24

_NOBR_RE = re.compile(r"</?nobr>")
_TABLE_RE = re.compile(r"<table>.*?</table>", re.DOTALL)
# Inline code without child elements; a highlighted block always has some.
_CODE_RE = re.compile(r"<code>([^<]+)</code>")
_TH_RE = re.compile(r"<th[^>]*>(.*?)</th>", re.DOTALL)
_TBODY_RE = re.compile(r"<tbody>.*?</tbody>", re.DOTALL)
_TR_RE = re.compile(r"<tr>.*?</tr>", re.DOTALL)
_TD_RE = re.compile(r"<td(?=[ >])")
_TAG_RE = re.compile(r"<[^>]+>")
_ENV_SEGMENT_RE = re.compile(r"(?<=__)(?!_)")
_BREAK_RE = re.compile(r"(?<=[_/.,:=])(?![_/.,:=])|(?<=[a-z])(?=[A-Z])")


def _chunks(text: str) -> "list[str]":
    """Split code into the pieces between two wrap opportunities."""
    if len(text) < MIN_LEN:
        return [text]
    if "__" in text and " " not in text:
        chunks = []
        for segment in _ENV_SEGMENT_RE.split(text):
            long = len(segment) > SEGMENT_LEN
            chunks += _BREAK_RE.split(segment) if long else [segment]
        return chunks
    return _BREAK_RE.split(text)


def _keep(chunk: str) -> str:
    if "-" in chunk and len(chunk) <= SEGMENT_LEN:
        return f'<span class="lk-keep">{chunk}</span>'
    return chunk


def _wrap_code(match: "re.Match[str]") -> str:
    chunks = [_keep(c) for c in _chunks(match.group(1)) if c]
    return "<code>" + "<wbr>".join(chunks) + "</code>"


def _label_rows(tbody: str, labels: "list[str]") -> str:
    def label_row(row: "re.Match[str]") -> str:
        cells = iter(labels)
        return _TD_RE.sub(
            lambda _td: f'<td data-label="{next(cells, "")}"', row.group(0)
        )

    return _TR_RE.sub(label_row, tbody)


def _wrap_table(match: "re.Match[str]") -> str:
    table = _CODE_RE.sub(_wrap_code, match.group(0))
    labels = [
        _TAG_RE.sub("", th).strip().replace('"', "&quot;")
        for th in _TH_RE.findall(table)
    ]
    return _TBODY_RE.sub(lambda m: _label_rows(m.group(0), labels), table)


def on_page_markdown(markdown: str, **_kwargs) -> str:
    """MkDocs hook: drop the legacy `<nobr>` wrapper."""
    return _NOBR_RE.sub("", markdown)


def on_page_content(html: str, **_kwargs) -> str:
    """MkDocs hook: wrap long inline code and label the cells of tables."""
    return _TABLE_RE.sub(_wrap_table, html)
