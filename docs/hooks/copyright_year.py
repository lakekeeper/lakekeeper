"""Substitute the current year into the site copyright at build time.

Replaces the literal ``{year}`` placeholder in ``config.copyright`` with the
current (UTC) year, so the footer year updates automatically on each build /
deploy instead of being hand-edited every January.

Wired in via ``mkdocs.yml``:

    hooks:
      - hooks/copyright_year.py
"""

from datetime import datetime, timezone


def on_config(config, **_kwargs):
    """MkDocs hook: expand ``{year}`` in the copyright to the current year."""
    text = config.get("copyright")
    if text and "{year}" in text:
        config["copyright"] = text.replace(
            "{year}", str(datetime.now(timezone.utc).year)
        )
    return config
