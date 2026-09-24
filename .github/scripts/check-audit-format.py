#!/usr/bin/env python3
"""The audit log format version: compute it, write it, and check it is right.

`AUDIT_FORMAT` is not edited by hand and does not move once per pull request. It is derived
from committed state:

    AUDIT_FORMAT = audit-format/released.json  raised once by  the highest level
                                                               in audit-format/unreleased/

So a release raises the version at most once however many changes it carries, and a major
change absorbs every minor change in the same cycle. Being a pure function also makes it
idempotent: withdrawing a fragment lowers the answer again rather than leaving the version
over-claimed.

What each pull request owes is a FRAGMENT, not a version number: one file declaring the
level of its change and describing it in the terms an operator parsing the log thinks in.
The fixture tests classify a format change, but only until someone regenerates the fixtures
— after `just update-audit-fixtures` they pass whether a fragment was written or not.
Relating the two is what this does, and it needs both the old and the new state, so it lives
in CI rather than in a Rust test.

Check a branch:  python3 .github/scripts/check-audit-format.py <base-ref> [--base-branch <name>]
Write the version: python3 .github/scripts/check-audit-format.py --write-version
Release notes:   python3 .github/scripts/check-audit-format.py --release-notes
Cut a release:   python3 .github/scripts/check-audit-format.py --release <lakekeeper-version>
Record shapes:   python3 .github/scripts/check-audit-format.py --summarise-records DIR OUT.json
Compare schemas: python3 .github/scripts/check-audit-format.py --compare-schemas A.json B.json
Self-test:       python3 .github/scripts/check-audit-format.py --self-test
Read version:    python3 .github/scripts/check-audit-format.py --print-version <rev>
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
import re
import subprocess
import sys
from pathlib import Path

AUDIT_DIR = "crates/lakekeeper/src/service/events/backends/audit"

# The committed schema of the emitter this repository owns: what the registry generates,
# committed by `just update-audit-schema`. Diffed across the merge base like the fixtures.
SCHEMA_PATH = "audit-format/schema.json"

# Optional per-repository overrides of the paths above, so a crate outside this repository
# runs the same checker against its own schema and declaration. Read once, from the working
# tree, before anything else.
CONFIG_PATH = "audit-format/config.json"

# The Rust constant every record's version is read from, and the tree searched for its
# declaration. Both are configurable because another repository names and places its own.
VERSION_CONST = "AUDIT_FORMAT"
VERSION_SEARCH_PATH = "crates/"

# The copy of the schema published to the documentation site: the file customers download.
# Written beside the schema by the same command, so a schema that moved without it means one
# of the two was not regenerated.
PUBLISHED_SCHEMA_PATH = "docs/docs/audit/schema.json"


def load_config() -> None:
    """Override the path constants from `CONFIG_PATH`, if the file exists.

    Everything this checker needs to find is named here, so the script runs unchanged in a
    repository laid out differently: the declaration it reads the version from, the tree it
    searches for that declaration, the schema, its published copy, the baseline and the
    fragments. The defaults are Lakekeeper's, so this repository needs no config file.
    """
    global AUDIT_DIR, SCHEMA_PATH, BASELINE_PATH, FRAGMENT_DIR
    global VERSION_CONST, VERSION_SEARCH_PATH, PUBLISHED_SCHEMA_PATH
    global GIT_PATTERN, VERSION_RE, VERSION_WRITE_RE
    path = Path(CONFIG_PATH)
    if not path.is_file():
        return
    try:
        config = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise SystemExit(f"::error::{CONFIG_PATH} is not valid JSON: {error}") from error
    if not isinstance(config, dict):
        raise SystemExit(f"::error::{CONFIG_PATH} must hold a JSON object.")
    AUDIT_DIR = config.get("audit_dir", AUDIT_DIR)
    SCHEMA_PATH = config.get("schema", SCHEMA_PATH)
    BASELINE_PATH = config.get("baseline", BASELINE_PATH)
    FRAGMENT_DIR = config.get("fragments", FRAGMENT_DIR)
    PUBLISHED_SCHEMA_PATH = config.get("published_schema", PUBLISHED_SCHEMA_PATH)
    VERSION_CONST = config.get("version_const", VERSION_CONST)
    VERSION_SEARCH_PATH = config.get("version_search_path", VERSION_SEARCH_PATH)
    GIT_PATTERN, VERSION_RE, VERSION_WRITE_RE = version_patterns(VERSION_CONST)
# Two patterns for the same thing: `git grep -E` is POSIX ERE, which has no `\s`.
#
# Both are anchored to a `pub const` DECLARATION rather than to any occurrence of the text.
# Without the anchor, prose that merely quotes the constant — a doc comment explaining the
# format, say — counts as a second declaration and fails the build, while a commented-out
# declaration would count as a real one.
def version_patterns(const: str) -> tuple[str, re.Pattern[str], re.Pattern[str]]:
    """The three patterns that find, read and rewrite the version declaration of `const`.

    Built from the constant's name rather than written out, so naming it in the config file
    changes all three together and they cannot disagree.
    """
    name = re.escape(const)
    return (
        rf'^[[:space:]]*pub const {const}: &str = "[0-9]+\.[0-9]+"',
        re.compile(rf'^\s*pub const {name}:\s*&str\s*=\s*"(\d+)\.(\d+)"'),
        re.compile(
            rf'(^[ \t]*pub const {name}:[ \t]*&str[ \t]*=[ \t]*")\d+\.\d+(")', re.MULTILINE
        ),
    )


GIT_PATTERN, VERSION_RE, VERSION_WRITE_RE = version_patterns(VERSION_CONST)


# ── git plumbing ────────────────────────────────────────────────────────────────


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def _git_grep(*args: str) -> str | None:
    """The matching lines, or None when nothing matched.

    `git grep` exits 1 for "no match" and greater than 1 for a real failure. Conflating
    the two is the dangerous direction: a git failure read as "no match" reads as "the
    version is not declared yet", which passes the whole check unconditionally.
    """
    result = subprocess.run(["git", "grep", *args], capture_output=True, text=True)
    if result.returncode == 1:
        return None
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr
        )
    return result.stdout


class AmbiguousVersion(Exception):
    """More than one file declares the version, so no single one is authoritative."""


def declared_versions(rev: str) -> dict[str, tuple[int, int]]:
    """Every declaration of the version at `rev`, keyed by file.

    Searched tree-wide so moving the module does not read as the version disappearing;
    keyed by path because a second match must be an error, not a silent pick.
    """
    out = _git_grep("-E", GIT_PATTERN, rev, "--", VERSION_SEARCH_PATH)
    if out is None:
        return {}
    found: dict[str, tuple[int, int]] = {}
    for line in out.splitlines():
        # `git grep <rev>` prefixes each hit with `rev:path:`, and a path cannot contain a
        # colon in git, so splitting from the left twice is exact.
        parts = line.split(":", 2)
        if len(parts) < 3:
            continue
        match = VERSION_RE.search(parts[2])
        if match:
            found[parts[1]] = (int(match.group(1)), int(match.group(2)))
    return found


def single_version(found: dict[str, tuple[int, int]], rev: str = "HEAD") -> tuple[int, int] | None:
    """The one declared version, or None. Raises if more than one file declares it.

    Agreeing values do not excuse a second declaration: it is a second place to bump, and
    which one wins is decided by sort order. Split from the git lookup so it is testable
    without a repository.
    """
    if len(found) > 1:
        listed = ", ".join(f"{path} ({v[0]}.{v[1]})" for path, v in sorted(found.items()))
        raise AmbiguousVersion(
            f"{len(found)} files declare AUDIT_FORMAT at {rev}: {listed}. Exactly one "
            f"declaration must exist — even when the values agree, because a bump then has "
            f"to be applied twice and this check reads whichever comes first."
        )
    return next(iter(found.values()), None)


def declared_version(rev: str) -> tuple[int, int] | None:
    """The single declared version at `rev`, or None if the field does not exist yet."""
    return single_version(declared_versions(rev), rev)


# ── the baseline and the fragments ──────────────────────────────────────────────

BASELINE_PATH = "audit-format/released.json"
FRAGMENT_DIR = "audit-format/unreleased/"

# `none` is a level, not the absence of one: a new action value changes nothing about the
# format but is still worth a line in the release notes. Ranked so that `max` over a set of
# fragments is exactly the arithmetic the version needs.
LEVELS = ("none", "minor", "major")
LEVEL_RANK = {level: rank for rank, level in enumerate(LEVELS)}

# What a fixture or schema verdict says a fragment must AT LEAST declare. `values` and
# `unknown` are deliberately absent: both hand the question to a human (see `DEFERRALS`),
# so neither can demand a fragment without taxing every change that merely makes a test
# input more realistic.
REQUIRED_LEVEL = {"breaking": "major", "additive": "minor"}

FRAGMENT_LEVEL_RE = re.compile(r"^level:[ \t]*(\S+)[ \t]*$", re.MULTILINE)


def parse_baseline(text: str, where: str) -> tuple[int, int] | None:
    """The released version recorded in `text`, or None when nothing has been released yet.

    A MISSING file is a different thing and is an error at the call site. Absent means
    someone deleted the baseline; `null` inside it means no release has carried an audit
    format yet. Conflating them lets a deleted file read as the bootstrap case, which
    demands 1.0 and would quietly renumber a format that had already shipped.
    """
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        raise SystemExit(f"::error::{where} is not valid JSON: {error}.") from error
    if not isinstance(parsed, dict) or "version" not in parsed:
        raise SystemExit(
            f'::error::{where} must be an object with a `version` key holding either '
            f'"MAJOR.MINOR" or null.'
        )
    version = parsed["version"]
    if version is None:
        return None
    match = re.fullmatch(r"(\d+)\.(\d+)", str(version))
    if not match:
        raise SystemExit(
            f"::error::{where} records version {version!r}, which is not MAJOR.MINOR. It is "
            f"written by the release recipe — do not edit it by hand."
        )
    return int(match.group(1)), int(match.group(2))


def parse_fragment(text: str, where: str) -> str:
    """The level a fragment declares.

    Every failure here stops the build. A fragment decides both whether a change reaches the
    release notes and what the version becomes, so one that cannot be read has to be an
    error — counting it as absent would lose the note and lower the version at once.
    """
    match = FRAGMENT_LEVEL_RE.search(text)
    if not match:
        raise SystemExit(
            f"::error::{where} declares no `level`. A fragment opens with a frontmatter "
            f"block:\n\n---\nlevel: minor\n---\n\nfollowed by the prose. See "
            f"audit-format/TEMPLATE.md."
        )
    level = match.group(1)
    if level not in LEVEL_RANK:
        raise SystemExit(
            f"::error::{where} declares level {level!r}, which is not one of: "
            f"{', '.join(LEVELS)}. See audit-format/README.md."
        )
    if not fragment_body(text).strip():
        raise SystemExit(
            f"::error::{where} declares `level: {level}` but has no prose under it. The body "
            f"is copied verbatim into the release notes, so an empty one ships a version "
            f"change no operator can read."
        )
    return level


def fragment_body(text: str) -> str:
    """The prose under a fragment's frontmatter, as it will appear in the release notes."""
    match = FRAGMENT_LEVEL_RE.search(text)
    body = text[match.end() :] if match else text
    body = body.lstrip()
    # The closing fence, when the frontmatter is fenced at all. Stripped rather than
    # required: a fragment without fences is still readable, and refusing it would be
    # pedantry at the cost of a failed build.
    if body.startswith("---"):
        body = body[3:]
    return body.strip()


def required_version(
    baseline: tuple[int, int] | None, level: str | None
) -> tuple[int, int]:
    """The version the next release must ship: the baseline raised ONCE by `level`.

    This is the whole scheme. Raising by the HIGHEST level among the unreleased fragments
    rather than once per fragment is what puts two major changes in one cycle on 4.0 instead
    of 5.0, and what makes a major absorb every minor around it. Being a pure function of
    committed state also makes it idempotent: dropping a fragment lowers the answer again
    rather than leaving the version over-claimed.

    A null baseline is the bootstrap. Nothing has been released carrying an audit format, so
    the first release declares 1.0 rather than changing anything.
    """
    if baseline is None:
        return 1, 0
    major, minor = baseline
    if level == "major":
        return major + 1, 0
    if level == "minor":
        return major, minor + 1
    return major, minor


def highest(levels) -> str | None:
    """The highest level among `levels`, or None when there are none."""
    return max(levels, key=LEVEL_RANK.__getitem__, default=None)


def baseline_at(rev: str) -> tuple[int, int] | None:
    """The baseline committed at `rev`. A missing file is an error, not a null baseline."""
    try:
        text = _git("show", f"{rev}:{BASELINE_PATH}")
    except subprocess.CalledProcessError as error:
        raise SystemExit(
            f"::error::{BASELINE_PATH} does not exist at {rev}, but AUDIT_FORMAT is declared "
            f"there. The baseline is what the required version is computed from, so without "
            f"it nothing can say whether the declared version is right. Restore it from git "
            f"history rather than writing a new one — its value is the version the last "
            f"release actually shipped."
        ) from error
    return parse_baseline(text, f"{BASELINE_PATH} at {rev}")


def fragment_paths(paths, where: str) -> list[str]:
    """The fragment paths among `paths`, rejecting any Markdown file below the top level.

    Shared by both readers — the committed tree at a revision and the working tree — because
    they have to agree and did not: `git ls-tree -r` descends into subdirectories and
    `Path.glob("*.md")` does not. A fragment one level down therefore raised the version CI
    demanded while the recipe that writes the version could not see it, and no rerun of that
    recipe could clear the failure.

    Nesting is an error rather than a quiet skip, which is the same choice `single_version`
    makes. A skipped fragment reads as "no fragment recorded" while its
    author is looking at the file they just wrote, its prose never reaches the release notes,
    and `do_release` leaves it behind to raise some later release's version instead.
    """
    markdown = [path for path in paths if path.endswith(".md")]
    nested = sorted(path for path in markdown if "/" in path[len(FRAGMENT_DIR) :])
    if nested:
        raise SystemExit(
            f"::error::{len(nested)} fragment(s) at {where} sit below {FRAGMENT_DIR}:\n  "
            + "\n  ".join(nested)
            + f"\n::notice::A fragment lives directly in {FRAGMENT_DIR}, one file per change. "
            f"Move these up a level; the file name is free, so spend it on what changed."
        )
    return sorted(markdown)


def fragments_at(rev: str) -> dict[str, str]:
    """The unreleased fragments at `rev`, as path -> level.

    `TEMPLATE.md` lives one level ABOVE the fragment directory, so it is outside this listing
    entirely: a template that parsed as a fragment would add a permanent phantom change to
    every release.
    """
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", FRAGMENT_DIR)
    return {
        path: parse_fragment(_git("show", f"{rev}:{path}"), f"{path} at {rev}")
        for path in fragment_paths(listing.splitlines(), rev)
    }


def fixture_dirs_at(rev: str) -> list[str]:
    """The fixture directories that exist at `rev`, as path prefixes."""
    prefix = f"{AUDIT_DIR}/fixtures/"
    # No `except`: `git ls-tree` exits 0 with empty output when nothing is there, so a
    # non-zero exit is a real failure and must not be read as "no fixtures".
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    dirs = {
        path[: len(prefix) + path[len(prefix) :].index("/") + 1]
        for path in listing.splitlines()
        if path.endswith(".json") and "/" in path[len(prefix) :]
    }
    return sorted(dirs)


def fixtures_at(rev: str, prefix: str) -> dict[str, object]:
    """The committed fixtures under `prefix` at `rev`, as parsed JSON."""
    # See `fixture_dirs_at`: an `ls-tree` failure is a failure, not an empty result.
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    out = {}
    for path in listing.splitlines():
        if not path.endswith(".json"):
            continue
        name = path[len(prefix) : -len(".json")]
        out[name] = json.loads(_git("show", f"{rev}:{path}"))
    return out


# ── shape ───────────────────────────────────────────────────────────────────────


def shape(value: object, path: str = "") -> set[str]:
    """Reduce a record to `<pointer>\\t<json type>` entries.

    Values are discarded: they change for reasons that are not format changes, and
    `values_changed` handles the ones that are. Array elements collapse onto one pointer,
    so arity is not a shape change. Containers record their own type as well as their
    contents, so `{}` is distinguishable from `[]` and from an absent field.
    """
    out: set[str] = set()
    if isinstance(value, dict):
        # Recorded before the contents: without it `{}` -> `[]` reads as no change, and
        # `{}` -> scalar reads as *additive* — a breaking change classified permissively.
        out.add(f"{path}\tobject")
        for key, sub in value.items():
            out |= shape(sub, f"{path}/{key}")
    elif isinstance(value, list):
        out.add(f"{path}\tarray")
        for sub in value:
            out |= shape(sub, f"{path}[]")
    else:
        kind = "null" if value is None else type(value).__name__
        out.add(f"{path}\t{kind}")
    return out


def classify_shape(base: dict[str, set[str]], head: dict[str, set[str]]) -> str:
    """`none`, `additive`, `breaking`, or `unknown`, comparing fixtures by name.

    A fixture that was added describes a previously untested scenario; one renamed or
    deleted took its evidence with it. Hence `unknown`: "no difference found" only means
    something if everything was compared. Positive findings are still trusted — a breaking
    or additive difference in a pair that DID match is real whatever happened to the
    others — so only a `none` verdict is ever downgraded to `unknown`.
    """
    compared = sorted(set(base) & set(head))
    verdict = "none"
    for name in compared:
        if base[name] - head[name]:
            return "breaking"
        if head[name] - base[name]:
            verdict = "additive"
    # "No change" is a claim, and only that claim needs everything that existed before to
    # have been compared: a fixture that lost its name took its evidence with it, and zero
    # comparisons is not a clean bill of health.
    #
    # An `additive` finding is NOT downgraded, for the same reason `breaking` returns early
    # above: the added field was seen in a pair that matched, and a fixture renamed
    # elsewhere does not unsee it. Downgrading it was actively harmful — `unknown` is
    # permissive for every bump, so adding a field AND renaming a fixture in the same
    # change passed with no bump at all, where the addition alone required a MINOR.
    if verdict == "none" and (set(base) - set(head) or not compared):
        return "unknown"
    return verdict


# ── action names ────────────────────────────────────────────────────────────────

def content(record: object) -> object:
    """A record with the version field removed, for comparing values.

    `audit_format` is itself a fixture value, so without this every correct bump reads as
    a value change and permanently suppresses the "bumped for nothing" rules. Load-bearing.
    """
    if isinstance(record, dict):
        return {k: v for k, v in record.items() if k != "audit_format"}
    return record


def values_changed(
    base: dict[str, object], head: dict[str, object], compared: list[str]
) -> bool:
    """Whether any compared fixture changed a value without changing its shape.

    Only reached when `classify_shape` returned `none`, which now requires every fixture
    that existed before to still exist under the same name — so `compared` is all of them.
    """
    return any(content(base[name]) != content(head[name]) for name in compared)


# ── the decision ────────────────────────────────────────────────────────────────


class CheckFailed(Exception):
    """A checked condition failed. The message is already GitHub-annotated."""


# The two verdicts that hand the question to a human rather than asserting either way.
# Neither demands a fragment, and both are reported as warnings. The reasons differ and both
# are load-bearing.
DEFERRALS = {
    "values": (
        "Fixture VALUES changed but no field was added, removed or retyped. That is either a "
        "test input made more realistic (no fragment) or a wire value being renamed, which "
        "breaks every consumer that switches on it (a `major` fragment). This check compares "
        "shapes, not values, so it cannot tell those apart — decide by hand, and see the "
        "audit log section of docs/docs/developer-guide.md."
    ),
    "unknown": (
        "Could not verify the change level: fixtures present before have no counterpart now "
        "(renamed, merged or removed), so there was nothing to compare them against. Check by "
        "hand that the fragments describe what actually changed."
    ),
}


def schema_at(rev: str) -> dict | None:
    """The committed schema at `rev`, or `None` when the file does not exist there."""
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", SCHEMA_PATH).strip()
    if not listing:
        return None
    text = _git("show", f"{rev}:{SCHEMA_PATH}")
    try:
        schema = json.loads(text)
    except json.JSONDecodeError as error:
        raise CheckFailed(f"::error::{SCHEMA_PATH} at {rev} is not valid JSON: {error}") from error
    if not isinstance(schema, dict) or not isinstance(schema.get("$defs"), dict):
        raise CheckFailed(f"::error::{SCHEMA_PATH} at {rev} has no `$defs` object.")
    return schema


def file_at(rev: str, path: str) -> str | None:
    """The contents of `path` at `rev`, or `None` when it does not exist there."""
    if not _git("ls-tree", "-r", "--name-only", rev, "--", path).strip():
        return None
    return _git("show", f"{rev}:{path}")


def require_published_schema_regenerated(merge_base: str, head_ref: str) -> None:
    """Refuse a schema change whose published copy did not move with it.

    Both are written by the same command, so one changing without the other means one was
    regenerated and committed and the other was not. The stale half is the one customers
    download.

    Silent when the repository publishes no copy: not every emitter does.
    """
    base = file_at(merge_base, PUBLISHED_SCHEMA_PATH)
    head = file_at(head_ref, PUBLISHED_SCHEMA_PATH)
    if base is None or head is None or base != head:
        return
    raise CheckFailed(
        f"::error::{SCHEMA_PATH} changed and {PUBLISHED_SCHEMA_PATH} did not. The published "
        f"copy is written from it; run `just update-audit-schema` and commit both."
    )


def emitter_of(schema: dict) -> tuple[str | None, str | None]:
    """The emitter a schema document names, as `(name, format)`.

    `(None, None)` when the document carries no stamp, which is not an error: a record-shape
    summary has none, because it describes what was observed rather than what one emitter
    declares.
    """
    stamp = schema.get("x-audit-emitter")
    if not isinstance(stamp, dict):
        return (None, None)
    name, version = stamp.get("name"), stamp.get("format")
    return (
        name if isinstance(name, str) else None,
        version if isinstance(version, str) else None,
    )


def require_same_emitter(base: dict, head: dict, whence: str) -> tuple[str | None, str, str]:
    """Refuse to compare two schemas that describe different emitters.

    A verdict across emitters is meaningless: they carry different vocabularies, are governed
    by whoever ships them, and move on their own release cycles. Every definition of the one
    would read as removed and every definition of the other as added. Returns the emitter's
    name and the two formats, for printing.
    """
    base_name, base_format = emitter_of(base)
    head_name, head_format = emitter_of(head)
    if base_name and head_name and base_name != head_name:
        raise CheckFailed(
            f"::error::{whence} was given schemas of two different emitters, `{base_name}` "
            f"and `{head_name}`. Compare a schema with its own predecessor."
        )
    return (head_name or base_name, base_format or "?", head_format or "?")


def _type_of(spec: object) -> str:
    """The comparable type of a property: its JSON type, `$ref`, or the shape of its variants."""
    if not isinstance(spec, dict):
        return json.dumps(spec, sort_keys=True)
    if "$ref" in spec:
        return f"ref:{spec['$ref']}"
    if "enum" in spec:
        return "enum"
    if "const" in spec:
        return f"const:{json.dumps(spec['const'], sort_keys=True)}"
    for key in ("anyOf", "oneOf"):
        if key in spec:
            return key + "[" + ",".join(sorted(_type_of(v) for v in spec[key])) + "]"
    kind = spec.get("type", "any")
    if isinstance(kind, list):
        kind = "|".join(sorted(str(k) for k in kind))
    if kind == "array":
        return f"array[{_type_of(spec.get('items', {}))}]"
    return str(kind)


def _branches(spec: dict) -> list | None:
    """The `oneOf`/`anyOf` branches of a definition, or `None` when it has neither."""
    for key in ("oneOf", "anyOf"):
        if isinstance(spec.get(key), list):
            return spec[key]
    return None


def classify_schema(base: dict, head: dict) -> tuple[str, list[str]]:
    """`none`, `additive` or `breaking` for the change from `base` to `head`, with the reasons.

    Definitions are the unit: a definition removed, a property removed or retyped, a property
    made required, or a name removed from a set breaks a parser. A definition or an optional
    property added, or a value added to a value set, does not. Descriptions carry no shape and
    are ignored. So is every `x-audit-*` annotation but one: `x-audit-kind` says whether a list
    of names holds a field's values or an object's keys, and an addition means different things
    for the two.
    """
    reasons: list[str] = []
    kind = "none"

    def bump(level: str, reason: str) -> None:
        nonlocal kind
        reasons.append(reason)
        if LEVEL_RANK.get(REQUIRED_LEVEL.get(level, "none"), 0) > LEVEL_RANK.get(
            REQUIRED_LEVEL.get(kind, "none"), 0
        ):
            kind = level

    base_defs, head_defs = base["$defs"], head["$defs"]
    for name in sorted(set(base_defs) - set(head_defs)):
        bump("breaking", f"definition `{name}` removed")
    for name in sorted(set(head_defs) - set(base_defs)):
        bump("additive", f"definition `{name}` added")
    for name in sorted(set(base_defs) & set(head_defs)):
        b, h = base_defs[name], head_defs[name]
        if isinstance(b.get("enum"), list) and isinstance(h.get("enum"), list):
            # Two kinds of name list, and they differ in what an addition means. A value
            # vocabulary lists what one field can hold, and the format promises that set is
            # open, so a new value changes nothing for a consumer. A key vocabulary lists the
            # KEYS of an object, so its names are field names: one more is one more field,
            # which is exactly what `minor` is for.
            keys = "keys" in (h.get("x-audit-kind"), b.get("x-audit-kind"))
            noun = "key" if keys else "value"
            for gone in sorted(set(b["enum"]) - set(h["enum"])):
                bump("breaking", f"`{name}` lost the {noun} `{gone}`")
            gained = sorted(set(h["enum"]) - set(b["enum"]))
            if gained and keys:
                for added in gained:
                    bump("additive", f"`{name}` gained the key `{added}`")
            elif gained:
                reasons.append(f"`{name}` gained values (no format change)")
            continue

        # A definition whose branches are `oneOf`/`anyOf` rather than an `enum` list: what
        # schemars writes for an enum whose variants carry doc comments, and for a tagged
        # union. Without this the whole definition falls through to the property comparison
        # with no properties on either side, and a removed branch reads as no change at all.
        # Compared as a MULTISET: two object branches both render as `object`, so a set would
        # collapse them and hide the loss of one.
        b_branches, h_branches = _branches(b), _branches(h)
        if b_branches is not None and h_branches is not None:
            b_rendered = Counter(_type_of(v) for v in b_branches)
            h_rendered = Counter(_type_of(v) for v in h_branches)
            for branch in sorted((b_rendered - h_rendered).elements()):
                bump("breaking", f"`{name}` lost the variant `{branch}`")
            gained = sorted((h_rendered - b_rendered).elements())
            if gained:
                # All-constant branches are a value set, where the format promises openness;
                # anything else is a new object shape a consumer has to be ready for.
                values_only = all("const" in v for v in b_branches + h_branches if isinstance(v, dict))
                for branch in gained:
                    if values_only:
                        reasons.append(f"`{name}` gained the value `{branch}` (no format change)")
                    else:
                        bump("additive", f"`{name}` gained the variant `{branch}`")
            continue
        b_props, h_props = b.get("properties", {}) or {}, h.get("properties", {}) or {}
        b_req, h_req = set(b.get("required", []) or []), set(h.get("required", []) or [])
        for prop in sorted(set(b_props) - set(h_props)):
            bump("breaking", f"`{name}.{prop}` removed")
        for prop in sorted(set(h_props) - set(b_props)):
            bump("additive", f"`{name}.{prop}` added" + (" (required)" if prop in h_req else ""))
        for prop in sorted(set(b_props) & set(h_props)):
            if _type_of(b_props[prop]) != _type_of(h_props[prop]):
                bump(
                    "breaking",
                    f"`{name}.{prop}` retyped: {_type_of(b_props[prop])} -> {_type_of(h_props[prop])}",
                )
            if prop in h_req and prop not in b_req:
                bump("breaking", f"`{name}.{prop}` became required")
            # The mirror case, and breaking for the same reason read the other way: a
            # consumer that relied on the field always being there now meets records
            # without it.
            if prop in b_req and prop not in h_req:
                bump("breaking", f"`{name}.{prop}` became optional")
        if _type_of(b.get("additionalProperties", True)) != _type_of(
            h.get("additionalProperties", True)
        ):
            bump("breaking", f"`{name}` changed what extra keys it accepts")
    return kind, reasons


# Which family a record belongs to. A record that carries `record_type` names its own; one
# that does not is read off which fields are present, which is what lets this summarise a
# revision from before the field existed.
FAMILY_BY_RECORD_TYPE = {
    "authorization": "AuthorizationRecord",
    "replay": "ReplayRecord",
    "operation": "OperationRecord",
}


def record_family(record: dict) -> str:
    """Which record family `record` belongs to."""
    declared = record.get("record_type")
    if isinstance(declared, str):
        return FAMILY_BY_RECORD_TYPE.get(declared, declared)
    if "decision" in record:
        return "AuthorizationRecord"
    if any(key in record for key in ("action", "actions", "entity", "entities")):
        return "ReplayRecord"
    return "OperationRecord"


def json_type(value: object) -> str:
    """The JSON type name of `value`. `bool` is checked first: in Python it is an `int`."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def _property_spec(types: set[str], item_types: set[str]) -> dict:
    """One property, as the comparison reads it: its type, and an array's item type."""
    kinds = sorted(types)
    spec: dict = {"type": kinds[0] if len(kinds) == 1 else kinds}
    if "array" in types and item_types:
        items = sorted(item_types)
        spec["items"] = {"type": items[0] if len(items) == 1 else items}
    return spec


def summarise_records(records_dir: str, out_path: str) -> int:
    """Describe the TOP-LEVEL shape of committed records, as a schema document.

    The generated schema describes the objects a record is assembled from, not the record
    itself, because the record shapes are not registered types. This reads emitted records
    instead and states, per family, which top-level keys appear, with which JSON types, and
    which appear in every sample. A key that is sometimes `null` is typed as such, so losing
    the `null` reads as a retype rather than passing unnoticed.

    Sampled, not exhaustive: a key no committed record carries is not here. The output is a
    schema document, so `--compare-schemas` reads it like any other.
    """
    directory = Path(records_dir)
    files = sorted(directory.glob("*.json"))
    if not files:
        print(f"::error::no records found in {records_dir}")
        return 1

    seen: dict[str, dict] = {}
    for path in files:
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            print(f"::error::{path} is not readable JSON: {error}")
            return 1
        if not isinstance(record, dict):
            print(f"::error::{path} is not a JSON object")
            return 1
        family = seen.setdefault(
            record_family(record), {"types": {}, "items": {}, "count": 0, "always": None}
        )
        family["count"] += 1
        for key, value in record.items():
            family["types"].setdefault(key, set()).add(json_type(value))
            if isinstance(value, list):
                for item in value:
                    family["items"].setdefault(key, set()).add(json_type(item))
        keys = set(record)
        family["always"] = keys if family["always"] is None else family["always"] & keys

    defs = {}
    for name in sorted(seen):
        family = seen[name]
        defs[name] = {
            "type": "object",
            "x-audit-kind": "shape",
            "x-audit-samples": family["count"],
            "properties": {
                key: _property_spec(family["types"][key], family["items"].get(key, set()))
                for key in sorted(family["types"])
            },
            "required": sorted(family["always"] or set()),
        }

    document = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Top-level shape of the audit records committed under " + records_dir,
        "x-audit-source": records_dir,
        "$defs": defs,
    }
    Path(out_path).write_text(json.dumps(document, indent=2) + "\n")
    total = sum(family["count"] for family in seen.values())
    print(f"{out_path}: {len(defs)} record families from {total} record(s) in {records_dir}")
    return 0


def compare_schemas(base_path: str, head_path: str) -> int:
    """Classify the difference between two committed schema documents.

    The same comparison the pull request check runs across the merge base, pointed at two
    files instead. A change that spans many commits, or one whose "before" lives in a frozen
    snapshot rather than in a parent revision, is read in one go — which is what the release
    note is written from.
    """
    try:
        base = json.loads(Path(base_path).read_text())
        head = json.loads(Path(head_path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        print(f"::error::cannot read the schemas: {error}")
        return 1
    name, base_format, head_format = require_same_emitter(base, head, "--compare-schemas")
    kind, reasons = classify_schema(base, head)
    print(f"Comparing:  {base_path} -> {head_path}")
    if name:
        moved = "" if base_format == head_format else f" -> {head_format}"
        print(f"Emitter:    `{name}`, format {base_format}{moved}")
    for reason in reasons:
        print(f"  {reason}")
    if not reasons:
        print("  no difference")
    required = REQUIRED_LEVEL.get(kind, "none")
    print(f"Verdict:    {kind}; a fragment must declare at least `{required}`")
    return 0


def classify_change(merge_base: str, head_ref: str) -> str:
    """`none`, `additive`, `breaking`, `values` or `unknown` for the change between the two
    revisions, printing how it was reached.

    Fixtures pin shape by example and the schema pins every declared field and value. The
    verdict does not demand a version bump; it demands a fragment that does not understate it.
    """
    base_dirs, head_dirs = fixture_dirs_at(merge_base), fixture_dirs_at(head_ref)
    # More than one is ambiguous and none at head means the checker has nothing to work with.
    # Either is a broken setup rather than an unverifiable change, so say so loudly instead of
    # deferring — deferring is how this went unnoticed.
    if len(head_dirs) != 1:
        raise CheckFailed(
            f"::error::expected exactly one audit fixture directory under "
            f"{AUDIT_DIR}/fixtures/, found {len(head_dirs)}: {head_dirs}. This checker cannot "
            f"compare anything without one, so fix the layout rather than trusting a pass "
            f"here.\n"
            f"::notice::If you added a directory for a new major version: RENAME the one that "
            f"is there, do not add a second. The directory is named for the major version its "
            f"fixtures describe, so `just update-audit-fixtures` renames it for you when the "
            f"fragments raise the major. There is no code change: the audit tests derive the "
            f"directory from AUDIT_FORMAT. This checker compares across the rename by file "
            f"name and reports it as `vN -> vN+1`. What does not work is KEEPING the old "
            f"directory: a fixture is generated by emitting an event with the CURRENT code, so "
            f"once the code emits the new format the old one is unreproducible — it could "
            f"never be regenerated or kept passing, and would rot into a file nothing "
            f"verifies. See the audit log section of docs/docs/developer-guide.md."
        )
    head_prefix = head_dirs[0]
    base_prefix = base_dirs[0] if len(base_dirs) == 1 else head_prefix
    base_records = fixtures_at(merge_base, base_prefix)
    head_records = fixtures_at(head_ref, head_prefix)
    base_shapes = {n: shape(r) for n, r in base_records.items()}
    head_shapes = {n: shape(r) for n, r in head_records.items()}
    shape_kind = classify_shape(base_shapes, head_shapes)
    compared = sorted(set(base_shapes) & set(head_shapes))
    # Only when the shapes agree: a shape verdict is the stronger statement. `values` exists
    # to stop `none` being asserted over a change this comparison is blind to.
    if shape_kind == "none" and values_changed(base_records, head_records, compared):
        shape_kind = "values"
    short = lambda d: d.rstrip("/").rsplit("/", 1)[-1]
    # What the fixtures alone say, reported as the statistic it is. It is NOT the verdict: the
    # value check below can still override it, and a line reading `=> none` above a failure
    # for a breaking change teaches a reader to distrust the whole log.
    print(
        f"Fixtures:   {short(base_prefix)} -> {short(head_prefix)}, {len(compared)} compared "
        f"({len(base_shapes)} before, {len(head_shapes)} after), shapes say {shape_kind}"
    )

    # The fixture shapes above see the record's KEYS. Its VALUES are strings, so renaming one
    # leaves every shape identical and a value no fixture happens to carry changes nothing at
    # all. The schema is what makes those visible: it lists every value of every vocabulary
    # enum, keyed by the type that owns it.
    return merge_schema_verdict(shape_kind, merge_base, head_ref)

def merge_schema_verdict(shape_kind: str, merge_base: str, head_ref: str) -> str:
    """Fold the schema comparison into the fixture verdict: the stronger statement wins.

    The schema is a declaration of every registered type, so it sees a field on a path no
    fixture exercises. Where both exist, a `breaking` from either side is the verdict; an
    `additive` from the schema raises a `none`; the schema never lowers a fixture verdict.
    """
    base_schema, head_schema = schema_at(merge_base), schema_at(head_ref)
    if head_schema is None:
        print(f"Schema:     no {SCHEMA_PATH} at HEAD; shape is checked by fixtures alone")
        return shape_kind
    if base_schema is None:
        print(f"Schema:     {SCHEMA_PATH} introduced on this branch; nothing to compare it with")
        return shape_kind
    name, base_format, head_format = require_same_emitter(
        base_schema, head_schema, "the schema comparison"
    )
    if name:
        moved = "" if base_format == head_format else f" -> {head_format}"
        print(f"Schema:     emitter `{name}`, format {base_format}{moved}")
    schema_kind, reasons = classify_schema(base_schema, head_schema)
    if reasons:
        require_published_schema_regenerated(merge_base, head_ref)
    for reason in reasons:
        print(f"Schema:     {reason}")
    print(
        f"Schema:     {len(head_schema['$defs'])} definition(s) compared, says {schema_kind}"
    )
    rank = lambda k: LEVEL_RANK.get(REQUIRED_LEVEL.get(k, "none"), 0)
    if rank(schema_kind) > rank(shape_kind):
        return schema_kind
    return shape_kind


# ── entry points ────────────────────────────────────────────────────────────────


def show(version: tuple[int, int] | None) -> str:
    """A version for printing. `None` is a state worth naming, not a blank."""
    return "none" if version is None else f"{version[0]}.{version[1]}"


def check_release_branch(
    base_branch: str,
    shape_kind: str,
    fragments: dict[str, str],
    head_version: tuple[int, int] | None,
    baseline: tuple[int, int] | None,
) -> int:
    """Patch releases never change the audit log format.

    A patch shipping 3.1 while main heads for 4.0 gives one number two meanings: a consumer
    reading `3.1` cannot tell which line produced it, and the two lines then evolve the same
    number independently forever. Checked three ways because a change can arrive by any of
    them — a fragment, a moved constant, or neither, by editing the emitter and leaving both
    alone.
    """
    problems = []
    if shape_kind in REQUIRED_LEVEL:
        problems.append(f"the emitted records changed ({shape_kind})")
    if fragments:
        problems.append(
            f"it carries {len(fragments)} audit format fragment(s): "
            + ", ".join(sorted(fragments))
        )
    if head_version != baseline:
        problems.append(
            f"AUDIT_FORMAT is {show(head_version)} but this branch released "
            f"{show(baseline)}"
        )
    if not problems:
        print(f"Branch:     {base_branch} is a release branch; the format is frozen. OK.")
        return 0
    print(
        f"::error::This pull request targets the release branch {base_branch}, and "
        + "; and ".join(problems)
        + "."
    )
    print(
        "::notice::Patch releases never change the audit log format. Rework the change so "
        "it leaves the records alone, or hold it for the next minor release on main. See "
        ".github/RELEASING.md."
    )
    return 1


def run(base_ref: str, base_branch: str | None = None) -> int:
    merge_base = _git("merge-base", base_ref, "HEAD").strip()
    head_version = declared_version("HEAD")
    base_version = declared_version(merge_base)

    print(f"Merge base: {merge_base}")

    # A branch from before the audit format existed has nothing to check. Kept distinct from
    # a REMOVED constant, which is the dangerous direction: `audit_format` is on every record
    # and consumers route on it, so losing it breaks them with no version left to say so.
    if head_version is None:
        if base_version is None:
            print(
                "OK: AUDIT_FORMAT is declared neither here nor at the merge base, so this "
                "branch predates the audit log format version."
            )
            return 0
        print(
            f"::error::AUDIT_FORMAT is gone; it was {show(base_version)} at the merge base. "
            f"Every audit record carries it and consumers route on it, so it must be declared."
        )
        return 1

    baseline = baseline_at("HEAD")
    # Tolerant at the merge base: the branch that INTRODUCES the baseline file has none to
    # compare against, which is not a baseline being moved. `baseline_at` is strict because
    # a baseline missing at HEAD beside a declared AUDIT_FORMAT is unrecoverable.
    try:
        base_baseline = baseline_at(merge_base)
        base_has_baseline = True
    except SystemExit:
        base_baseline = None
        base_has_baseline = False
    head_fragments = fragments_at("HEAD")
    base_fragments = fragments_at(merge_base)

    # The baseline is what the required version is computed FROM, so reading it only at HEAD
    # makes both sides of that comparison move together: a branch that edits it moves the
    # version every record carries and still reports OK. The one branch allowed to move it is
    # the release, which sets it to what the fragments it consumes implied and clears them.
    if base_has_baseline and baseline != base_baseline:
        release_shape = (
            baseline == required_version(base_baseline, highest(base_fragments.values()))
            and not head_fragments
        )
        if not release_shape:
            print(
                f"::error::{BASELINE_PATH} moved from {show(base_baseline)} to "
                f"{show(baseline)} on this branch. The baseline is the version the most "
                f"recent release shipped and is what AUDIT_FORMAT is computed from, so "
                f"editing it silently redefines the version every audit record carries."
            )
            print(
                "::notice::Only the release recipe moves it — `just audit-format-release "
                "<version>`, which also clears the fragments it consumed. If this came from a "
                "merge or a rebase across a release commit, restore the baseline from the "
                "branch you are targeting."
            )
            return 1
    # What THIS branch contributes: a fragment it adds, or one whose level it raises. Adequacy
    # is judged against these rather than against every unreleased fragment, because a `major`
    # left by an earlier pull request in the same cycle would otherwise excuse this one
    # declaring `minor` — the version would still come out right, and the release notes would
    # describe this change wrongly.
    contributed = {
        path: level
        for path, level in head_fragments.items()
        if base_fragments.get(path) != level
    }
    withdrawn = {
        path: level for path, level in base_fragments.items() if path not in head_fragments
    }
    declared_level = highest(head_fragments.values())
    required = required_version(baseline, declared_level)

    print(
        f"Baseline:   {show(baseline)}"
        + ("" if baseline else " — nothing released with an audit format yet")
    )
    print(
        f"Fragments:  {len(head_fragments)} unreleased, highest {declared_level or '-'}; "
        f"this branch adds {len(contributed)}, withdraws {len(withdrawn)}"
    )
    print(f"Version:    {show(head_version)} declared, {show(required)} required")

    # The schema is generated from the constant, so the two disagree only when the file was
    # hand-edited or never regenerated. Either way a consumer would read a format the records
    # do not carry.
    committed_schema = schema_at("HEAD")
    if committed_schema is not None and head_version is not None:
        _, stamped = emitter_of(committed_schema)
        if stamped is not None and stamped != show(head_version):
            raise CheckFailed(
                f"::error::{SCHEMA_PATH} is stamped format {stamped}, but AUDIT_FORMAT is "
                f"{show(head_version)}. Run `just update-audit-schema`."
            )

    shape_kind = classify_change(merge_base, "HEAD")
    detected_level = REQUIRED_LEVEL.get(shape_kind)
    # With no baseline there is no released format, so a shape verdict describes the
    # difference between two unreleased states. Demanding a fragment for it while the
    # bootstrap guard below rejects the tree for carrying one would leave no state of the
    # tree that passes, so the demand is suppressed until the first release sets a baseline.
    bootstrap = baseline is None
    if bootstrap:
        detected_level = None
    print(
        f"Verdict:    format {shape_kind}"
        + (f", needs a fragment of at least `{detected_level}`" if detected_level else "")
        + (
            " — no baseline yet, so nothing has been released for it to differ from and no "
            "fragment is required"
            if bootstrap and shape_kind in REQUIRED_LEVEL
            else ""
        )
    )

    # Before the release-branch return: these two verdicts exist because the comparison
    # could not decide, and that is exactly what a human has to be told on the branch
    # where the format is frozen. Returning first replaced them with an unqualified OK.
    if shape_kind in DEFERRALS:
        print(f"::warning::{DEFERRALS[shape_kind]}")

    if base_branch is not None and base_branch.startswith("rel-"):
        return check_release_branch(
            base_branch, shape_kind, head_fragments, head_version, baseline
        )

    # The bootstrap. No release has carried an audit FORMAT VERSION, so the version derives
    # to the first one whatever the fragments say and none is demanded. Fragments are still
    # allowed, and usually wanted: released builds have emitted audit records for some time
    # without a version field, so a consumer may well be parsing them already, and a change
    # to what they receive is a change to describe whether or not a number moves.
    if baseline is None and head_fragments:
        print(
            f"Bootstrap:  {len(head_fragments)} fragment(s) present with no released "
            f"baseline. The version derives to {show(required)} regardless; the fragments "
            f"are the release note for consumers already parsing these records."
        )

    # A pull request that only WITHDRAWS fragments is undoing a change no release has
    # carried, so the comparison's verdict describes the removal of something consumers never
    # saw. Reported, never enforced: enforcing it would demand a `major` fragment for
    # reverting a `minor` addition made three days ago.
    reverting = withdrawn and not contributed
    if reverting and detected_level:
        print(
            f"::warning::This branch withdraws {len(withdrawn)} fragment(s) and adds none, so "
            f"the `{shape_kind}` verdict above is read as a revert of unreleased work and no "
            f"fragment is required. If that is wrong — if this removes something a release "
            f"actually shipped — add a `major` fragment."
        )
    elif detected_level:
        contributed_level = highest(contributed.values())
        if contributed_level is None or LEVEL_RANK[contributed_level] < LEVEL_RANK[detected_level]:
            print(
                f"::error::This pull request makes a `{detected_level}` change to the audit "
                f"log format, but "
                + (
                    "records no fragment describing it."
                    if contributed_level is None
                    else f"the fragment(s) it adds declare at most `{contributed_level}`."
                )
            )
            print(
                f"::notice::Copy audit-format/TEMPLATE.md to "
                f"{FRAGMENT_DIR}<descriptive-name>.md, set `level: {detected_level}`, and "
                f"describe the change in the terms an operator parsing the log thinks in. "
                f"Then run `just update-audit-fixtures`, which computes AUDIT_FORMAT from the "
                f"fragments. You are not asked to pick a version number. See the audit log "
                f"section of docs/docs/developer-guide.md."
            )
            return 1

    # Equality, not "did it move". The version is derived from committed state, so any other
    # value is wrong in a way that says something false to consumers — and equality is what
    # makes a withdrawn fragment lower the version again instead of leaving it over-claimed.
    if head_version != required:
        print(
            f"::error::AUDIT_FORMAT is {show(head_version)} but {show(required)} is required: "
            f"the baseline is {show(baseline)} and the highest unreleased fragment is "
            f"`{declared_level or 'none'}`."
        )
        print(
            "::notice::Do not edit the constant by hand — run `just update-audit-fixtures`, "
            "which computes it from audit-format/released.json and the fragments, renames the "
            "fixture directory when the major moves, and regenerates everything downstream."
        )
        return 1

    print(
        f"OK: AUDIT_FORMAT {show(head_version)} is what a baseline of {show(baseline)} and "
        f"{len(head_fragments)} unreleased fragment(s) require."
    )
    return 0


# ── the working tree ────────────────────────────────────────────────────────────
#
# The check above reads committed state at a revision; everything below reads the working
# tree, because it runs from `just` before anything is committed and the fragment the
# developer just wrote has to count. Only the READING differs — `required_version` and the
# parsers are shared, so the version the recipe writes and the version CI demands cannot
# come apart.


def worktree_baseline() -> tuple[int, int] | None:
    path = Path(BASELINE_PATH)
    if not path.is_file():
        raise SystemExit(
            f"::error::{BASELINE_PATH} does not exist. It records the audit format version "
            f"the last release shipped, and the required version is computed from it. Restore "
            f"it from git history rather than writing a new one."
        )
    return parse_baseline(path.read_text(), BASELINE_PATH)


def worktree_fragments() -> dict[str, str]:
    # `rglob`, not `glob`: the point is to SEE a nested fragment so `fragment_paths` can
    # reject it. Globbing the top level only would hide it here while `git ls-tree -r` still
    # found it in CI, which is the divergence this shares a function to prevent.
    return {
        path: parse_fragment(Path(path).read_text(), path)
        for path in fragment_paths(
            [str(path) for path in Path(FRAGMENT_DIR).rglob("*.md")], "the working tree"
        )
    }


def worktree_declaration() -> tuple[str, tuple[int, int]]:
    """The file declaring AUDIT_FORMAT in the working tree, and its value."""
    found: dict[str, tuple[int, int]] = {}
    for line in (_git_grep("-E", GIT_PATTERN, "--", VERSION_SEARCH_PATH) or "").splitlines():
        # No revision, so `git grep` prefixes `path:` only — and a path cannot contain a
        # colon in git, so partitioning from the left is exact.
        path, _, body = line.partition(":")
        match = VERSION_RE.search(body)
        if match:
            found[path] = (int(match.group(1)), int(match.group(2)))
    version = single_version(found, "the working tree")
    if version is None:
        raise SystemExit(
            "::error::no `pub const AUDIT_FORMAT: &str = \"MAJOR.MINOR\"` in the working "
            "tree. It must be declared exactly once, in tracked code under crates/."
        )
    return next(iter(found)), version


def rename_fixture_dir(old_major: int, new_major: int) -> None:
    """Move the fixture directory to the major it now describes.

    The tests derive the directory name from AUDIT_FORMAT, so leaving it behind makes every
    fixture test fail with a missing file. Done here rather than left to the developer
    because the version it has to follow is itself computed here, and keeping two derived
    things in step by hand is how they drift.
    """
    old = Path(f"{AUDIT_DIR}/fixtures/v{old_major}")
    new = Path(f"{AUDIT_DIR}/fixtures/v{new_major}")
    if not old.is_dir():
        print(f"::notice::{old} does not exist, so there is nothing to rename.")
        return
    if new.exists():
        raise SystemExit(
            f"::error::cannot rename {old} to {new}: {new} already exists. Exactly one fixture "
            f"directory may exist — a fixture records what the CURRENT code emits, so the old "
            f"format is unreproducible once the code emits the new one and a directory left "
            f"behind could never be regenerated or kept passing."
        )
    subprocess.run(["git", "mv", str(old), str(new)], check=True)
    print(f"Renamed {old} -> {new}; the directory is named for the major version.")


def write_version() -> int:
    """Compute the required version from the working tree and write it into the constant."""
    baseline = worktree_baseline()
    fragments = worktree_fragments()
    level = highest(fragments.values())
    required = required_version(baseline, level)
    path, current = worktree_declaration()
    print(
        f"Baseline {show(baseline)}, {len(fragments)} unreleased fragment(s), highest "
        f"`{level or 'none'}` -> {show(required)}"
    )
    if current == required:
        print(f"{path}: AUDIT_FORMAT is already {show(required)}. Nothing to write.")
        return 0
    source = Path(path)
    text, count = VERSION_WRITE_RE.subn(
        rf"\g<1>{required[0]}.{required[1]}\g<2>", source.read_text()
    )
    if count != 1:
        raise SystemExit(
            f"::error::expected exactly one AUDIT_FORMAT declaration to rewrite in {path}, "
            f"matched {count}."
        )
    source.write_text(text)
    print(f"{path}: AUDIT_FORMAT {show(current)} -> {show(required)}")
    if current[0] != required[0]:
        rename_fixture_dir(current[0], required[0])
    return 0


HEADINGS = {
    "major": "**Breaking changes** — an existing parser must be updated:",
    "minor": "**Additions** — an existing parser keeps working:",
    "none": "**Also worth knowing** — the format itself did not change:",
}


def release_notes() -> int:
    """Print the audit log block for the release notes. Mutates nothing.

    Every unreleased fragment appears, grouped by level, however few version numbers the
    cycle consumed. That is the point of the split: the version says how badly a consumer is
    affected, and this list says what actually happened.
    """
    baseline = worktree_baseline()
    fragments = worktree_fragments()
    if not fragments:
        print("_No audit log format changes in this release._")
        return 0
    required = required_version(baseline, highest(fragments.values()))

    print("### Audit log format")
    print()
    for level in ("major", "minor", "none"):
        bodies = [
            fragment_body(Path(path).read_text())
            for path in sorted(fragments)
            if fragments[path] == level
        ]
        if not bodies:
            continue
        print(HEADINGS[level])
        print()
        for body in bodies:
            lines = body.splitlines()
            print(f"- {lines[0]}")
            for line in lines[1:]:
                print(f"  {line}" if line.strip() else "")
        print()
    previous = "the first version" if baseline is None else f"was {show(baseline)}"
    print(f"Records now carry `audit_format` **{show(required)}** ({previous}).")
    return 0


RELEASE_NOTES_PATH = "site/docs/about/release-notes.md"


def release_notes_section(text: str, lakekeeper_version: str) -> str | None:
    """The body of this release's `## vX.Y.Z (date)` section, or None if there is not one.

    Matched on the first word after the heading marker so the date, or its absence, does not
    matter. Bounded by the next `## ` because the page is newest-first: an unbounded search
    would find last release's audit block and call this one written.
    """
    lines = text.splitlines()
    wanted = {f"v{lakekeeper_version}", lakekeeper_version}
    start = None
    for index, line in enumerate(lines):
        if line.startswith("## ") and line[3:].split()[:1] and line[3:].split()[0] in wanted:
            start = index + 1
            break
    if start is None:
        return None
    for index in range(start, len(lines)):
        if lines[index].startswith("## "):
            return "\n".join(lines[start:index])
    return "\n".join(lines[start:])


def unwritten_fragments(section: str, bodies: dict[str, str]) -> list[str]:
    """The paths in `bodies` whose prose is not in `section`.

    Compared on the first line of the body, which `release_notes` prints verbatim, so this is
    an exact substring test rather than a fuzzy match on prose. It cannot tell a paraphrase
    from an omission, but it does not have to: the block is meant to be pasted.

    Takes the bodies rather than reading them, so the rule that decides whether the only copy
    of a change description may be deleted is testable without a filesystem.
    """
    return [
        path
        for path, body in sorted(bodies.items())
        if body.splitlines()[0] not in section
    ]


def do_release(lakekeeper_version: str) -> int:
    """Move the baseline to the version this release ships, and clear the fragments."""
    baseline = worktree_baseline()
    fragments = worktree_fragments()
    _, current = worktree_declaration()
    required = required_version(baseline, highest(fragments.values()))
    # Releasing with the constant out of step would record a baseline no build ever emitted,
    # and every pull request in the next cycle would then be measured against a fiction.
    if current != required:
        raise SystemExit(
            f"::error::AUDIT_FORMAT is {show(current)} but the fragments require "
            f"{show(required)}. Run `just update-audit-fixtures` and commit the result before "
            f"releasing."
        )

    # Clearing the fragments is the only destructive step in this scheme: their prose exists
    # nowhere else, so deleting it before it reaches the release notes loses the only
    # consumer-facing description of the change, and the version alone does not say what
    # moved. Checked BEFORE anything is written, so a failure here leaves the tree untouched
    # and the command can simply be rerun.
    if fragments:
        notes = Path(RELEASE_NOTES_PATH)
        if not notes.is_file():
            raise SystemExit(
                f"::error::{RELEASE_NOTES_PATH} does not exist, so there is nowhere for "
                f"{len(fragments)} fragment(s) to have been written. See .github/RELEASING.md."
            )
        section = release_notes_section(notes.read_text(), lakekeeper_version)
        if section is None:
            raise SystemExit(
                f"::error::{RELEASE_NOTES_PATH} has no section for {lakekeeper_version}. Add "
                f"the `## v{lakekeeper_version} (date)` section first, then paste the block "
                f"from `just audit-format-release-notes` into it, and rerun this. Nothing has "
                f"been changed."
            )
        bodies = {path: fragment_body(Path(path).read_text()) for path in fragments}
        missing = unwritten_fragments(section, bodies)
        if missing:
            raise SystemExit(
                f"::error::{len(missing)} of {len(fragments)} audit format fragment(s) do not "
                f"appear in the {lakekeeper_version} section of {RELEASE_NOTES_PATH}:\n  "
                + "\n  ".join(missing)
                + f"\n::notice::Run `just audit-format-release-notes` and paste its block into "
                f"that section, then rerun this. The prose in a fragment exists nowhere else — "
                f"clearing it before it reaches the notes leaves the version as the only record "
                f"that anything changed, and a version does not say what moved. Nothing has "
                f"been changed."
            )

    Path(BASELINE_PATH).write_text(
        json.dumps({"version": show(required), "released_in": lakekeeper_version}, indent=2)
        + "\n"
    )
    for path in sorted(fragments):
        # Plain unlink rather than `git rm`, which fails outright on a fragment that is not
        # yet tracked and would leave the baseline already rewritten. Git sees the deletion
        # of a tracked file either way.
        Path(path).unlink()
    print(
        f"{BASELINE_PATH}: baseline {show(baseline)} -> {show(required)}, released in "
        f"{lakekeeper_version}."
    )
    print(
        f"Cleared {len(fragments)} fragment(s) from {FRAGMENT_DIR}, all of them present in the "
        f"{lakekeeper_version} section of {RELEASE_NOTES_PATH}."
    )
    # The standing table in the logging docs is what a consumer actually looks up: "which
    # format does the version I am running emit?". Printed rather than written, because the
    # table is prose with a history and this only knows the one new row. A release that did
    # not move the format needs no row — the previous one still describes it.
    if required != baseline:
        print()
        print("Add this row to the release table in docs/docs/logging.md:")
        print()
        print(f"| {lakekeeper_version} | `{show(required)}` |")
    return 0


def self_test() -> int:
    failures = []

    def check(label, got, want):
        if got != want:
            failures.append(f"{label}: got {got!r}, want {want!r}")

    # ── the arithmetic ──────────────────────────────────────────────────────────
    #
    # The whole scheme is `required_version`. Every check below was a stated requirement, so
    # a rewrite that gets one wrong fails here rather than at a release.
    check("no fragments leaves the baseline", required_version((3, 4), None), (3, 4))
    check("a `none` fragment leaves the baseline", required_version((3, 4), "none"), (3, 4))
    check("a minor raises the minor", required_version((3, 4), "minor"), (3, 5))
    check("a major raises the major, resetting the minor", required_version((3, 4), "major"), (4, 0))
    check("a null baseline is 1.0", required_version(None, None), (1, 0))
    check("a null baseline is 1.0 whatever the level", required_version(None, "major"), (1, 0))
    check("no levels at all", highest([]), None)

    # THE requirement. A release raises the version at most once however many changes it
    # carries, because the baseline is raised by the HIGHEST level rather than once per
    # fragment. Two major changes in one cycle ship 4.0, not 5.0.
    check(
        "two majors in one cycle is one major bump",
        required_version((3, 0), highest(["major", "major"])),
        (4, 0),
    )
    check(
        "five minors in one cycle is one minor bump",
        required_version((3, 0), highest(["minor"] * 5)),
        (3, 1),
    )
    # And a major ABSORBS the minors, wherever in the cycle they were written. One major with
    # two minors before it and three after is 4.0, not 4.3.
    for order in (
        ["minor", "minor", "major", "minor", "minor", "minor"],
        ["major", "minor", "minor", "minor", "minor", "minor"],
        ["minor", "minor", "minor", "minor", "minor", "major"],
    ):
        check(
            f"a major absorbs the minors, {order.index('major')} of them before it",
            required_version((3, 0), highest(order)),
            (4, 0),
        )
    check("a `none` never lifts a minor", required_version((3, 0), highest(["none", "minor"])), (3, 1))
    check("a `none` never lifts a major", required_version((3, 0), highest(["none", "major"])), (4, 0))

    # Idempotence is what lets the check be an equality rather than a delta: the same state
    # always gives the same answer, so withdrawing a fragment lowers the version again rather
    # than leaving it over-claimed.
    check(
        "withdrawing the major drops back to a minor",
        required_version((3, 0), highest(["minor", "minor"])),
        (3, 1),
    )
    check(
        "withdrawing every fragment drops back to the baseline",
        required_version((3, 0), highest([])),
        (3, 0),
    )

    # ── what a verdict demands ──────────────────────────────────────────────────
    #
    # `values` and `unknown` must never become a requirement: `values` fires whenever a test
    # input is made more realistic, and `unknown` whenever a fixture is renamed, so demanding
    # a fragment for either taxes changes that did nothing to the format.
    for kind in ("unknown", "values"):
        check(f"{kind} demands no fragment", REQUIRED_LEVEL.get(kind), None)
        check(f"{kind} still says something", bool(DEFERRALS.get(kind)), True)
    check("breaking demands a major fragment", REQUIRED_LEVEL["breaking"], "major")
    check("additive demands a minor fragment", REQUIRED_LEVEL["additive"], "minor")
    check("none demands no fragment", REQUIRED_LEVEL.get("none"), None)

    # ── discovering fragments ───────────────────────────────────────────────────
    #
    # One function, used by the git reader and the working-tree reader alike. They used
    # different rules — `git ls-tree -r` descends, `Path.glob("*.md")` does not — so a
    # fragment one level down raised the version CI demanded while `--write-version` could
    # not see it. Rerunning the recipe could not clear the failure.
    def paths(listing):
        try:
            return fragment_paths(listing, "test")
        except SystemExit as error:
            return f"rejected: {error}"

    TOP = FRAGMENT_DIR + "a.md"
    DEEP = FRAGMENT_DIR + "authz/b.md"
    check("a top-level fragment is found", paths([TOP]), [TOP])
    check("non-markdown is ignored", paths([TOP, FRAGMENT_DIR + ".gitkeep"]), [TOP])
    check("the result is sorted", paths([FRAGMENT_DIR + "b.md", TOP]), [TOP, FRAGMENT_DIR + "b.md"])
    # Rejected, never skipped. A skipped fragment reads as "no fragment recorded" while its
    # author is looking at the file they just wrote, and its prose never reaches the notes.
    check("a nested fragment is REJECTED", str(paths([DEEP])).startswith("rejected"), True)
    check("the offending path is named", DEEP in str(paths([DEEP])), True)
    check(
        "a nested fragment is rejected even beside a valid one",
        str(paths([TOP, DEEP])).startswith("rejected"),
        True,
    )
    check("nothing at all is not an error", paths([]), [])

    # ── clearing the fragments ──────────────────────────────────────────────────
    #
    # The one destructive step in the scheme: a fragment's prose exists nowhere else, so
    # deleting it before it reaches the release notes loses the only consumer-facing
    # description of the change. The version alone does not say what moved.
    PAGE = """# Release Notes

## v0.14.0 (2026-09-14)

### Audit log format

**Breaking changes** — an existing parser must be updated:

- `privilege` was removed from the grant context.

## v0.13.3 (2026-06-01)

- Something else entirely.
"""
    section = release_notes_section(PAGE, "0.14.0")
    check("the section is found by bare version", "privilege` was removed" in section, True)
    check("and by tagged version", release_notes_section(PAGE, "v0.14.0"), section)
    # The page is newest-first, so an unbounded search would find the PREVIOUS release's audit
    # block and call this release's fragments written.
    check("the section stops at the next release", "Something else entirely" in section, False)
    check("a release with no section", release_notes_section(PAGE, "0.15.0"), None)

    check(
        "a fragment that was pasted is not missing",
        unwritten_fragments(section, {"a.md": "`privilege` was removed from the grant context."}),
        [],
    )
    check(
        "a fragment that was not pasted IS missing",
        unwritten_fragments(section, {"b.md": "`resource_type` was renamed to `resource`."}),
        ["b.md"],
    )
    check(
        "one pasted and one not reports only the one",
        sorted(
            unwritten_fragments(
                section,
                {
                    "a.md": "`privilege` was removed from the grant context.",
                    "b.md": "`resource_type` was renamed to `resource`.",
                },
            )
        ),
        ["b.md"],
    )
    # Only the first line is compared, so a fragment whose later paragraphs were trimmed on
    # the way into the notes still counts as written.
    check(
        "a body trimmed after its first line still counts",
        unwritten_fragments(
            section, {"a.md": "`privilege` was removed from the grant context.\n\nMore detail."}
        ),
        [],
    )

    # ── fragments ───────────────────────────────────────────────────────────────
    def fragment(text):
        try:
            return parse_fragment(text, "f.md")
        except SystemExit as error:
            return f"rejected: {error}"

    check("a fenced fragment", fragment("---\nlevel: major\n---\n\nA field moved.\n"), "major")
    check("an unfenced fragment", fragment("level: minor\n\nA field appeared.\n"), "minor")
    check("level none", fragment("---\nlevel: none\n---\n\nA new action.\n"), "none")
    check("no level at all", str(fragment("Just some prose.\n")).startswith("rejected"), True)
    # Every one of these is a plausible thing to write, and every one must be REJECTED rather
    # than read as "no fragment": a fragment that silently fails to parse loses the release
    # note and lowers the version in the same move.
    for bad in ("level: breaking\n\nprose\n", "level: patch\n\nprose\n", "level: MAJOR\n\nprose\n"):
        check(
            f"{bad.splitlines()[0]!r} is not a level",
            str(fragment(bad)).startswith("rejected"),
            True,
        )
    check(
        "an empty body is rejected",
        str(fragment("---\nlevel: major\n---\n\n   \n")).startswith("rejected"),
        True,
    )
    check(
        "the body is the prose, without the fence",
        fragment_body("---\nlevel: major\n---\n\n`a` moved to `b`.\n"),
        "`a` moved to `b`.",
    )
    check(
        "a multi-paragraph body survives",
        fragment_body("---\nlevel: major\n---\n\nOne.\n\nTwo.\n"),
        "One.\n\nTwo.",
    )

    # ── the baseline ────────────────────────────────────────────────────────────
    def baseline(text):
        try:
            return parse_baseline(text, "released.json")
        except SystemExit as error:
            return f"rejected: {error}"

    check("a released baseline", baseline('{"version": "3.4"}'), (3, 4))
    # `null` means no release has carried an audit format yet, and must stay distinct from a
    # MISSING file, which means someone deleted the baseline. Reading the second as the first
    # renumbers a format that already shipped, back to 1.0.
    check("a null baseline", baseline('{"version": null}'), None)
    check("no version key", str(baseline("{}")).startswith("rejected"), True)
    check("not an object", str(baseline('"3.4"')).startswith("rejected"), True)
    check("a version that is not MAJOR.MINOR", str(baseline('{"version": "3"}')).startswith("rejected"), True)
    check("not json at all", str(baseline("{")).startswith("rejected"), True)


    # `audit_format` is excluded from the value comparison, so a correctly regenerated
    # bump still reads as `none` and the "bumped for nothing" rules keep working.
    check(
        "version bump alone is not a value change",
        values_changed(
            {"f": {"audit_format": "1.0", "a": 1}},
            {"f": {"audit_format": "1.1", "a": 1}},
            ["f"],
        ),
        False,
    )
    check(
        "a renamed wire value is a value change",
        values_changed(
            {"f": {"audit_format": "1.0", "entity_type": "namespace"}},
            {"f": {"audit_format": "1.0", "entity_type": "schema"}},
            ["f"],
        ),
        True,
    )
    check(
        "nested value changes count",
        values_changed({"f": {"a": {"b": 1}}}, {"f": {"a": {"b": 2}}}, ["f"]),
        True,
    )
    check(
        "identical records are not a value change",
        values_changed({"f": {"a": 1}}, {"f": {"a": 1}}, ["f"]),
        False,
    )
    check(
        "an ADDED fixture alone does not",
        values_changed({"f": {"a": 1}}, {"f": {"a": 1}, "g": {"a": 1}}, ["f"]),
        False,
    )

    # Exactly one declaration is required, and agreeing values do not excuse a second one.
    def ambiguity(found):
        try:
            return single_version(found)
        except AmbiguousVersion as error:
            return f"ambiguous: {error}"

    check("no declaration", ambiguity({}), None)
    check("one declaration", ambiguity({"a.rs": (1, 4)}), (1, 4))
    differing = ambiguity({"a.rs": (1, 4), "b.rs": (2, 0)})
    agreeing = ambiguity({"a.rs": (1, 4), "b.rs": (1, 4)})
    check("two declarations, differing", str(differing).startswith("ambiguous"), True)
    check("two declarations, agreeing", str(agreeing).startswith("ambiguous"), True)
    check("both paths named", "a.rs" in str(agreeing) and "b.rs" in str(agreeing), True)

    a = {"x": {"a": 1}}

    # `unknown`: a fixture that existed before has no counterpart now, so "no difference
    # found" is not evidence of "no difference". These must not be reported as `none`.
    check("pure rename", classify_shape({"old": shape(a)}, {"new": shape(a)}), "unknown")
    check(
        "rename plus a change",
        classify_shape({"old": shape({"a": 1})}, {"new": shape({"a": 1, "b": 2})}),
        "unknown",
    )
    check(
        "rename plus a removal",
        classify_shape({"old": shape({"a": 1, "b": 2})}, {"new": shape({"a": 1})}),
        "unknown",
    )
    check(
        "two merged into one shape",
        classify_shape({"f": shape(a), "g": shape(a)}, {"fg": shape(a)}),
        "unknown",
    )
    check(
        "a fixture removed",
        classify_shape({"f": shape(a), "g": shape(a)}, {"f": shape(a)}),
        "unknown",
    )
    check("nothing either side", classify_shape({}, {}), "unknown")
    # Positive findings survive an incomplete comparison: a real difference in a pair that
    # did match is real whatever happened to the rest.
    check(
        "breaking despite a rename",
        classify_shape(
            {"f": shape({"a": 1, "b": 2}), "g": shape(a)},
            {"f": shape({"a": 1}), "h": shape(a)},
        ),
        "breaking",
    )
    # This asserted `unknown` — the downgrade — and that was wrong, not conservative.
    # `unknown` is permissive for every bump, so a change that added a field AND renamed a
    # fixture passed with no bump at all, while the addition on its own demanded a MINOR.
    # The addition was seen in a pair that matched; a rename elsewhere does not unsee it.
    check(
        "additive alongside a rename is still additive",
        classify_shape(
            {"f": shape({"a": 1}), "g": shape(a)},
            {"f": shape({"a": 1, "b": 2}), "h": shape(a)},
        ),
        "additive",
    )
    check(
        "additive alongside a rename and a deletion is still additive",
        classify_shape(
            {"f": shape({"a": 1}), "g": shape(a), "gone": shape(a)},
            {"f": shape({"a": 1, "b": 2}), "h": shape(a)},
        ),
        "additive",
    )
    # Shape classification, including the cases that must NOT count as format changes.
    check("identical", classify_shape({"f": shape(a)}, {"f": shape(a)}), "none")
    check(
        "value changed",
        classify_shape({"f": shape({"x": {"a": 1}})}, {"f": shape({"x": {"a": 2}})}),
        "none",
    )
    check(
        "bool flipped",
        classify_shape({"f": shape({"ok": True})}, {"f": shape({"ok": False})}),
        "none",
    )
    check(
        "arity grew",
        classify_shape(
            {"f": shape({"xs": [{"a": 1}]})}, {"f": shape({"xs": [{"a": 1}, {"a": 2}]})}
        ),
        "none",
    )
    check(
        "field added",
        classify_shape({"f": shape({"a": 1})}, {"f": shape({"a": 1, "b": 2})}),
        "additive",
    )
    check(
        "field removed",
        classify_shape({"f": shape({"a": 1, "b": 2})}, {"f": shape({"a": 1})}),
        "breaking",
    )
    check(
        "field renamed",
        classify_shape({"f": shape({"a-b": 1})}, {"f": shape({"a_b": 1})}),
        "breaking",
    )
    check(
        "type changed",
        classify_shape({"f": shape({"a": 1})}, {"f": shape({"a": "1"})}),
        "breaking",
    )
    check(
        "null vs string",
        classify_shape({"f": shape({"a": None})}, {"f": shape({"a": "x"})}),
        "breaking",
    )
    check(
        "nested added",
        classify_shape(
            {"f": shape({"a": {"b": 1}})}, {"f": shape({"a": {"b": 1, "c": 2}})}
        ),
        "additive",
    )
    # Empty and absent containers. Every one of these was misclassified before container
    # types were recorded, and the last was the worst: a breaking type change reported as
    # additive, which a minor bump would then have satisfied.
    check(
        "empty object -> empty array",
        classify_shape({"f": shape({"c": {}})}, {"f": shape({"c": []})}),
        "breaking",
    )
    check(
        "empty array -> empty object",
        classify_shape({"f": shape({"c": []})}, {"f": shape({"c": {}})}),
        "breaking",
    )
    check(
        "absent -> empty object",
        classify_shape({"f": shape({})}, {"f": shape({"c": {}})}),
        "additive",
    )
    check(
        "absent -> empty array",
        classify_shape({"f": shape({})}, {"f": shape({"c": []})}),
        "additive",
    )
    check(
        "empty object -> absent",
        classify_shape({"f": shape({"c": {}})}, {"f": shape({})}),
        "breaking",
    )
    check(
        "empty object -> scalar",
        classify_shape({"f": shape({"c": {}})}, {"f": shape({"c": "x"})}),
        "breaking",
    )
    check(
        "empty object -> populated",
        classify_shape({"f": shape({"c": {}})}, {"f": shape({"c": {"a": 1}})}),
        "additive",
    )
    check(
        "populated -> empty object",
        classify_shape({"f": shape({"c": {"a": 1}})}, {"f": shape({"c": {}})}),
        "breaking",
    )
    check(
        "object -> array of same",
        classify_shape({"f": shape({"c": {"a": 1}})}, {"f": shape({"c": [{"a": 1}]})}),
        "breaking",
    )

    check(
        "fixture added",
        classify_shape({"f": shape(a)}, {"f": shape(a), "g": shape(a)}),
        "none",
    )

    # ── the schema comparison ──
    def schema(defs: dict) -> dict:
        return {"$schema": "x", "$defs": defs}

    actor = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}}, "required": ["actor_type"]}
    actor_email = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}, "email": {"type": "string"}}, "required": ["actor_type"]}
    actor_req = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}}, "required": ["actor_type", "principal"]}
    actor_retyped = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "integer"}}, "required": ["actor_type"]}
    decision = {"type": "string", "enum": ["allowed", "denied"]}
    decision_more = {"type": "string", "enum": ["allowed", "denied", "deferred"]}
    decision_less = {"type": "string", "enum": ["allowed"]}
    check("schema: identical is none", classify_schema(schema({"A": actor}), schema({"A": actor}))[0], "none")
    check("schema: optional property added is additive", classify_schema(schema({"A": actor}), schema({"A": actor_email}))[0], "additive")
    check("schema: property removed is breaking", classify_schema(schema({"A": actor_email}), schema({"A": actor}))[0], "breaking")
    check("schema: property made required is breaking", classify_schema(schema({"A": actor}), schema({"A": actor_req}))[0], "breaking")
    # The mirror of the case above, and breaking for the same reason read the other way: a
    # consumer that relied on the field always being there now meets records without it.
    check("schema: property made optional is breaking", classify_schema(schema({"A": actor_req}), schema({"A": actor}))[0], "breaking")
    check(
        "schema: property made optional is named",
        any("became optional" in r for r in classify_schema(schema({"A": actor_req}), schema({"A": actor}))[1]),
        True,
    )
    check("schema: property retyped is breaking", classify_schema(schema({"A": actor}), schema({"A": actor_retyped}))[0], "breaking")
    check("schema: definition added is additive", classify_schema(schema({"A": actor}), schema({"A": actor, "B": actor}))[0], "additive")
    check("schema: definition removed is breaking", classify_schema(schema({"A": actor, "B": actor}), schema({"A": actor}))[0], "breaking")
    check("schema: enum value added is none", classify_schema(schema({"D": decision}), schema({"D": decision_more}))[0], "none")
    check("schema: enum value removed is breaking", classify_schema(schema({"D": decision}), schema({"D": decision_less}))[0], "breaking")

    # A key vocabulary. Its names are the keys of an object, so the same edit that is no
    # change on a value vocabulary adds a field here. Reading the two alike reported a new
    # audit field as no format change at all.
    keys = {"type": "string", "enum": ["queue_name", "self_read"], "x-audit-kind": "keys", "x-audit-keys-of": "context"}
    keys_more = {**keys, "enum": keys["enum"] + ["entity_id"]}
    keys_less = {**keys, "enum": ["queue_name"]}
    check("schema: a new key is a new field", classify_schema(schema({"K": keys}), schema({"K": keys_more}))[0], "additive")
    check(
        "schema: the new key is named",
        any("gained the key `entity_id`" in r for r in classify_schema(schema({"K": keys}), schema({"K": keys_more}))[1]),
        True,
    )
    check("schema: a key removed is breaking", classify_schema(schema({"K": keys}), schema({"K": keys_less}))[0], "breaking")
    check(
        "schema: the lost key is named as a key",
        any("lost the key `self_read`" in r for r in classify_schema(schema({"K": keys}), schema({"K": keys_less}))[1]),
        True,
    )

    # A value set written as `oneOf` of constants, which is what schemars produces for an
    # enum whose variants carry doc comments. Without the branch comparison the whole
    # definition falls through with no properties on either side and a removal reads as
    # no change.
    effect = {"oneOf": [{"type": "string", "const": "permit"}, {"type": "string", "const": "forbid"}]}
    effect_less = {"oneOf": [{"type": "string", "const": "permit"}]}
    effect_more = {"oneOf": effect["oneOf"] + [{"type": "string", "const": "defer"}]}
    check("schema: a constant lost from a oneOf is breaking", classify_schema(schema({"E": effect}), schema({"E": effect_less}))[0], "breaking")
    check("schema: the lost constant is named",
          any('const:"forbid"' in r for r in classify_schema(schema({"E": effect}), schema({"E": effect_less}))[1]), True)
    check("schema: a constant added to a oneOf is not a format change", classify_schema(schema({"E": effect}), schema({"E": effect_more}))[0], "none")

    # Two object branches render alike, so the comparison must be a multiset: a set would
    # collapse them and hide the loss of one.
    tagged = {"oneOf": [{"type": "object", "properties": {"a": {"type": "string"}}}, {"type": "object", "properties": {"b": {"type": "string"}}}]}
    tagged_less = {"oneOf": [tagged["oneOf"][0]]}
    check("schema: one of two object variants removed is breaking", classify_schema(schema({"T": tagged}), schema({"T": tagged_less}))[0], "breaking")
    check("schema: an object variant added is additive", classify_schema(schema({"T": tagged_less}), schema({"T": tagged}))[0], "additive")
    check("schema: description change is none", classify_schema(schema({"A": actor}), schema({"A": {**actor, "description": "x"}}))[0], "none")

    # THE regression. `get_metadata` is emitted by six action enums, so renaming ONE of them is
    # invisible to any comparison over the flattened union of every name: the union still holds
    # `get_metadata` from the other five. This is the real case that got through —
    # `CatalogTableAction::GetMetadata` renamed to `FetchMetadata`, the wire value for every
    # table event changed, CI green. Definitions are keyed by the type that owns the values,
    # which is what makes it visible. A comparison that flattens them cannot see it.
    masking_base = schema({
        "CatalogTableAction": {"type": "string", "enum": ["get_metadata", "drop"]},
        "CatalogViewAction": {"type": "string", "enum": ["get_metadata"]},
    })
    masking_head = schema({
        "CatalogTableAction": {"type": "string", "enum": ["fetch_metadata", "drop"]},
        "CatalogViewAction": {"type": "string", "enum": ["get_metadata"]},
    })
    masking_kind, masking_reasons = classify_schema(masking_base, masking_head)
    check("schema: a rename is breaking while another owner still emits the name", masking_kind, "breaking")

    # ── the emitter stamp ──
    stamped = {"x-audit-emitter": {"name": "lakekeeper", "format": "1.0"}, "$defs": {}}
    other = {"x-audit-emitter": {"name": "lakekeeper-plus", "format": "1.0"}, "$defs": {}}
    unstamped = {"$defs": {}}
    check("stamp: read", emitter_of(stamped), ("lakekeeper", "1.0"))
    check("stamp: absent is not an error", emitter_of(unstamped), (None, None))
    check("stamp: a malformed stamp reads as absent",
          emitter_of({"x-audit-emitter": "lakekeeper"}), (None, None))
    check("stamp: the same emitter compares",
          require_same_emitter(stamped, stamped, "t"), ("lakekeeper", "1.0", "1.0"))
    # An unstamped document is a record summary; it claims no emitter, so it blocks nothing.
    check("stamp: one side unstamped still compares",
          require_same_emitter(unstamped, stamped, "t"), ("lakekeeper", "?", "1.0"))
    refused = False
    try:
        require_same_emitter(stamped, other, "t")
    except CheckFailed:
        refused = True
    check("stamp: two emitters are refused", refused, True)
    # The check that the committed schema agrees with the constant it was generated from.
    # The three version patterns are built from one name, so a repository that names its
    # constant differently changes all three at once and they cannot disagree.
    git_pattern, read_re, write_re = version_patterns("PLUS_AUDIT_FORMAT")
    line = '    pub const PLUS_AUDIT_FORMAT: &str = "2.1";'
    check("const: the reader finds a renamed constant",
          read_re.search(line).groups() if read_re.search(line) else None, ("2", "1"))
    check("const: the writer rewrites only the value",
          write_re.sub(r"\g<1>3.0\g<2>", line),
          '    pub const PLUS_AUDIT_FORMAT: &str = "3.0";')
    check("const: the grep pattern names it", "PLUS_AUDIT_FORMAT" in git_pattern, True)
    check("const: the reader ignores another constant",
          version_patterns("AUDIT_FORMAT")[1].search(line), None)

    check("stamp: a format the constant does not declare is caught",
          emitter_of({"x-audit-emitter": {"name": "lakekeeper", "format": "2.0"}})[1] != show((1, 0)),
          True)

    # ── record families and the shape summary ──
    check("family: decision names an authorization record",
          record_family({"decision": "allowed", "action": {}}), "AuthorizationRecord")
    check("family: action without decision is a replay",
          record_family({"action": {}, "operation": "idempotent_replay"}), "ReplayRecord")
    check("family: neither is an operation record",
          record_family({"operation": "grant_created", "outcome": "success"}), "OperationRecord")
    check("family: record_type wins once the shapes carry one",
          record_family({"record_type": "operation", "decision": "allowed"}), "OperationRecord")
    check("family: an unknown record_type is kept verbatim",
          record_family({"record_type": "something_new"}), "something_new")

    # `bool` before `int`: in Python `True` is an `int`, and a boolean typed as an integer
    # would make a real type change invisible.
    check("json type: boolean is not integer", json_type(True), "boolean")
    check("json type: null", json_type(None), "null")
    check("json type: object", json_type({"a": 1}), "object")

    check("property: one observed type", _property_spec({"string"}, set()), {"type": "string"})
    check("property: a sometimes-null field keeps both",
          _property_spec({"string", "null"}, set()), {"type": ["null", "string"]})
    check("property: an array records its item type",
          _property_spec({"array"}, {"object"}), {"type": "array", "items": {"type": "object"}})
    # The whole point of typing a null: losing it must read as a change.
    check("a field that stops being null is a retype",
          _type_of(_property_spec({"string", "null"}, set())) != _type_of(_property_spec({"string"}, set())),
          True)
    check(
        "schema: the owner that lost the value is named",
        any("CatalogTableAction" in reason and "get_metadata" in reason for reason in masking_reasons),
        True,
    )
    # The same masking applies across fields, which is why the owning type is the key: `read`
    # can be an action name and another field's value at once.
    check(
        "schema: a rename is breaking while another field carries that value",
        classify_schema(
            schema({"A": {"type": "string", "enum": ["read"]}, "D": {"type": "string", "enum": ["read"]}}),
            schema({"A": {"type": "string", "enum": ["fetch"]}, "D": {"type": "string", "enum": ["read"]}}),
        )[0],
        "breaking",
    )

    for line in failures:
        print(f"FAIL {line}")
    print(
        f"\n{'FAILED' if failures else 'all self-tests passed'} "
        f"({len(failures)} failure(s))"
    )
    return 1 if failures else 0


def print_version(rev: str) -> int:
    """Print `MAJOR.MINOR` at `rev`, or nothing if undeclared.

    Exists so that anything else needing the version — the CI shell, for one — calls this
    rather than keeping its own copy of the grep. Two implementations of the same lookup
    is one of them being wrong later.
    """
    version = declared_version(rev)
    if version is not None:
        print(f"{version[0]}.{version[1]}")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "base_ref",
        nargs="?",
        help="the base revision of the pull request; the check runs against the merge base",
    )
    parser.add_argument(
        "--base-branch",
        help="the branch the pull request targets. A `rel-*` branch freezes the format.",
    )
    parser.add_argument(
        "--summarise-records",
        nargs=2,
        metavar=("RECORDS_DIR", "OUT"),
        help="write the top-level shape of the records in RECORDS_DIR as a schema document",
    )
    parser.add_argument(
        "--compare-schemas",
        nargs=2,
        metavar=("BASE", "HEAD"),
        help="classify the difference between two schema documents and list every change",
    )
    parser.add_argument("--self-test", action="store_true", help="run the built-in tests")
    parser.add_argument("--print-version", metavar="REV", help="print AUDIT_FORMAT at REV")
    parser.add_argument(
        "--write-version",
        action="store_true",
        help="compute the required version from the working tree and write it in",
    )
    parser.add_argument(
        "--release-notes",
        action="store_true",
        help="print the audit log block for the release notes; mutates nothing",
    )
    parser.add_argument(
        "--release",
        metavar="LAKEKEEPER_VERSION",
        help="move the baseline to the version this release ships and clear the fragments",
    )
    args = parser.parse_args(argv)

    load_config()
    if args.summarise_records is not None:
        return summarise_records(*args.summarise_records)
    if args.compare_schemas is not None:
        return compare_schemas(*args.compare_schemas)
    if args.self_test:
        return self_test()
    if args.print_version is not None:
        return print_version(args.print_version)
    if args.write_version:
        return write_version()
    if args.release_notes:
        return release_notes()
    if args.release is not None:
        return do_release(args.release)
    if args.base_ref is None:
        parser.error("a base revision is required unless one of the other modes is given")
    return run(args.base_ref, args.base_branch)


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except CheckFailed as error:
        # A rejected change, reported on stdout like every other `::error::` line the run
        # prints, so the log reads in order.
        print(error)
        print("\nSee the audit log section of docs/docs/developer-guide.md.")
        sys.exit(1)
    except AmbiguousVersion as error:
        # A broken declaration, not a rejected change — exit 2 so the two are distinguishable.
        print(f"::error::{error}", file=sys.stderr)
        sys.exit(2)
    except subprocess.CalledProcessError as error:
        # A bad revision or a shallow clone is a usage problem, not a format problem, and a
        # raw traceback in a CI log obscures which of the two happened.
        command = " ".join(error.cmd)
        print(
            f"::error::`{command}` failed. If it names a revision, check that it exists and "
            f"that the checkout has enough history (the workflow uses fetch-depth: 0).",
            file=sys.stderr,
        )
        print((error.stderr or "").strip(), file=sys.stderr)
        sys.exit(2)
