#!/usr/bin/env python3
"""The audit log format version: compute it, write it, and check it is right.

`AUDIT_FORMAT` is not edited by hand and does not move once per pull request. It is derived
from committed state:

    AUDIT_FORMAT = the version the last release tag declares, raised once by the highest
                   level among the fragments in audit-format/unreleased/ that tag lacks

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
After a release: python3 .github/scripts/check-audit-format.py --release <version>
Self-test:       python3 .github/scripts/check-audit-format.py --self-test
Read version:    python3 .github/scripts/check-audit-format.py --print-version <rev>
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
import re
import hashlib
import subprocess
import sys
from pathlib import Path

AUDIT_DIR = "crates/lakekeeper/src/service/events/backends/audit"

# The published schema of the emitter this repository owns: what the registry generates,
# written by `just update-audit-schema` to the file customers download. Diffed across the merge
# base like the fixtures.
SCHEMA_PATH = "docs/docs/audit/schema.json"

# Optional per-repository overrides of the paths above, so a crate outside this repository
# runs the same checker against its own schema and declaration. Read once, from the working
# tree, before anything else.
CONFIG_PATH = "audit-format/config.json"

# The Rust constant every record's version is read from, and the tree searched for its
# declaration. Both are configurable because another repository names and places its own.
VERSION_CONST = "AUDIT_FORMAT"
VERSION_SEARCH_PATH = "crates/"


def load_config() -> None:
    """Override the path constants from `CONFIG_PATH`, if the file exists.

    Everything this checker needs to find is named here, so the script runs unchanged in a
    repository laid out differently: the declaration it reads the version from, the tree it
    searches for that declaration, the schema, the fragments, the pattern a release tag
    matches and the release notes file. The defaults are Lakekeeper's, so this repository needs no config file.
    """
    global AUDIT_DIR, SCHEMA_PATH, FRAGMENT_DIR, RELEASE_TAG_PATTERN, RELEASE_NOTES_PATH
    global RELEASE_TABLE_PATH
    global VERSION_CONST, VERSION_SEARCH_PATH
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
    RELEASE_TAG_PATTERN = config.get("release_tag_pattern", RELEASE_TAG_PATTERN)
    RELEASE_NOTES_PATH = config.get("release_notes", RELEASE_NOTES_PATH)
    RELEASE_TABLE_PATH = config.get("release_table", RELEASE_TABLE_PATH)
    FRAGMENT_DIR = config.get("fragments", FRAGMENT_DIR)
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

FRAGMENT_DIR = "audit-format/unreleased/"

# A release tag: the baseline is the version the last one reachable from a revision declares.
# A prerelease such as `v1.2.0-rc.1` does not match, so it never becomes the baseline.
RELEASE_TAG_PATTERN = r"v(\d+)\.(\d+)\.(\d+)"

# Where the release notes live, for the check that a fragment reached them before it goes.
RELEASE_NOTES_PATH = "site/docs/about/release-notes.md"

# The page whose table says which format each release emits, or `None` for a repository
# without one.
RELEASE_TABLE_PATH: str | None = "docs/docs/logging.md"

# `none` is a level, not the absence of one: a new action value changes nothing about the
# format but is still worth a line in the release notes. Ranked so that `max` over a set of
# fragments is exactly the arithmetic the version needs.
LEVELS = ("none", "minor", "major")
LEVEL_RANK = {level: rank for rank, level in enumerate(LEVELS)}

# What a fixture or schema verdict says a fragment must AT LEAST declare. `unknown` is
# deliberately absent: it hands the question to a human (see `DEFERRALS`), so it cannot
# demand a fragment without taxing every change that merely renames a fixture.
REQUIRED_LEVEL = {"breaking": "major", "additive": "minor"}

FRAGMENT_LEVEL_RE = re.compile(r"^level:[ \t]*(\S+)[ \t]*$", re.MULTILINE)


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


def release_tags(rev: str) -> list[str]:
    """The release tags reachable from `rev`, oldest first by version."""
    pattern = re.compile(RELEASE_TAG_PATTERN)
    tagged = []
    for tag in _git("tag", "--merged", rev).split():
        match = pattern.fullmatch(tag)
        if match:
            tagged.append((tuple(int(group) for group in match.groups()), tag))
    return [tag for _, tag in sorted(tagged)]


def last_release_tag(rev: str) -> str:
    """The highest release tag reachable from `rev`.

    No tag at all is an error: a checkout without tags would otherwise read as a repository
    that never released, and the bootstrap rules would wave through any change.
    """
    tags = release_tags(rev)
    if not tags:
        raise SystemExit(
            f"::error::no tag matching `{RELEASE_TAG_PATTERN}` is reachable from {rev}. The "
            f"last release is the baseline, so this cannot check anything without one. Fetch "
            f"the tags (`git fetch --tags`, or `fetch-tags: true` in CI) and rerun."
        )
    return tags[-1]


def split_fragments(
    levels: dict[str, str], digests: dict[str, str], at_tag: dict[str, str], tag: str
) -> tuple[dict[str, str], dict[str, str]]:
    """The fragments as `(unreleased, released)`, each path -> level.

    A fragment absent at the last release tag is unreleased: it raises the version. One that
    the tag already carries, unchanged, shipped with that release and only waits to be
    cleared. One the tag carries with different text describes a release that already
    happened, so editing it is an error: write a new fragment instead.
    """
    edited = sorted(path for path in levels if path in at_tag and at_tag[path] != digests[path])
    if edited:
        raise SystemExit(
            f"::error::{len(edited)} fragment(s) shipped with {tag} and were edited since:\n  "
            + "\n  ".join(edited)
            + "\n::notice::A released fragment describes that release and is cleared by "
            "`just audit-format-release`. Restore it, and describe a new change in a new fragment."
        )
    unreleased = {path: level for path, level in levels.items() if path not in at_tag}
    released = {path: level for path, level in levels.items() if path in at_tag}
    return unreleased, released


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
    """The fragments at `rev`, as path -> level.

    `TEMPLATE.md` lives one level ABOVE the fragment directory, so it is outside this listing
    entirely: a template that parsed as a fragment would add a permanent phantom change to
    every release.
    """
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", FRAGMENT_DIR)
    return {
        path: parse_fragment(_git("show", f"{rev}:{path}"), f"{path} at {rev}")
        for path in fragment_paths(listing.splitlines(), rev)
    }


def fragment_bodies_at(rev: str) -> dict[str, str]:
    """The fragments at `rev`, as path -> a digest of the file.

    Separate from the levels because a branch contributes by REWORDING a fragment as often as
    by adding one: folding a second change into the fragment that already covers the field is
    the documented way to keep the release note describing the final state, and it usually
    leaves the level alone. Comparing only levels would read that as no contribution.
    """
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", FRAGMENT_DIR)
    return {
        path: hashlib.sha256(_git("show", f"{rev}:{path}").encode()).hexdigest()
        for path in fragment_paths(listing.splitlines(), rev)
    }


def fragment_demand(
    base: dict[str, str],
    head: dict[str, str],
    base_bodies: dict[str, str],
    head_bodies: dict[str, str],
) -> dict[str, str]:
    """What this branch contributes to the release notes.

    A contribution is a fragment added, one whose level moved, or one whose text changed.
    Adequacy is judged against these rather than against every unreleased fragment, because a
    `major` left by an earlier pull request in the same cycle would otherwise excuse this one
    declaring `minor` — the version would still come out right and the release notes would
    describe this change wrongly.
    """
    contributed = {
        path: level
        for path, level in head.items()
        if base.get(path) != level or base_bodies.get(path) != head_bodies.get(path)
    }
    return contributed


def fixtures_at(rev: str) -> dict[str, object]:
    """The committed fixtures at `rev`, as file name -> parsed JSON.

    Keyed by the file name alone, so a fixture compares with its older self wherever under
    the fixture directory either revision kept it.
    """
    prefix = f"{AUDIT_DIR}/fixtures/"
    # No `except`: `git ls-tree` exits 0 with empty output when nothing is there, so a
    # non-zero exit is a real failure and must not be read as "no fixtures".
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    out = {}
    for path in listing.splitlines():
        if path.endswith(".json"):
            out[path.rsplit("/", 1)[-1][: -len(".json")]] = json.loads(_git("show", f"{rev}:{path}"))
    return out


# ── shape ───────────────────────────────────────────────────────────────────────


def shape(value: object, path: str = "") -> set[str]:
    """Reduce a record to `<pointer>\\t<json type>` entries.

    Values are discarded: they change for reasons that are not format changes, and the
    schema comparison sees the ones that are. Array elements collapse onto one pointer,
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


# ── the decision ────────────────────────────────────────────────────────────────


class CheckFailed(Exception):
    """A checked condition failed. The message is already GitHub-annotated."""


# The verdict that hands the question to a human rather than asserting either way. It
# demands no fragment and is reported as a warning.
DEFERRALS = {
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


def emitter_of(schema: dict) -> tuple[str | None, str | None]:
    """The emitter a schema document names, as `(name, format)`; `(None, None)` when it
    carries no stamp."""
    stamp = schema.get("x-audit-emitter")
    if not isinstance(stamp, dict):
        return (None, None)
    name, version = stamp.get("name"), stamp.get("format")
    return (
        name if isinstance(name, str) else None,
        version if isinstance(version, str) else None,
    )


def require_same_emitter(base: dict, head: dict, whence: str) -> tuple[str, str, str]:
    """Refuse to compare two schemas that describe different emitters.

    A verdict across emitters is meaningless: they carry different vocabularies, are governed
    by whoever ships them, and move on their own release cycles. Every definition of the one
    would read as removed and every definition of the other as added. Returns the emitter's
    name and the two formats, for printing.
    """
    base_name, base_format = emitter_of(base)
    head_name, head_format = emitter_of(head)
    if base_name is None or head_name is None:
        raise CheckFailed(
            f"::error::{whence} was given a schema that names no emitter. Every generated schema "
            f"carries `x-audit-emitter`; regenerate it with `just update-audit-schema`."
        )
    if base_name != head_name:
        raise CheckFailed(
            f"::error::{whence} was given schemas of two different emitters, `{base_name}` "
            f"and `{head_name}`. Compare a schema with its own predecessor."
        )
    return (head_name, base_format or "?", head_format or "?")


# Keywords that carry prose, or say which document this is, and nothing about the shape of a
# record. Stripped before two schemas are compared.
PROSE_KEYWORDS = frozenset(
    {
        "description",
        "title",
        "$schema",
        "$comment",
        "examples",
        "x-audit-descriptions",
        "x-audit-emitter",
    }
)


def canonical(spec: object) -> object:
    """`spec` with its prose stripped and its equivalent spellings made equal.

    A `oneOf` or `anyOf` whose branches are all constants is a closed list of values, the same
    statement as an `enum`, so it becomes one. A list of types is a set. Everything else is
    kept, so a difference the diff below does not understand still shows as a difference.
    """
    if isinstance(spec, list):
        return [canonical(item) for item in spec]
    if not isinstance(spec, dict):
        return spec
    out = {key: canonical(value) for key, value in spec.items() if key not in PROSE_KEYWORDS}
    for key in ("oneOf", "anyOf"):
        branches = out.get(key)
        if (
            isinstance(branches, list)
            and branches
            and all(
                isinstance(branch, dict) and "const" in branch and set(branch) <= {"const", "type"}
                for branch in branches
            )
        ):
            del out[key]
            out["enum"] = sorted((branch["const"] for branch in branches), key=json.dumps)
            types = {branch.get("type") for branch in branches}
            if len(types) == 1 and None not in types:
                out.setdefault("type", types.pop())
    if isinstance(out.get("type"), list):
        out["type"] = sorted(out["type"])
    return out


def fingerprint(spec: object) -> str:
    """A canonical spelling of `spec`, equal for two schemas that say the same thing."""
    return json.dumps(canonical(spec), sort_keys=True)


def _branch_key(branch: object) -> str | None:
    """What pairs one branch of a union or an `allOf` with its older self.

    The value a consumer routes on: the branch's own constant, the constant its `type`
    property pins, the constant its `if` matches, or the definition it points at. `None` for a
    branch that has none of these: it is compared by its whole content instead.
    """
    if not isinstance(branch, dict):
        return None
    if "const" in branch:
        return f"const {json.dumps(branch['const'])}"
    tag = (branch.get("properties") or {}).get("type")
    if isinstance(tag, dict) and "const" in tag:
        return f"type {json.dumps(tag['const'])}"
    for matched in ((branch.get("if") or {}).get("properties") or {}).values():
        if isinstance(matched, dict) and "const" in matched:
            return str(matched["const"])
    if "$ref" in branch:
        return f"$ref {branch['$ref']}"
    return None


def _values(spec: dict) -> tuple[list | None, bool]:
    """A definition's list of values and whether the set is closed: an `enum` is closed, an
    open set lists its values under `x-audit-values`."""
    if isinstance(spec.get("enum"), list):
        return spec["enum"], True
    if isinstance(spec.get("x-audit-values"), list):
        return spec["x-audit-values"], False
    return None, False


def diff_schemas(base: object, head: object, path: str, out: list) -> None:
    """Every difference between two schemas made [`canonical`], as `(level, path, reason)`
    tuples.

    Fails closed: a difference no rule below names is `major`. The rules:

    - A property removed is `major`, added is `minor`, also when it is required. An existing
      property becoming required or optional is `major`.
    - A value removed from a set is `major`. A value added is `none` for an open set and
      `major` for a closed one. Opening a set is `none`; closing it is `major`.
    - Union and `allOf` branches are paired by `_branch_key`; the rest are compared as a
      multiset of fingerprints. A branch removed is `major`. An `allOf` branch added is
      `minor`: it says what a new value of the field carries. A union branch added is `major`:
      a validator holding the older schema rejects a record that takes it.
    """
    if fingerprint(base) == fingerprint(head):
        return
    if not (isinstance(base, dict) and isinstance(head, dict)):
        out.append(("major", path, "changed"))
        return
    handled = {"properties", "required", "enum", "x-audit-values", "oneOf", "anyOf", "allOf",
               "items", "additionalProperties", "$defs", "then", "if"}

    b_props, h_props = base.get("properties") or {}, head.get("properties") or {}
    b_req, h_req = set(base.get("required") or []), set(head.get("required") or [])
    for prop in sorted(set(b_props) - set(h_props)):
        out.append(("major", f"{path}.{prop}", "removed"))
    for prop in sorted(set(h_props) - set(b_props)):
        out.append(("minor", f"{path}.{prop}", "added" + (" (required)" if prop in h_req else "")))
    for prop in sorted(set(b_props) & set(h_props)):
        if prop in h_req and prop not in b_req:
            out.append(("major", f"{path}.{prop}", "became required"))
        if prop in b_req and prop not in h_req:
            out.append(("major", f"{path}.{prop}", "became optional"))
        diff_schemas(b_props[prop], h_props[prop], f"{path}.{prop}", out)
    # A name required without being a property: compared as a set.
    for gone in sorted((b_req - set(b_props)) ^ (h_req - set(h_props))):
        out.append(("major", path, f"required `{gone}` changed"))

    b_values, b_closed = _values(base)
    h_values, h_closed = _values(head)
    if b_values is not None or h_values is not None:
        if b_values is None or h_values is None:
            out.append(("major", path, "became a set of values" if b_values is None else "is no longer a set of values"))
        else:
            for gone in sorted(set(map(json.dumps, b_values)) - set(map(json.dumps, h_values))):
                out.append(("major", path, f"lost the value {gone}"))
            for added in sorted(set(map(json.dumps, h_values)) - set(map(json.dumps, b_values))):
                if h_closed:
                    out.append(("major", path, f"closed set gained the value {added}"))
                else:
                    out.append(("none", path, f"gained the value {added}"))
            if b_closed and not h_closed:
                out.append(("none", path, "opened its set of values"))
            if h_closed and not b_closed:
                out.append(("major", path, "closed its set of values"))

    for key in ("oneOf", "anyOf", "allOf"):
        b_branches, h_branches = base.get(key), head.get(key)
        if b_branches is None and h_branches is None:
            continue
        # An object with no conditionals is one with an empty list of them: the first one is
        # a branch added like any other.
        if key == "allOf":
            b_branches = [] if b_branches is None else b_branches
            h_branches = [] if h_branches is None else h_branches
        if not isinstance(b_branches, list) or not isinstance(h_branches, list):
            out.append(("major", path, f"`{key}` added" if b_branches is None else f"`{key}` removed"))
            continue
        b_keyed = {k: v for v in b_branches if (k := _branch_key(v)) is not None}
        h_keyed = {k: v for v in h_branches if (k := _branch_key(v)) is not None}
        added_level = "minor" if key == "allOf" else "major"
        for gone in sorted(set(b_keyed) - set(h_keyed)):
            out.append(("major", f"{path}[{gone}]", "branch removed"))
        for added in sorted(set(h_keyed) - set(b_keyed)):
            out.append((added_level, f"{path}[{added}]", "branch added"))
        for both in sorted(set(b_keyed) & set(h_keyed)):
            b_branch, h_branch = b_keyed[both], h_keyed[both]
            if key == "allOf":
                diff_schemas(b_branch.get("then"), h_branch.get("then"), f"{path}[{both}]", out)
                if fingerprint(b_branch.get("if")) != fingerprint(h_branch.get("if")):
                    out.append(("major", f"{path}[{both}]", "matches differently"))
            else:
                diff_schemas(b_branch, h_branch, f"{path}[{both}]", out)
        b_rest = Counter(fingerprint(v) for v in b_branches if _branch_key(v) is None)
        h_rest = Counter(fingerprint(v) for v in h_branches if _branch_key(v) is None)
        for gone in sorted((b_rest - h_rest).elements()):
            out.append(("major", path, f"lost the untagged branch {gone}"))
        for added in sorted((h_rest - b_rest).elements()):
            out.append((added_level, path, f"gained the untagged branch {added}"))

    for key in ("items", "additionalProperties"):
        if key in base or key in head:
            b_sub, h_sub = base.get(key, True), head.get(key, True)
            if isinstance(b_sub, dict) and isinstance(h_sub, dict):
                diff_schemas(b_sub, h_sub, f"{path}.<{key}>", out)
            elif fingerprint(b_sub) != fingerprint(h_sub):
                out.append(("major", path, f"`{key}` changed"))

    for key in sorted((set(base) | set(head)) - handled - PROSE_KEYWORDS):
        if fingerprint(base.get(key)) != fingerprint(head.get(key)):
            out.append(("major", path, f"`{key}` went from {json.dumps(base.get(key))} to {json.dumps(head.get(key))}"))


def schema_findings(base: dict, head: dict) -> list[tuple[str, str, str]]:
    """Every difference from `base` to `head` as `(level, path, reason)`.

    Definitions are compared by name: one removed is `major`, one added `minor`, and a pair
    is compared by [`diff_schemas`].
    """
    found: list[tuple[str, str, str]] = []
    base, head = canonical(base), canonical(head)
    base_defs, head_defs = base.get("$defs") or {}, head.get("$defs") or {}
    for name in sorted(set(base_defs) - set(head_defs)):
        found.append(("major", name, "definition removed"))
    for name in sorted(set(head_defs) - set(base_defs)):
        found.append(("minor", name, "definition added"))
    for name in sorted(set(base_defs) & set(head_defs)):
        diff_schemas(base_defs[name], head_defs[name], name, found)
    rest_base = {k: v for k, v in base.items() if k != "$defs"}
    rest_head = {k: v for k, v in head.items() if k != "$defs"}
    diff_schemas(rest_base, rest_head, "(root)", found)
    return found


def classify_schema(base: dict, head: dict) -> tuple[str, list[str]]:
    """`none`, `additive` or `breaking` for the change from `base` to `head`, with the reasons.
    Reasons name the path, so a reader finds the change."""
    found = schema_findings(base, head)
    level = highest(level for level, _, _ in found) or "none"
    kind = {"major": "breaking", "minor": "additive"}.get(level, "none")
    reasons = [
        f"`{where}` {why}" + ("" if lvl != "none" else " (no format change)")
        for lvl, where, why in found
    ]
    return kind, reasons


def classify_change(merge_base: str, head_ref: str) -> str:
    """`none`, `additive`, `breaking` or `unknown` for the change between the two revisions,
    printing how it was reached.

    Fixtures pin shape by example and the schema pins every declared field and value. The
    verdict does not demand a version bump; it demands a fragment that does not understate it.
    """
    base_records = fixtures_at(merge_base)
    head_records = fixtures_at(head_ref)
    if not head_records:
        raise CheckFailed(
            f"::error::no audit fixtures under {AUDIT_DIR}/fixtures/ at {head_ref}. This "
            f"checker cannot compare anything without them, so fix the layout rather than "
            f"trusting a pass here."
        )
    base_shapes = {n: shape(r) for n, r in base_records.items()}
    head_shapes = {n: shape(r) for n, r in head_records.items()}
    shape_kind = classify_shape(base_shapes, head_shapes)
    compared = sorted(set(base_shapes) & set(head_shapes))
    print(
        f"Fixtures:   {len(compared)} compared ({len(base_shapes)} before, "
        f"{len(head_shapes)} after), shapes say {shape_kind}"
    )

    # The fixture shapes above see the record's KEYS. Its VALUES are strings, so renaming one
    # leaves every shape identical. The schema is what makes those visible: it lists every
    # value of every vocabulary.
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
    for reason in reasons:
        print(f"Schema:     {reason}")
    print(
        f"Schema:     {len(head_schema['$defs'])} definition(s) compared, says {schema_kind}"
    )
    rank = lambda k: LEVEL_RANK.get(REQUIRED_LEVEL.get(k, "none"), 0)
    if rank(schema_kind) > rank(shape_kind):
        return schema_kind
    return shape_kind


# ── what changed since the release ──────────────────────────────────────────────


def change_reasons(old: str, new: str) -> set[tuple[str, str]]:
    """Every format change from `old` to `new`, as `(level, reason)`: fixture shapes compared
    by name, and the schema. A `none` finding changes no format and is left out."""
    found: set[tuple[str, str]] = set()
    before, after = fixtures_at(old), fixtures_at(new)
    for name in sorted(set(before) & set(after)):
        was, now = shape(before[name]), shape(after[name])
        for entry in sorted(was - now):
            found.add(("major", f"fixture `{name}`: {entry.replace(chr(9), ' ')} is gone"))
        for entry in sorted(now - was):
            found.add(("minor", f"fixture `{name}`: {entry.replace(chr(9), ' ')} is new"))
    old_schema, new_schema = schema_at(old), schema_at(new)
    if old_schema is not None and new_schema is not None:
        for level, where, why in schema_findings(old_schema, new_schema):
            if level != "none":
                found.add((level, f"schema `{where}` {why}"))
    return found


def decide(
    released: set[tuple[str, str]],
    new: set[tuple[str, str]],
    unreleased: dict[str, str],
    contributed: dict[str, str],
) -> list[str]:
    """What is wrong with a branch, measured against the last release. Empty when nothing is.

    Two checks. Everything that changed since the release must be covered by the unreleased
    fragments together: a branch cannot delete an old fragment and so make a released change
    undescribed. And what this branch changed must be covered by a fragment this branch adds,
    raises or rewords: an earlier pull request's `major` does not describe this one's `minor`.
    A revert of unreleased work changes nothing since the release, so it owes nothing.
    """
    rank = lambda level: LEVEL_RANK.get(level, -1)
    errors = []
    needed = highest(level for level, _ in released)
    covered = highest(unreleased.values())
    if needed and rank(covered) < rank(needed):
        errors.append(
            f"the format changed at `{needed}` level since the last release, but the highest "
            f"unreleased fragment is `{covered or 'none'}`"
        )
    needed_here = highest(level for level, _ in new)
    contributed_level = highest(contributed.values())
    if needed_here and rank(contributed_level) < rank(needed_here):
        errors.append(
            f"this pull request makes a `{needed_here}` change, but "
            + (
                "adds, raises or rewords no fragment"
                if contributed_level is None
                else f"the fragments it adds, raises or rewords declare at most `{contributed_level}`"
            )
        )
    return errors


# ── entry points ────────────────────────────────────────────────────────────────


def show(version: tuple[int, int] | None) -> str:
    """A version for printing. `None` is a state worth naming, not a blank."""
    return "none" if version is None else f"{version[0]}.{version[1]}"


def check_release_branch(
    base_branch: str,
    released: set[tuple[str, str]],
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
    if released:
        problems.append(
            f"the emitted records changed since the release ({len(released)} change(s))"
        )
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

    # Everything below is read from the commit, not from the working tree, so a developer who
    # has edited but not committed is told about the previous commit. Saying so is cheaper
    # than reading the tree: the fixtures and the schema are generated, and comparing a
    # half-regenerated tree against a commit reports differences that are nobody's change.
    watched = [f"{AUDIT_DIR}/fixtures", SCHEMA_PATH, FRAGMENT_DIR, VERSION_SEARCH_PATH]
    dirty = _git("status", "--porcelain", "--", *watched).strip()
    if dirty:
        print(
            "Reading:   HEAD — these paths have uncommitted changes, which are NOT checked:\n"
            + "\n".join(f"             {line}" for line in dirty.splitlines()[:10])
        )

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

    tag = last_release_tag("HEAD")
    baseline = declared_version(tag)
    print(f"Release:    {tag}, which declares {show(baseline)}")
    head_fragments, released = split_fragments(
        fragments_at("HEAD"), fragment_bodies_at("HEAD"), fragment_bodies_at(tag), tag
    )
    base_fragments = {
        path: level for path, level in fragments_at(merge_base).items() if path not in released
    }
    if released:
        print(
            f"::warning::{len(released)} fragment(s) shipped with {tag} and are still here:\n  "
            + "\n  ".join(sorted(released))
            + "\n::notice::They raise nothing. Clear them with `just audit-format-release`, "
            "which checks that their text reached the release notes first."
        )

    contributed = fragment_demand(
        base_fragments,
        head_fragments,
        fragment_bodies_at(merge_base),
        fragment_bodies_at("HEAD"),
    )
    declared_level = highest(head_fragments.values())
    required = required_version(baseline, declared_level)

    print(
        f"Baseline:   {show(baseline)}"
        + ("" if baseline else " — no release has carried an audit format yet")
    )
    print(
        f"Fragments:  {len(head_fragments)} unreleased, highest {declared_level or '-'}; "
        f"this branch adds, raises or rewords {len(contributed)}"
    )
    print(f"Version:    {show(head_version)} declared, {show(required)} required")

    # The schema is generated from the constant, so the two disagree only when the file was
    # hand-edited or never regenerated. Either way a consumer would read a format the records
    # do not carry.
    committed_schema = schema_at("HEAD")
    if committed_schema is not None:
        _, stamped = emitter_of(committed_schema)
        if stamped is not None and stamped != show(head_version):
            raise CheckFailed(
                f"::error::{SCHEMA_PATH} is stamped format {stamped}, but AUDIT_FORMAT is "
                f"{show(head_version)}. Run `just update-audit-schema`."
            )

    if baseline is None:
        # The bootstrap. No release has carried an audit format version, so nothing released
        # can differ and the version derives to the first one. The comparison with the merge
        # base is printed for the reviewer; it demands nothing. Fragments are still wanted:
        # released builds emitted audit records before the version field existed, so a
        # consumer may already parse them.
        shape_kind = classify_change(merge_base, "HEAD")
        print(f"Verdict:    format {shape_kind} — no release has carried an audit format yet")
        if shape_kind in DEFERRALS:
            print(f"::warning::{DEFERRALS[shape_kind]}")
        if base_branch is not None and base_branch.startswith("rel-"):
            return check_release_branch(base_branch, set(), head_fragments, head_version, baseline)
        if head_fragments:
            print(
                f"Bootstrap:  {len(head_fragments)} fragment(s) present with no released "
                f"baseline. The version derives to {show(required)} regardless; the fragments "
                f"are the release note for consumers already parsing these records."
            )
    else:
        released_reasons = change_reasons(tag, "HEAD")
        new_reasons = released_reasons - change_reasons(tag, merge_base)
        for level, reason in sorted(released_reasons):
            mark = "this branch" if (level, reason) in new_reasons else "earlier"
            print(f"Since {tag}: [{level}, {mark}] {reason}")
        if base_branch is not None and base_branch.startswith("rel-"):
            return check_release_branch(
                base_branch, released_reasons, head_fragments, head_version, baseline
            )
        errors = decide(released_reasons, new_reasons, head_fragments, contributed)
        if errors:
            for error in errors:
                print(f"::error::{error[0].upper()}{error[1:]}.")
            print(
                f"::notice::Copy audit-format/TEMPLATE.md to {FRAGMENT_DIR}<descriptive-name>.md, "
                f"set its `level`, and describe the change in the terms an operator parsing the "
                f"log thinks in. Then run `just update-audit-fixtures`, which computes "
                f"AUDIT_FORMAT from the fragments. See the audit log section of "
                f"docs/docs/developer-guide.md."
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
            "which computes it from the last release tag and the fragments, and regenerates "
            "everything downstream."
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


def worktree_split() -> tuple[str, tuple[int, int] | None, dict[str, str], dict[str, str]]:
    """The last release tag, the version it declares, and the working tree's fragments split
    into `(unreleased, released)` against it."""
    tag = last_release_tag("HEAD")
    levels = worktree_fragments()
    digests = {
        path: hashlib.sha256(Path(path).read_text().encode()).hexdigest() for path in levels
    }
    unreleased, released = split_fragments(levels, digests, fragment_bodies_at(tag), tag)
    return tag, declared_version(tag), unreleased, released


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


def write_version() -> int:
    """Compute the required version from the working tree and write it into the constant."""
    tag, baseline, fragments, _ = worktree_split()
    level = highest(fragments.values())
    required = required_version(baseline, level)
    path, current = worktree_declaration()
    print(
        f"{tag} declares {show(baseline)}, {len(fragments)} unreleased fragment(s), highest "
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
    return 0


HEADINGS = {
    "major": "**Breaking changes** — an existing parser must be updated:",
    "minor": "**Additions** — an existing parser keeps working:",
    "none": "**Also worth knowing** — the format itself did not change:",
}


def release_notes() -> int:
    """Print the audit log block for the release notes of the last release. Mutates nothing.

    Every fragment that release shipped appears, grouped by level, however few version
    numbers the cycle consumed. That is the point of the split: the version says how badly a
    consumer is affected, and this list says what actually happened.
    """
    tag, version, _, fragments = worktree_split()
    if not fragments:
        print("_No audit log format changes in this release._")
        return 0
    tags = release_tags("HEAD")
    previous = declared_version(tags[-2]) if len(tags) > 1 else None

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
    was = "the first version" if previous is None else f"was {show(previous)}"
    print(f"Records from {tag} carry `audit_format` **{show(version)}** ({was}).")
    return 0


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


def do_release(version: str) -> int:
    """Clear the fragments the release `version` shipped, once their text is in its notes.

    Run after the release is tagged. The tag is the baseline, so nothing is moved: the
    fragments it carries already raise nothing, and this only removes them.
    """
    tag, declared, _, fragments = worktree_split()
    if tag.lstrip("v") != version.lstrip("v"):
        raise SystemExit(
            f"::error::the last release tag is {tag}, not v{version.lstrip('v')}. Tag the "
            f"release first; its fragments are the ones present at the tag. Nothing has been "
            f"changed."
        )

    # Clearing the fragments is the only destructive step in this scheme: their prose exists
    # nowhere else, so deleting it before it reaches the release notes loses the only
    # consumer-facing description of the change. Checked BEFORE anything is removed, so a
    # failure here leaves the tree untouched and the command can simply be rerun.
    if fragments:
        notes = Path(RELEASE_NOTES_PATH)
        if not notes.is_file():
            raise SystemExit(
                f"::error::{RELEASE_NOTES_PATH} does not exist, so there is nowhere for "
                f"{len(fragments)} fragment(s) to have been written. See .github/RELEASING.md."
            )
        section = release_notes_section(notes.read_text(), version)
        if section is None:
            raise SystemExit(
                f"::error::{RELEASE_NOTES_PATH} has no section for {version}. Add the "
                f"`## v{version} (date)` section first, then paste the block from "
                f"`just audit-format-release-notes` into it, and rerun this. Nothing has been "
                f"changed."
            )
        bodies = {path: fragment_body(Path(path).read_text()) for path in fragments}
        missing = unwritten_fragments(section, bodies)
        if missing:
            raise SystemExit(
                f"::error::{len(missing)} of {len(fragments)} audit format fragment(s) do not "
                f"appear in the {version} section of {RELEASE_NOTES_PATH}:\n  "
                + "\n  ".join(missing)
                + "\n::notice::Run `just audit-format-release-notes` and paste its block into "
                "that section, then rerun this. The prose in a fragment exists nowhere else. "
                "Nothing has been changed."
            )

    for path in sorted(fragments):
        Path(path).unlink()
    print(
        f"Cleared {len(fragments)} fragment(s) shipped with {tag}, all of them present in the "
        f"{version} section of {RELEASE_NOTES_PATH}."
    )
    # The standing table in the logging docs is what a consumer actually looks up: "which
    # format does the version I am running emit?". Printed, because the table is prose.
    tags = release_tags("HEAD")
    previous = declared_version(tags[-2]) if len(tags) > 1 else None
    if declared != previous and RELEASE_TABLE_PATH is not None:
        print()
        print(f"Add this row to the release table in {RELEASE_TABLE_PATH}:")
        print()
        print(f"| {version} | `{show(declared)}` |")
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
    # `unknown` must never become a requirement: it fires whenever a fixture is renamed, so
    # demanding a fragment for it taxes changes that did nothing to the format.
    check("unknown demands no fragment", REQUIRED_LEVEL.get("unknown"), None)
    check("unknown still says something", bool(DEFERRALS.get("unknown")), True)
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

    # ── the release tag and the fragments it shipped ────────────────────────────
    tag_re = re.compile(RELEASE_TAG_PATTERN)
    check("a release tag matches", bool(tag_re.fullmatch("v0.13.1")), True)
    check("a prerelease tag does not", bool(tag_re.fullmatch("v0.14.0-rc.1")), False)
    check("a bare version does not", bool(tag_re.fullmatch("0.13.1")), False)

    def split(levels, digests, at_tag):
        try:
            return split_fragments(levels, digests, at_tag, "v1.0.0")
        except SystemExit as error:
            return f"rejected: {error}"

    check(
        "a fragment the tag lacks is unreleased",
        split({"a.md": "minor"}, {"a.md": "x"}, {}),
        ({"a.md": "minor"}, {}),
    )
    check(
        "a fragment the tag carries unchanged is released",
        split({"a.md": "minor"}, {"a.md": "x"}, {"a.md": "x"}),
        ({}, {"a.md": "minor"}),
    )
    check(
        "a released fragment edited since is an error",
        str(split({"a.md": "minor"}, {"a.md": "y"}, {"a.md": "x"})).startswith("rejected"),
        True,
    )
    check(
        "the two sides are split",
        split({"a.md": "minor", "b.md": "major"}, {"a.md": "x", "b.md": "z"}, {"a.md": "x"}),
        ({"b.md": "major"}, {"a.md": "minor"}),
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

    def verdict(base: dict, head: dict) -> str:
        return classify_schema(schema(base), schema(head))[0]

    def names(base: dict, head: dict, text: str) -> bool:
        return any(text in r for r in classify_schema(schema(base), schema(head))[1])

    actor = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}}, "required": ["actor_type"]}
    actor_email = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}, "email": {"type": "string"}}, "required": ["actor_type"]}
    actor_email_req = {**actor_email, "required": ["actor_type", "email"]}
    actor_req = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "string"}}, "required": ["actor_type", "principal"]}
    actor_retyped = {"type": "object", "properties": {"actor_type": {"type": "string"}, "principal": {"type": "integer"}}, "required": ["actor_type"]}
    actor_described = {**actor, "description": "Who acted.", "properties": {**actor["properties"], "principal": {"type": "string", "description": "The id."}}}
    check("schema: identical is none", verdict({"A": actor}, {"A": actor}), "none")
    check("schema: a description changed is none", verdict({"A": actor}, {"A": actor_described}), "none")
    check("schema: optional property added is additive", verdict({"A": actor}, {"A": actor_email}), "additive")
    check("schema: a new required property is additive", verdict({"A": actor}, {"A": actor_email_req}), "additive")
    check("schema: property removed is breaking", verdict({"A": actor_email}, {"A": actor}), "breaking")
    check("schema: the removed property is named by its path", names({"A": actor_email}, {"A": actor}, "`A.email` removed"), True)
    check("schema: property made required is breaking", verdict({"A": actor}, {"A": actor_req}), "breaking")
    check("schema: property made optional is breaking", verdict({"A": actor_req}, {"A": actor}), "breaking")
    check("schema: property made optional is named by its path", names({"A": actor_req}, {"A": actor}, "`A.principal` became optional"), True)
    check("schema: property retyped is breaking", verdict({"A": actor}, {"A": actor_retyped}), "breaking")
    check("schema: definition added is additive", verdict({"A": actor}, {"A": actor, "B": actor}), "additive")
    check("schema: definition removed is breaking", verdict({"A": actor, "B": actor}, {"A": actor}), "breaking")
    check("schema: a type list is a set", verdict({"T": {"type": ["string", "null"]}}, {"T": {"type": ["null", "string"]}}), "none")

    # Value sets: an open set lists its values under `x-audit-values`, a closed one as `enum`.
    open_set = {"type": "string", "x-audit-values": ["allowed", "denied"], "x-audit-kind": "enum", "x-audit-field": "outcome"}
    open_more = {**open_set, "x-audit-values": ["allowed", "denied", "deferred"]}
    open_less = {**open_set, "x-audit-values": ["allowed"]}
    closed_set = {"type": "string", "enum": ["allowed", "denied"], "x-audit-kind": "enum", "x-audit-field": "decision"}
    closed_more = {**closed_set, "enum": ["allowed", "denied", "deferred"]}
    closed_less = {**closed_set, "enum": ["allowed"]}
    check("schema: an open set gaining a value is none", verdict({"D": open_set}, {"D": open_more}), "none")
    check("schema: an open set losing a value is breaking", verdict({"D": open_set}, {"D": open_less}), "breaking")
    check("schema: a closed set gaining a value is breaking", verdict({"D": closed_set}, {"D": closed_more}), "breaking")
    check("schema: a closed set losing a value is breaking", verdict({"D": closed_set}, {"D": closed_less}), "breaking")
    check("schema: the lost value is named", names({"D": closed_set}, {"D": closed_less}, 'lost the value "denied"'), True)
    reopened = {**{k: v for k, v in closed_set.items() if k != "enum"}, "x-audit-values": closed_set["enum"]}
    check("schema: opening a set is none", verdict({"D": closed_set}, {"D": reopened}), "none")
    check("schema: closing a set is breaking", verdict({"D": reopened}, {"D": closed_set}), "breaking")
    check("schema: a vocabulary changing its field is breaking", verdict({"V": open_set}, {"V": {**open_set, "x-audit-field": "decision"}}), "breaking")
    check("schema: a vocabulary changing kind is breaking", verdict({"V": open_set}, {"V": {**open_set, "x-audit-kind": "part"}}), "breaking")
    check("schema: an unknown x-audit keyword changing is breaking", verdict({"V": open_set}, {"V": {**open_set, "x-audit-new": True}}), "breaking")
    check("schema: a format changing is breaking", verdict({"T": {"type": "string"}}, {"T": {"type": "string", "format": "date-time"}}), "breaking")

    # A union of constants is the same statement as an `enum`, and closed like one.
    effect = {"oneOf": [{"type": "string", "const": "permit"}, {"type": "string", "const": "forbid"}]}
    effect_less = {"oneOf": [{"type": "string", "const": "permit"}]}
    effect_more = {"oneOf": effect["oneOf"] + [{"type": "string", "const": "defer"}]}
    check("schema: a constant lost from a oneOf is breaking", verdict({"E": effect}, {"E": effect_less}), "breaking")
    check("schema: the lost constant is named", names({"E": effect}, {"E": effect_less}, 'lost the value "forbid"'), True)
    check("schema: a constant added to a oneOf closes nothing new but is breaking", verdict({"E": effect}, {"E": effect_more}), "breaking")
    check("schema: a oneOf of constants equals the enum", verdict({"E": effect}, {"E": {"type": "string", "enum": ["forbid", "permit"]}}), "none")

    # Conditional branches: how a flattened object says which keys one of its values brings.
    def carries(**per_action: dict) -> dict:
        return {
            "type": "object",
            "properties": {"action_name": {"type": "string"}},
            "allOf": [
                {
                    "if": {"properties": {"action_name": {"const": act}}, "required": ["action_name"]},
                    "then": {"properties": props},
                }
                for act, props in per_action.items()
            ],
        }

    drop = carries(drop={"force": {"type": "boolean"}, "purge": {"type": "boolean"}})
    drop_one = carries(drop={"force": {"type": "boolean"}})
    drop_str = carries(drop={"force": {"type": "boolean"}, "purge": {"type": "string"}})
    drop_more = carries(drop={"force": {"type": "boolean"}, "purge": {"type": "boolean"}, "recursive": {"type": "boolean"}})
    drop_and_commit = carries(drop={"force": {"type": "boolean"}}, commit={"target_refs": {"type": "array"}})
    drop_set = carries(drop={"root_level": {"$ref": "#/$defs/R"}})
    drop_open = carries(drop={"root_level": {"type": "string"}})
    check("schema: a branch losing a key is breaking", verdict({"A": drop}, {"A": drop_one}), "breaking")
    check("schema: a carried key retyped is breaking", verdict({"A": drop}, {"A": drop_str}), "breaking")
    check("schema: a branch gaining a key is additive", verdict({"A": drop}, {"A": drop_more}), "additive")
    check("schema: an action losing its branch is breaking", verdict({"A": drop_and_commit}, {"A": drop_one}), "breaking")
    check("schema: an action gaining a branch is additive", verdict({"A": drop_one}, {"A": drop_and_commit}), "additive")
    check("schema: the first conditional is additive", verdict({"A": {"type": "object", "properties": {"action_name": {"type": "string"}}}}, {"A": drop}), "additive")
    check("schema: dropping every branch is breaking", verdict({"A": drop}, {"A": {"type": "object", "properties": {"action_name": {"type": "string"}}}}), "breaking")
    check("schema: a key losing its value set is breaking", verdict({"A": drop_set}, {"A": drop_open}), "breaking")
    check("schema: the action that lost a key is named by its path", names({"A": drop}, {"A": drop_one}, "`A[drop].purge` removed"), True)

    # A map whose values change type: what a free-form `context` object holds.
    free_map = {"type": "object", "additionalProperties": {"type": "string"}}
    check("schema: a map's value retyped is breaking", verdict({"M": free_map}, {"M": {"type": "object", "additionalProperties": {"type": "object"}}}), "breaking")
    check("schema: a map closed is breaking", verdict({"M": free_map}, {"M": {"type": "object", "additionalProperties": False}}), "breaking")

    # Two untagged object branches render alike, so they are compared as a multiset.
    tagless = {"oneOf": [{"type": "object", "properties": {"a": {"type": "string"}}}, {"type": "object", "properties": {"b": {"type": "string"}}}]}
    tagless_less = {"oneOf": [tagless["oneOf"][0]]}
    check("schema: an untagged branch removed is breaking", verdict({"T": tagless}, {"T": tagless_less}), "breaking")
    check("schema: an untagged branch added is breaking", verdict({"T": tagless_less}, {"T": tagless}), "breaking")

    # A tagged union pairs its branches by the tag, which is what a consumer switches on.
    factor = {"oneOf": [{"type": "object", "properties": {"type": {"const": "policy"}, "policy-id": {"type": "string"}}}]}
    renamed = {"oneOf": [{"type": "object", "properties": {"type": {"const": "policy"}, "policy_id": {"type": "string"}}}]}
    widened = {"oneOf": [{"type": "object", "properties": {"type": {"const": "policy"}, "policy-id": {"type": "string"}, "note": {"type": "string"}}}]}
    retagged = {"oneOf": [{"type": "object", "properties": {"type": {"const": "rule"}, "policy-id": {"type": "string"}}}]}
    two_kinds = {"oneOf": factor["oneOf"] + [{"type": "object", "properties": {"type": {"const": "rule"}}}]}
    check("schema: a rename inside a tagged branch is breaking", verdict({"F": factor}, {"F": renamed}), "breaking")
    check("schema: the renamed branch property is named by its path", names({"F": factor}, {"F": renamed}, '`F[type "policy"].policy-id` removed'), True)
    check("schema: a field added to a tagged branch is additive", verdict({"F": factor}, {"F": widened}), "additive")
    check("schema: an unchanged tagged branch is no change", verdict({"F": factor}, {"F": factor}), "none")
    check("schema: a renamed tag is breaking", verdict({"F": factor}, {"F": retagged}), "breaking")
    check("schema: a tagged branch added is breaking", verdict({"F": factor}, {"F": two_kinds}), "breaking")

    # What a branch owes the release notes. Pure in its four maps, so every arrangement the
    # gate acts on is checkable here rather than only on a real pull request.
    none_: dict[str, str] = {}
    one = {"a.md": "minor"}
    one_body = {"a.md": "h1"}
    check("demand: a fragment added is contributed", fragment_demand(none_, one, none_, one_body), one)
    check("demand: a fragment removed is not contributed", fragment_demand(one, none_, one_body, none_), {})
    check("demand: an untouched fragment is not contributed", fragment_demand(one, one, one_body, one_body), {})
    check("demand: a raised level is contributed", fragment_demand(one, {"a.md": "major"}, one_body, one_body), {"a.md": "major"})
    # Folding a second change into an existing fragment is the documented way to keep the
    # note describing the final state. It usually leaves the level alone.
    check("demand: a reworded fragment is contributed", fragment_demand(one, one, one_body, {"a.md": "h2"}), one)
    check("demand: a renamed fragment is contributed", fragment_demand(one, {"b.md": "minor"}, one_body, {"b.md": "h1"}), {"b.md": "minor"})

    # What a branch owes, measured against the last release.
    added = ("minor", "schema `A.email` added")
    removed = ("major", "schema `A.principal` removed")
    earlier = ("major", "schema `B.kind` removed")
    check("decide: a revert of unreleased work owes nothing", decide(set(), set(), {}, {}), [])
    check(
        "decide: deleting an old fragment and removing a released field fails",
        len(decide({removed}, {removed}, {}, {})),
        2,
    )
    check(
        "decide: withdrawing a fragment alone fails",
        len(decide({earlier}, set(), {}, {})),
        1,
    )
    check(
        "decide: an earlier major does not excuse this branch's minor",
        len(decide({earlier, added}, {added}, {"old.md": "major"}, {})),
        1,
    )
    check(
        "decide: a minor fragment covers this branch's minor",
        decide({earlier, added}, {added}, {"old.md": "major", "new.md": "minor"}, {"new.md": "minor"}),
        [],
    )
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
    other = {"x-audit-emitter": {"name": "lakekeeper_plus", "format": "1.0"}, "$defs": {}}
    unstamped = {"$defs": {}}
    check("stamp: read", emitter_of(stamped), ("lakekeeper", "1.0"))
    check("stamp: absent is not an error", emitter_of(unstamped), (None, None))
    check("stamp: a malformed stamp reads as absent",
          emitter_of({"x-audit-emitter": "lakekeeper"}), (None, None))
    check("stamp: the same emitter compares",
          require_same_emitter(stamped, stamped, "t"), ("lakekeeper", "1.0", "1.0"))
    unstamped_refused = False
    try:
        require_same_emitter(unstamped, stamped, "t")
    except CheckFailed:
        unstamped_refused = True
    check("stamp: an unstamped schema is refused", unstamped_refused, True)
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
        metavar="VERSION",
        help="after the release VERSION is tagged, clear the fragments it shipped",
    )
    args = parser.parse_args(argv)

    load_config()
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
