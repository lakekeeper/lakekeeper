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
Self-test:       python3 .github/scripts/check-audit-format.py --self-test
Read version:    python3 .github/scripts/check-audit-format.py --print-version <rev>
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

AUDIT_DIR = "crates/lakekeeper/src/service/events/backends/audit"
# Two patterns for the same thing: `git grep -E` is POSIX ERE, which has no `\s`.
#
# Both are anchored to a `pub const` DECLARATION rather than to any occurrence of the text.
# Without the anchor, prose that merely quotes the constant — a doc comment explaining the
# format, say — counts as a second declaration and fails the build, while a commented-out
# declaration would count as a real one.
GIT_PATTERN = r'^[[:space:]]*pub const AUDIT_FORMAT: &str = "[0-9]+\.[0-9]+"'
VERSION_RE = re.compile(r'^\s*pub const AUDIT_FORMAT:\s*&str\s*=\s*"(\d+)\.(\d+)"')


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
    out = _git_grep("-E", GIT_PATTERN, rev, "--", "crates/")
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

# What a fixture or manifest verdict says a fragment must AT LEAST declare. `values` and
# `unknown` are deliberately absent: both hand the question to a human (see `DEFERRALS`),
# so neither can demand a fragment without taxing every change that merely makes a test
# input more realistic.
REQUIRED_LEVEL = {"breaking": "major", "additive": "minor"}

FRAGMENT_LEVEL_RE = re.compile(r"^level:[ \t]*(\S+)[ \t]*$", re.MULTILINE)
# Anchored to the declaration, like `VERSION_RE`, and capturing everything either side of
# the value so a rewrite cannot disturb the rest of the line.
VERSION_WRITE_RE = re.compile(
    r'(^[ \t]*pub const AUDIT_FORMAT:[ \t]*&str[ \t]*=[ \t]*")\d+\.\d+(")', re.MULTILINE
)


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


def fragments_at(rev: str) -> dict[str, str]:
    """The unreleased fragments at `rev`, as path -> level.

    Only `*.md` directly under the fragment directory. `TEMPLATE.md` lives one level up for
    this reason: a template that parsed as a fragment would add a permanent phantom change
    to every release.
    """
    listing = _git("ls-tree", "-r", "--name-only", rev, "--", FRAGMENT_DIR)
    return {
        path: parse_fragment(_git("show", f"{rev}:{path}"), f"{path} at {rev}")
        for path in listing.splitlines()
        if path.endswith(".md")
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

MANIFEST_NAME = "wire_values.json"

def manifest_paths(rev: str) -> list[str]:
    """Every committed wire-value manifest at `rev`.

    Discovered rather than listed, because the vocabulary is not all in one crate.
    `CatalogAction` is a public trait, so an authorizer contributes `action_name` values
    Lakekeeper cannot enumerate; each such crate commits its own manifest next to the types
    that produce it, and every one of them is compared here by the same rule.
    """
    listing = _git("ls-tree", "-r", "--name-only", rev)
    return sorted(
        path for path in listing.splitlines() if path.rsplit("/", 1)[-1] == MANIFEST_NAME
    )


def read_manifest(rev: str, path: str) -> dict:
    """One committed manifest at `rev`.

    Only ever called for a path `manifest_paths` just reported at the same revision, so a
    failure here is a real git failure and is left to propagate. Reading it as "no manifest"
    would turn the whole comparison off without printing a word.
    """
    raw = _git("show", f"{rev}:{path}")
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as error:
        raise SystemExit(
            f"::error::{path} at {rev} is not valid JSON: {error}. A wire-value manifest is "
            f"generated by a test — regenerate it with `just update-audit-fixtures` rather "
            f"than editing it by hand."
        ) from error
    if not isinstance(manifest, dict):
        # Discovery is by filename, so an unrelated file of the same name lands here. Name it
        # rather than failing later with an AttributeError nobody can trace back to a path.
        raise SystemExit(
            f"::error::{path} at {rev} is named like a wire-value manifest but is a "
            f"{type(manifest).__name__}, not an object. Either it is one and is malformed, or "
            f"it is something else that needs a different name — discovery matches any file "
            f"called {MANIFEST_NAME}."
        )
    return manifest


def manifest_entries(manifest: dict) -> set[tuple[str, str, str]]:
    """A manifest flattened to `(wire field, owning type, value)` triples.

    The shape is `{"<field>": {"<owner>": ["<value>", ...]}}`. Flattened with the owner kept,
    never to bare values: most verbs are shared — six action enums emit `get_metadata` — so a
    comparison that dropped the owner would not notice one of them renaming it.
    """
    entries: set[tuple[str, str, str]] = set()
    for field, owners in (manifest or {}).items():
        if not isinstance(owners, dict):
            continue
        for owner, values in owners.items():
            if not isinstance(values, list):
                continue
            entries.update(
                (field, owner, value) for value in values if isinstance(value, str)
            )
    return entries


def losses_by_path(
    base: dict[str, set[tuple[str, str, str]]], head: dict[str, set[tuple[str, str, str]]]
) -> dict[str, list[str]]:
    """Values present at base and emitted by nothing at head, keyed by the manifest that held
    them.

    The pure half of `lost_wire_values`, split out so the rule this whole check rests on is
    testable without a repository — and so a rewrite of it cannot pass the self-test by virtue
    of never being called.

    An entry is `(field, owner, value)` and is lost only when it is absent from EVERY head
    manifest, not merely from the one it used to live in. The owner is part of the key, so
    that distinction does the right thing in both directions: moving a manifest to a new path,
    or moving an owner from one crate's manifest to another's, changes where a value is
    recorded but not whether it is emitted, and is not a loss. Renaming one owner's value
    while a DIFFERENT owner keeps the same string still is — six action enums emit
    `get_metadata`, and one of them dropping it must not be masked by the other five.

    Additions are not losses; see the audit log section of `docs/docs/developer-guide.md`.
    """
    still_emitted: set[tuple[str, str, str]] = set()
    for entries in head.values():
        still_emitted |= entries

    lost: dict[str, list[str]] = {}
    for path, before in base.items():
        gone = before - still_emitted
        if gone:
            lost[path] = [
                f"{field}: {owner} -> {value}" for field, owner, value in sorted(gone)
            ]
    return lost


def lost_wire_values(rev_base: str, rev_head: str) -> dict[str, list[str]]:
    """Values emitted at `rev_base` and no longer emittable at `rev_head`, by manifest.

    These reach the log as strings consumers switch on, so losing one breaks them while the
    record's shape stays byte-identical — which is exactly what the fixture comparison cannot
    see. The git reads live here; the comparison itself is `losses_by_path`.
    """
    return losses_by_path(
        {path: manifest_entries(read_manifest(rev_base, path)) for path in manifest_paths(rev_base)},
        {path: manifest_entries(read_manifest(rev_head, path)) for path in manifest_paths(rev_head)},
    )


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


def classify_change(merge_base: str, head_ref: str) -> str:
    """`none`, `additive`, `breaking`, `values` or `unknown` for the change between the two
    revisions, printing how it was reached.

    This half did not change with the move to derived versions: fixtures still pin shape by
    example and the manifests still pin values exhaustively. What changed is what the verdict
    is used FOR. It no longer demands a version bump — it demands a fragment that does not
    understate it.
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

    # The fixture shapes above see the record's KEYS. Its VALUES — `action_name`,
    # `entity_type`, `decision` and the rest — are strings, so renaming one leaves every shape
    # identical, and a value no fixture happens to carry changes nothing at all. The committed
    # manifests are what make those visible. Only losses matter: a rename or a removal breaks
    # every consumer switching on the value, while a new value leaves the format unchanged.
    base_manifests = manifest_paths(merge_base)
    if not base_manifests:
        # Said out loud on purpose: a skipped check that prints nothing is indistinguishable
        # from a check that ran and found nothing.
        print(
            f"Values:     SKIPPED — no {MANIFEST_NAME} at {merge_base}, so there is nothing to "
            f"compare. A renamed or removed wire value is NOT checked in this run; check by "
            f"hand."
        )
        return shape_kind

    lost = lost_wire_values(merge_base, head_ref)
    for path, entries in sorted(lost.items()):
        for entry in entries:
            print(f"Values:     {path} no longer emits {entry}")
    if lost:
        # A string consumers switch on is gone. That breaks them whatever the fixtures showed,
        # so it decides the verdict.
        return "breaking"
    total = sum(
        len(manifest_entries(read_manifest(merge_base, path))) for path in base_manifests
    )
    print(
        f"Values:     {total} value(s) across {len(base_manifests)} manifest(s) compared, "
        f"none lost"
    )
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
    head_fragments = fragments_at("HEAD")
    base_fragments = fragments_at(merge_base)
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

    shape_kind = classify_change(merge_base, "HEAD")
    detected_level = REQUIRED_LEVEL.get(shape_kind)
    print(
        f"Verdict:    format {shape_kind}"
        + (f", needs a fragment of at least `{detected_level}`" if detected_level else "")
    )

    if base_branch is not None and base_branch.startswith("rel-"):
        return check_release_branch(
            base_branch, shape_kind, head_fragments, head_version, baseline
        )

    # The bootstrap. Nothing has been released carrying an audit format, so there is no
    # format to have changed and nothing for a fragment to describe. Rejected rather than
    # ignored: a fragment here reads as a change that a reader would go looking for in the
    # previous release notes, and there are none.
    if baseline is None and head_fragments:
        print(
            f"::error::{BASELINE_PATH} records no released version, so the first release "
            f"declares {show(required)} rather than changing anything — but "
            f"{len(head_fragments)} fragment(s) are present: {', '.join(sorted(head_fragments))}. "
            f"Delete them and describe the audit log in the release notes as a new feature."
        )
        return 1

    if shape_kind in DEFERRALS:
        print(f"::warning::{DEFERRALS[shape_kind]}")

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
    return {
        str(path): parse_fragment(path.read_text(), str(path))
        for path in sorted(Path(FRAGMENT_DIR).glob("*.md"))
    }


def worktree_declaration() -> tuple[str, tuple[int, int]]:
    """The file declaring AUDIT_FORMAT in the working tree, and its value."""
    found: dict[str, tuple[int, int]] = {}
    for line in (_git_grep("-E", GIT_PATTERN, "--", "crates/") or "").splitlines():
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
    if baseline is None and fragments:
        raise SystemExit(
            f"::error::{BASELINE_PATH} records no released version, so the first release "
            f"declares {show(required_version(None, None))} rather than changing anything — "
            f"but {len(fragments)} fragment(s) are present. Delete them and describe the audit "
            f"log in the release notes as a new feature."
        )
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

    # Wire values, flattened per (field, owner). Pure dict comparisons on purpose: the git
    # lookups live in `read_manifest`, so the comparison stays testable without a repository.
    #
    # THE regression. `get_metadata` is emitted by six enums, so renaming ONE of them is
    # invisible to any comparison over the flattened union of all the names: the union
    # still contains `get_metadata` from the other five. This is the real case that got
    # through — `CatalogTableAction::GetMetadata` renamed to `FetchMetadata`, the wire
    # value for every table event changed, CI green. Reintroduce a flattened comparison
    # and this check is the one that fails.
    masking_base = {
        "action_name": {
            "CatalogTableAction": ["get_metadata", "drop"],
            "CatalogViewAction": ["get_metadata"],
        }
    }
    masking_head = {
        "action_name": {
            "CatalogTableAction": ["fetch_metadata", "drop"],
            "CatalogViewAction": ["get_metadata"],
        }
    }
    check(
        "a rename is a loss even while another owner still emits that name",
        manifest_entries(masking_base) - manifest_entries(masking_head),
        {("action_name", "CatalogTableAction", "get_metadata")},
    )
    check(
        "the flattened union is what cannot see it",
        {value for _, _, value in manifest_entries(masking_base)}
        - {value for _, _, value in manifest_entries(masking_head)},
        set(),
    )
    # The same masking applies ACROSS fields, which is why the field is part of the key:
    # `read` is both an action name and, hypothetically, some other field's value.
    check(
        "a rename is a loss even while another FIELD carries that value",
        manifest_entries({"action_name": {"A": ["read"]}, "decision": {"D": ["read"]}})
        - manifest_entries({"action_name": {"A": ["fetch"]}, "decision": {"D": ["read"]}}),
        {("action_name", "A", "read")},
    )
    check(
        "an added value is not a loss",
        manifest_entries({"action_name": {"A": ["x"]}})
        - manifest_entries({"action_name": {"A": ["x", "y"]}}),
        set(),
    )
    check(
        "an added owner is not a loss",
        manifest_entries({"action_name": {"A": ["x"]}})
        - manifest_entries({"action_name": {"A": ["x"], "B": ["z"]}}),
        set(),
    )
    check(
        "an added field is not a loss",
        manifest_entries({"action_name": {"A": ["x"]}})
        - manifest_entries({"action_name": {"A": ["x"]}, "entity_type": {"E": ["t"]}}),
        set(),
    )
    check(
        "an owner removed entirely loses every one of its values",
        manifest_entries({"action_name": {"A": ["x"], "B": ["y", "z"]}})
        - manifest_entries({"action_name": {"A": ["x"]}}),
        {("action_name", "B", "y"), ("action_name", "B", "z")},
    )
    check(
        "a field removed entirely loses every one of its values",
        manifest_entries({"action_name": {"A": ["x"]}, "entity_type": {"E": ["tag"]}})
        - manifest_entries({"action_name": {"A": ["x"]}}),
        {("entity_type", "E", "tag")},
    )
    identical = {"action_name": {"A": ["x", "y"], "B": ["y"]}, "decision": {"D": ["allowed"]}}
    check(
        "identical manifests lose nothing",
        manifest_entries(identical) - manifest_entries(dict(identical)),
        set(),
    )
    check(
        "a malformed manifest yields no entries rather than raising",
        manifest_entries({"action_name": "not-an-object", "entity_type": {"E": "not-a-list"}}),
        set(),
    )

    # `losses_by_path` is the function `run()` actually calls, so the checks below target it
    # rather than the set arithmetic it happens to use. A rewrite that never calls it — or
    # that reintroduces a flattened comparison inside it — fails here.
    def entries(manifest):
        return manifest_entries(manifest)

    lake = "crates/lakekeeper/src/service/events/backends/audit/wire_values.json"
    fga = "crates/authz-openfga/wire_values.json"

    # THE regression. `get_metadata` is emitted by six enums, so renaming ONE of them is
    # invisible to any comparison over the flattened union: the union still contains
    # `get_metadata` from the other five. This is the real case that got through —
    # `CatalogTableAction::GetMetadata` renamed to `FetchMetadata`, the wire value for every
    # table event changed, CI green.
    check(
        "a rename is a loss even while another owner still emits that name",
        losses_by_path(
            {lake: entries({"action_name": {"A": ["get_metadata", "drop"], "B": ["get_metadata"]}})},
            {lake: entries({"action_name": {"A": ["fetch_metadata", "drop"], "B": ["get_metadata"]}})},
        ),
        {lake: ["action_name: A -> get_metadata"]},
    )
    check(
        "a rename is a loss even while another FIELD carries that value",
        losses_by_path(
            {lake: entries({"action_name": {"A": ["read"]}, "decision": {"D": ["read"]}})},
            {lake: entries({"action_name": {"A": ["fetch"]}, "decision": {"D": ["read"]}})},
        ),
        {lake: ["action_name: A -> read"]},
    )
    # Every manifest is compared, not just the first: the whole point of the cross-crate
    # seam is that an authorizer's own vocabulary is checked too.
    check(
        "a loss in a second crate's manifest is reported",
        losses_by_path(
            {lake: entries({"action_name": {"A": ["x"]}}),
             fga: entries({"action_name": {"R": ["can_assume", "can_read"]}})},
            {lake: entries({"action_name": {"A": ["x"]}}),
             fga: entries({"action_name": {"R": ["can_assume"]}})},
        ),
        {fga: ["action_name: R -> can_read"]},
    )
    check(
        "a manifest deleted outright loses everything it held",
        losses_by_path(
            {lake: entries({"action_name": {"A": ["x"]}}),
             fga: entries({"action_name": {"R": ["can_assume"]}})},
            {lake: entries({"action_name": {"A": ["x"]}})},
        ),
        {fga: ["action_name: R -> can_assume"]},
    )
    # A directory rename is not a wire change. Without this the checker demands a MAJOR bump
    # for moving a file.
    check(
        "a manifest MOVED to a new path loses nothing",
        losses_by_path(
            {fga: entries({"action_name": {"R": ["can_assume", "can_read"]}})},
            {"crates/renamed/wire_values.json": entries(
                {"action_name": {"R": ["can_assume", "can_read"]}}
            )},
        ),
        {},
    )
    check(
        "a move that also drops a value still reports the drop",
        losses_by_path(
            {fga: entries({"action_name": {"R": ["can_assume", "can_read"]}})},
            {"crates/renamed/wire_values.json": entries({"action_name": {"R": ["can_assume"]}})},
        ),
        {fga: ["action_name: R -> can_read"]},
    )
    for label, added in (
        ("an added value", {"action_name": {"A": ["x", "y"]}}),
        ("an added owner", {"action_name": {"A": ["x"], "B": ["z"]}}),
        ("an added field", {"action_name": {"A": ["x"]}, "entity_type": {"E": ["t"]}}),
    ):
        check(
            f"{label} is not a loss",
            losses_by_path({lake: entries({"action_name": {"A": ["x"]}})}, {lake: entries(added)}),
            {},
        )
    check(
        "identical manifests lose nothing",
        losses_by_path(
            {lake: entries({"action_name": {"A": ["x", "y"]}})},
            {lake: entries({"action_name": {"A": ["x", "y"]}})},
        ),
        {},
    )
    # The two relocation cases. A value still emitted SOMEWHERE has not been lost to a
    # consumer, whichever manifest happens to record it — but "somewhere" must mean the same
    # OWNER, or the masking regression comes back.
    check(
        "an owner moving between two existing manifests is not a loss",
        losses_by_path(
            {lake: entries({"action_name": {"Shared": ["x"]}}),
             fga: entries({"action_name": {"R": ["y"]}})},
            {lake: entries({"action_name": {}}),
             fga: entries({"action_name": {"R": ["y"], "Shared": ["x"]}})},
        ),
        {},
    )
    check(
        "a deleted manifest is not masked by an unrelated one appearing",
        losses_by_path(
            {fga: entries({"action_name": {"R": ["can_assume"]}})},
            {"crates/other/wire_values.json": entries({"action_name": {"Other": ["x"]}})},
        ),
        {fga: ["action_name: R -> can_assume"]},
    )

    check(
        "a malformed manifest yields no entries rather than raising",
        manifest_entries({"action_name": "not-an-object", "entity_type": {"E": "not-a-list"}}),
        set(),
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
        metavar="LAKEKEEPER_VERSION",
        help="move the baseline to the version this release ships and clear the fragments",
    )
    args = parser.parse_args(argv)

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
