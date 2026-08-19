#!/usr/bin/env python3
"""Check that a change to the audit log format carries the right `audit_format` bump.

The fixture tests classify a format change, but only until someone regenerates the
fixtures — after `just update-audit-fixtures` they pass whether the version was bumped
correctly, bumped the wrong way, or not touched at all. Nothing else relates the version
(the claim) to the fixtures (the evidence), which is what this does.

It needs both the old and the new state, so it lives in CI rather than in a Rust test.

Run locally:   python3 .github/scripts/check-audit-format-bump.py <base-ref>
Self-test:     python3 .github/scripts/check-audit-format-bump.py --self-test
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

AUDIT_DIR = "crates/lakekeeper/src/service/events/backends/audit"
# Two patterns for the same thing: `git grep -E` is POSIX ERE, which has no `\s`.
GIT_PATTERN = r'AUDIT_FORMAT: &str = "[0-9]+\.[0-9]+"'
VERSION_RE = re.compile(r'AUDIT_FORMAT:\s*&str\s*=\s*"(\d+)\.(\d+)"')


# ── git plumbing ────────────────────────────────────────────────────────────────

def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def declared_version(rev: str) -> tuple[int, int] | None:
    """The version declared at `rev`, or None if the field does not exist yet.

    Searched across the tree rather than read from a fixed path, so that moving the
    module does not look like the version disappearing.
    """
    try:
        out = _git("grep", "-h", "-E", GIT_PATTERN, rev, "--", "crates/")
    except subprocess.CalledProcessError:
        return None
    match = VERSION_RE.search(out)
    return (int(match.group(1)), int(match.group(2))) if match else None


def fixtures_at(rev: str, major: int) -> dict[str, object]:
    """The committed fixtures for format major version `major`, as parsed JSON."""
    prefix = f"{AUDIT_DIR}/fixtures/v{major}/"
    try:
        listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    except subprocess.CalledProcessError:
        return {}
    out = {}
    for path in listing.splitlines():
        if not path.endswith(".json"):
            continue
        name = path[len(prefix):-len(".json")]
        out[name] = json.loads(_git("show", f"{rev}:{path}"))
    return out


# ── shape ───────────────────────────────────────────────────────────────────────

def shape(value: object, path: str = "") -> set[str]:
    """Reduce a record to `<pointer>\\t<json type>` entries.

    Values are discarded deliberately. A fixture's values change for reasons that are
    not format changes — a scenario gets more realistic, an id is made deterministic —
    and demanding a version bump for those would be wrong. Array elements collapse onto
    one pointer, so arity is not a shape change either.
    """
    out: set[str] = set()
    if isinstance(value, dict):
        for key, sub in value.items():
            out |= shape(sub, f"{path}/{key}")
    elif isinstance(value, list):
        for sub in value:
            out |= shape(sub, f"{path}[]")
    else:
        kind = "null" if value is None else type(value).__name__
        out.add(f"{path}\t{kind}")
    return out


def classify_shape(base: dict[str, set[str]], head: dict[str, set[str]]) -> str:
    """`none`, `additive`, or `breaking`, over the fixtures present in both revisions.

    Fixtures added or removed are coverage changes rather than format changes, so they
    are not compared: a new fixture describes a scenario that was previously untested,
    not a format that previously differed.
    """
    verdict = "none"
    for name in sorted(set(base) & set(head)):
        removed = base[name] - head[name]
        added = head[name] - base[name]
        if removed:
            return "breaking"
        if added:
            verdict = "additive"
    return verdict


# ── version bump ────────────────────────────────────────────────────────────────

def classify_bump(
    base: tuple[int, int] | None, head: tuple[int, int] | None
) -> tuple[str, str | None]:
    """`introduced`, `none`, `minor`, or `major`; or an error describing an illegal bump."""
    if head is None:
        return "none", "AUDIT_FORMAT is gone. It must be declared on every audit record."
    if base is None:
        return "introduced", None

    (base_major, base_minor), (head_major, head_minor) = base, head
    if (head_major, head_minor) == (base_major, base_minor):
        return "none", None
    if (head_major, head_minor) < (base_major, base_minor):
        return "none", (
            f"the version went backwards, {base_major}.{base_minor} -> "
            f"{head_major}.{head_minor}. Consumers key on it increasing."
        )
    if head_major != base_major:
        if head_major != base_major + 1:
            return "major", (
                f"the major version jumped {base_major} -> {head_major}. Bump it by one."
            )
        if head_minor != 0:
            return "major", (
                f"a major bump resets the minor to zero, so {base_major}.{base_minor} -> "
                f"{head_major}.0, not {head_major}.{head_minor}. Bumping both at once "
                f"says two different things happened."
            )
        return "major", None
    if head_minor != base_minor + 1:
        return "minor", (
            f"the minor version jumped {base_minor} -> {head_minor}. Bump it by one."
        )
    return "minor", None


# ── the decision ────────────────────────────────────────────────────────────────

DECISIONS: dict[tuple[str, str], tuple[bool, str]] = {
    ("none", "none"): (True, "No format change and no version change."),
    ("none", "minor"): (False,
        "The version was bumped but the format did not change. Leave AUDIT_FORMAT alone: "
        "a bump tells every consumer to re-check their parser, so an empty one costs "
        "them work for nothing."),
    ("none", "major"): (False,
        "The major version was bumped but the format did not change. Leave AUDIT_FORMAT "
        "alone: a major bump tells consumers their parser is broken."),
    ("additive", "none"): (False,
        "Fields were added and nothing else changed, but the version is unchanged. Bump "
        "the MINOR half so consumers can tell which builds carry the new fields."),
    ("additive", "minor"): (True, "Additive change, minor bump. Correct."),
    ("additive", "major"): (False,
        "Only additive changes were made, but the MAJOR version was bumped. A major bump "
        "tells consumers their existing parser will break, and it forces a new fixture "
        "directory that has to be kept green forever. Bump the MINOR instead."),
    ("breaking", "none"): (False,
        "A field was removed, renamed, or retyped, which breaks existing consumers, but "
        "the version is unchanged. Bump the MAJOR half."),
    ("breaking", "minor"): (False,
        "A field was removed, renamed, or retyped, which breaks existing consumers. A "
        "minor bump says the opposite — that old parsers keep working. Bump the MAJOR."),
    ("breaking", "major"): (True, "Breaking change, major bump. Correct."),
}


def decide(shape_kind: str, bump_kind: str) -> tuple[bool, str]:
    if bump_kind == "introduced":
        return True, "AUDIT_FORMAT is being introduced; there is no previous version."
    return DECISIONS[(shape_kind, bump_kind)]


# ── entry points ────────────────────────────────────────────────────────────────

def run(base_ref: str, head_ref: str = "HEAD") -> int:
    merge_base = _git("merge-base", base_ref, head_ref).strip()
    base_version = declared_version(merge_base)
    head_version = declared_version(head_ref)

    bump_kind, bump_error = classify_bump(base_version, head_version)
    fmt = lambda v: "absent" if v is None else f"{v[0]}.{v[1]}"
    print(f"Merge base: {merge_base}")
    print(f"Version:    {fmt(base_version)} -> {fmt(head_version)} ({bump_kind})")

    if bump_error:
        print(f"::error::Illegal audit_format version change: {bump_error}")
        return 1

    base_major = base_version[0] if base_version else (head_version[0] if head_version else 1)
    head_major = head_version[0] if head_version else base_major
    base_shapes = {n: shape(r) for n, r in fixtures_at(merge_base, base_major).items()}
    head_shapes = {n: shape(r) for n, r in fixtures_at(head_ref, head_major).items()}
    shape_kind = classify_shape(base_shapes, head_shapes)
    compared = sorted(set(base_shapes) & set(head_shapes))
    print(f"Fixtures:   v{base_major} -> v{head_major}, {len(compared)} compared "
          f"({len(base_shapes)} before, {len(head_shapes)} after) => {shape_kind}")

    # A major bump must leave the previous version's fixtures in place: someone replaying
    # older logs still needs them to be correct.
    if bump_kind == "major" and not fixtures_at(head_ref, base_major):
        print(f"::error::The major version was bumped but fixtures/v{base_major}/ is gone. "
              f"Keep it, and keep it passing — old records do not stop existing.")
        return 1

    ok, message = decide(shape_kind, bump_kind)
    if ok:
        print(f"OK: {message}")
        return 0
    print(f"::error::{message}")
    print("\nSee the audit log section of docs/docs/developer-guide.md.", file=sys.stderr)
    return 1


def self_test() -> int:
    failures = []

    def check(label, got, want):
        if got != want:
            failures.append(f"{label}: got {got!r}, want {want!r}")

    # Every combination the decision table has to answer for.
    for shape_kind in ("none", "additive", "breaking"):
        for bump_kind in ("none", "minor", "major"):
            legal = (shape_kind, bump_kind) in {
                ("none", "none"), ("additive", "minor"), ("breaking", "major")}
            check(f"decide({shape_kind}, {bump_kind})", decide(shape_kind, bump_kind)[0], legal)

    # Bumping both halves at once, and other malformed bumps.
    check("major resets minor", classify_bump((1, 4), (2, 1))[1] is not None, True)
    check("major bump ok", classify_bump((1, 4), (2, 0)), ("major", None))
    check("minor bump ok", classify_bump((1, 4), (1, 5)), ("minor", None))
    check("minor skip", classify_bump((1, 4), (1, 7))[1] is not None, True)
    check("major skip", classify_bump((1, 0), (3, 0))[1] is not None, True)
    check("downgrade", classify_bump((2, 0), (1, 9))[1] is not None, True)
    check("unchanged", classify_bump((1, 4), (1, 4)), ("none", None))
    check("introduced", classify_bump(None, (1, 0)), ("introduced", None))
    check("removed", classify_bump((1, 0), None)[1] is not None, True)

    # Shape classification, including the cases that must NOT count as format changes.
    a = {"x": {"a": 1}}
    check("identical", classify_shape({"f": shape(a)}, {"f": shape(a)}), "none")
    check("value changed", classify_shape({"f": shape({"x": {"a": 1}})},
                                          {"f": shape({"x": {"a": 2}})}), "none")
    check("bool flipped", classify_shape({"f": shape({"ok": True})},
                                         {"f": shape({"ok": False})}), "none")
    check("arity grew", classify_shape({"f": shape({"xs": [{"a": 1}]})},
                                       {"f": shape({"xs": [{"a": 1}, {"a": 2}]})}), "none")
    check("field added", classify_shape({"f": shape({"a": 1})},
                                        {"f": shape({"a": 1, "b": 2})}), "additive")
    check("field removed", classify_shape({"f": shape({"a": 1, "b": 2})},
                                          {"f": shape({"a": 1})}), "breaking")
    check("field renamed", classify_shape({"f": shape({"a-b": 1})},
                                          {"f": shape({"a_b": 1})}), "breaking")
    check("type changed", classify_shape({"f": shape({"a": 1})},
                                         {"f": shape({"a": "1"})}), "breaking")
    check("null vs string", classify_shape({"f": shape({"a": None})},
                                           {"f": shape({"a": "x"})}), "breaking")
    check("nested added", classify_shape({"f": shape({"a": {"b": 1}})},
                                         {"f": shape({"a": {"b": 1, "c": 2}})}), "additive")
    check("fixture added", classify_shape({"f": shape(a)}, {"f": shape(a), "g": shape(a)}), "none")
    check("fixture removed", classify_shape({"f": shape(a), "g": shape(a)}, {"f": shape(a)}), "none")

    for line in failures:
        print(f"FAIL {line}")
    print(f"\n{'FAILED' if failures else 'all self-tests passed'} "
          f"({len(failures)} failure(s))")
    return 1 if failures else 0


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        sys.exit(self_test())
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        sys.exit(2)
    sys.exit(run(sys.argv[1], *sys.argv[2:]))
