#!/usr/bin/env python3
"""Check that a change to the audit log format carries the right `audit_format` bump.

The fixture tests classify a format change, but only until someone regenerates the
fixtures — after `just update-audit-fixtures` they pass whether the version was bumped
correctly, bumped the wrong way, or not touched at all. Nothing else relates the version
(the claim) to the fixtures (the evidence), which is what this does.

It needs both the old and the new state, so it lives in CI rather than in a Rust test.

Run locally:   python3 .github/scripts/check-audit-format-bump.py <base-ref>
Self-test:     python3 .github/scripts/check-audit-format-bump.py --self-test
Read version:  python3 .github/scripts/check-audit-format-bump.py --print-version <rev>
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

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


class AmbiguousVersion(Exception):
    """More than one file declares the version, so no single one is authoritative."""


def declared_versions(rev: str) -> dict[str, tuple[int, int]]:
    """Every declaration of the version at `rev`, keyed by the file that holds it.

    Searched across the tree rather than read from a fixed path, so that moving the module
    does not look like the version disappearing. Returned per path rather than as a single
    value because "the first match" is not a safe answer: any other text in `crates/`
    matching the pattern — a doc comment quoting it, for instance — would silently become
    the version this checker scores the change against.
    """
    try:
        out = _git("grep", "-E", GIT_PATTERN, rev, "--", "crates/")
    except subprocess.CalledProcessError:
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
    """The one declared version, or None if there is none. Raises if there is more than one.

    Agreeing values do not make two declarations acceptable: they are two places a future
    bump has to be applied, and which one this checker reads is decided by sort order.

    Separate from the git lookup so the rule can be tested without a repository.
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


def fixture_dirs_at(rev: str) -> list[str]:
    """The fixture directories that exist at `rev`, as path prefixes."""
    prefix = f"{AUDIT_DIR}/fixtures/"
    try:
        listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    except subprocess.CalledProcessError:
        return []
    dirs = {
        path[: len(prefix) + path[len(prefix) :].index("/") + 1]
        for path in listing.splitlines()
        if path.endswith(".json") and "/" in path[len(prefix) :]
    }
    return sorted(dirs)


def fixtures_at(rev: str, prefix: str) -> dict[str, object]:
    """The committed fixtures under `prefix` at `rev`, as parsed JSON."""
    try:
        listing = _git("ls-tree", "-r", "--name-only", rev, "--", prefix)
    except subprocess.CalledProcessError:
        return {}
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

    Values are discarded deliberately. A fixture's values change for reasons that are
    not format changes — a scenario gets more realistic, an id is made deterministic —
    and demanding a version bump for those would be wrong. Array elements collapse onto
    one pointer, so arity is not a shape change either.

    Containers record their own type as well as their contents, so that an empty object
    and an empty array are distinguishable from each other and from an absent field.
    """
    out: set[str] = set()
    if isinstance(value, dict):
        # The container's own type is recorded before its contents. Without this an empty
        # object, an empty array and an absent field all reduce to nothing, so `{}`
        # becoming `[]` reads as no change, and `{}` becoming a scalar reads as *additive*
        # — a breaking type change classified in the permissive direction.
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

    Only fixtures present under both names are compared. A fixture that was added
    describes a scenario that was previously untested, not a format that was previously
    different; one that was renamed, merged away or deleted takes its evidence with it.

    Hence `unknown`, which is the important case: finding no difference is only meaningful
    if everything was actually compared. If a fixture that existed before has no
    counterpart now, the absence of a detected change proves nothing, and saying `none`
    there would report "the format did not change" on the strength of having looked at
    nothing. Positive findings are still trusted — a breaking difference in a pair that
    *did* match is real regardless of what happened to the others.
    """
    compared = sorted(set(base) & set(head))
    verdict = "none"
    for name in compared:
        if base[name] - head[name]:
            return "breaking"
        if head[name] - base[name]:
            verdict = "additive"
    # Reporting "no change" requires having compared something, and having accounted for
    # everything that existed before. Zero comparisons is not a clean bill of health.
    #
    # A fixture whose name changed but whose shape did not is still accounted for: match
    # the leftovers by shape before giving up, so that renaming a fixture — which is a
    # normal thing to do when a scenario is described better — does not cost the check its
    # verdict. A fixture that was renamed *and* changed will not match, and correctly
    # leaves the result unknown.
    unmatched_before = [base[n] for n in set(base) - set(head)]
    leftovers_after = [head[n] for n in set(head) - set(base)]
    for shapes in unmatched_before:
        if shapes in leftovers_after:
            leftovers_after.remove(shapes)
        else:
            return "unknown"
    if verdict == "none" and not compared and not unmatched_before:
        return "unknown"
    return verdict


# ── version bump ────────────────────────────────────────────────────────────────


def classify_bump(
    base: tuple[int, int] | None, head: tuple[int, int] | None
) -> tuple[str, str | None]:
    """`introduced`, `none`, `minor`, or `major`; or an error describing an illegal bump."""
    if head is None:
        return (
            "none",
            "AUDIT_FORMAT is gone. It must be declared on every audit record.",
        )
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
    ("none", "minor"): (
        False,
        "The version was bumped but the format did not change. Leave AUDIT_FORMAT alone: "
        "a bump tells every consumer to re-check their parser, so an empty one costs "
        "them work for nothing.",
    ),
    ("none", "major"): (
        False,
        "The major version was bumped but the format did not change. Leave AUDIT_FORMAT "
        "alone: a major bump tells consumers their parser is broken.",
    ),
    ("additive", "none"): (
        False,
        "Fields were added and nothing else changed, but the version is unchanged. Bump "
        "the MINOR half so consumers can tell which builds carry the new fields.",
    ),
    ("additive", "minor"): (True, "Additive change, minor bump. Correct."),
    ("additive", "major"): (
        False,
        "Only additive changes were made, but the MAJOR version was bumped. A major bump "
        "tells consumers their existing parser will break, and it forces a new fixture "
        "directory that has to be kept green forever. Bump the MINOR instead.",
    ),
    ("breaking", "none"): (
        False,
        "A field was removed, renamed, or retyped, which breaks existing consumers, but "
        "the version is unchanged. Bump the MAJOR half.",
    ),
    ("breaking", "minor"): (
        False,
        "A field was removed, renamed, or retyped, which breaks existing consumers. A "
        "minor bump says the opposite — that old parsers keep working. Bump the MAJOR.",
    ),
    ("breaking", "major"): (True, "Breaking change, major bump. Correct."),
}


def decide(shape_kind: str, bump_kind: str) -> tuple[bool, str]:
    if bump_kind == "introduced":
        return True, "AUDIT_FORMAT is being introduced; there is no previous version."
    if shape_kind == "unknown":
        # Deliberately permissive. Fixtures were renamed, merged or removed, so there is
        # no basis for a verdict; asserting one would fail legitimate changes. The Rust
        # fixture tests still compare emitted output against the committed files, so the
        # change itself is not unchecked — only the *bump* is unverified here.
        return True, (
            "Could not verify the bump: fixtures present before have no counterpart now "
            "(renamed, merged or removed), so there was nothing to compare them against. "
            "Check by hand that the version bump matches what actually changed."
        )
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

    base_dirs, head_dirs = fixture_dirs_at(merge_base), fixture_dirs_at(head_ref)
    # More than one is ambiguous and none at head means the checker has nothing to work
    # with. Either is a broken setup rather than an unverifiable change, so say so loudly
    # instead of deferring — deferring is how this went unnoticed.
    if len(head_dirs) != 1:
        print(
            f"::error::expected exactly one audit fixture directory under "
            f"{AUDIT_DIR}/fixtures/, found {len(head_dirs)}: {head_dirs}. This checker "
            f"cannot compare anything without one, so fix the layout rather than "
            f"trusting a pass here."
        )
        return 1
    head_prefix = head_dirs[0]
    base_prefix = base_dirs[0] if len(base_dirs) == 1 else head_prefix
    base_shapes = {n: shape(r) for n, r in fixtures_at(merge_base, base_prefix).items()}
    head_shapes = {n: shape(r) for n, r in fixtures_at(head_ref, head_prefix).items()}
    shape_kind = classify_shape(base_shapes, head_shapes)
    compared = sorted(set(base_shapes) & set(head_shapes))
    short = lambda d: d.rstrip("/").rsplit("/", 1)[-1]
    print(
        f"Fixtures:   {short(base_prefix)} -> {short(head_prefix)}, {len(compared)} "
        f"compared ({len(base_shapes)} before, {len(head_shapes)} after) => {shape_kind}"
    )

    ok, message = decide(shape_kind, bump_kind)
    if ok:
        print(f"OK: {message}")
        return 0
    print(f"::error::{message}")
    print(
        "\nSee the audit log section of docs/docs/developer-guide.md.", file=sys.stderr
    )
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
                ("none", "none"),
                ("additive", "minor"),
                ("breaking", "major"),
            }
            check(
                f"decide({shape_kind}, {bump_kind})",
                decide(shape_kind, bump_kind)[0],
                legal,
            )

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
    # A pure rename is recoverable: same shape under a new name is still accounted for.
    check("pure rename", classify_shape({"old": shape(a)}, {"new": shape(a)}), "none")
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
    check(
        "additive despite a rename",
        classify_shape(
            {"f": shape({"a": 1}), "g": shape(a)},
            {"f": shape({"a": 1, "b": 2}), "h": shape(a)},
        ),
        "additive",
    )
    # `unknown` defers rather than guessing, in either direction.
    for bump in ("none", "minor", "major"):
        check(f"unknown + {bump} defers", decide("unknown", bump)[0], True)

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


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        sys.exit(self_test())
    if len(sys.argv) == 3 and sys.argv[1] == "--print-version":
        try:
            sys.exit(print_version(sys.argv[2]))
        except AmbiguousVersion as error:
            print(f"::error::{error}", file=sys.stderr)
            sys.exit(2)
        except subprocess.CalledProcessError as error:
            print(f"::error::`{' '.join(error.cmd)}` failed.", file=sys.stderr)
            sys.exit(2)
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        sys.exit(2)
    try:
        sys.exit(run(sys.argv[1], *sys.argv[2:]))
    except AmbiguousVersion as error:
        # A broken declaration, not a rejected bump — exit 2 so the two are distinguishable.
        print(f"::error::{error}", file=sys.stderr)
        sys.exit(2)
    except subprocess.CalledProcessError as error:
        # A bad revision or a shallow clone is a usage problem, not a format problem, and
        # a raw traceback in a CI log obscures which of the two happened.
        command = " ".join(error.cmd)
        print(
            f"::error::`{command}` failed. Check the revisions exist and that the "
            f"checkout has enough history (the workflow uses fetch-depth: 0).",
            file=sys.stderr,
        )
        print((error.stderr or "").strip(), file=sys.stderr)
        sys.exit(2)
