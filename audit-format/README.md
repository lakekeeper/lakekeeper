# Audit log format changes

`AUDIT_FORMAT` is derived from the last release tag and the fragments in `unreleased/`. Never edit it by hand. The audit log section of `docs/docs/developer-guide.md` has the full rules.

## Files

| Path | What |
|------|------|
| `unreleased/*.md` | One fragment per change: its level, and text for the release notes. Written in the pull request that makes the change. Fragments the last release tag carries with the same text shipped with it; `just audit-format-release` clears them. |
| `TEMPLATE.md` | What a fragment looks like. Not a fragment: only `unreleased/*.md` is read. |

## Writing a fragment

Read the fragments already in `unreleased/` first; extend one that covers the same field. Otherwise copy `TEMPLATE.md` to `unreleased/<something-descriptive>.md`, set `level`, and write the text. Then run `just update-audit-fixtures`, which computes `AUDIT_FORMAT` and writes it.

`level` is one of:

| Level | Meaning |
|-------|---------|
| `major` | An existing parser can break: a field removed, renamed or retyped, a value renamed or removed, or a value added to a `closed` set. |
| `minor` | An existing parser keeps working: a field or key added. |
| `none` | The format did not change, but operators should hear about it, for example a new action or entity type. Optional. |
