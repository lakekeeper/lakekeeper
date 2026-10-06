# Audit log format changes

`AUDIT_FORMAT` is not edited by hand and does not move once per pull request. It is
derived:

```text
AUDIT_FORMAT = the version the last release tag declares, raised once by the highest level
               among the fragments in unreleased/ that tag does not carry
```

So a release raises the audit format version at most once however many changes it
carries, and a major change absorbs every minor change in the same cycle.

## Files

| Path | What |
|------|------|
| `unreleased/*.md` | One fragment per change: its level, and prose for the release notes. Written in the pull request that makes the change. A fragment the last release tag carries shipped with it and is cleared by `just audit-format-release`. |
| `TEMPLATE.md` | What a fragment looks like. Not a fragment — only `unreleased/*.md` is read. |

## Writing a fragment

Copy `TEMPLATE.md` to `unreleased/<something-descriptive>.md`, set `level`, and write
the prose. Then run `just update-audit-fixtures`, which computes `AUDIT_FORMAT` from
these files and writes it for you.

`level` is one of:

| Level | Meaning |
|-------|---------|
| `major` | An existing parser breaks: a field removed, renamed or retyped, or a wire value renamed. |
| `minor` | An existing parser keeps working: a field added. |
| `none` | The format did not move, but operators should still hear about it — a new action or entity value, for instance. Optional; nothing requires one. |

The full rules, including which changes are which, are in the audit log section of
`docs/docs/developer-guide.md`.
