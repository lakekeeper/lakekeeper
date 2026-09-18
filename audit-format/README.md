# Audit log format changes

`AUDIT_FORMAT` is not edited by hand and does not move once per pull request. It is
derived:

```text
AUDIT_FORMAT = released.json
```

raised once by the highest level in unreleased/

So a release raises the audit format version at most once however many changes it
carries, and a major change absorbs every minor change in the same cycle.

## Files

| Path | What |
|------|------|
| `released.json` | The audit format version the most recent release on this branch shipped, and which release that was. `null` until the first release carries one. Maintained by the release recipe. |
| `unreleased/*.md` | One fragment per change: its level, and prose for the release notes. Written in the pull request that makes the change. |
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
