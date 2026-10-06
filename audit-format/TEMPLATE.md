---
level: minor
---

Replace all of this text. What you write is copied verbatim into the release notes, for someone parsing the log.

Before you write: read the fragments already in `unreleased/`. If one covers the field you
change, reword it to describe the final state (`git mv` it if its name does not fit). If
your change undoes one, delete it.

Write: what changed, in one sentence. Then the fields or values affected, as a list or a
`before` / `after` block, whichever is shorter. Label every code block `text`. Then:

**What to do:** the action the reader takes, addressed to them. "Nothing." is a complete
answer.

Address the reader directly. Leave out reasons and any history beyond the change itself.
