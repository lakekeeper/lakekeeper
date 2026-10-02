---
level: minor
---

**The schema says which `context` keys each action can carry.**

An action's name and its context keys sit in one flat object, so the schema states the pairing
with ordinary JSON Schema conditionals — one `if`/`then` per action under `ActionRecord`:

```
"if":   {"properties": {"action_name": {"const": "drop"}}}
"then": {"properties": {"force": {"type": "string"}, "purge": {"type": "string"}}}
```

Each key is typed, so you know whether to expect a string, an array or an object before you
read one. The object stays open: an action no branch names still validates, and a key is
listed because it *can* appear, not because it always does — a key absent from a record was
not asked for by that request.

**What to do:** nothing, unless you want it. Validating against the schema behaves as before.
If you generate parsers or build a reader per action, the branches tell you which keys to
expect on each one instead of having to infer it from traffic.
