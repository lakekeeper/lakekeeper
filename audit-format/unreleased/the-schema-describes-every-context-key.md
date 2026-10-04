---
level: minor
---

**The schema says which `context` keys each action can carry, and what each one holds.**

An action's name and its context keys sit in one flat object, so the schema states the pairing
with ordinary JSON Schema conditionals — one `if`/`then` per action under `ActionRecord`:

```
"if":   {"properties": {"action_name": {"const": "drop"}}}
"then": {"properties": {"force": {"type": "boolean"}, "purge": {"type": "boolean"}}}
```

Every key is typed. A key drawn from a closed set points at that set rather than saying
`string` — `root_level`, `privilege_scope`, `resource_types` and `update_kinds` do — and every
list says what it holds:

```
"root_level":     {"$ref": "#/$defs/RootLevelGrants"}
"resource_types": {"type": "array", "items": {"$ref": "#/$defs/ResourceType"}}
```

The record's own `context` object is typed the same way. Its definition lists the keys
Lakekeeper declares with their types, and every key vocabulary — Lakekeeper's and any other
product's — carries `x-audit-key-types`, mapping each of its keys to the type it holds:

```
"HandlerContext":    {"properties": {"self_read": {"type": "boolean"}, …}}
"HandlerContextKey": {"x-audit-key-types": {"self_read": "boolean", "queue_name": "string", …}}
```

Everything stays open: an action no branch names still validates, `context` still accepts keys
from any product, and those value sets stay open the way every vocabulary in this log is open —
a new name may appear at any version, so treat one you do not recognise as opaque.

**What to do:** nothing, unless you want it. Validating against the schema behaves as before
for anything you already send. If you generate parsers, the branches tell you which keys to
expect on each action and in what type, the four closed sets generate as the enumerations they
always were, and the `context` keys are no longer untyped. Read `x-audit-key-types` on a
product's own key vocabulary for the keys it contributes.
