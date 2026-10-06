---
level: minor
---

**The schema says which `context` keys each action can carry, and what each one holds.**

An action's name and its context keys sit in one flat object, so the schema states the pairing
with ordinary JSON Schema conditionals — one `if`/`then` per action under `ActionRecord`:

```text
"if":   {"properties": {"action_name": {"const": "drop"}}}
"then": {"properties": {"force": {"type": "boolean"}, "purge": {"type": "boolean"}}}
```

Every key is typed. A key drawn from a closed set points at that set rather than saying
`string` — `root_level`, `privilege_scope`, `resource_types` and `update_kinds` do — and every
list says what it holds:

```text
"root_level":     {"$ref": "#/$defs/RootLevelGrants"}
"resource_types": {"type": "array", "items": {"$ref": "#/$defs/ResourceType"}}
```

The record's own `context` object is typed the same way — its definition lists each key with
the type it holds:

```text
"HandlerContext": {"properties": {"self_read": {"type": "boolean"}, …},
                   "additionalProperties": true}
```

Each product describes its own half. A deployment running another product on top finds that
product's actions and `context` keys in *its* schema, under definitions of the same names, and
`emitters` on the record says whose schemas apply.

Everything stays open: an action no branch names still validates, and `context` still accepts keys
from any product.

**What to do:** nothing, unless you want it. Validating against the schema behaves as before
for anything you already send. If you generate parsers, the branches tell you which keys to
expect on each action and in what type, the four closed sets generate as enumerations, and the
`context` keys are no longer untyped. For a record naming more than one
product, read each product's schema for the half it contributes.
