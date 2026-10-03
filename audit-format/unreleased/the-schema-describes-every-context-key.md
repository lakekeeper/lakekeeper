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

The object stays open: an action no branch names still validates, and those sets stay open the
way every vocabulary in this log is open — a new name may appear at any version, so treat one
you do not recognise as opaque.

**What to do:** nothing, unless you want it. Validating against the schema behaves as before.
If you generate parsers, the branches tell you which keys to expect on each action and in what
type, and the four closed sets generate as the enumerations they always were.
