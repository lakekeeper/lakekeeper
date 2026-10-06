---
level: none
---

**In the schema, every key of an `entity`, an action and the `context` object is a documented property of that object, and the key-set definitions are gone.**

```text
before  "EntityRecord": {"additionalProperties": {"type": "string"}}
        "EntityField": {"x-audit-kind": "keys", "enum": ["namespace", "table", …]}
after   "EntityRecord": {"properties": {"table": {"type": "string", "description": "…"}, …}}
```

- `EntityRecord` lists each entity key under `properties`, with its description. It still accepts other keys.
- Each `if`/`then` branch of `ActionRecord` gives every key it lists a description.
- `HandlerContext` lists each `context` key under `properties`, as before.
- The definitions `EntityField`, `ActionContextKey` and `HandlerContextKey` are removed, with the keywords `x-audit-keys-of` and `x-audit-key-shapes`.

**What to do:** Nothing for records. If you read keys from the removed definitions, read them from the object's `properties` and from the `ActionRecord` branches.
