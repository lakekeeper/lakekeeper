---
level: minor
---

**A `context` key whose value comes from a closed set now says which set, and every list says what it holds.**

Four keys carry a value this log draws from a fixed vocabulary. The schema already published each
vocabulary; now the key points at it:

```
before  "root_level":     {"type": "string"}
after   "root_level":     {"$ref": "#/$defs/RootLevelGrants"}

before  "resource_types": {"type": "array"}
after   "resource_types": {"type": "array", "items": {"$ref": "#/$defs/ResourceType"}}
```

The keys are `root_level`, `privilege_scope`, `resource_types` and `update_kinds`. Every other list
in an action now carries `"items": {"type": "string"}` rather than no `items` at all.

No value changed. These sets stay open the way every other vocabulary in this log is open: a new
name may appear at any version, so treat one you do not recognise as opaque.

**What to do:** nothing. If you generate code from the schema, these four keys now generate as the
enums they always were, and lists generate as lists of strings.
