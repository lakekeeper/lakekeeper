---
level: major
---

**A `context` value may be an object, not only a string.**

```
before  "context": {"dry_run": "true", "applied": "{\"created\":1,\"deleted\":0}"}
after   "context": {"dry_run": "true", "applied": {"created": 1, "deleted": 0}}
```

No key Lakekeeper declares carries an object, so a record from a plain Lakekeeper deployment
is unchanged. A product that plugs in may declare one. The schema says which: a key
vocabulary lists its shaped keys under `x-audit-key-shapes`, each pointing at a definition in
the schema of whoever declared the key. A key absent from that map carries a string.

**What to do:** stop assuming a `context` value is a string. Check the type where you read
one, or read `x-audit-key-shapes` for the keys you consume.
