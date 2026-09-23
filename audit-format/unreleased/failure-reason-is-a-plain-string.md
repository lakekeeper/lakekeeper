---
level: major
---

`failure_reason` on a denied authorization record is now the reason itself, a plain
string such as `"ActionForbidden"`. It was previously an object with the reason as
its single key and an empty array as its value: `{"ActionForbidden": []}`.

The set of reasons and their spelling are unchanged, so a consumer that read the
object's key can read the string directly. In `jq`, `.failure_reason | keys[0]`
becomes `.failure_reason`.
