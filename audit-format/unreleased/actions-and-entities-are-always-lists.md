---
level: major
---

`action` and `entity` are gone from the top level of authorization and replay records.
They are replaced by `actions` and `entities`, which are always arrays and always
present, however many elements they hold.

Previously the field *name* changed with the element count: exactly one action was
written as `action` holding an object, and any other number — none, or several — as
`actions` holding an array. A query written against one spelling silently returned
nothing for the other.

Every key inside those objects moved with them, which is where most of the work is:
`.entity.warehouse-id` becomes `.entities[0].warehouse-id`, `.action.action_name`
becomes `.actions[0].action_name`. Most real queries address the inner keys rather
than the container. Where a record can carry more than one, iterate rather than
taking the first.

The per-decision entries inside `authorizations[]` are unchanged: each still carries a
singular `action` and `entity`, because each entry describes exactly one of each.
