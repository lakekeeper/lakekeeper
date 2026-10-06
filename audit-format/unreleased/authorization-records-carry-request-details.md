---
level: minor
---

**Authorization records carry three new optional fields about the request: `user_agent`, `break_glass` and `idempotency_key`.**

- `user_agent`: the request's `User-Agent` header, as sent and unverified.
- `break_glass`: the reason the caller stated in the `x-break-glass` header, as sent and unverified.
- `idempotency_key`: the request's `Idempotency-Key`.

Each is absent when the request did not send it.

**What to do:** nothing. Treat `user_agent` and `break_glass` as claims, never as identity.
