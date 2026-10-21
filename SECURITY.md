# Security Policy

## Supported Versions

Security fixes are applied to the latest minor release only.

| Version | Supported |
| ------- | --------- |
| latest `0.x` minor | ✅ |
| older releases | ❌ |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report privately via GitHub Security Advisories:
https://github.com/lakekeeper/lakekeeper/security/advisories/new

Please include:
- Affected version(s) and deployment setup (authz backend, storage, catalog backend)
- Steps to reproduce or a proof of concept
- Impact assessment (e.g. privilege escalation, credential vending leak, authz bypass)

## Response Process

- **Acknowledgement:** reports are acknowledged as soon as possible after receipt
- **Initial assessment:** maintainers assess severity and confirm whether the issue is accepted
- **Status updates:** reporters receive regular updates until the issue is resolved
- **Fix and disclosure:** fixes are released and published via a GitHub Security Advisory, with a CVE requested where applicable
- **Coordinated disclosure:** the advisory is published together with the fix release. Reporters are asked not to disclose publicly until the advisory is published, and disclosure timing is agreed between the reporter and maintainers.

## Scope

In scope: Lakekeeper server, authorization (OpenFGA/Cedar integrations), credential vending, REST catalog API.
Out of scope: vulnerabilities in third-party dependencies without a demonstrated Lakekeeper-specific impact.