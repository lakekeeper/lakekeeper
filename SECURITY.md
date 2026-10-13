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

- Acknowledgement within <N> business days
- Initial assessment within <N> business days
- Fix and coordinated disclosure via a GitHub Security Advisory, with CVE requested where applicable

## Scope

In scope: Lakekeeper server, authorization (OpenFGA/Cedar integrations), credential vending, REST catalog API.
Out of scope: vulnerabilities in third-party dependencies without a demonstrated Lakekeeper-specific impact.