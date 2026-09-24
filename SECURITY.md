# Security Policy

## Supported Versions

This project is in a pre-release "proposed/discovery" stage. Only the code currently on each environment branch is supported — there is no long-term support for older commits.

| Branch | Environment | Status |
|---|---|---|
| `main` | Production | Supported |
| `staging` | Staging | Supported |
| `develop` | Development | Supported |

## Reporting a Vulnerability

This repository doesn't have a public issue tracker, so please don't report security concerns that way. Use one of:

- GitHub's [private vulnerability reporting](https://github.com/NelsonGrey/unified-distributed-data-platform/security/advisories/new) (enabled on this repo), or
- Email **support@nelsongrey.com**

Either way, include:

- A description of the vulnerability and its potential impact
- Steps to reproduce, or a proof of concept if available
- Any relevant logs, request/response samples, or affected endpoints

You should get an acknowledgement within a few business days.

## Automated Dependency Scanning

Dependabot alerts and security updates, and native GitHub secret scanning (with push protection) are enabled on this repository. Code scanning (CodeQL) is not yet configured here. Avoid committing credentials or secrets regardless — no secrets manager is wired up yet, so keep runtime credentials in local environment variables or your deployment platform's secret store, never in version control.

Note for this project specifically: the [Technical Requirements](docs/TECHNICAL_REQUIREMENTS.md#7-security-and-privacy) and [Traceability and Validation](docs/TRACEABILITY_AND_VALIDATION.md#required-test-suites) docs define the target threat model, secret-handling design, and security test suite for the platform itself (tenant isolation, key rotation, parser fuzzing, penetration testing as a GA gate). This policy covers vulnerabilities in this repository's own code and CI configuration in the meantime — it is not a substitute for that eventual review.
