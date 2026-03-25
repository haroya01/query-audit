# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.2.x   | Yes                |
| < 0.2   | No                 |

## Reporting a Vulnerability

If you discover a security vulnerability in Query Guard, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please use [GitHub Security Advisories](https://github.com/query-audit/query-audit/security/advisories/new) to report the vulnerability privately.

### What to include

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response timeline

- **Acknowledgment**: within 48 hours
- **Assessment**: within 1 week
- **Fix release**: within 2 weeks for critical issues

### Scope

Query Guard is a **test-time dependency** and does not run in production. Security issues most relevant to this project include:

- SQL injection through the library's own query generation (e.g., `SHOW INDEX`)
- Denial of service via crafted SQL input causing regex catastrophic backtracking
- Information disclosure through report output
- Dependency vulnerabilities in transitive dependencies
