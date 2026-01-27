# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please report it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: chronicle-security@googlegroups.com

Include the following information in your report:

- Type of vulnerability (e.g., buffer overflow, SQL injection, XSS)
- Full paths of source files related to the vulnerability
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue and how an attacker might exploit it

### Response Timeline

- **Initial Response:** Within 48 hours
- **Status Update:** Within 7 days
- **Resolution Target:** Within 90 days (depending on complexity)

### What to Expect

1. We will acknowledge receipt of your report within 48 hours
2. We will investigate and provide an initial assessment
3. We will work with you to understand and reproduce the issue
4. We will develop and test a fix
5. We will coordinate disclosure timing with you
6. We will credit you in the security advisory (unless you prefer anonymity)

## Security Best Practices for Users

When using Chronicle in production:

1. **File Permissions:** Ensure database files have appropriate permissions
2. **Network Security:** If using the HTTP API, run behind a reverse proxy with TLS
3. **Input Validation:** Validate data before writing to the database
4. **Monitoring:** Monitor disk usage and set appropriate retention policies
5. **Updates:** Keep Chronicle updated to the latest version

## Security Features

Chronicle includes several security-conscious design decisions:

- No `unsafe` package usage in core code
- Request body size limits on HTTP endpoints
- Query timeout limits to prevent DoS
- Input validation on configuration parameters

## Disclosure Policy

- Security issues will be disclosed via GitHub Security Advisories
- CVE identifiers will be requested for significant vulnerabilities
- We follow responsible disclosure practices
