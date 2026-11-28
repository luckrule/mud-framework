# Security Policy

## Supported versions

This project is pre-release. No version is supported for production use. Report issues against the `main` branch and include the commit SHA.

## Reporting a vulnerability

Do not open a public issue. Use GitHub private vulnerability reporting:

https://github.com/luckrule/mud-framework/security/advisories/new

Include:

- affected commit or version
- reproduction steps or proof of concept
- impact and affected surface (core, connector-ws, storage-sqlite, Portal-Server IPC, event delivery, hot upgrade)
- any suggested fix

Reports are acknowledged on a best-effort basis. Disclosure is coordinated after a fix is available. No SLA is offered.

## Scope

In scope: framework core, WebSocket transport, SQLite backend, Portal-Server IPC, process lifecycle, and hot upgrade.

Out of scope: third-party deployments, game logic built on the framework, and dependency vulnerabilities without a demonstrated impact.
