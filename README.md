# agent-kit-go

Shared Go code for standalone "agent API" services that pair with the
controller cluster (duplicacy-agent, gdrive-agent, future agents).

## Packages

- **scheduler** — per-agent cron scheduler with disk-cache, miss-policy
  catch-up, and a public refresh nudge. Mirrors the design that originated
  in `duplicacy-agent-api/schedules.go`. Each agent supplies a `FireFunc`
  callback that decides what to do when a schedule matches; everything
  else (cron matching, controller pull loop, disk persistence,
  missed-run recovery) is shared.

## Why this exists

Per `/home/user/ansible/duplicacy-agent-api/CLAUDE.md` and the
controller `CLAUDE.md` notes on standalone agent APIs, every agent
follows the same operational pattern: outbound HTTP via the host's local
Traefik with bearer auth, inbound mTLS on a hostname like
`<prefix>-agent.<host>.<domain>`, and per-host cron schedules pulled from
controller. The pattern was previously copied byte-for-byte between
agents. This module factors out the parts that don't differ between
agents so the next agent only writes its own job runner.

## Versioning

Tagged releases (v0.1.x, ...). Each agent pins a specific version in its
`go.mod`. Breaking changes get a new major.
