# ADR 0001: Provider Authorization And Resource Model

Status: Accepted for implementation planning

Date: 2026-07-17

## Context

The phase-3 baseline is a workspace-based TikTok analytics application with MariaDB-backed sessions, CSRF, RBAC, encrypted TikTok credentials, background sync, snapshots, and dashboard APIs. The legacy TikTok Looker Studio connector remains supported beside it.

The multi-platform product requires a richer model where one OAuth grant can discover and back multiple provider resources, while every selected resource remains workspace-owned and tenant-scoped. Existing TikTok connections must keep working without token loss or forced reconnection.

## Decision

Add a normalized provider integration model with these concepts:

- Provider authorization: one OAuth grant by an actor for a provider.
- Provider credential: encrypted token material for an authorization, with key version, expiry metadata, and no plaintext tokens in JSON metadata.
- Provider scope: granted, missing, denied, or revoked scope state recorded per authorization.
- Provider resource: a provider-side account, Page, channel, or GA4 property discovered from an authorization.
- Workspace provider connection: the workspace-owned selected resource used for sync, dashboards, and reports.
- Capability state: provider/resource-specific features and unavailable reasons.
- Sync state: provider cursor, API version, data-through timestamp, retry state, and quota metadata.
- Revocation state: local and provider-side disconnect/revocation audit.

## Tenant And Security Rules

- A provider resource can only become usable through a workspace connection row owned by one workspace.
- All dashboard, report, sync, and disconnect queries must filter by workspace ownership.
- OAuth state must bind actor, workspace, provider, intended flow, exact callback, and return path.
- Credentials must stay server-side and be encrypted with a recorded key version.
- Token replacement must be expand-contract: keep the last working credential until refresh or exchange succeeds.
- Raw provider responses are not stored by default.

## Compatibility Path

The additive migration must mirror existing TikTok `data_sources`, `provider_accounts`, `oauth_credentials`, `provider_scopes`, and `sync_jobs` into the new provider authorization/resource tables without decrypting or rewriting token ciphertext. The current TikTok lifecycle remains the source of truth until adapter code is moved over in a later vertical slice.

## Consequences

This model supports Meta and Google multi-resource authorization without duplicating plaintext credentials. It also creates the data boundary needed for capability-aware dashboards and asynchronous PDF reports.
