# ADR 0004: Provider Observations And Report Foundation

Status: Accepted and implemented

Date: 2026-07-18

Migrations: `009_observations_and_report_foundation.sql`, `010_provider_report_tenant_integrity.sql`

## Context

TikTok, YouTube, and Meta already have tested provider-specific snapshot tables. GA4 needs aggregated daily and dimension data, and the PDF pipeline needs durable report definitions, asynchronous runs, and protected artifact metadata. Replacing the working provider tables with a universal metric table would risk TikTok compatibility and erase important provider date and availability semantics.

## Decision

Keep existing provider-specific content and analytics tables as their source of truth. Add three provider-neutral observation envelopes for new or genuinely compatible data:

- `provider_resource_observations` records immutable property/account/channel attributes and availability at a sync boundary.
- `provider_metric_observations` records one namespaced numeric metric with explicit grain, period, unit, definition version, and availability. A returned zero is distinct from an unavailable null.
- `provider_dimension_observations` records aggregated dimension rows, a deterministic hash of canonical dimension values, namespaced metric values, per-metric availability, provider thresholding, and row order.

Namespaced keys such as `ga4.sessions`, `youtube.views`, and `facebook.page_follows` preserve provider semantics. The observation contract rejects cross-provider keys and requires every missing value to have an explicit availability state. Raw provider responses, tokens, individual-user data, and arbitrary metadata are not stored in these tables.

Add a report foundation with:

- workspace-owned report definitions and relationally selected connections;
- immutable, idempotent report runs with a DB lease, bounded attempts, progress, configuration snapshot, metric-definition snapshot, and sanitized failure fields;
- run resource snapshots so generated reports retain the names and data-through timestamps used;
- private artifact metadata containing only an opaque storage key, sanitized download filename, hash, size, page count, and expiry;
- hashed, expiring, one-time download grants.

The PDF renderer and report services will be implemented in the reporting phase. No request handler will render a complex PDF.

## Tenant Integrity

Composite foreign keys bind provider authorizations, resources, selected connections, observations, report definitions, and report runs to the same workspace and provider. Application services must still authorize every operation through workspace membership and RBAC. Database constraints are a second boundary against accidental cross-workspace references.

## Compatibility

- Existing TikTok ciphertext and legacy tables are not read, rewritten, or deleted by these migrations.
- Existing TikTok, YouTube, and Meta snapshot tables remain unchanged.
- `sync_runs` gains only nullable connection/API-version columns.
- GA4 is added to the existing provider checks but remains disabled and unimplemented until its complete vertical slice passes validation.

## Consequences

Cross-platform views and reports can use a shared availability/definition envelope without summing unlike metrics. Provider dashboards remain free to query richer provider-specific tables. Disconnect and retention services must delete new observation rows through their connection/run cascades and must separately expire private report artifacts on disk.
