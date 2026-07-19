# ADR 0003: Meta Read-Only Facebook Login Vertical

Status: Accepted; source implemented behind independent provider gates

Date: 2026-07-18

## Context

Meta dashboard evidence exposes `pages_show_list`, `pages_read_engagement`, and `read_insights` for Pages. Instagram insights require the Facebook Login path and the exact `instagram_basic` plus `instagram_manage_insights` permissions. The product must not acquire publishing, comment-management, messaging, ads, demographic, webhook, or broad business-management authority.

## Decision

Use Facebook Login for Business for Page authorization and the Instagram API with Facebook Login for linked professional accounts. Use a separate Login for Business configuration ID for each provider, with only that provider's exact approved permissions; `config_id` replaces the normal URL `scope` parameter. Keep Facebook Pages and Instagram independently feature-flagged and require exact runtime scope assertions before either becomes connectable.

Authorization and selected resources remain separate. One encrypted app-user token owns discovered resources; each Page token uses a resource-level encrypted credential. Creating an authorization never creates or replaces a workspace data source. Selection is explicit, and reauthorization marks missing resources unavailable rather than selecting a replacement.

Only the worker fetches insights and content. The web callback is limited to token exchange/validation and `/me/accounts` discovery required for selection. Stored dashboard APIs do not call Graph. Graph is pinned to `v25.0`, adds `appsecret_proof`, and uses bounded retries/time/item/page limits plus usage-header backoff.

Instagram Stories are not collected. Reliable history would require a webhook subscription and a materially different authorization/data-retention design, which is outside this read-only scope.

Most Instagram account insights are provider `total_value` metrics rather than daily time series. Store exact rolling 7/30/90-day snapshots with their source ranges, and do not fabricate daily values or custom-range totals. Meta documents that some Business Manager Page-role access paths also require ads permissions; those resources are explicitly unsupported because this slice cannot request them.

Final-resource disconnect attempts authorization-wide Meta revocation and performs unconditional local purge. A valid Meta deauthorization or data-deletion `signed_request` performs the same purge across both Meta providers. Signed requests are HMAC-verified, time-bounded, and replay-protected.

## Consequences

- Facebook Pages is source-complete but disabled until production Meta configuration and review gates are satisfied.
- Instagram is source-complete but remains blocked unless the dashboard and runtime expose the exact four-scope Facebook Login set.
- An Instagram account that requires `ads_management` or `ads_read` through its Business Manager access path is not eligible and cannot be enabled by broadening scopes.
- Multiple selected resources can share an authorization without duplicating a user token.
- Removing one of several Facebook or Instagram resources does not revoke a working app-user sibling, including in another workspace; removing the final selected Meta resource revokes the grant.
- Historical dashboards remain provider-reported snapshots with explicit missing/partial states; the application does not fabricate zeroes or deprecated impression metrics.
- Any future feature needing write, messaging, comment-management, ads, demographics, webhooks, or `business_management` requires a separate ADR and authorization review.
