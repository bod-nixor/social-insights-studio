# Cross-Platform Overview

## Purpose

The authenticated Overview presents stored analytics from TikTok, YouTube, Facebook Pages, Instagram professional accounts, and GA4 side by side. It is a comparison surface, not a universal analytics total.

The API route is:

```text
GET /api/workspaces/:workspaceId/cross-platform-overview
```

It accepts the existing `range=7d|30d|90d|custom`, `from`, `to`, and comparison-window semantics. Session membership and the `viewDashboard` capability are required.

## Stored-Only Boundary

The request path composes the existing stored-snapshot dashboard services. It never calls TikTok, Google, Meta, YouTube, or GA4 directly. Provider HTTP remains worker-only.

The service first lists the workspace's selected resources, then loads each resource explicitly by connection ID. Multiple YouTube channels, Facebook Pages, Instagram accounts, or GA4 properties become separate source summaries. Metrics and content from sibling resources are not silently merged. Dashboard content queries for Meta are constrained to the selected data source.

## Metric Safety

Every source includes provider-specific metrics with its own:

- provider and selected resource;
- metric key, label, family, unit, definition, and semantics where available;
- current value, matching stored baseline, delta, and percent change;
- availability status and reason;
- reporting range, provider timezone when applicable, and data-through date.

The response deliberately has no cross-provider analytics `total`. Counts such as connected resources and resources with stored data describe source health only. Facebook reach, Instagram reach, YouTube views, TikTok views, GA4 views, followers, subscribers, users, and sessions are never summed or relabeled as equivalent.

Trend series are rendered as small multiples with independent scales. Website landing pages remain `website_path` items and are never presented as social posts. Missing, delayed, partial, and privacy-thresholded values remain `N/A` or carry an explicit availability state.

## Freshness And Alerts

Each source exposes:

- connection status;
- last successful sync and next scheduled sync;
- provider data-through date;
- freshness state (`ready`, `sample`, `stale`, `delayed`, `partial`, `thresholded`, `empty`, `pending`, `failed`, `reconnect_required`, `configuration_required`, or `disconnected`);
- one sanitized actionable alert when appropriate.

An active source becomes stale after 30 hours without a successful sync. Reconnect and failed-sync states are critical. Delay, thresholding, partial data, and staleness are warnings. A connected source waiting for its first stored observation is informational. An unconfigured disconnected provider is visible but does not create a workspace alert.

## Navigation And Filters

- **Overview** shows all selected resources side by side, with source health, provider-specific metrics, small-multiple trends, top content or landing pages, alerts, and methodology.
- **Sources** preserves the full provider-specific dashboards and their hierarchy.
- Date and previous-period controls exist in both views.
- Sources includes a connected-resource selector for YouTube, Facebook Pages, Instagram, and GA4. TikTok retains its existing single-account contract.
- `Open source` carries the provider and connection ID to Sources without selecting or replacing any resource silently.

## Validation

Focused unit tests cover metric separation, unavailable values, threshold alerts, deterministic freshness, GA4 landing-page normalization, bounded concurrent loading, and explicit multi-resource connection IDs. The real MariaDB suite exercises the authenticated route and labeled local fixture state.

Responsive QA is performed at desktop and 360-pixel mobile widths. The current gate requires five source cards, working source drill-down, no document-level horizontal overflow, and no console exceptions. The in-app browser cannot currently authorize loopback navigation, so the documented local Chrome/CDP fallback is used for localhost inspection.
