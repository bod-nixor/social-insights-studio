# Google Analytics 4 Read-Only Integration

Documentation verification date: 2026-07-18

This document describes the locally implemented GA4 slice. It does not claim that Google has approved the OAuth consent screen, that production credentials exist, or that a live property has been exercised.

## Authorization Boundary

The product requests exactly one Google product-authorization scope:

- `https://www.googleapis.com/auth/analytics.readonly`

The exact callback is:

- `https://<verified-production-domain>/api/integrations/google-analytics/callback`

GA4 uses a dedicated OAuth web client. Startup validation rejects a GA4 client ID that matches either the Google sign-in client or the YouTube client. The connector is disabled by default with `FEATURE_GA4_CONNECTOR=false` and fails closed unless the client ID, client secret, exact callback, encryption key, database foundation, and worker support are ready.

Authorization uses S256 PKCE, a hashed single-use state value, offline access, and a short-lived transaction bound to the initiating workspace, user, session, requested scope set, callback, and optional reconnect target. The callback rejects missing, additional, or substituted scopes. Google sign-in identity tokens are never reused for Analytics access.

No Google Ads, publishing, messaging, profile, email, demographic, user-level, mutation, or broad cloud-management scope is requested. The implementation does not call the Google Ads API or any GA4 Admin mutation method.

## Resource Discovery And Selection

After token exchange, the server calls Analytics Admin API `accountSummaries.list`, then `properties.get` for bounded discovered property names. Discovery creates no data source. A workspace owner or admin must explicitly select each property.

A property is selectable only when Google returns:

- an opaque `properties/<number>` resource name;
- a display name;
- a valid IANA property timezone; and
- a three-letter property currency code.

Reauthorization is connection-bound. If Google no longer returns the selected property, the connection becomes reconnect-required and no different property is selected silently. One authorization may back multiple explicitly selected properties.

## Worker-Only Read Methods

Browser and dashboard requests never call Google Analytics. The bounded worker uses only:

| API | Method | Purpose |
| --- | --- | --- |
| Analytics Admin API v1beta | `accountSummaries.list` | Authorization-time account/property discovery |
| Analytics Admin API v1beta | `properties.get` | Confirm property identity, timezone, currency, property type, and service level |
| Analytics Data API v1beta | `properties.getMetadata` | Confirm metrics and dimensions exposed for the selected property |
| Analytics Data API v1beta | `properties.checkCompatibility` | Reject unsupported metric/dimension combinations before reporting |
| Analytics Data API v1beta | `properties.runReport` | Read aggregate daily, range, and breakdown reports |

Default discovery is bounded to 10 account-summary pages and 100 properties. A normal full sync makes one property request, one metadata request, eight compatibility requests, one daily report, six current/previous summary reports, and eighteen breakdown reports. Retries, timeouts, worker budget, lookback, dimensions, and row counts are independently bounded by environment settings.

## Metrics And Date Semantics

The connector stores these GA4 metrics with versioned definitions:

| GA4 API metric | Product key | Unit | Aggregation rule |
| --- | --- | --- | --- |
| `activeUsers` | `ga4.active_users` | count | Exact provider-reported range; never summed from daily distinct-user rows |
| `newUsers` | `ga4.new_users` | count | Exact provider-reported range |
| `sessions` | `ga4.sessions` | count | Exact range; daily rows may be summed only for a custom range when no exact report exists |
| `screenPageViews` | `ga4.screen_page_views` | count | Exact range; daily rows may be summed only for a custom range when no exact report exists |
| `engagementRate` | `ga4.engagement_rate` | ratio | Provider-computed range value |
| `bounceRate` | `ga4.bounce_rate` | ratio | Provider-computed range value |
| `averageSessionDuration` | `ga4.average_session_duration` | seconds | Provider-computed range value |
| `sessionsPerUser` | `ga4.sessions_per_user` | ratio | Provider-computed range value |
| `screenPageViewsPerUser` | `ga4.screen_page_views_per_user` | ratio | Provider-computed range value |

The worker stores exact current and previous 7-, 30-, and 90-day reports plus a bounded daily lookback. Every date is calculated in the selected property's timezone and ends on the last complete property day. Custom ranges never synthesize users or rates from daily rows; unavailable exact values remain `N/A`.

## Aggregate Breakdowns

The connector requests only aggregate dimensions needed by the visible dashboard:

- session source and medium;
- page path and page title;
- landing page plus query string;
- device category;
- country; and
- city.

No user ID, device ID, age, gender, interest, audience, or other user-level/demographic dimension is requested. Each breakdown is compatibility-checked and limited to a bounded number of rows. Dimension values are normalized as aggregate labels and stored with a deterministic hash; individual visitor records are not stored.

## Thresholding, Data Quality, And Quota

`ResponseMetaData.subjectToThresholding` is preserved on stored breakdown rows and shown in the dashboard. Thresholded or withheld data is never estimated. `dataLossFromOtherRow`, data-through dates, missing metadata, blocked reasons, incompatible combinations, and missing report values remain explicit availability states rather than zero.

The worker asks GA4 to return property quota information but stores only a small allowlisted summary of consumed and remaining counters. Raw responses, request bodies, tokens, and arbitrary provider error payloads are not retained. Request telemetry contains only method/category, attempt count, row/page counts, sanitized failure category, and retry timing.

Provider failures are categorized as authentication, scope, quota, rate limit, transient provider, timeout/network, malformed response, or internal. Retryable calls use bounded exponential backoff and `Retry-After`; terminal authentication or scope failures stop sync and purge the unusable authorization.

## Credential Storage, Revocation, And Deletion

Access tokens, refresh tokens, and PKCE verifiers use the existing AES-256-GCM envelope and key-version rotation support. Tokens remain server-side and never enter the Vite bundle, browser storage, report artifact, audit metadata, or request telemetry.

Disconnecting one of several selected properties removes that property's data source, jobs, capabilities, and observations while preserving the shared authorization. Disconnecting the final property makes one bounded request to `https://oauth2.googleapis.com/revoke`, then purges local credentials, scopes, transactions, discovered properties, connections, jobs, observations, and request events even if Google revocation fails. Users can also revoke access from [Google Account third-party connections](https://myaccount.google.com/connections); a later terminal refresh failure performs the same local purge.

## Runtime Configuration

Required when enabled:

- `FEATURE_GA4_CONNECTOR=true`
- `GA4_CLIENT_ID`
- `GA4_CLIENT_SECRET`
- `GA4_REDIRECT_URI=https://<verified-production-domain>/api/integrations/google-analytics/callback`
- `ENCRYPTION_KEY` and `ENCRYPTION_KEY_VERSION`
- MariaDB migrations 001-010
- the bounded worker cron command

Optional bounded controls are documented in `.env.example`: request timeout, OAuth-state lifetime, retry count, job budget, analytics lookback, dimension-row limit, discovery-page limit, and property limit.

Required Google Cloud APIs:

- Google Analytics Admin API
- Google Analytics Data API

## Reviewer Walkthrough

1. Show the verified-domain homepage, privacy, terms, support, status, and deletion pages.
2. Open Connections and choose **Connect Website Analytics**.
3. Show that the Google consent screen requests only read-only Analytics access.
4. Return through the exact callback and show that no property was selected automatically.
5. Select one eligible non-production GA4 property explicitly and show its timezone/currency.
6. Run the bounded worker, then open Overview > Website.
7. Show 7/30/90-day metrics, exact previous-period comparisons, daily traffic, all compatible breakdowns, and a threshold/delay or `N/A` state without fabricated zeros.
8. Reauthorize the selected property and demonstrate that a missing property is not silently replaced.
9. Disconnect one sibling property while preserving the grant, then disconnect the final property and show local purge after the Google revocation attempt.
10. Show the Google Account third-party connections and public deletion instructions.

The recording must not expose client secrets, tokens, authorization codes, personal visitor data, raw provider responses, terminal output, or production logs.

## Readiness

| Level | Status on 2026-07-18 | Evidence or blocker |
| --- | --- | --- |
| Locally implemented | Yes | Exact-scope OAuth, discovery, explicit selection, encrypted credentials, Admin/Data adapters, compatibility checks, worker observations, stored dashboard, revocation/deletion, unit tests, and real-MariaDB lifecycle coverage are present. |
| Configuration-ready | Yes | Dedicated disabled-by-default environment contract, exact redirect validation, health/readiness state, and production preflight checks exist. |
| Test-user ready | No — blocked | Requires a Google Cloud project/client, both enabled APIs, exact test redirect, consent-screen test user, and eligible non-production GA4 property. |
| Verification-submission ready | No — blocked | Requires verified domain, finalized consent-screen branding/contact data, legal review, reviewer credentials, live screenshots, and recorded walkthrough. |
| Verified/live | No — blocked | No Google approval, production credential configuration, deployment, or live-provider smoke test was performed. |

## Official Sources Verified

- [Analytics Admin API `accountSummaries.list`](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/accountSummaries/list)
- [Analytics Admin API Property resource](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties)
- [Analytics Data API `runReport`](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
- [Analytics Data API `getMetadata`](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata)
- [Analytics Data API `checkCompatibility`](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/checkCompatibility)
- [GA4 API dimensions and metrics](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
- [GA4 Data API quotas](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
- [GA4 reporting expectations and thresholding](https://developers.google.com/analytics/devguides/reporting/data/v1/basics)
- [Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy)
