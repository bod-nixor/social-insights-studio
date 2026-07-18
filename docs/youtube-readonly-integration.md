# YouTube Read-Only Integration

Date: 2026-07-18

Status: implemented in source, disabled by default, not deployed or submitted from this worktree.

Official-document verification date: 2026-07-18. The current Analytics `reports.query` reference explicitly requires `youtube.readonly` in addition to the Analytics read-only scope, so the intended two-scope contract remains correct and was not expanded.

## Authorization Contract

The integration requests exactly these scopes:

- `https://www.googleapis.com/auth/youtube.readonly`
- `https://www.googleapis.com/auth/yt-analytics.readonly`

No YouTube write, upload, comment-management, advertising, messaging, revenue, or monetary-data scope is requested. The scope pair supports [`channels.list`](https://developers.google.com/youtube/v3/docs/channels/list), video/channel metadata through the [YouTube Data API](https://developers.google.com/youtube/v3/docs), and channel/video reports through [`reports.query`](https://developers.google.com/youtube/analytics/reference/reports/query).

The server-side OAuth flow uses:

- random state stored only as a SHA-256 hash;
- S256 PKCE with an encrypted verifier;
- exact callback matching at `/api/integrations/youtube/callback`;
- user, session, workspace, provider, requested-scope, target-connection, and relative return-path binding;
- a 10-minute default state lifetime and atomic one-time consumption;
- `access_type=offline` and incremental authorization, with `prompt=consent` only when no usable stored refresh token exists;
- encrypted AES-256-GCM access and refresh tokens with key-version rotation support.

The callback always returns a `303` to a clean internal URL. Authorization codes, state, tokens, and provider payloads are not rendered into the application URL, response body, audit metadata, or request-event records.

## Configuration

`YOUTUBE_ENABLED=false` is the default. Enabling requires all of:

- `YOUTUBE_CLIENT_ID`
- `YOUTUBE_CLIENT_SECRET`
- `YOUTUBE_REDIRECT_URI`, exactly `${BASE_URL}/api/integrations/youtube/callback`
- a valid `ENCRYPTION_KEY`
- migration `004_youtube_readonly_vertical`
- worker support

Production requires HTTPS and rejects placeholder credentials. `/health/ready` reports only sanitized YouTube status/warning codes. A disabled or incomplete configuration cannot start OAuth, request a manual sync, or let a claimed worker job call Google.

Normal server startup remains safe when YouTube is enabled incorrectly: the provider stays non-connectable and readiness/catalog responses carry sanitized configuration warnings. The explicit production preflight command fails the release when an enabled YouTube configuration is incomplete or contains placeholders.

## Resource And Data Model

After consent, `channels.list?mine=true` discovers accessible channels. The user must explicitly select each channel before a workspace connection and sync job are created. One authorization may own multiple selected channel connections.

The worker stores normalized immutable observations rather than raw responses:

- channel subscriber count or an explicit hidden/unavailable state;
- lifetime channel views and current public video count;
- uploads playlist video identity, title, publication date, duration, thumbnail URL, privacy/content-type availability, and Data API lifetime counters;
- daily views, estimated minutes watched, average view duration/percentage, subscribers gained/lost, likes, comments, and shares;
- 7, 30, and 90-day video-level versions of the same non-monetary Analytics metrics;
- provider request method, estimated Data API quota cost, page/item counts, attempts, retry category, and `Retry-After` metadata without response payloads.

Dashboard requests query stored snapshots only. They display the YouTube data-through date, preserve `null` as `N/A`, distinguish hidden/unavailable/delayed states from zero, and do not invent engagement or revenue metrics. Custom date ranges support channel-period totals and daily trends; video ranking is explicitly limited to the stored 7/30/90-day reports.

## Sync And Quota Bounds

Defaults per connection/run:

- 180-second job budget and 10-second request timeout;
- at most two retries for retryable requests, with bounded exponential backoff/jitter and `Retry-After` support;
- at most five `playlistItems.list` pages of 50 items;
- at most 250 videos by default, with a configurable 50-1000 cap rounded down to a complete 50-ID `videos.list` batch;
- 180-day daily Analytics lookback;
- at most 200 videos in each 7/30/90-day Analytics report.

The default maximum Data API estimate is 11 quota units: one `channels.list`, five `playlistItems.list`, and five `videos.list` calls. Analytics requests are separately recorded as request metadata. These costs follow the [YouTube Data API quota calculator](https://developers.google.com/youtube/v3/determine_quota_cost).

Rate limits and transient 5xx/network/timeouts are retryable within the request and job budgets. Quota exhaustion, missing scopes, malformed responses, and reporting delay are explicit categories. `invalid_grant` and terminal authorization-wide authentication/scope failures stop retries and trigger authorization-wide local purge. If one selected channel becomes inaccessible while the shared authorization remains usable, only that connection is paused for reauthorization; sibling channels and the shared authorization are retained until reauthorization or the validation deadline. A request is not started after its job deadline, and its timeout is capped to the remaining job budget.

Manual YouTube sync requests only queue the connection's sync job as due with a manual trigger. The Express request returns after queueing; the bounded worker performs every Google and YouTube request and writes the resulting sync history.

## Revocation, Deletion, And Retention

In-product disconnect attempts the Google [token revocation endpoint](https://developers.google.com/identity/protocols/oauth2/web-server#tokenrevoke), then immediately deletes local credentials, scopes, one-time OAuth records, discovered resources, workspace connections, sync jobs, content, request telemetry, and YouTube snapshots even if provider revocation fails. A minimal sanitized authorization tombstone, revocation outcome, and audit event remain for security/accountability.

Users can also revoke access in [Google Account third-party connections](https://myaccount.google.com/connections). Terminal external revocation causes the same purge when detected by the next bounded sync. A verified deletion request follows the public `/data-deletion` process and purges the associated Authorized Data promptly after identity and workspace authority are verified. The implementation and operating process must continue to satisfy the [YouTube API Services Developer Policies](https://developers.google.com/youtube/terms/developer-policies) and [Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy), including Limited Use.

Every successful authorization or selected-channel validation advances a deletion/validation deadline by no more than 30 calendar days. Normal six-hour syncs validate much more frequently. Active authorizations with no selected channel are automatically purged at the deadline; a selected authorization that reaches the deadline and cannot be validated is also purged instead of retaining stale Authorized Data indefinitely.

## Verification Evidence Checklist

Before enabling production or submitting for Google verification:

1. Verify the production domain and configure the exact homepage, privacy, terms, support, and data-deletion URLs on that same domain.
2. Configure the exact HTTPS redirect URI and the exact two scope strings above.
3. Prepare a reviewer account/workspace and eligible YouTube channel with non-empty sample data.
4. Record one continuous demo showing sign-in, YouTube authorization, the exact consent scopes, channel discovery/selection, sync, dashboard/reporting-delay states, reauthorization, disconnect, Google revocation outcome, and local deletion.
5. Provide a per-scope justification tied to visible features and the API methods in the approval matrix.
6. Confirm public pages are accessible without sign-in and the feature is not hidden behind reviewer-inaccessible navigation.
7. Run repository format, lint, typecheck, build, backend/MariaDB tests, worker smoke, dependency audits, and desktop/mobile browser QA from the exact candidate commit.

Google's current verification overview is maintained at [OAuth App Verification Help Center](https://support.google.com/cloud/answer/13464321). Provider-console changes, production credentials, domain verification, deployment, and live channel testing are external release actions and are not performed by repository validation.
