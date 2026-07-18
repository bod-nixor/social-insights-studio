# Meta Read-Only Integration

Documentation verification date: 2026-07-18

This document describes the source implementation and its fail-closed release gates. It does not claim Meta App Review approval, Advanced Access, production enablement, or a completed live-provider test.

## Scope Boundary

Facebook Pages requests exactly:

- `pages_show_list`
- `pages_read_engagement`
- `read_insights`

Instagram via Facebook Login requests exactly:

- `instagram_basic`
- `instagram_manage_insights`
- `pages_show_list`
- `pages_read_engagement`

Meta may return the automatic `public_profile` permission. The callback accepts that automatic permission and the union of the two approved read-only product sets so one app-scoped user can authorize both connectors. It rejects missing required scopes and any permission outside that approved universe. The implementation never requests or consumes publishing, Page/Instagram management, comment-management, messaging, ads, app-events, demographic, webhook, or broad `business_management` permissions.

The Instagram source is implemented but disabled by default. Runtime enablement requires an operator assertion containing the exact four-scope set, a Facebook Login for Business configuration, and the exact callback. This preserves the blocker until the Meta dashboard confirms that `instagram_basic` and `instagram_manage_insights` are available to the configured Facebook Login path. Meta's current Instagram insights reference also says some accounts reached through a Business Manager Page role require `ads_management` and `ads_read`; those resources are ineligible for this slice and Instagram must remain disabled if the reviewer/test path cannot work without them.

## Authorization And Selection

Both products use Facebook Login for Business with a pinned Graph API `v25.0` login URL and separate `META_FACEBOOK_LOGIN_CONFIG_ID` and `META_INSTAGRAM_LOGIN_CONFIG_ID` user-access-token configurations. The IDs must differ when both providers are enabled, and each dashboard configuration must contain only its provider's exact approved permission set. In accordance with Meta's configuration-based login contract, the login URL uses `config_id` instead of a `scope` parameter. OAuth transactions still bind the asserted exact permission set, hashed single-use state, workspace, initiating user, server session, provider, exact redirect URI, optional reauthorization target, and a local-only return path; the callback independently verifies the permissions actually granted.

Exact callback routes:

- Facebook Pages: `/api/integrations/facebook/callback`
- Instagram: `/api/integrations/instagram/callback`

The callback exchanges the authorization code, requires a long-lived user token, validates the token against the configured App ID, reads the permission grant, verifies the app-scoped subject, and discovers eligible resources through `/me/accounts`. Only Pages whose returned tasks include `ANALYZE` are selectable. Instagram resources must be professional accounts linked through an eligible Page.

Authorization never creates a data source automatically. Discovered resources are shown in the Connections UI, and the user must select each Page or professional account explicitly. Reauthorization preserves existing selected resources; a resource that is no longer returned is marked unavailable/reconnect-required instead of being silently replaced. Reauthorization also rejects a different app-scoped Meta user; switching Meta users requires disconnecting the existing authorization first.

## Credential Boundary

- The long-lived app-user token is encrypted in `provider_authorization_credentials`.
- Every discovered Page token is separately encrypted in `provider_resource_credentials` and bound to one `provider_resources` row.
- AES-256-GCM envelopes use the existing current/previous key-version foundation.
- No Meta token, app secret, authorization code, or signed request is returned to the browser, stored in an audit payload, or stored in normalized snapshots.
- The web process performs only OAuth validation, discovery needed for selection, revocation, and signed callback handling. Account insights and content data calls run from the bounded worker only.

## Worker Synchronization

The worker uses only the selected resource's encrypted Page token and adds `appsecret_proof` to Graph requests. Requests use the pinned Graph version, timeouts, bounded retries, a per-job deadline, item/page caps, sanitized request telemetry, `Retry-After`, and Meta usage headers. When app/Page/business usage reaches the configured threshold, the run preserves completed data and becomes partial/delayed.

Facebook Page account metrics:

- `page_follows`
- `page_daily_follows_unique`
- `page_daily_unfollows_unique`
- `page_post_engagements`
- `page_media_view`
- `page_total_media_view_unique`

Facebook content uses Page post metadata and available reaction/comment/share counts, plus `post_media_view` and `post_total_media_view_unique`. Deprecated Page impression metrics are not requested.

Instagram account metrics:

- `views`
- `reach`
- `accounts_engaged`
- `total_interactions`
- `likes`
- `comments`
- `saves`
- `shares`

Meta exposes most of these account metrics as `total_value`, not daily time series. The worker therefore stores separate provider-reported rolling totals for 7, 30, and 90 complete days; it never labels a multi-day total as a daily observation. The stored-only dashboard uses an exact matching preset window. For a custom range, account totals remain unavailable while profile and content snapshots remain visible. A previous-period comparison appears only when an older matching rolling snapshot exists.

Instagram content stores Feed, carousel, video, and Reels metadata plus the supported read-only media insight set for the returned media type. Stories are deliberately excluded: without webhooks, this slice cannot promise reliable retention of insight data that is available for only a short lifetime. No webhook permission or subscription is requested.

Account insight rows are stored in `meta_account_insight_snapshots`; Facebook uses provider daily rows and Instagram uses explicit 7/30/90-day period rows. Content and content metrics use the existing normalized `content_items` and `content_metric_snapshots` tables. Dashboard APIs and UI views read stored rows only; they do not call Meta.

Dashboard and manual-sync routes:

- `/api/workspaces/:workspaceId/providers/facebook_pages/dashboard`
- `/api/workspaces/:workspaceId/providers/instagram/dashboard`
- `/api/workspaces/:workspaceId/providers/facebook_pages/sync-runs`
- `/api/workspaces/:workspaceId/providers/instagram/sync-runs`

Manual sync only queues a worker job. It never performs Graph data calls in the request process.

## Disconnect, Deauthorization, And Data Deletion

Disconnecting one selected resource while another selected Facebook or Instagram resource still uses the same app-scoped Meta user deletes only the requested local authorization/resource data and preserves the shared Meta grant, including across workspaces. Disconnecting the final selected Meta resource attempts `DELETE /me/permissions` first and then purges all local credentials, scopes, resources, sources, jobs, content, and Meta snapshots even if Meta revocation fails. When a stored token expires without a completed reauthorization, the worker retains a bounded 30-day deletion grace window and then purges the expired local authorization, including unselected discovered resources.

Meta callbacks:

- Data deletion: `POST /api/integrations/meta/data-deletion`
- Deauthorization: `POST /api/integrations/meta/deauthorize`
- Deletion status: `GET /api/integrations/meta/deletion-status/:confirmationCode`

Both callbacks require a fresh `signed_request` using `HMAC-SHA256` with the app secret. The server verifies the signature, algorithm, timestamp, expiry, and app-scoped subject; hashes the signed request for replay protection; purges matching Facebook and Instagram authorizations; and stores only a hashed subject plus deletion status. The data-deletion response returns Meta's required status URL and opaque confirmation code.

## Runtime Configuration

Common settings:

- `META_APP_ID`
- `META_APP_SECRET`
- `META_FACEBOOK_LOGIN_CONFIG_ID`
- `META_INSTAGRAM_LOGIN_CONFIG_ID`
- `META_GRAPH_API_VERSION=v25.0` (a different configured value fails closed)

Facebook Pages:

- `FEATURE_FACEBOOK_PAGES_CONNECTOR=true`
- `META_FACEBOOK_LOGIN_CONFIG_ID=<configuration containing exactly the three Pages permissions>`
- `FACEBOOK_REDIRECT_URI=https://<origin>/api/integrations/facebook/callback`
- `META_FACEBOOK_APPROVED_SCOPES=pages_show_list,pages_read_engagement,read_insights`

Instagram:

- `FEATURE_INSTAGRAM_CONNECTOR=true`
- `META_INSTAGRAM_LOGIN_CONFIG_ID=<configuration containing exactly the four Instagram/Page discovery permissions>`
- `INSTAGRAM_REDIRECT_URI=https://<origin>/api/integrations/instagram/callback`
- `META_INSTAGRAM_APPROVED_SCOPES=instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement`

The scope assertion is order-insensitive but must be an exact set; missing or extra values make the provider non-connectable. Production callbacks must be HTTPS, must contain no query or fragment, and must equal `BASE_URL` plus the exact path.

## Review Notes And Remaining Gates

Repository evidence now covers migration constraints, exact scope/config validation, OAuth binding, permission rejection, explicit resource selection, encrypted user/Page tokens, bounded worker reads, stored dashboards, Instagram Story exclusion, disconnect/revocation, and signed deletion replay handling.

External gates remain:

1. Confirm the dedicated Facebook Pages Login for Business user-token configuration contains only the three Pages permissions for the Pages flow.
2. Confirm the separate Instagram Login for Business user-token configuration exposes both `instagram_basic` and `instagram_manage_insights` with only the two necessary Page discovery permissions.
3. Confirm the selected Instagram professional account is directly accessible through the exact read-only set. If Meta requires `ads_management` or `ads_read` because access is derived through a Business Manager Page role, stop: that resource is outside this slice.
4. Confirm Standard versus Advanced Access and test-user eligibility in the Meta dashboard; do not claim approval based on permission visibility alone.
5. Configure exact production callbacks, deauthorization URL, data-deletion URL, Privacy Policy, Terms, support contact, app domain, and reviewer credentials.
6. Exercise eligible non-production Page and Instagram professional resources and record a reviewer walkthrough showing consent, explicit selection, stored dashboards, missing metric states, reauthorization, and deletion.
7. Keep both feature flags disabled until configuration, legal review, provider review, and live smoke evidence are complete.

If Meta requires ads, publishing, messaging, comment-management, webhooks, demographic, or broad business-management permission for any proposed feature, that feature is outside this slice and must remain stopped.

## Official References

- https://developers.facebook.com/documentation/pages-api
- https://developers.facebook.com/docs/facebook-login/facebook-login-for-business/
- https://developers.facebook.com/documentation/pages-api/manage-pages
- https://developers.facebook.com/documentation/pages-api/platforminsights/page
- https://developers.facebook.com/documentation/pages-api/platforminsights/page/deprecated-metrics
- https://developers.facebook.com/documentation/instagram-platform/overview
- https://developers.facebook.com/documentation/instagram-platform/instagram-api-with-facebook-login/get-started
- https://developers.facebook.com/documentation/instagram-platform/api-reference/instagram-user/insights
- https://developers.facebook.com/documentation/instagram-platform/reference/instagram-media/insights
- https://developers.facebook.com/documentation/instagram-platform/app-review
- https://developers.facebook.com/docs/permissions/
- https://developers.facebook.com/docs/graph-api/guides/versioning/
- https://developers.facebook.com/docs/development/create-an-app/app-dashboard/data-deletion-callback
