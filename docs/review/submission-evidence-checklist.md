# Provider Submission Evidence Checklist

Date: 2026-07-18

This package is prepared for future provider review but must not be submitted until every external item is complete. It does not claim or guarantee approval.

## Reviewer Account And Resource Requirements

- One dedicated reviewer user with an email the provider reviewer can access and an active Analyst-or-higher membership in a clearly named reviewer workspace.
- No production customer data. Use eligible non-production resources with representative but non-sensitive content and enough history to demonstrate populated and missing states.
- TikTok: an authorized account with at least one published video, plus a deterministic/no-post demonstration for the empty state.
- Facebook Pages: an eligible Page the test user can authorize through the exact Login for Business configuration; include posts and supported insights.
- Instagram: a linked business/creator professional account reachable without `ads_read` or `ads_management`; include supported media and 7/30/90 insight history. If this cannot be satisfied, keep Instagram disabled and do not submit it.
- YouTube: an owned channel with an uploads playlist, public/unlisted fixture videos as permitted, analytics history, and at least one unavailable/hidden statistic case.
- GA4: a dedicated non-production property with timezone/currency metadata, daily activity, aggregate breakdown rows, and a low-volume or deterministic threshold/missing demonstration.
- Reviewer instructions must state exact sign-in steps, workspace name, resource names, and any test credentials through the provider's approved secure channel. Never commit credentials here.

## Per-Provider Demo Scripts

### TikTok

1. Open the public homepage, Privacy, Terms, Support, Status, and Data Deletion pages; confirm Social Insights Studio name/logo match the provider listing.
2. Sign in to the reviewer workspace and open Connections.
3. Select **Connect TikTok**, show the four exact read scopes, complete consent, and return through the exact callback.
4. Show the connected account identity, run a bounded sync, and open Sources and Content.
5. Show profile totals, stored trend, content/detail, CSV/PDF use, and missing values as unavailable.
6. Show the no-post report/empty state.
7. Disconnect; show provider revoke was attempted and local credentials/data were removed without leaking tokens/codes.

### Facebook Pages

1. Show only `pages_show_list`, `pages_read_engagement`, and `read_insights` in the dedicated Login for Business configuration.
2. Choose **Connect Facebook Page**, complete consent, and show `selection_required` with no silent Page attachment.
3. Select the reviewer Page explicitly. Run the worker and show Page identity, provider-defined KPI/trend, posts, unavailable states, data-through date, and report section.
4. Reauthorize the same connection. Demonstrate that a missing selected Page becomes unavailable rather than being replaced.
5. Disconnect a sibling if present, then the final Page; show final grant revocation attempt and local purge.
6. Exercise a valid signed Meta deletion callback and opaque status response in the non-production setup.

### Instagram

1. First show the dedicated configuration contains exactly `instagram_basic`, `instagram_manage_insights`, `pages_show_list`, and `pages_read_engagement`; stop if it does not.
2. Complete Facebook Login with the eligible test user/Page/professional account. Show explicit account selection.
3. Run the worker and show account/media insights, exact 7/30/90 period semantics, missing metric behavior, and custom-range account totals unavailable.
4. State visibly that Stories/webhooks, messaging, publishing, comments, ads, and ads-required Business Manager resources are unsupported.
5. Show sibling-aware disconnect/final revoke and signed deletion behavior.

### YouTube / Google OAuth Verification

1. Show the verified domain, matching consent-screen application name/logo/support email, public legal URLs, and exact callback.
2. Show the product client is distinct from Google sign-in and GA4 and requests exactly `youtube.readonly` plus `yt-analytics.readonly`.
3. Complete consent, return to explicit channel selection, and select the reviewer-owned channel.
4. Run bounded sync and show channel identity/lifetime labels, daily/range analytics, uploads/content, hidden/deleted/private and unavailable handling, delay/data-through state, and PDF output.
5. Show no upload/write/comments/ads/revenue scopes or controls.
6. Disconnect and show bounded Google revoke attempt plus complete local purge.

### Website Analytics / GA4 Scope Justification

1. Show a dedicated OAuth client and only `analytics.readonly`; show the exact callback and enabled Admin/Data APIs.
2. Complete consent and show accessible properties without automatic selection.
3. Select the fixture property explicitly; show property timezone/currency and run the worker.
4. Show exact current/previous range metrics, daily traffic, aggregate source/page/landing/device/country/city breakdowns, `(not set)`, compatibility errors, data-through delay, and threshold states.
5. Explain that distinct users and provider-computed rates are not summed and no individual visitor, Ads, profile/email, demographic, or mutation access exists.
6. Disconnect a sibling then the final property; show final Google revoke attempt and local purge.

## Per-Permission Recording Rule

For every row in `exact-permission-matrix.md`, the recording must visibly connect the permission to its requesting action and feature. If one continuous segment supports multiple permissions, add timestamps for each row. Do not submit a permission whose feature is hidden, disabled for the reviewer, unused, or only described verbally.

## Screenshot Checklist

- Public homepage at desktop and mobile widths with consistent name/logo and links to Privacy, Terms, Support, Status, and Data Deletion.
- Each public legal page from the exact verified production origin.
- Sign-in and reviewer workspace landing without debug/demo secrets.
- Connections default, disabled/configuration, selection-required, active, reconnect, partial, and error states as applicable.
- Exact provider consent screen and exact callback in the address bar; redact only secrets, never the requested permission names.
- Explicit resource selection for every multi-resource provider.
- Each provider Source page with resource identity, date range, KPI labels, trend, content/breakdowns, data-through and missing/threshold/delay states.
- Cross-platform Overview proving provider metrics remain side by side and unsummed.
- Reports builder, preview, queued/running/completed/expired states, downloaded sample pages, and delete confirmation.
- Members/roles, sync history sanitized errors, account sessions, disconnect, account/workspace deletion request, and Meta deletion status where applicable.
- No screenshot may contain a client secret, token, authorization code, PKCE verifier, cookie, CSRF value, raw provider payload, terminal, environment panel, personal customer data, or one-time download token.

## Screencast Checklist

- Record one continuous English walkthrough per provider or a clearly indexed combined recording at legible 1080p or better.
- Start with public/legal identity, then sign-in, exact user action/consent, explicit selection, worker-updated visible feature, unavailable state, report use, revoke/disconnect, and deletion.
- Keep browser URL and product labels legible; do not use developer tools or terminal as product evidence.
- Add a timestamp index mapping every exact permission row to the visible action and feature.
- Demonstrate denial/partial consent safely where the provider permits it, plus state replay/mix-up rejection through test evidence rather than exposing raw state values.
- Demonstrate no-content, missing metric, threshold/delay, reconnect, and final purge paths.
- End with the exact provider-console listing status and a statement that review is requested, not already granted.

## External Configuration Checklist

- Verified production domain and HTTPS certificate; exact origin matches public pages and callbacks.
- Social Insights Studio name, logo, support contact, authorized domain, homepage, Privacy, Terms, Support, Status, and Data Deletion URLs match every provider console.
- Exact callbacks: TikTok `/api/integrations/tiktok/callback`, YouTube `/api/integrations/youtube/callback`, Facebook `/api/integrations/facebook/callback`, Instagram `/api/integrations/instagram/callback`, GA4 `/api/integrations/google-analytics/callback`.
- Separate Google sign-in, YouTube, and GA4 OAuth clients; correct APIs enabled; test users added; authoritative scope classification reconfirmed immediately before submission.
- Separate Facebook Pages and Instagram Login for Business configuration IDs with exact sets; access level and App Review selections confirmed; deauthorization/data-deletion callbacks configured.
- TikTok Login Kit/Display products, redirect, requested scopes, application listing, public URLs, and reviewer accounts configured.
- Production environment preflight passes with feature flags still disabled; activate one provider only after its live reviewer resource works.
- Reviewer account credentials delivered through an approved secure provider channel, never repository text or video captions.

## Legal And Compliance Stop Conditions

Do not submit or enable publicly until the owner supplies/approves the legal entity, address/contact, privacy contact, launch jurisdictions, hosting/subprocessors, support expectation, analytics/backup retention, deletion verification/turnaround, reviewer accounts, and production data-processing facts listed in `docs/compliance-blockers.md`.

Do not submit Instagram if the exact permission set or no-ads access path is unavailable. Do not request ads, publishing, upload, comments, messaging, business management, demographics, app events, webhooks, monetary data, profile/email identity, or broad administrative access to bypass a blocker.
