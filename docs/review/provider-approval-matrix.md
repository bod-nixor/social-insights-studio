# Provider Approval Matrix

Documentation verification date: 2026-07-18

This matrix records implementation and submission evidence. It does not claim that Google, YouTube, TikTok, Meta, or any other provider has approved the application.

Readiness labels:

- **Locally implemented**: source and synthetic tests exist in this repository.
- **Configuration-ready**: the source fails closed and exposes the exact environment/configuration contract.
- **Test-user ready**: provider-console test credentials, test users, redirect URI, and eligible resources have been configured and exercised.
- **Verification-submission ready**: verified-domain public pages, reviewer account, justifications, and demo evidence are complete.
- **Verified/live**: provider approval and production enablement have both been confirmed externally.
- **Blocked**: an implementation, configuration, policy, legal, or provider-review dependency is missing.

## Provider Summary

| Provider | Scope or permission | Visible feature | API methods | Stored data | Disconnect/deletion | Readiness |
| --- | --- | --- | --- | --- | --- | --- |
| TikTok | `user.info.basic`, `user.info.profile`, `user.info.stats`, `video.list` | Existing account, dashboard, content, export, and Looker Studio features | `/v2/user/info/`, `/v2/video/list/` | Encrypted token and normalized snapshots; no raw response by default | Provider revoke attempt plus existing local lifecycle | Locally implemented; external live status unchanged by this work |
| Instagram | `instagram_business_basic`, `instagram_business_manage_insights` | Planned professional-account discovery and insights | Not implemented | None | Not implemented | Blocked |
| Facebook Pages | `pages_show_list`, `pages_read_engagement`, `read_insights` | Planned Page discovery and insights | Not implemented | None | Not implemented | Blocked |
| YouTube | Exact two-scope set documented below | Channel connection, stored analytics dashboard, and disconnect/delete | `channels.list`, `playlistItems.list`, `videos.list`, Analytics `reports.query`, OAuth token/revoke endpoints | Encrypted tokens plus normalized immutable channel, content, and Analytics snapshots; no raw response retention | Google revoke attempt followed by immediate local authorization-wide purge | Locally implemented and configuration-ready; test-user/verification/live blocked |
| Website Analytics | `https://www.googleapis.com/auth/analytics.readonly` | Planned GA4 property discovery and reports | Not implemented | None | Not implemented | Blocked |

## YouTube Exact Authorization Contract

The product authorization request contains exactly these scopes, in this order:

1. `https://www.googleapis.com/auth/youtube.readonly`
2. `https://www.googleapis.com/auth/yt-analytics.readonly`

The YouTube Analytics [`reports.query`](https://developers.google.com/youtube/analytics/reference/reports/query) documentation explicitly states that `youtube.readonly` is now also required. No OpenID, profile, email, upload, write, partner, monetary, advertising, Gmail, Drive, or channel-management scope is requested for this product authorization.

| Exact scope | User-facing reason | Exact methods enabled | Data normalized and stored | Current Google classification evidence | Reviewer steps |
| --- | --- | --- | --- | --- | --- |
| `https://www.googleapis.com/auth/youtube.readonly` | After the user chooses **Connect YouTube**, discover channels they own, let them explicitly select one, and show channel identity, current/lifetime counters, uploads, and video metadata. | Data API `channels.list` with `mine=true` for discovery; `channels.list` by opaque channel ID for sync; `playlistItems.list` for the uploads playlist; batched `videos.list` for up to 50 IDs. | Encrypted authorization tokens; granted-scope row; opaque channel/video IDs; channel title/thumbnail/uploads-playlist ID; nullable subscriber, lifetime-view, public-video, video view/like/comment counters; permitted title/description/date/duration/thumbnail/URL; explicit availability metadata. Raw provider payloads are not retained. | Expected **sensitive, not restricted**. Google's public scope catalog lists the scope but says the authoritative category appears in the Cloud Console. Reconfirm the category in Data Access immediately before submission; do not represent this row as provider approval. | Sign in to the reviewer workspace; open Connections; choose Connect YouTube; show the English consent screen and exact scope; return to channel selection; select the fixture channel; run a bounded sync; show channel snapshot, uploads, unavailable markers, and lifetime labels. |
| `https://www.googleapis.com/auth/yt-analytics.readonly` | Populate the selected-period YouTube dashboard with non-monetary daily and per-video performance. | YouTube Analytics v2 `reports.query` for `day` and `video` dimensions using only views, estimated minutes watched, average view duration/percentage, subscribers gained/lost, likes, comments, and shares. | Granted-scope row and immutable daily/7d/30d/90d video observations with nullable metrics, availability flags, requested range, and actual data-through date. No revenue, CPM, advertising, content-owner, or partner metrics. | Expected **sensitive, not restricted**; re-confirm in the Cloud Console as above. Projects using only sensitive scopes do not require the restricted-scope security assessment, but still require the applicable OAuth verification. | In the same reviewer flow, open Overview > YouTube; switch 7/30/90-day ranges; show summary labels, daily charts, period comparison, reporting-delay state, and per-video table; demonstrate that missing values display as `N/A`, not zero. |

## YouTube API And Quota Plan

| Request | Normal use | Estimated Data API quota | Bound |
| --- | --- | --- | --- |
| `channels.list?mine=true&part=id,snippet,statistics,contentDetails` | Authorization-time resource discovery | 1 unit | One call per completed authorization attempt; no retry for auth/scope failures |
| `channels.list?id=...&part=id,snippet,statistics,contentDetails` | Channel validation/profile snapshot | 1 unit | One per selected-channel sync |
| `playlistItems.list?playlistId=<uploads>&maxResults=50` | Upload discovery | 1 unit per page | Default 5 pages; configurable maximum 20 |
| `videos.list?id=<up to 50>&part=snippet,contentDetails,statistics,status,liveStreamingDetails` | Batch metadata/statistics | 1 unit per batch | Default 250 videos/5 batches; hard maximum 1,000 videos |
| Analytics `reports.query` with `dimensions=day` | Daily channel activity | Recorded separately from Data API units | One bounded lookback request per sync |
| Analytics `reports.query` with `dimensions=video&sort=-views&maxResults<=200` | 7/30/90-day video performance | Recorded separately from Data API units | Three bounded requests per sync |

The default maximum Data API estimate for a full selected-channel sync is 11 units: one channel call, five uploads pages, and five video batches. Normal discovery uses the uploads playlist and never uses the 100-unit `search.list` method. Every request record contains method/category, estimated cost, page/item counts, attempt count, retry class, and sanitized failure metadata—never tokens or raw payloads.

## Retention, Reconciliation, Revocation, And Deletion

- Access tokens, refresh tokens, and PKCE verifiers use the server-side AES-256-GCM envelope with an explicit key version. Browser code receives only a Google authorization URL; the client secret and tokens never enter the Vite bundle.
- A successful validation sets a deadline no more than 30 calendar days away. Normal six-hour channel syncs validate authorization and refresh stored observations well inside that limit. An unselected/no-channel authorization is purged when its validation deadline expires. If a selected authorization reaches that deadline and cannot be validated, its local authorization and YouTube data are purged.
- A Google refresh response without `refresh_token` preserves the existing unexpired refresh token. A supplied replacement is encrypted and rotated. Terminal `invalid_grant` or authorization-wide missing-scope failures trigger local authorization-wide purge. A single inaccessible selected channel is paused for reauthorization without purging usable sibling channel connections.
- User disconnect first makes a bounded call to `https://oauth2.googleapis.com/revoke`, then removes local credentials, scopes, OAuth transactions, discovered resources, connections, jobs, content, and YouTube snapshots even when Google revocation fails.
- Deletion retains only sanitized authorization/revocation/audit tombstones needed to record workspace, provider/event, timestamp, and outcome category. Tokens and deleted Authorized Data are not stored in audit payloads.
- Users may also revoke the grant from [Google Account third-party connections](https://myaccount.google.com/connections). A later bounded sync that observes terminal revocation performs the same local purge.

These controls implement the current [YouTube API Services Developer Policies](https://developers.google.com/youtube/terms/developer-policies), including the 30-day authorization/data refresh boundary. Legal approval of the final product retention statement remains external.

## Required Google Cloud Configuration

Required APIs:

- YouTube Data API v3
- YouTube Analytics API

Required web-client values:

- Application type: Web application
- Candidate exact production redirect URI: `https://lstc.nixorcorporate.com/api/integrations/youtube/callback`
- Portable placeholder if the production origin changes: `https://<verified-production-domain>/api/integrations/youtube/callback`
- Candidate homepage: `https://lstc.nixorcorporate.com/`
- Privacy: `https://lstc.nixorcorporate.com/privacy`
- Terms: `https://lstc.nixorcorporate.com/terms`
- Support: `https://lstc.nixorcorporate.com/support`
- Data deletion: `https://lstc.nixorcorporate.com/data-deletion`

Before test-user or submission readiness, an authorized project owner/editor must verify the production domain in Google Search Console, list that same domain in the consent screen's authorized domains, configure the exact two scopes, add test users, and confirm the exact redirect character-for-character. The homepage must describe the application and link the same privacy policy used in the consent screen.

The Connections action must say **Connect YouTube** (or an equally clear authorization label), identify Social Insights Studio as the requesting product, and follow current Google/YouTube branding rules for any Google or YouTube marks. Do not imply that Google or YouTube endorses the product. Final button artwork/brand review is an external verification gate.

## Reviewer Demo Shot List

Record a single continuous, legible video in English that shows:

1. Public homepage, privacy, terms, support, and deletion pages on the verified domain.
2. Sign-in to the dedicated reviewer account and entry into the reviewer workspace.
3. Connections with YouTube in its current connectable state and the two exact requested scopes.
4. Connect YouTube and the complete Google consent screen, including app name/client context and both scopes.
5. Clean callback return without `code` or `state` in the application URL.
6. Channel discovery with no automatic selection, followed by explicit channel selection.
7. First bounded sync and current connected identity/capabilities/last-successful-sync state.
8. Overview > YouTube cards, daily views/watch-time/subscriber charts, 7/30/90-day comparison, and video table.
9. Hidden/unavailable metric and reporting-delay behavior showing `N/A`/data-through semantics rather than zero.
10. Reauthorization of the existing channel, demonstrating that the selected resource is not silently replaced.
11. Disconnect confirmation, Google revocation outcome category, immediate local deletion, and the now-disconnected state.
12. The public deletion instructions and Google Account third-party connections link.

The recording must not expose a client secret, token, authorization code, personal channel data outside the reviewer fixture, terminal output, or production logs.

## Current YouTube Readiness

| Level | Status on 2026-07-18 | Evidence/blocker |
| --- | --- | --- |
| Locally implemented | Yes | Source, migration, synthetic provider mocks, MariaDB lifecycle tests, and stored-snapshot UI are present. Final status depends on the candidate validation suite. |
| Configuration-ready | Yes | Disabled-by-default environment contract, exact redirect validation, readiness warnings, and production preflight checks exist. |
| Test-user ready | No — blocked | Requires Google Cloud project/client, enabled APIs, exact test redirect, test users, credentials, and an eligible non-production YouTube channel. |
| Verification-submission ready | No — blocked | Requires verified domain, finalized consent-screen branding/contact data, legal review, live reviewer walkthrough, and demo video. |
| Verified/live | No — blocked | No Google approval, production credential configuration, deployment, or live validation was performed by this repository pass. |

## Official Sources Verified

- [OAuth for YouTube web-server applications](https://developers.google.com/youtube/v3/guides/auth/server-side-web-apps)
- [YouTube Data API reference](https://developers.google.com/youtube/v3/docs)
- [`channels.list`](https://developers.google.com/youtube/v3/docs/channels/list)
- [YouTube Analytics `reports.query`](https://developers.google.com/youtube/analytics/reference/reports/query)
- [YouTube Analytics channel reports](https://developers.google.com/youtube/analytics/channel_reports)
- [YouTube API Services Developer Policies](https://developers.google.com/youtube/terms/developer-policies)
- [Google OAuth verification requirements](https://support.google.com/cloud/answer/13464321)
