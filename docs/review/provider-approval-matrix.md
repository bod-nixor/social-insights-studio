# Provider Approval Matrix

Date: 2026-07-17

Status values:

- Implemented: currently implemented in the phase-3 dashboard or retained connector.
- Planned: implementation and evidence required before requesting permission.
- Blocked: external facts, provider console work, credentials, legal review, or live verification required.

| Provider | Scope or permission | Access level | User action | Visible feature | Endpoint or API | Storage and retention | Disconnect/deletion | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TikTok | `user.info.basic` | Read | Connect TikTok | Account identity in dashboard and Looker Studio connector data | `/v2/user/info/` | Encrypted token, stored snapshots, no raw provider response by default | Provider revoke plus local disable where available | Implemented |
| TikTok | `user.info.profile` | Read | Connect TikTok | Profile fields where returned by TikTok | `/v2/user/info/` | Encrypted token, stored snapshots, no raw provider response by default | Provider revoke plus local disable where available | Implemented |
| TikTok | `user.info.stats` | Read | Connect TikTok | Follower, following, likes, video count metrics | `/v2/user/info/` | Encrypted token, stored snapshots, no raw provider response by default | Provider revoke plus local disable where available | Implemented |
| TikTok | `video.list` | Read | Connect TikTok | Video rows and available video statistics | `/v2/video/list/` | Encrypted token, stored snapshots, no raw provider response by default | Provider revoke plus local disable where available | Implemented |
| Instagram | `instagram_business_basic` | Read | Connect Instagram | Discover and display professional accounts | Instagram Platform / Graph API | Normalized resources and snapshots after implementation | Provider revocation plus local deletion | Planned |
| Instagram | `instagram_business_manage_insights` | Read | Connect Instagram | Account and media insights dashboards/reports | Instagram insights endpoints | Normalized observations, no raw response by default | Provider revocation plus local deletion | Planned |
| Facebook Pages | `pages_show_list` | Read | Connect Facebook Pages | Page discovery and selection | Meta Graph API Pages | Normalized resources after implementation | Provider revocation plus local deletion | Planned |
| Facebook Pages | `pages_read_engagement` | Read | Connect Facebook Pages | Page/post identity and engagement analytics | Meta Graph API Pages/posts | Normalized resources and observations | Provider revocation plus local deletion | Planned |
| Facebook Pages | `read_insights` | Read | Connect Facebook Pages | Page and post insight metrics | Page Insights | Normalized observations | Provider revocation plus local deletion | Planned |
| YouTube | `https://www.googleapis.com/auth/youtube.readonly` | Read | Connect YouTube | Channel discovery and video metadata | YouTube Data API | Normalized resources and content snapshots | Google revocation plus local deletion | Planned |
| YouTube | `https://www.googleapis.com/auth/yt-analytics.readonly` | Read | Connect YouTube | Channel/video analytics dashboards and reports | YouTube Analytics API | Normalized observations with data-through dates | Google revocation plus local deletion | Planned |
| Website Analytics | `https://www.googleapis.com/auth/analytics.readonly` | Read | Connect Website Analytics | GA4 property discovery and reports | GA4 Admin/Data APIs | Normalized observations and compatibility metadata | Google revocation plus local deletion | Planned |

## Required Evidence Before Submission

- Homepage, privacy, terms, support, and deletion pages on the verified production domain.
- Screencast for each provider showing consent, granted scopes, resource discovery, dashboard use, disconnect, and deletion path.
- Reviewer test workspace with eligible resources and non-empty sample content where provider review requires it.
- Per-scope written justification tied to the exact visible feature and endpoint.
- Feature flag and environment validation proving disabled providers do not expose broken OAuth buttons.
- Legal/operator facts, subprocessors, retention terms, and contact details supplied by the product owner and reviewed before submission.
