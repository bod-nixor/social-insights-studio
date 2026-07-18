# ADR 0002: Provider Scope And API Baseline

Status: Accepted; YouTube source implementation complete

Date: 2026-07-18

Documentation access date: 2026-07-18

## Decision

Use incremental provider authorization and request only read-only scopes that map to visible analytics features. New providers remain disabled by feature flag until implementation, review evidence, and sandbox/live verification are complete.

## Provider Baseline

| Provider | API route | Starting scopes or permissions | Product state |
| --- | --- | --- | --- |
| TikTok | Login Kit for Web plus Display API | `user.info.basic`, `user.info.profile`, `user.info.stats`, `video.list` | Keep current dashboard and Looker connector behavior intact. |
| Instagram | Current Instagram Platform for professional accounts | Candidate read-only analytics set: `instagram_business_basic`, `instagram_business_manage_insights` | Feature flagged until Meta docs, app review, and reviewer evidence are complete. |
| Facebook Pages | Meta Graph API Pages and Page Insights | `pages_show_list`, `pages_read_engagement`, `read_insights` | Feature flagged until Page discovery and Page insights screens exist. |
| YouTube | Google OAuth incremental auth, YouTube Data API, YouTube Analytics API | `https://www.googleapis.com/auth/youtube.readonly`, `https://www.googleapis.com/auth/yt-analytics.readonly` | Source-complete and disabled by default; production enablement remains gated on Google configuration, review evidence, and live verification. |
| Website Analytics | Google OAuth incremental auth, GA4 Admin/Data APIs | `https://www.googleapis.com/auth/analytics.readonly` | Feature flagged until GA4 property discovery, compatibility checks, and dashboard/report views exist. |

## Official References

- TikTok App Review Guidelines: https://developers.tiktok.com/doc/app-review-guidelines/
- TikTok Login Kit for Web: https://developers.tiktok.com/doc/login-kit-web/
- TikTok Display API overview: https://developers.tiktok.com/doc/display-api-overview/
- TikTok user access token management: https://developers.tiktok.com/doc/oauth-user-access-token-management
- Instagram Platform overview: https://developers.facebook.com/documentation/instagram-platform/overview
- Instagram Platform insights: https://developers.facebook.com/documentation/instagram-platform/insights
- Instagram App Review: https://developers.facebook.com/documentation/instagram-platform/app-review
- Meta permissions reference: https://developers.facebook.com/docs/permissions/
- Facebook Page insights: https://developers.facebook.com/docs/graph-api/reference/page/insights/
- YouTube Data API channels list: https://developers.google.com/youtube/v3/docs/channels/list
- YouTube Analytics channel reports: https://developers.google.com/youtube/analytics/channel_reports
- YouTube API Services Developer Policies: https://developers.google.com/youtube/terms/developer-policies
- GA4 Data API REST: https://developers.google.com/analytics/devguides/reporting/data/v1/rest
- GA4 runReport: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport
- GA4 getMetadata: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata
- GA4 checkCompatibility: https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/checkCompatibility
- Google OAuth scopes: https://developers.google.com/identity/protocols/oauth2/scopes
- Google OAuth verification requirements: https://support.google.com/cloud/answer/13464321

## Notes From Verification

- TikTok review requires a fully developed public website, visible Privacy Policy and Terms of Service links, a matching app name/icon, and demo video evidence for every selected product and scope.
- Google OAuth verification requires the homepage to be on a verified owned domain, not only a login page, and requires narrowest scopes with demo evidence for the requested user-facing functionality.
- GA4 `runReport` accepts `analytics.readonly` for read-only reporting; use `getMetadata` and `checkCompatibility` before assuming metrics and dimensions can be combined.
- Meta permission names and Graph API versions change. Re-verify exact permission names and app review classifications in the Meta dashboard immediately before requesting permissions or submitting review.
