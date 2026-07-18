# Social Insights Studio

Social Insights Studio is a workspace-based, multiplatform analytics application with read-only TikTok plus gated YouTube, Facebook Pages, Instagram, and Google Analytics 4 verticals. The legacy TikTok Looker Studio connector remains preserved beside them. The standalone dashboard uses Express, React/Vite, MariaDB, server-side sessions, encrypted provider credentials, bounded one-shot worker syncs, stored snapshots, and CSV exports.

The production domain assumption remains `https://lstc.nixorcorporate.com`. Production deployment is cPanel/Passenger-compatible: the web process serves the API and compiled Vite app, migrations run as an explicit release command, and cron runs the bounded worker.

Production alignment is currently an evidence-backed assumption: local `phase-3-dashboard` commit `cc8a30818a96f2fbf11018580033942c31f0e622` matched the deployed frontend asset hashes and runtime health endpoints during read-only checks, but that does not prove deployed commit identity. Future releases should inject `APP_COMMIT_SHA` so `/health/version` can report the exact source revision.

## Current Architecture

- `server/index.js` keeps the legacy connector routes and mounts the new application API under `/api`.
- `server/platform/` contains auth, sessions, RBAC, provider connection lifecycles, sync, dashboard, and export services.
- `server/migrations/` contains explicit MariaDB migrations. Migrations never run from normal web requests.
- `server/worker.js` runs bounded cron-safe jobs for due TikTok and enabled YouTube, Facebook Pages, Instagram, and Google Analytics 4 connections.
- `apps/web/` contains the React/Vite application shell served by Express at `/` after `npm run web:build`.
- `Code.gs` remains the legacy Apps Script connector. Do not delete the encrypted file store until a production migration/retirement plan is approved.
- `server/platform/provider-registry.js` records the current provider catalog. TikTok, YouTube, Facebook Pages, Instagram, and Website Analytics are implemented. Newer provider verticals are disabled by default until their exact OAuth configuration, provider-review evidence, and eligible live test resources are ready.
- Express serves hashed frontend files only from `/assets/`, serves the logo and standalone compliance pages from an explicit allowlist, and returns JSON for unknown `/api/*` routes.
- `/privacy`, `/terms`, `/support`, `/data-deletion`, and `/status` remain public without a session. Their `.html` aliases remain available for provider-console compatibility.
- Legacy `/app`, `/app/`, and `/app/*` URLs redirect permanently to the equivalent canonical-root path while preserving query parameters. `BASE_URL` remains the site origin with no path suffix.

## Local MariaDB

Local development and integration tests use the repository-managed Docker Compose MariaDB service. It binds only to `127.0.0.1`, uses a named volume, and creates both development and test databases. Real local credentials live in ignored `.env.database.local`; `.env.example` contains only placeholders.

| Command | Purpose |
| :--- | :--- |
| `npm run db:up` | Start MariaDB 11.4 on `127.0.0.1:3307`. |
| `npm run db:wait` | Wait until MariaDB is ready for the app user. |
| `npm run db:migrate:dev` | Apply migrations to `social_insights_dev`. |
| `npm run db:migrate:test` | Apply migrations to `social_insights_test`. |
| `npm run db:status` | Show development migration status. |
| `node server/scripts/migrate.js status --database test` | Show test migration status. |
| `npm run db:seed` | Insert clearly labeled local demo fixtures; refuses production/non-local database URLs. |
| `npm run db:reset` | Drop and recreate only the local development/test databases; refuses non-local hosts. |
| `npm run test:db` | Run real MariaDB integration tests. |
| `npm run db:down` | Stop the local service without deleting the named volume. |

Do not point destructive tests or reset commands at production or shared remote data.

## Application Commands

| Command | Purpose |
| :--- | :--- |
| `npm --prefix server test` | Run backend tests, including Phase 0 regressions and MariaDB integration tests. |
| `npm run web:build` | Type-check and build the React dashboard. |
| `npm run worker -- sync-due --time-budget-seconds 240` | Run a bounded due-sync worker suitable for cron. |
| `npm --prefix server audit --omit=dev` | Audit backend production dependencies. |
| `npm --prefix apps/web audit --omit=dev` | Audit web production dependencies. |

## Implemented Platform Flows

- Email magic-link sign-in with hashed, single-use, short-lived tokens and development-only token return when `AUTH_DEV_MAGIC_LINKS=true`.
- Opaque server-side sessions in `HttpOnly`, `SameSite=Lax` cookies with CSRF protection for state-changing routes.
- Workspaces, owner/admin/analyst/viewer memberships, invitations, last-owner protection, and centralized server-enforced RBAC.
- Workspace-bound TikTok OAuth start/callback flow with hashed state, relative-only internal return paths, encrypted AES-256-GCM credentials, scope state, reconnect-required handling, and audit events.
- TikTok disconnect attempts provider revoke before local credential disabling and stops future sync jobs.
- Workspace/session/user/scope/redirect-bound YouTube OAuth uses hashed state, S256 PKCE, offline incremental authorization, exact read-only scopes, encrypted access/refresh tokens, and explicit discovered-channel selection.
- YouTube disconnect attempts Google revocation and immediately purges local credentials, resources, connections, and snapshots. Terminal external revocation (`invalid_grant`) causes the same local purge on detection.
- Facebook Login for Business authorizations bind state to the workspace, session, user, provider, exact scopes, and exact callback. Page and linked Instagram professional resources require explicit selection; reauthorization never silently replaces the selected resource.
- Meta app-user and Page tokens use the existing AES-256-GCM envelope. Page/content/Instagram insight requests run only in the worker, add `appsecret_proof`, and are bounded by time, page, item, retry, and provider-usage limits.
- Meta disconnect, deauthorization, and data-deletion callbacks revoke where possible and purge local credentials, resources, jobs, content, and snapshots. Signed callbacks are HMAC-verified, time-bounded, and replay-protected.
- Website Analytics uses a dedicated Google OAuth client with exactly `analytics.readonly`, S256 PKCE, encrypted access/refresh tokens, bounded Admin/Data API discovery, and explicit GA4 property selection. Worker-only reports store property-timezone metrics, daily traffic, compatible aggregate breakdowns, data-through dates, and privacy-threshold states.
- GA4 disconnect preserves a shared authorization while sibling properties remain selected; removing the final property attempts Google revocation and purges local credentials, resources, observations, and jobs. Terminal external revocation performs the same local purge.
- Bounded worker syncs use MariaDB leases, refresh credentials when needed, write immutable profile/content/provider Analytics snapshots, record request/quota/retry metadata and partial/failed states, preserve last valid data, and stagger six-hour schedules.
- Dashboard APIs read stored snapshots only; page requests do not fetch provider APIs directly.
- CSV content exports are workspace-scoped, analyst-or-higher, formula-injection safe, and recorded in export tables.

## Required Environment

Copy `.env.example` to a real environment file or configure variables in the hosting panel. Never commit secrets.

Important variables:

- `BASE_URL`
- `DATABASE_URL`
- `DATABASE_TEST_URL`
- `ENCRYPTION_KEY`
- `ENCRYPTION_KEY_VERSION`
- `TIKTOK_CLIENT_KEY`
- `TIKTOK_CLIENT_SECRET`
- `TIKTOK_REDIRECT_URI`
- `YOUTUBE_ENABLED` (defaults to disabled)
- `YOUTUBE_CLIENT_ID`
- `YOUTUBE_CLIENT_SECRET`
- `YOUTUBE_REDIRECT_URI`
- `FEATURE_FACEBOOK_PAGES_CONNECTOR` (defaults to disabled)
- `FEATURE_INSTAGRAM_CONNECTOR` (defaults to disabled)
- `META_APP_ID`
- `META_APP_SECRET`
- `META_FACEBOOK_LOGIN_CONFIG_ID`
- `META_INSTAGRAM_LOGIN_CONFIG_ID`
- `META_GRAPH_API_VERSION`
- `FACEBOOK_REDIRECT_URI`
- `INSTAGRAM_REDIRECT_URI`
- `META_FACEBOOK_APPROVED_SCOPES`
- `META_INSTAGRAM_APPROVED_SCOPES`
- `FEATURE_GA4_CONNECTOR` (defaults to disabled)
- `GA4_CLIENT_ID`
- `GA4_CLIENT_SECRET`
- `GA4_REDIRECT_URI`
- `GOOGLE_OIDC_CLIENT_ID` and mail settings when production auth providers are enabled
- `LOOKER_CLIENT_ID`
- `LOOKER_REDIRECT_URIS`
- `SYNC_INTERVAL_SECONDS`
- `MANUAL_SYNC_COOLDOWN_SECONDS`
- `WORKER_TIME_BUDGET_SECONDS`
- `TRUST_PROXY`
- `APP_COMMIT_SHA`
- `APP_BUILD_TIME` or `APP_RELEASE`
- provider feature gates such as `FEATURE_TIKTOK_CONNECTOR`, `YOUTUBE_ENABLED`, `FEATURE_FACEBOOK_PAGES_CONNECTOR`, `FEATURE_INSTAGRAM_CONNECTOR`, and `FEATURE_GA4_CONNECTOR`

Google OIDC currently fails closed unless configured and completed; magic links are available with a development-only mail adapter boundary.

## Deployment Provenance

The backend exposes `GET /health/version` with sanitized deployment metadata:

- `APP_COMMIT_SHA`: exact git SHA for the deployed source state.
- `APP_BUILD_TIME`: optional build timestamp.
- `APP_RELEASE` or `APP_RELEASE_ID`: optional human release identifier.

`/health/ready` warns when production commit metadata is missing, but readiness does not fail solely because build time or release labels are absent. Do not expose host paths, full environment dumps, dependency inventories, or secret values in provenance output. The Vite build also accepts the same metadata at build time for the dashboard account screen.

## Production Deployment Notes

For a controlled cPanel staging deployment and TikTok Sandbox verification, use
[`docs/cpanel-staging-runbook.md`](docs/cpanel-staging-runbook.md).

1. Install dependencies for `server/` and `apps/web/`.
2. Build the web app with `npm run web:build`.
3. Export `APP_COMMIT_SHA=$(git rev-parse HEAD)` in the cPanel/Passenger environment for the deployed source revision.
4. Set Passenger to run `server/index.js`.
5. Ensure the cPanel domain document root does not contain a separately served `index.html`; Express/Passenger must own `/`.
6. Configure production MariaDB and set `DATABASE_URL`.
7. Run migrations explicitly with `node server/scripts/migrate.js up --database dev` or the production-equivalent target command.
8. Add a cron entry similar to:

   ```bash
   cd /path/to/social && npm run worker -- sync-due --time-budget-seconds 240
   ```

9. Keep private token/state file-store paths configured for the legacy connector until retirement is approved.
10. Set `LOOKER_REDIRECT_URIS` to the exact Apps Script callback URI for the retained connector.
11. Use a precise `TRUST_PROXY` hop count for Passenger/Cloudflare.

Backups, restore testing, legal retention approvals, Google OAuth/YouTube/Analytics verification, Meta dashboard configuration/review, mail delivery, and production provider reviews are external release gates. Reviewer and operations notes are documented in [`docs/youtube-readonly-integration.md`](docs/youtube-readonly-integration.md), [`docs/meta-readonly-integration.md`](docs/meta-readonly-integration.md), and [`docs/google-analytics-readonly-integration.md`](docs/google-analytics-readonly-integration.md).

## Legacy Looker Connector

The Apps Script connector still supports the existing Looker flow. Phase 0 hardening is preserved:

- exact Apps Script redirect allowlisting;
- client and redirect binding for internal authorization codes;
- no insecure `unused` client-secret default;
- provider HTTP timeouts and categorized failures;
- provider revoke attempted before local deletion;
- no silent Apps Script partial success on video fetch failure.

The legacy TikTok callback remains `/auth/tiktok/callback`; the standalone dashboard callback is `/api/integrations/tiktok/callback`.

## TikTok Scopes

Only these scopes are requested for the standalone dashboard:

- `user.info.basic`
- `user.info.profile`
- `user.info.stats`
- `video.list`

TikTok cover-image URLs are treated as ephemeral and are not stored as durable media assets.

## YouTube Read-Only Integration

YouTube is gated by `YOUTUBE_ENABLED=false` by default and requests exactly:

- `https://www.googleapis.com/auth/youtube.readonly`
- `https://www.googleapis.com/auth/yt-analytics.readonly`

The worker reads owned/managed channel identity, uploads/video metadata, lifetime channel/video counters, and non-monetary YouTube Analytics metrics. A default run uses at most five upload-playlist pages and five 50-video batches, for an estimated maximum of 11 YouTube Data API quota units plus bounded Analytics requests. The dashboard exposes provider data-through dates and leaves unavailable metrics as `N/A`; it does not infer engagement or revenue.

## Meta Read-Only Integrations

Facebook Pages is gated by `FEATURE_FACEBOOK_PAGES_CONNECTOR=false` by default and requests exactly:

- `pages_show_list`
- `pages_read_engagement`
- `read_insights`

Instagram uses Facebook Login only and is independently gated by `FEATURE_INSTAGRAM_CONNECTOR=false`. It requests exactly:

- `instagram_basic`
- `instagram_manage_insights`
- `pages_show_list`
- `pages_read_engagement`

Runtime scope assertions must match those sets exactly or the provider remains non-connectable. The callbacks are `/api/integrations/facebook/callback` and `/api/integrations/instagram/callback`. Neither flow requests publishing, comment-management, messaging, ads, app events, demographics, webhooks, or `business_management`. Instagram must remain disabled until those exact permissions are confirmed for the configured Facebook Login for Business path. Resources whose Business Manager access path requires Meta's additional ads permissions are deliberately unsupported. Instagram account totals are stored as explicit provider-reported 7/30/90-day windows, not synthetic daily values.

## Google Analytics 4 Read-Only Integration

Website Analytics is gated by `FEATURE_GA4_CONNECTOR=false` by default and requests exactly:

- `https://www.googleapis.com/auth/analytics.readonly`

It uses the exact callback `/api/integrations/google-analytics/callback` and a dedicated OAuth client that must differ from Google sign-in and YouTube. Authorization discovers GA4 properties but never selects one automatically. The worker uses only Analytics Admin/Data read methods, stores aggregate metrics and compatible breakdowns, honors the property timezone, preserves threshold/delay states, and never sums distinct-user or provider-computed rate metrics across days. See [`docs/google-analytics-readonly-integration.md`](docs/google-analytics-readonly-integration.md).
