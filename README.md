# Social Insights Studio

Social Insights Studio is now a workspace-based TikTok analytics application with the legacy Looker Studio connector preserved beside it. The standalone dashboard uses Express, React/Vite, MariaDB, server-side sessions, database-backed TikTok OAuth credentials, one-shot worker syncs, stored snapshots, and CSV exports.

The production domain assumption remains `https://lstc.nixorcorporate.com`. Production deployment is cPanel/Passenger-compatible: the web process serves the API and compiled Vite app, migrations run as an explicit release command, and cron runs the bounded worker.

## Current Architecture

- `server/index.js` keeps the legacy connector routes and mounts the new application API under `/api`.
- `server/platform/` contains auth, sessions, RBAC, TikTok connection lifecycle, sync, dashboard, and export services.
- `server/migrations/` contains explicit MariaDB migrations. Migrations never run from normal web requests.
- `server/worker.js` runs bounded cron-safe jobs such as due TikTok syncs.
- `apps/web/` contains the React/Vite dashboard shell served by Express under `/app` after `npm run web:build`.
- `Code.gs` remains the legacy Apps Script connector. Do not delete the encrypted file store until a production migration/retirement plan is approved.

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
- Bounded worker syncs use MariaDB leases, refresh credentials when needed, write immutable profile/content snapshots, record partial/failed states, preserve last valid data, and stagger six-hour schedules.
- Dashboard APIs read stored snapshots only; page requests do not fetch TikTok directly.
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
- `GOOGLE_OIDC_CLIENT_ID` and mail settings when production auth providers are enabled
- `LOOKER_CLIENT_ID`
- `LOOKER_REDIRECT_URIS`
- `SYNC_INTERVAL_SECONDS`
- `MANUAL_SYNC_COOLDOWN_SECONDS`
- `WORKER_TIME_BUDGET_SECONDS`
- `TRUST_PROXY`

Google OIDC currently fails closed unless configured and completed; magic links are available with a development-only mail adapter boundary.

## Production Deployment Notes

For a controlled cPanel staging deployment and TikTok Sandbox verification, use
[`docs/cpanel-staging-runbook.md`](docs/cpanel-staging-runbook.md).

1. Install dependencies for `server/` and `apps/web/`.
2. Build the web app with `npm run web:build`.
3. Set Passenger to run `server/index.js`.
4. Configure production MariaDB and set `DATABASE_URL`.
5. Run migrations explicitly with `node server/scripts/migrate.js up --database dev` or the production-equivalent target command.
6. Add a cron entry similar to:

   ```bash
   cd /path/to/social && npm run worker -- sync-due --time-budget-seconds 240
   ```

7. Keep private token/state file-store paths configured for the legacy connector until retirement is approved.
8. Set `LOOKER_REDIRECT_URIS` to the exact Apps Script callback URI for the retained connector.
9. Use a precise `TRUST_PROXY` hop count for Passenger/Cloudflare.

Backups, restore testing, legal retention approvals, final Google OIDC verification, mail delivery, and production TikTok review are external release gates.

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
