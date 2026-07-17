# cPanel Staging and TikTok Sandbox Runbook

This runbook prepares a controlled staging deployment for `https://lstc.nixorcorporate.com`.
It does not require Docker in staging and does not migrate legacy connector credentials automatically.
Do not modify DNS, TikTok production settings, production credentials, or existing legacy connector data while following it.

## Verified Routes and Runtime Shape

- Standalone dashboard TikTok callback route: `GET /api/integrations/tiktok/callback`
- Exact standalone staging callback URI: `https://lstc.nixorcorporate.com/api/integrations/tiktok/callback`
- Legacy connector TikTok callback route retained for Looker: `GET /auth/tiktok/callback`
- React dashboard URL: `https://lstc.nixorcorporate.com/app/`
- API mount: `https://lstc.nixorcorporate.com/api/`
- Health endpoints:
  - `https://lstc.nixorcorporate.com/health/live`
  - `https://lstc.nixorcorporate.com/health/ready`
- Passenger entry point: `server/index.js`
- Frontend build output: `apps/web/dist`, served by Express under `/app` when `npm run web:build` has been run.
- Vite base path: `/app/`

## Supported Node Version

Use Node.js `22.12+` or `24.x` in cPanel. Local verification was performed with Node.js `24.18.0`.
The frontend uses Vite 7, which requires Node.js `20.19+` or `22.12+`; prefer cPanel Node.js 22 LTS or newer.

## Staging Safety Model

- `NODE_ENV=production` is required.
- `BASE_URL` must be `https://lstc.nixorcorporate.com`; localhost and HTTP are rejected in production.
- `AUTH_DEV_MAGIC_LINKS=true` is rejected in production.
- `MAIL_ADAPTER=development` or missing production mail settings are rejected in production.
- Wildcard `ALLOWED_ORIGINS` and wildcard `LOOKER_REDIRECT_URIS` are rejected.
- Placeholder database URLs, TikTok credentials, encryption keys, key versions, and backend JWT secrets are rejected by production preflight.
- `TIKTOK_REDIRECT_URI` must exactly match `https://lstc.nixorcorporate.com/api/integrations/tiktok/callback`.
- Migration execution is explicit through `server/scripts/migrate.js`; migrations are not run from web requests.
- Local `db:seed` and `db:reset` refuse `NODE_ENV=production`; do not run either command on staging.
- Standalone OAuth transactions, sessions, CSRF, credentials, sync jobs, snapshots, and audit logs are stored in MariaDB.
- Legacy Looker token/state file stores remain file-backed and must stay outside `public_html`.
- Cron workers use MariaDB leases in `sync_jobs` and bounded time budgets.
- Production cookies are emitted with `Secure`, session cookies are `HttpOnly`, and CSRF cookies use `SameSite=Lax`.

## cPanel UI Actions

1. Create a staging MariaDB database in cPanel, for example `CPANELUSER_social_staging`.
2. Create a least-privilege MariaDB user, for example `CPANELUSER_sis_stage`.
3. Grant that user privileges only on the staging database. Required privileges: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `INDEX`, `REFERENCES`, and `DROP` for controlled rollback/forward-fix windows. Remove `DROP` after migration/reset procedures if your cPanel workflow allows narrower day-to-day grants.
4. Confirm MariaDB is reachable only from the hosting account or localhost. Do not enable remote/public MySQL access.
5. In cPanel Setup Node.js App:
   - Node version: Node.js 22.12+ or 24.x.
   - Application mode: `production`.
   - Application root: the repository staging directory.
   - Application startup file: `server/index.js`.
   - Application URL: `https://lstc.nixorcorporate.com`.
6. Configure environment variables from `.env.staging.example` with real staging values. Do not paste secrets into Git.
7. Create a private writable directory outside public web root, for example `/home/CPANEL_USER/secure/social-insights-staging`, with permissions `700`.
8. Configure cron after the app and migrations pass, using the worker command below.
9. Restart the Passenger app only after preflight and migrations pass.

## Required Environment Variables

Use `.env.staging.example` as the sanitized template. Required staging values include:

- `NODE_ENV=production`
- `BASE_URL=https://lstc.nixorcorporate.com`
- `DATABASE_URL`
- `SESSION_TTL_SECONDS`
- `SESSION_IDLE_SECONDS`
- `AUTH_DEV_MAGIC_LINKS=false`
- `MAIL_ADAPTER=smtp`
- `MAIL_FROM`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_SECURE`
- `SMTP_REQUIRE_TLS`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `GOOGLE_OIDC_CLIENT_ID` and `GOOGLE_OIDC_CLIENT_SECRET` if Google sign-in is enabled
- `TIKTOK_CLIENT_KEY`
- `TIKTOK_CLIENT_SECRET`
- `TIKTOK_REDIRECT_URI=https://lstc.nixorcorporate.com/api/integrations/tiktok/callback`
- `ENCRYPTION_KEY`
- `ENCRYPTION_KEY_VERSION`
- `ENCRYPTION_PREVIOUS_KEYS` when rotating keys
- `BACKEND_JWT_SECRET`
- `ALLOWED_ORIGINS=https://lstc.nixorcorporate.com`
- `LOOKER_CLIENT_ID`
- `LOOKER_CLIENT_SECRET` only if the legacy connector is changed to send one
- `LOOKER_REDIRECT_URIS` with exact Apps Script callback URLs only
- `TOKEN_STORE_PATH`, `TOKEN_LOCK_PATH`, `STATE_STORE_PATH`, `STATE_LOCK_PATH`
- `TRUST_PROXY`
- `SYNC_INTERVAL_SECONDS`, `SYNC_STAGGER_SECONDS`, `SYNC_LEASE_SECONDS`, `SYNC_RETRY_BASE_SECONDS`, `SYNC_RETRY_MAX_SECONDS`, `MANUAL_SYNC_COOLDOWN_SECONDS`, `WORKER_TIME_BUDGET_SECONDS`
- `LONG_TERM_RETENTION_ENABLED=false`
- `RETENTION_POLICY_VERSION=unapproved-staging`

For Cloudflare plus Passenger, start with `TRUST_PROXY=2`. If only Passenger sets the forwarded headers, use `TRUST_PROXY=1`.

## cPanel Commands in Execution Order

Run these from cPanel Terminal or SSH after uploading/checking out the repository into the staging app root. Replace uppercase placeholders.

```bash
cd /home/CPANEL_USER/apps/social-insights-staging/current
node -v
npm -v
```

Create private writable paths:

```bash
mkdir -p /home/CPANEL_USER/secure/social-insights-staging
chmod 700 /home/CPANEL_USER/secure/social-insights-staging
```

Install production backend dependencies and frontend build dependencies:

```bash
npm --prefix server ci --omit=dev
npm --prefix apps/web ci
```

Build the frontend:

```bash
npm run web:build
```

Optionally prune frontend dev-only packages after the build:

```bash
npm --prefix apps/web prune --omit=dev
```

Validate production configuration without starting Passenger:

```bash
npm run preflight:production
```

Back up before migrations:

```bash
mkdir -p /home/CPANEL_USER/backups/social-insights-staging
mysqldump --single-transaction --routines --triggers \
  -u CPANEL_DB_USER -p CPANEL_DB_NAME \
  > /home/CPANEL_USER/backups/social-insights-staging/db-$(date +%Y%m%d-%H%M%S).sql
tar -czf /home/CPANEL_USER/backups/social-insights-staging/files-$(date +%Y%m%d-%H%M%S).tgz \
  --exclude=node_modules --exclude=apps/web/node_modules --exclude=server/node_modules \
  --exclude=apps/web/dist --exclude=.git .
```

Check and apply migrations. In this migration script, `--database dev` means the primary `DATABASE_URL`; it is not the local Docker database when `DATABASE_URL` points at cPanel MariaDB.

```bash
node server/scripts/migrate.js status --database dev
node server/scripts/migrate.js up --database dev
node server/scripts/migrate.js status --database dev
```

Restart Passenger:

```bash
mkdir -p tmp
touch tmp/restart.txt
```

If cPanel's Node.js UI exposes a Restart button, use that after `touch tmp/restart.txt`.

Health/readiness smoke:

```bash
curl -fsS https://lstc.nixorcorporate.com/health/live
curl -fsS https://lstc.nixorcorporate.com/health/ready
curl -fsS -I https://lstc.nixorcorporate.com/app/
```

Worker smoke:

```bash
node server/worker.js sync-due --time-budget-seconds 1
```

Cron command:

```bash
cd /home/CPANEL_USER/apps/social-insights-staging/current && \
node server/worker.js sync-due --time-budget-seconds 240 \
>> /home/CPANEL_USER/logs/social-insights-worker.log 2>&1
```

If cPanel cron does not inherit the Node app environment variables, create an untracked `.env` from `.env.staging.example` with real values and permissions `600`, or use a private shell wrapper that exports the same variables before running the worker.

## Staging Smoke Tests

1. Open `https://lstc.nixorcorporate.com/app/`.
2. Request a sign-in code with a staging email address.
3. Confirm the code arrives through the real SMTP adapter.
4. Verify that the API response does not include `dev_token`.
5. Paste the email code into the app and sign in.
6. Create a new staging workspace.
7. Confirm workspace navigation, account menu, and sign-out work.
8. Confirm unauthenticated API requests return `401` without stack traces:

   ```bash
   curl -i https://lstc.nixorcorporate.com/api/session
   ```

9. Confirm readiness does not expose credentials:

   ```bash
   curl -fsS https://lstc.nixorcorporate.com/health/ready
   ```

10. Do not run `npm run db:seed` or `npm run db:reset` on staging.

## TikTok Sandbox Portal Actions

Use TikTok Sandbox mode only. Do not edit the live production app configuration.

1. Create or select the TikTok Sandbox app.
2. Use the sandbox client key and sandbox client secret in staging env.
3. Set the sandbox website URL/domain to `https://lstc.nixorcorporate.com`.
4. Register this exact redirect URI:

   ```text
   https://lstc.nixorcorporate.com/api/integrations/tiktok/callback
   ```

5. Request only these scopes:
   - `user.info.basic`
   - `user.info.profile`
   - `user.info.stats`
   - `video.list`
6. Add the intended TikTok test account as a sandbox target user.
7. Save sandbox settings and wait for the portal to show them as active.

## TikTok Sandbox Test Checklist

Do not claim success until the real sandbox flow completes.

- Start from `https://lstc.nixorcorporate.com/app/`.
- Sign in with staging mail or Google OIDC if enabled.
- Create or select a staging workspace.
- Start TikTok connection from the Connections view.
- Approve consent with the sandbox target user.
- Verify callback state succeeds and returns to `/app/`.
- Confirm a provider account and encrypted OAuth credential row are created without plaintext tokens.
- Confirm granted scopes are recorded and missing scopes are surfaced if any are denied.
- Run initial worker sync:

  ```bash
  node server/worker.js sync-due --time-budget-seconds 240
  ```

- Confirm profile snapshots, content items, and content metric snapshots appear.
- Confirm pagination by using a sandbox account with enough videos or by checking logged video-list page calls if available.
- Confirm dashboard overview and content table show stored snapshot data only.
- Confirm CSV export contains no credentials and remains formula-injection safe.
- Confirm token refresh by using an expired/near-expired sandbox access token if the sandbox supports it, then run the worker.
- Test consent denial and verify the app reports failure without leaking provider code details to the browser.
- Test invalid/replayed callback state and verify failure.
- Test reconnect-required behavior by removing a required scope or using an expired/revoked sandbox token.
- Test disconnect and verify provider revoke is attempted before local disablement.
- Record provider error code, description, and TikTok log ID when available. Do not record access tokens, refresh tokens, authorization codes, cookies, or CSRF tokens.
- Verify no credentials appear in browser storage, logs, CSV exports, audit metadata, or dashboard JSON responses.

## Backup and Rollback

Before every migration or app replacement:

1. Run `mysqldump --single-transaction --routines --triggers`.
2. Archive the current app directory excluding dependency and build output directories.
3. Record the current Git commit or release artifact identifier.
4. Record the currently applied migration status.

Rollback app build:

1. Restore the prior app directory or switch the app root symlink back to the previous release.
2. Run `npm --prefix server ci --omit=dev` if dependencies changed.
3. Restore or reuse the prior `apps/web/dist` artifact.
4. Restart Passenger with `touch tmp/restart.txt` or the cPanel UI.
5. Check `/health/live`, `/health/ready`, and `/app/`.

Database rollback:

- Prefer forward-fix migrations for schema mistakes after a migration has been applied.
- Restore from `mysqldump` only if the staging database can be fully replaced and no useful staging data needs to be preserved.
- Never restore over production or a shared remote database.

## Remaining External Gates

- cPanel staging database and least-privilege user creation.
- Real staging SMTP credentials and delivery verification.
- Optional Google OIDC client setup and browser UI enablement if Google sign-in is required.
- TikTok Sandbox app settings, sandbox user access, and real consent flow completion.
- Backup restore drill.
- cPanel/Passenger restart validation on the actual host.
- Confirmation that the legacy Looker deployment remains untouched.
