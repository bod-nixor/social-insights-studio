# Production Operations Runbook

Date: 2026-07-18

This runbook describes the production boundary for Social Insights Studio on cPanel/Passenger, MariaDB, cron workers, and an optional Cloudflare proxy. Repository completion does not authorize deployment, provider-console changes, DNS changes, live account use, or review submission.

## Safe Activation Order

1. Deploy source and lockfile with every new provider and `FEATURE_PDF_REPORTS` disabled.
2. Install production dependencies with `npm ci --omit=dev` in the root, server, and web workspaces as applicable; build the Vite bundle during the release, not in Passenger requests.
3. Back up the database and current release. Record `APP_COMMIT_SHA` and release ID.
4. Run migration status, apply forward migrations once, and run status again.
5. Set cryptographic/session/mail/base URL values and run `npm run preflight:production`.
6. Start/restart Passenger and verify live, version, readiness, auth, and public legal routes with providers/reporting still disabled.
7. Start the sync cron and confirm an idle bounded invocation.
8. Configure one provider's exact external OAuth settings and reviewer fixture, run preflight again, enable only that provider, restart, and perform its consent/sync/dashboard/revoke/delete smoke tests.
9. Repeat one provider at a time. Do not enable Instagram if its exact Facebook Login permission set or eligible no-ads access path is uncertain.
10. Configure the private report root, verify ownership/permissions and free space, enable reports, restart, then run the report worker smoke and a single-resource generated/download/delete test.

Never add a broad scope to make a resource work. Stop and keep the connector disabled when the exact read-only contract is insufficient.

## Startup And Readiness

`npm run preflight:production` validates the common production boundary and every enabled provider:

- HTTPS non-loopback `BASE_URL`, exact callbacks, no wildcard origins, bounded trust proxy, production database, real mail, random JWT/encryption values, and deployment-safe store paths.
- YouTube exact two-scope client/configuration.
- Facebook Pages and Instagram independent Facebook Login for Business configuration IDs, exact callback and asserted permission sets.
- GA4 dedicated client, exact callback, and read-only configuration.
- PDF report private absolute storage root outside all public roots.

`GET /health/live` proves only that the process responds. `GET /health/version` returns sanitized release metadata. `GET /health/ready` checks database/schema-facing provider readiness, provider configuration, PDF configuration, and whether sync/report jobs are overdue. Queue states are categorical and expose no connection, user, path, token, or job identifier.

An overdue queue warning does not make readiness return 503 because automatically restarting a healthy web process does not repair a stopped cron. Alert on `sync_jobs_overdue` or `report_jobs_overdue` and inspect cron/worker logs. `WORKER_OVERDUE_SECONDS` defaults to 1,800 and is bounded from 300 to 86,400 seconds.

## Cron Workers And Cadence

Use separate cron invocations so provider sync and report rendering cannot starve each other:

```sh
cd /home/CPANEL_USER/apps/social-insights/current
/opt/cpanel/ea-nodejs22/bin/node server/worker.js sync-due --time-budget-seconds 240 >> /home/CPANEL_USER/logs/social-insights-sync.log 2>&1
/opt/cpanel/ea-nodejs22/bin/node server/worker.js reports-due --time-budget-seconds 240 >> /home/CPANEL_USER/logs/social-insights-reports.log 2>&1
```

Start both every five minutes. MariaDB leases prevent normal overlap. Never use a daemon, unbounded loop, or background process that cPanel cannot supervise.

Provider baseline cadence and policy:

| Provider | Normal selected-resource cadence | Principal bounds and quota policy |
| --- | --- | --- |
| TikTok | Six hours plus stagger | User/profile once; video list default 20 per page, max 20 pages; bounded HTTP timeout/retry category. |
| YouTube | Six hours plus stagger; authorization/data validation within 30 days | Default five upload pages and 250 videos in 50-ID batches; about 11 Data API units plus bounded Analytics reports; no `search.list`. |
| Facebook Pages | Six hours plus stagger | Maximum five content pages/100 items, 90-day lookback, bounded retry/time, usage-delay threshold at 80%; Graph calls use pinned version and `appsecret_proof`. |
| Instagram | Six hours plus stagger | Same request bounds as Meta; explicit 7/30/90 provider periods; Stories/webhooks excluded; ads-required resources rejected. |
| GA4 | Six hours plus stagger | 180-day lookback, 100 rows per aggregate dimension report, bounded Admin discovery, metadata/compatibility checks, quota summaries without raw responses. |
| PDF reports | Five-minute queue poll | 240-second invocation, leased runs, three attempts, 20 resources, 366 days, 80 pages, 20 MiB, seven-day cleanup. |

Tune downward when provider quotas or shared-hosting limits require it. Changing an API version, scope, metric set, retention boundary, or hard ceiling requires code/review documentation and a full validation pass.

## Logging, Correlation, And Redaction

- HTTP requests receive a correlation ID; OAuth/provider audit records and sync runs preserve sanitized categories/IDs where implemented.
- Worker commands emit one JSON result object to stdout. Keep log access restricted to operators and rotate it through the hosting control plane.
- Never log authorization codes, state/nonce/PKCE secrets, cookies, CSRF tokens, access/refresh/Page tokens, client secrets, SMTP passwords, encryption keys, raw provider payloads, signed Meta callbacks, one-time report tokens, artifact contents, or full environment objects.
- Log only bounded opaque IDs, provider, method/category, attempt, status, quota estimate, duration, page/item counts, sanitized provider code, retryability, and correlation ID.
- Before release, search built assets and logs for credential names and recognizable fixture/real token values. A secret found in a log is an incident even if the underlying call failed.

## Backup, Restore, And Artifact Boundary

Back up MariaDB before every migration and at the owner-approved recurring interval. Encrypt backups, restrict them to the production operator, and store them outside the public root. Record the database name, release SHA, migration status, timestamp, tool version, checksum, and retention expiry.

Generated PDF artifacts are derived, short-lived exports. Do not include `REPORT_ARTIFACT_ROOT` in routine long-term backups. Restoring an artifact without its exact active DB row/grant/expiry state can bypass the intended lifecycle. If policy explicitly requires artifact backup, it needs separate legal approval and a restore process that preserves expiry and access controls; no such approval is assumed here.

Restore drill:

1. Provision an isolated non-production MariaDB database and private application directory.
2. Verify the dump checksum and restore only into that isolated target.
3. Check migration status, row counts, tenant constraints, encryption key availability, and a read-only sign-in/dashboard smoke.
4. Do not connect restored data to provider APIs or send email without explicit test authorization.
5. Record restore duration and data age. Product owners must approve RPO/RTO and backup retention; this repository does not invent them.

Never restore a full dump as a normal rollback after a forward migration has served writes. Prefer a tested forward fix.

## Migration And Rollback Policy

- Migrations are immutable, ordered, and explicitly applied before Passenger restart.
- Validate clean install, upgrade from the previous supported schema, and status/idempotence on an isolated database before production.
- Take a backup and maintenance window when a migration rewrites constraints or large tables.
- Application rollback is allowed only when the earlier release understands the current schema. Keep schema-compatible code for one release boundary where practical.
- When a migration has committed and accepted writes, create a forward corrective migration. Do not manually edit `schema_migrations`, drop columns, or restore over the live database.
- Roll back only the release symlink/Passenger source and compiled assets after confirming schema compatibility. Preserve audit logs and new data.

## Report Storage And Cleanup

- Use an absolute path such as `/home/CPANEL_USER/secure/social-insights/report-artifacts`, not a filesystem root, `public_html`, the deployed source tree, `/tmp`, or any shared web-readable directory.
- Own it by the Passenger/cron application account, directory mode `0700`; artifacts are worker-created mode `0600`.
- Passenger and cron must use the same environment and root.
- Alert before disk exhaustion. The worker returns sanitized `report_storage_unavailable` for permission/space failures and retries within its bounded attempts.
- Run `reports-due` even during low report volume because it also expires grants and seven-day artifacts.
- Do not serve the directory directly through Apache/Nginx. Downloads must pass through the authorized one-time route.

## Incident Response

1. Contain: disable the affected provider/report flag, restart Passenger, stop only the relevant cron if continuing work could expand impact, and preserve sanitized logs/audit records.
2. Classify: credential exposure, unauthorized workspace access, deletion failure, provider outage/rate limit, corrupt report, database availability, or storage exhaustion.
3. Revoke/rotate: revoke provider grants and rotate provider secrets, JWT secret, mail password, or encryption key as appropriate. Do not rotate everything blindly if it prevents investigation or recovery.
4. Purge: run the documented provider/account/workspace deletion path for affected Authorized Data. Remove derived report artifacts/grants when the report or workspace boundary is affected.
5. Recover: apply a reviewed forward fix, migrate, preflight, enable one bounded component, and smoke with non-production fixtures.
6. Notify: follow owner-approved legal/provider/user notification obligations. Do not claim a response deadline until the owner supplies one.
7. Review: document timeline, correlation IDs, affected scopes/resources, root cause, evidence, rotations, deletions, and prevention actions without copying secrets into the incident report.

## Encryption Key Rotation

1. Generate a new random 32-byte key and a new non-placeholder `ENCRYPTION_KEY_VERSION` in the secret manager/control panel.
2. Put the prior version/key in `ENCRYPTION_PREVIOUS_KEYS`; keep the new key as `ENCRYPTION_KEY`.
3. Run preflight and deterministic decrypt tests in staging.
4. Deploy/restart. New or refreshed credentials write the new version; old envelopes remain readable through the previous-key map.
5. Re-encrypt remaining old-version credentials through a reviewed bounded migration/job before removing the prior key.
6. Query version counts and verify zero old envelopes before deleting a previous key. Backups containing old ciphertext still require the corresponding protected key for their approved retention.

Never change only the version label or remove a previous key while ciphertext still references it.

## Provider Secret Rotation

- Create/activate the replacement in the provider console only with explicit owner authorization.
- Add the new secret in staging, verify exact callbacks/scopes and token exchange, then schedule production cutover.
- Update the cPanel environment, restart Passenger, and test new authorization plus existing-token refresh behavior where supported.
- Revoke the old secret after the overlap window and evidence pass. A client-secret rotation is distinct from revoking individual user grants.
- Keep Meta app secret rotation coordinated with `appsecret_proof` and signed callback validation. Keep Google clients separated among sign-in, YouTube, and GA4.

## Deletion Runbooks

- Provider disconnect: attempt provider revocation first where supported, then purge local credentials, scopes, transactions, selected resource/data source, jobs, observations/content, and request metadata. A provider revocation failure must not prevent local deletion.
- Meta signed deauthorization/data deletion: validate HMAC/freshness/replay boundary, purge the matched authorization and snapshots, and return only the opaque confirmation/status contract.
- Account/workspace request: require the existing authenticated confirmation and authority workflow. Operators must verify identity/ownership, process all provider credentials/data, reports/grants/artifacts, memberships/sessions, and approved backups, then record completion without retaining deleted Authorized Data.
- Reports: user delete immediately invalidates grants and removes the active file; cron expires remaining artifacts after seven days.

Legal identity checks, deletion response targets, backup erasure timing, and completion notices remain owner inputs in `docs/compliance-blockers.md`.

## Passenger, Cloudflare, And ModSecurity

- Set the cPanel application root to the release directory and startup file to `server/index.js`; serve the compiled app through Express. Keep secrets in cPanel environment settings or a mode-`0600` untracked file outside public roots.
- Use one exact `TRUST_PROXY` hop for Passenger alone and normally two for Cloudflare plus Passenger. Confirm the real chain; never use `true`, arbitrary networks, or client-controlled forwarded headers.
- Cloudflare should pass HTTPS scheme/host consistently. Keep OAuth callbacks unmodified, uncached, and character-for-character identical to provider settings. Bypass caching for `/api/*`, `/oauth/*`, `/health/*`, and authenticated HTML.
- Start with normal ModSecurity rules. If a reproducible false positive blocks an exact OAuth callback or JSON endpoint, capture rule ID, route, sanitized request shape, and timestamp; create a narrow exception for that rule and exact route only. Never disable ModSecurity globally or exclude all `/api` traffic.
- Permit report downloads as `application/pdf` with `Content-Disposition: attachment` only on `/api/report-downloads/<opaque-token>`. Do not map the artifact directory into the web server.

The detailed staging command sequence remains in `docs/cpanel-staging-runbook.md`.
