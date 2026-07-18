# Social Insights Studio Implementation Status Matrix

Assessment date: 2026-07-18

Baseline commit: `281d1c17913db2b9a5a54da58e392f5666efef8c`

Assessment HEAD at start: `281d1c17913db2b9a5a54da58e392f5666efef8c`

Branch at start: `production-multiplatform`

This matrix is the engineering baseline for the complete multi-platform continuation. It distinguishes source implementation from external provider approval or live operation. A provider is not described as live merely because deterministic tests pass.

## Status Definitions

- **Complete**: implemented and covered by current local validation for its stated repository scope.
- **Partially complete**: useful implementation exists, but one or more required workflows or hardening items remain.
- **Implemented but disabled**: the complete local slice exists behind a fail-closed feature gate.
- **Not implemented**: only catalog, schema-design, or documentation placeholders exist, or no implementation exists.
- **Externally blocked**: repository work is complete enough for the stated level, but credentials, provider-console state, legal facts, provider approval, or live infrastructure are required.
- **No longer applicable**: superseded by the continuation or a later accepted architecture decision.

## Baseline Evidence

| Check | Result |
| --- | --- |
| Required Meta commit in `HEAD` history | Complete - `git merge-base --is-ancestor` returned success. |
| Worktree at start | Complete - clean. |
| Tracked-artifact hygiene | Complete - no real environment file, `node_modules`, build output, coverage output, generated PDF/report, file-store data, or database volume is tracked. Only sanitized environment examples and intentional migration/design SQL are tracked. |
| Format, lint, type check, server syntax | Complete - all passed. |
| Web production build | Complete - final Vite build passed; the 717.63 kB main-chunk warning remains a non-blocking optimization item. |
| Backend tests | Complete - 127/127 passed in the final sequential run. |
| Real-MariaDB tests | Complete - 39/39 passed in the focused final run. |
| Backend coverage | Complete for current gate - 87.91% lines, 68.41% branches, and 92.34% functions. |
| Migrations | Complete for 001-010 on development and test databases; clean migration, TikTok compatibility, provider/report tenant-integrity constraints, and repeated no-op application are covered. |
| Worker smoke | Complete - separate bounded sync and report commands both exited successfully with zero due work and no provider calls. |
| Dependency audits | Complete - root, server, and web production audits reported zero known vulnerabilities. |
| Production preflight | Complete - synthetic production-safe configuration passed with reporting disabled and again with reporting enabled at a private absolute artifact root. |
| Reference PDF audit | Complete - all five pages rendered and inspected; the existing PDF audit correctly identifies raw nulls, incorrect labels, broken provider pages, cramped tables, and over-dark styling. |
| Responsive browser baseline | Complete - 26 public/application route audits covered 360, 768, 1366, and 1920 pixels, all navigation areas, provider/dashboard/connection states, direct refresh, report generation/download, onboarding/session restoration, visible focus, and reduced motion. No horizontal overflow, unnamed control, unexpected console error, or unexpected failed request was observed. Local Chrome/CDP was used because the in-app browser backend rejected localhost navigation. |
| Local database tooling | Complete - destructive local reset/seed checks remain restricted to loopback and now require the exact configured `MARIADB_PORT`; the configured 3317 instance is accepted without weakening the guard. |

## Product And UX Requirements

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| React application canonical at `/` | Complete | Express serves the Vite shell at root. |
| Legacy `/app` redirects with query preservation | Complete | Permanent same-origin redirects are implemented and tested. |
| Public product homepage | Complete for the current product shell | The logged-out page describes the five-source product and data use without release-gate or implementation language. Product screenshots can be added later but are not required for truthful operation. |
| Authenticated app replaces public marketing | Complete | Restored sessions render the application shell. |
| Public privacy, terms, support, deletion, and status routes plus `.html` compatibility | Complete | Routes and aliases are tested. Final legal facts remain externally blocked. |
| Unknown API routes return JSON and SPA exclusions are safe | Complete | API/OAuth/health/legal/artifact boundaries and traversal cases are tested. |
| Production connection cards and state-specific actions | Complete for all five current providers | Cards expose friendly access summaries, explicit selected resources, real availability/reconnect/sync/disconnect actions, and no raw setup warnings or scope identifiers. |
| Account profile and authentication methods | Complete | Users can edit their display name and see email-code or Google sign-in identities without exposing identity subjects. |
| Active-session list, current/all-session sign-out, session revocation | Complete | Active sessions are user-bound, labelled with a coarse device/browser description, and support one, other, or all-session revocation. Raw user agents and locations are not stored. |
| Account privacy controls and account deletion request | Complete for request intake | Authenticated, CSRF-protected, email-confirmed requests are idempotent and visible with status. Permanent processing remains governed by the owner-supplied deletion policy. |
| Invitation send/list/accept | Complete | Email-bound, token-hashed invitations are explicit, replay-safe, audited, and restore removed memberships only after acceptance. |
| Invitation resend/revoke with rate limits | Complete | Resends rotate the secret, extend expiry, enforce a 60-second cooldown and five-send cap, and revocation is owner/admin controlled and audited. |
| Member role changes/removal/last-owner protection | Complete | Server-enforced RBAC, audit logs, production wording, and last-owner coverage exist. |
| Accessible temporary notifications | Complete for current actions | Success notifications auto-dismiss, errors use polite/alert live regions with user-facing text, and durable provider state remains in the relevant view. |
| Provider empty/stale/partial/error states | Complete for current dashboard scope | The cross-platform endpoint and UI normalize stored-only sample, ready, stale, delayed, thresholded, partial, empty, pending, failed, reconnect, configuration, and disconnected states without hiding provider availability. |
| Mobile navigation and no horizontal overflow | Complete for current views | Overview, Content, Reports, public surfaces, and the scrollable bottom navigation were verified at 360 pixels; all principal views were audited at larger required widths. |
| Keyboard access, visible focus, reduced motion | Complete for current repository gate | Browser focus traversal produced a visible solid focus indicator, controls passed accessible-name checks, and reduced-motion emulation reduced animation to one 0.01 ms iteration. Full assistive-technology user testing remains a release practice, not an unimplemented control. |
| Clean URL state | Complete | Routes remain refreshable while each view writes only its own relevant filters; invitation secrets are removed after authenticated accept/dismiss. |
| Internal implementation terminology removed | Complete for current primary screens | Cookie/CSRF, deployment, feature-gate, raw scope/configuration, worker, and provider-error codes are no longer rendered in the account, member, public, connection, or sync-history UX. |

## Architecture And Data Model

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| MariaDB modular-monolith and bounded worker | Complete | Current cPanel-compatible architecture is accepted and tested. |
| One authorization backing multiple resources | Complete for YouTube, Meta, and GA4 | Shared authorization/resource/connection rows and explicit selection are implemented. |
| Independent workspace-owned data sources and tenant isolation | Complete for current providers | Cross-workspace tests fail closed. |
| Exact granted/missing/denied/revoked scopes | Complete for current provider foundation | Authorization scope rows preserve exact state; product copy summarizes partial consent without exposing raw implementation identifiers. |
| Encrypted, versioned credentials | Complete | AES-256-GCM current/previous-key support covers authorization and resource credentials. |
| Incremental provider authorization | Complete for YouTube; provider-specific for Meta/TikTok | Google product auth is separate from OIDC. Meta uses dedicated Login for Business configurations. |
| Provider API version and capability metadata | Complete for current provider slices | Catalog, authorization, connection capability, and sync-state fields exist. |
| Sync cursor/checkpoint/quota/request metadata | Complete and exercised by GA4 | Connection sync state, per-run provider API version, cursors, data-through timestamps, retry state, bounded quota summaries, and request telemetry are populated without raw payloads. |
| Immutable account/profile and content observations | Complete for TikTok, YouTube, and Meta | Provider-specific snapshot tables preserve null and date semantics. |
| Dimension/breakdown observations | Complete for GA4 | Six aggregate GA4 breakdown families use canonical dimension hashes, explicit per-metric availability, threshold flags, period semantics, and tenant constraints. |
| Report definitions, runs, jobs, and protected artifacts | Complete | Workspace definitions, relational resources, idempotent leased runs, immutable data/metric-definition snapshots, private artifact metadata, renderer jobs, expiry, deletion, and hashed one-time grants are implemented and tested. |
| Live-compatible TikTok migration | Complete | Provider-foundation upgrade preserves ciphertext without token rewrite. |
| Universal metrics avoided | Complete in current registry and overview | Every advertised metric has a versioned provider-specific definition, unit, aggregation, date semantics, and unavailable rule. The cross-platform contract separates every resource and provider, has no analytics total, and uses independent trend scales. GA4 active users are explicitly not summed across daily rows. |
| Executable provider adapter contract | Complete for new integrations | A validated versioned boundary covers authorization, refresh/revocation, scope inspection, discovery/selection, synchronization, and deletion. Existing providers remain tested compatibility adapters until refactoring can occur without behavior change. |
| Observation tenant integrity | Complete | Composite workspace/provider foreign keys reject cross-workspace authorization, resource, observation, definition, and run references in real MariaDB tests. |

## Provider Status

| Provider | Engineering status | Exact authorization | Remaining gate |
| --- | --- | --- | --- |
| TikTok | Complete for current dashboard/CSV/legacy behavior | `user.info.basic`, `user.info.profile`, `user.info.stats`, `video.list` | Production retention/legal confirmation and live deployment verification are external. No-post review fixture remains external. |
| Facebook Pages | Implemented but disabled | `pages_show_list`, `pages_read_engagement`, `read_insights` through Facebook Login for Business | Exact production config, access level, reviewer Page, legal review, provider review, and live smoke are external. |
| Instagram professional accounts | Implemented but disabled and externally blocked | `instagram_basic`, `instagram_manage_insights`, `pages_show_list`, `pages_read_engagement` through Facebook Login | First-party documentation re-verified 2026-07-18. Supplied dashboard evidence proves the Page permission inventory but not both Instagram permissions in the dedicated Login for Business configuration; an eligible linked account that needs no ads permissions is also required. |
| YouTube channels | Implemented but disabled | `https://www.googleapis.com/auth/youtube.readonly`, `https://www.googleapis.com/auth/yt-analytics.readonly` | First-party documentation re-verified 2026-07-18, including the current `reports.query` requirement for both scopes. Google project/consent configuration, verified domain, test channel, legal review, verification, and live smoke are external. |
| Google Analytics 4 | Implemented but disabled | Exact scope: `https://www.googleapis.com/auth/analytics.readonly` | Local OAuth/discovery, explicit selection, Admin/Data adapters, metadata/compatibility checks, worker observations, stored dashboard, revocation/deletion, UI, tests, and review notes are complete. Google client/API/consent configuration, test property, legal review, verification evidence, and live smoke are external. |

## Analytics, Navigation, And Export Requirements

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Cross-platform Overview | Complete for current providers | A stored-only API and responsive UI render every selected resource separately with health/freshness, provider-defined metrics, independent-scale trends, top content or landing pages, alerts, comparisons, and methodology. Unlike metrics are never summed. |
| Platforms/Sources navigation | Complete for current providers | Overview is the comparison surface; Sources preserves all five provider dashboards and carries explicit provider/resource drill-down state. |
| Provider dashboards | Complete for all five current providers | Stored-snapshot APIs and visible provider-specific views exist, including GA4 exact-range metrics, daily traffic, property timezone/currency, compatible breakdowns, data-through state, and threshold warnings. |
| Resource filter | Complete for multi-resource providers | Sources exposes the selected connection for YouTube, Facebook Pages, Instagram, and GA4. Cross-platform cards load every connection by ID. TikTok retains its existing single-account contract. |
| Date and comparison filters | Complete for current provider dashboards | 7/30/90/custom and comparison semantics exist, with provider-specific limitations. |
| Timezone semantics | Complete for GA4 property reports | GA4 ranges use the selected property's timezone and the UI displays it. A user-selectable timezone override is deliberately rejected so provider date semantics are not relabeled. |
| Cross-provider content and capability-aware columns | Complete for social content | The global Content API/UI/CSV support explicit TikTok, YouTube, Facebook Pages, Instagram, and selected-resource filters; rows/details identify their provider/resource, preserve unavailable values, and retain provider semantics. GA4 paths and aggregate breakdowns remain correctly in the Website Analytics Source view. |
| CSV export | Complete for current filtered content | Workspace-scoped, analyst-or-higher, bounded, audited, and spreadsheet-injection safe. |
| Reports navigation and builder | Complete | Analyst-or-higher users can select explicit resources, ranges/comparison/timezone/title/sections, preview, queue, monitor, download once, and delete; Viewers fail closed. |

## PDF Reporting

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Async DB-backed report pipeline | Complete | HTTP requests freeze/idempotently queue work; the bounded leased report command renders, retries, completes transactionally, and performs expiry cleanup outside Passenger requests. |
| Stored-only report data selection | Complete | Every run freezes selected stored dashboard observations, provider definitions, resource identities, missing states, and data-through dates; rendering makes no provider or remote request. |
| Protected artifact storage/download/delete | Complete | Generated-only keys resolve below a private root, files use restrictive modes, downloads require user-bound one-time grants, deletion invalidates grants/removes files, and artifacts expire after seven days. |
| cPanel-compatible renderer | Complete | Pure-Node PDFKit is pinned in the server lockfile and requires no system browser or remote asset. |
| Branded cover, cross-platform summary, provider sections, methodology | Complete | The renderer emits the required hierarchy with explicit provider/resource semantics and unavailable-state notes. |
| Deterministic five-provider fixtures and generated samples | Complete | Ten ignored samples cover every provider, all-platform, no-content, missing-metric, long-title, and long-table cases under `output/pdf/`. |
| Text, page, size, injection, traversal, expiry, render-to-PNG, and visual QA | Complete | Independent pypdf/pdfplumber checks passed all ten files and all 48 final Poppler-rendered pages were visually inspected without clipping, overlap, blank provider pages, broken glyphs, or unsafe links/files. |

## Review, Legal, And Operations

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Per-permission provider approval matrix | Complete for preparation | All 14 exact requested permissions have a consolidated row with classification, justification, action/feature/API/data/retention/revoke/delete/test/recording/docs/implementation/console fields. External console confirmation remains explicit. |
| Reviewer scripts and screencast checklists | Complete for preparation | Consolidated TikTok, Facebook, Instagram, YouTube, and GA4 scripts plus account/resource, screenshot, screencast, configuration, and stop-condition checklists are present. No submission was performed. |
| Public legal/support/deletion surfaces | Partially complete | Accurate implementation boundaries are present. Legal entity, address/contact, jurisdictions, subprocessors, SLA, and approved retention remain external blockers. |
| Startup validation and feature flags | Complete | TikTok, YouTube, Meta, GA4, and PDF reporting fail closed; exact callbacks/scopes/clients and private report storage are validated when enabled. |
| Provider cadence/quota/retry documentation | Complete for current providers | The operations runbook records bounded cadence, lookbacks/pages/items/quota behavior, retries, leases, and stop rules for all five providers and reports. |
| Structured logs, correlation IDs, and secret redaction | Complete for current repository scope | Core request/provider/job flows use sanitized IDs/categories, tests reject secret leakage, and the operations contract defines allowed fields, forbidden values, rotation, and alert handling. External log shipping is an operator choice. |
| Stale-worker/overdue-source checks | Complete | Readiness exposes only categorical sync/report queue states plus sanitized overdue warnings, with configurable bounded thresholds and a regression test. |
| Backup/restore, migration, rollback | Complete for engineering documentation | Safe activation, isolated restore drill, artifact-backup exclusion, schema-compatible rollback/forward-fix, and migration sequence are documented. Owner-approved RPO/RTO/retention remain external policy inputs. |
| Incident response and secret/key rotation runbooks | Complete | Incident containment/recovery, encryption current/previous-key rotation, provider-secret rotation, and provider/account/workspace/report deletion runbooks are documented. |
| cPanel/Passenger/Cloudflare/ModSecurity guidance | Complete | Private report storage/cron, Passenger ownership, trust-proxy/Cloudflare cache boundaries, and narrow route/rule-specific ModSecurity exceptions are documented. |

## Superseded Requirements

| Earlier requirement | Status | Reason |
| --- | --- | --- |
| TikTok-only implementation/non-goal for later providers | No longer applicable | The multi-platform continuation explicitly supersedes the original TikTok-only exclusion. |
| Managed PostgreSQL as the immediate target | No longer applicable | The accepted implementation uses cPanel/Passenger plus MariaDB as the sole current production database path. |
| Looker Studio as the primary product | No longer applicable | The standalone React application is canonical; the connector remains a compatibility integration. |
| Repairing the legacy report PDF as a data contract | No longer applicable | The attached PDF is only a hierarchy/visual reference and contains known metric and missing-data defects. |

## Safe External Activation Order

The independent repository phases above are complete. External activation must still follow this order:

1. Deploy/migrate with every new provider and PDF reporting disabled; verify public/auth/readiness surfaces and both idle bounded commands.
2. Configure, preflight, enable, smoke, revoke, and delete one provider at a time with its eligible reviewer resource.
3. Keep Instagram disabled unless the dedicated Facebook Login configuration exposes the exact four-scope set and a no-ads eligible professional account works.
4. Configure private report storage and monitoring, enable reports, then smoke generate/download/delete/expiry.
5. Obtain owner legal/retention/RPO/RTO inputs and provider approvals before making submission-ready or live claims.
