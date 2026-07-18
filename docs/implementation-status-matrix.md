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
| Web production build | Complete - passed after the account lifecycle slice; the 676.23 kB main-chunk warning remains an optimization item. |
| Backend tests | Complete for current scope - 94/94 passed. |
| Real-MariaDB tests | Complete for current scope - 35/35 passed. |
| Backend coverage | Complete for current gate - 87.44% lines, 68.36% branches, 92.92% functions. New security/report modules still require focused branch coverage. |
| Migrations | Complete for 001-010 on development and test databases; clean migration, TikTok compatibility, provider/report tenant-integrity constraints, and repeated no-op application are covered. |
| Worker smoke | Complete for current scope - bounded no-work run exited successfully without provider calls. |
| Dependency audits | Complete - root, server, and web production audits reported zero known vulnerabilities. |
| Production preflight | Complete for current enabled TikTok/YouTube/Meta contract with synthetic configuration; only missing deployment provenance was warned. |
| Reference PDF audit | Complete - all five pages rendered and inspected; the existing PDF audit correctly identifies raw nulls, incorrect labels, broken provider pages, cramped tables, and over-dark styling. |
| Responsive browser baseline | Complete for Phase 0/1 - public, member, invitation, and account/session screens were inspected at 360, 768, 1366, and 1920 pixels through local Chrome; no horizontal document overflow, console exception, or unexpected failed request was observed. Focus traversal and reduced-motion emulation were verified. The in-app browser backend rejected localhost navigation, so local Chrome/CDP was used. Full all-provider/report interaction coverage remains a final gate. |
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
| Production connection cards and state-specific actions | Complete for current providers | Cards expose friendly access summaries, explicit selected resources, real availability/reconnect actions, and no raw setup warnings or scope identifiers. Further consolidation will accompany GA4 and the cross-platform view. |
| Account profile and authentication methods | Complete | Users can edit their display name and see email-code or Google sign-in identities without exposing identity subjects. |
| Active-session list, current/all-session sign-out, session revocation | Complete | Active sessions are user-bound, labelled with a coarse device/browser description, and support one, other, or all-session revocation. Raw user agents and locations are not stored. |
| Account privacy controls and account deletion request | Complete for request intake | Authenticated, CSRF-protected, email-confirmed requests are idempotent and visible with status. Permanent processing remains governed by the owner-supplied deletion policy. |
| Invitation send/list/accept | Complete | Email-bound, token-hashed invitations are explicit, replay-safe, audited, and restore removed memberships only after acceptance. |
| Invitation resend/revoke with rate limits | Complete | Resends rotate the secret, extend expiry, enforce a 60-second cooldown and five-send cap, and revocation is owner/admin controlled and audited. |
| Member role changes/removal/last-owner protection | Complete | Server-enforced RBAC, audit logs, production wording, and last-owner coverage exist. |
| Accessible temporary notifications | Complete for current actions | Success notifications auto-dismiss, errors use polite/alert live regions with user-facing text, and durable provider state remains in the relevant view. |
| Provider empty/stale/partial/error states | Partially complete | TikTok, YouTube, and Meta state distinctions exist; cross-provider consistency and product wording remain incomplete. |
| Mobile navigation and no horizontal overflow | Complete for current views | Fixed mobile navigation is usable at 360 pixels; full keyboard and screen-reader QA remains. |
| Keyboard access, visible focus, reduced motion | Partially complete | Semantic controls and reduced-motion CSS exist in places, but complete route/control verification is outstanding. |
| Clean URL state | Complete | Routes remain refreshable while each view writes only its own relevant filters; invitation secrets are removed after authenticated accept/dismiss. |
| Internal implementation terminology removed | Complete for current primary screens | Cookie/CSRF, deployment, feature-gate, raw scope/configuration, worker, and provider-error codes are no longer rendered in the account, member, public, connection, or sync-history UX. |

## Architecture And Data Model

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| MariaDB modular-monolith and bounded worker | Complete | Current cPanel-compatible architecture is accepted and tested. |
| One authorization backing multiple resources | Complete for YouTube and Meta | Shared authorization/resource/connection rows and explicit selection are implemented. |
| Independent workspace-owned data sources and tenant isolation | Complete for current providers | Cross-workspace tests fail closed. |
| Exact granted/missing/denied/revoked scopes | Complete for current provider foundation | Authorization scope rows preserve exact state; product copy summarizes partial consent without exposing raw implementation identifiers. |
| Encrypted, versioned credentials | Complete | AES-256-GCM current/previous-key support covers authorization and resource credentials. |
| Incremental provider authorization | Complete for YouTube; provider-specific for Meta/TikTok | Google product auth is separate from OIDC. Meta uses dedicated Login for Business configurations. |
| Provider API version and capability metadata | Complete for current provider slices | Catalog, authorization, connection capability, and sync-state fields exist. |
| Sync cursor/checkpoint/quota/request metadata | Complete as a shared foundation | Connection sync state, per-run provider API version, cursors, data-through timestamps, retry state, and request telemetry are modeled. GA4 will populate them in its vertical slice. |
| Immutable account/profile and content observations | Complete for TikTok, YouTube, and Meta | Provider-specific snapshot tables preserve null and date semantics. |
| Dimension/breakdown observations | Complete as a storage/contract foundation | Namespaced aggregate rows have canonical dimension hashes, explicit per-metric availability, threshold flags, period semantics, and tenant constraints. GA4 will be the first writer. |
| Report definitions, runs, jobs, and protected artifacts | Complete as a schema foundation | Workspace definitions, relational resources, idempotent leased runs, resource/metric snapshots, private artifact metadata, expiry, and hashed one-time grants exist. Rendering/API/worker behavior remains the PDF phase. |
| Live-compatible TikTok migration | Complete | Provider-foundation upgrade preserves ciphertext without token rewrite. |
| Universal metrics avoided | Complete in current registry | Every advertised metric now has a versioned provider-specific definition, unit, aggregation, date semantics, and unavailable rule. GA4 active users are explicitly not summed across daily rows. |
| Executable provider adapter contract | Complete for new integrations | A validated versioned boundary covers authorization, refresh/revocation, scope inspection, discovery/selection, synchronization, and deletion. Existing providers remain tested compatibility adapters until refactoring can occur without behavior change. |
| Observation tenant integrity | Complete | Composite workspace/provider foreign keys reject cross-workspace authorization, resource, observation, definition, and run references in real MariaDB tests. |

## Provider Status

| Provider | Engineering status | Exact authorization | Remaining gate |
| --- | --- | --- | --- |
| TikTok | Complete for current dashboard/CSV/legacy behavior | `user.info.basic`, `user.info.profile`, `user.info.stats`, `video.list` | Production retention/legal confirmation and live deployment verification are external. No-post review fixture remains external. |
| Facebook Pages | Implemented but disabled | `pages_show_list`, `pages_read_engagement`, `read_insights` through Facebook Login for Business | Exact production config, access level, reviewer Page, legal review, provider review, and live smoke are external. |
| Instagram professional accounts | Implemented but disabled and externally blocked | `instagram_basic`, `instagram_manage_insights`, `pages_show_list`, `pages_read_engagement` through Facebook Login | First-party documentation re-verified 2026-07-18. Supplied dashboard evidence proves the Page permission inventory but not both Instagram permissions in the dedicated Login for Business configuration; an eligible linked account that needs no ads permissions is also required. |
| YouTube channels | Implemented but disabled | `https://www.googleapis.com/auth/youtube.readonly`, `https://www.googleapis.com/auth/yt-analytics.readonly` | First-party documentation re-verified 2026-07-18, including the current `reports.query` requirement for both scopes. Google project/consent configuration, verified domain, test channel, legal review, verification, and live smoke are external. |
| Google Analytics 4 | Not implemented | Candidate exact scope: `https://www.googleapis.com/auth/analytics.readonly` | Implement OAuth, discovery, Admin/Data adapters, metadata/compatibility checks, worker snapshots, dashboard, deletion, tests, and review evidence before external configuration. |

## Analytics, Navigation, And Export Requirements

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Cross-platform Overview | Not implemented | Current overview switches between provider pages; it does not render side-by-side health, trends, alerts, comparisons, and top content. |
| Platforms/Sources navigation | Not implemented | Connection management exists, but no dedicated source-detail navigation model. |
| Provider dashboards | Complete for TikTok/YouTube/Facebook/Instagram current capabilities | Stored-snapshot APIs and visible provider-specific views exist. GA4 is absent. |
| Resource filter | Partially complete | APIs support connection/data-source targeting in newer providers, but a consistent global resource filter is absent. |
| Date and comparison filters | Complete for current provider dashboards | 7/30/90/custom and comparison semantics exist, with provider-specific limitations. |
| Timezone filter | Not implemented | Storage is UTC and GA4 property timezone is not yet modeled in the UI/report filter. |
| Cross-provider content and capability-aware columns | Partially complete | Normalized content/filter/detail exists, but the UI remains TikTok-shaped and lacks the full dynamic provider/resource column model. |
| CSV export | Complete for current filtered content | Workspace-scoped, analyst-or-higher, bounded, audited, and spreadsheet-injection safe. |
| Reports navigation and builder | Not implemented | No Reports view or API exists. |

## PDF Reporting

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Async DB-backed report pipeline | Partially complete | Additive definitions and idempotent leased run/job tables exist; worker claims, rendering, retries, and retention execution remain. |
| Stored-only report data selection | Not implemented | Dashboard query foundations can be reused, but report snapshots/definitions are absent. |
| Protected artifact storage/download/delete | Partially complete | Private artifact metadata, expiry, hashes, size/page limits, and one-time grant records exist; filesystem validation and authorized APIs remain. |
| cPanel-compatible renderer | Not implemented | A pure renderer must be selected and documented; system Chromium cannot be assumed. |
| Branded cover, cross-platform summary, provider sections, methodology | Not implemented | Reference hierarchy is documented only. |
| Deterministic five-provider fixtures and generated samples | Not implemented | No local sample PDFs exist. |
| Text, page, size, injection, traversal, expiry, render-to-PNG, and visual QA | Not implemented | Required before the reporting phase can be complete. |

## Review, Legal, And Operations

| Requirement | Status | Evidence and remaining work |
| --- | --- | --- |
| Per-permission provider approval matrix | Partially complete | YouTube and Meta contain strong evidence; TikTok needs one row per scope and GA4 needs a complete package. |
| Reviewer scripts and screencast checklists | Partially complete | YouTube and Meta walkthroughs exist; TikTok and GA4 need equivalent per-scope assets and a consolidated screenshot checklist. |
| Public legal/support/deletion surfaces | Partially complete | Accurate implementation boundaries are present. Legal entity, address/contact, jurisdictions, subprocessors, SLA, and approved retention remain external blockers. |
| Startup validation and feature flags | Complete for current providers | TikTok/YouTube/Meta fail closed. GA4 and report settings remain to be added. |
| Provider cadence/quota/retry documentation | Partially complete | TikTok/YouTube/Meta are covered to differing depth; GA4 is absent. |
| Structured logs, correlation IDs, and secret redaction | Partially complete | Core flows are sanitized, but a consolidated production observability/alert integration and operational log contract are incomplete. |
| Stale-worker/overdue-source checks | Not implemented | Readiness does not yet expose a complete operational freshness summary. |
| Backup/restore, migration, rollback | Partially complete | Staging backup and rollback steps exist; a documented restore drill, RPO/RTO policy, report-artifact backup boundary, and all-provider release sequence are incomplete. |
| Incident response and secret/key rotation runbooks | Partially complete | Encryption previous-key behavior exists, but complete incident/provider-secret/deletion runbooks are absent. |
| cPanel/Passenger/Cloudflare/ModSecurity guidance | Partially complete | Strong TikTok staging guidance exists; complete all-provider/report activation and artifact storage guidance remains. |

## Superseded Requirements

| Earlier requirement | Status | Reason |
| --- | --- | --- |
| TikTok-only implementation/non-goal for later providers | No longer applicable | The multi-platform continuation explicitly supersedes the original TikTok-only exclusion. |
| Managed PostgreSQL as the immediate target | No longer applicable | The accepted implementation uses cPanel/Passenger plus MariaDB as the sole current production database path. |
| Looker Studio as the primary product | No longer applicable | The standalone React application is canonical; the connector remains a compatibility integration. |
| Repairing the legacy report PDF as a data contract | No longer applicable | The attached PDF is only a hierarchy/visual reference and contains known metric and missing-data defects. |

## First Safe Implementation Order

1. Implement GA4 behind a disabled-by-default exact-scope gate.
2. Build the cross-platform overview and global provider/resource/timezone filters.
3. Implement the protected asynchronous PDF report pipeline and deterministic render QA.
4. Complete per-scope review packages and full operations documentation.
5. Run the final clean-install/upgrade/idempotence, full browser/accessibility, report-render, worker, coverage, preflight, audit, and secret checks.
