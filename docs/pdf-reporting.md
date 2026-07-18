# Protected PDF Reporting

Date: 2026-07-18

The Reports area is a production-oriented, read-only export path for stored Social Insights Studio observations. It is implemented locally; no deployment, provider-console change, live account access, or review submission was performed.

## User Contract

Workspace owners, admins, and analysts can select one or more explicit connected resources, choose 7/30/90 days or a bounded custom range, enable previous-period comparison, set a report timezone, enter a title/subtitle, preview the outline, queue generation, monitor status, download a completed artifact, and delete it. Viewers fail closed with `permission_denied`.

Statuses exposed to the product are `queued`, `running`, `completed`, `failed`, and `expired`. A report is never rendered during the HTTP request.

## Data And Execution Flow

1. The API validates workspace membership, Analyst-or-higher RBAC, title/subtitle, IANA timezone, date range, section allowlist, resource count, UUIDs, and exact workspace/provider ownership.
2. The API loads only stored dashboard observations. It freezes the selected resources, normalized metrics, trends, top content or website paths, explicit missing states, data-through dates, methodology, and versioned provider metric definitions in `report_runs.configuration_snapshot` and `metric_definitions_snapshot`.
3. A client request ID becomes a workspace/user-bound SHA-256 idempotency key. A replay returns the existing run.
4. The API commits a queued run and returns `202`; it creates no file.
5. `node server/worker.js reports-due --time-budget-seconds 240` leases due runs in MariaDB. The worker uses the frozen snapshot and a pure-Node PDFKit renderer. It makes no provider or remote network request.
6. The worker writes a mode-`0600` temporary artifact under a mode-`0700` server-generated run directory, checks page and byte limits, computes SHA-256, atomically publishes the final file, and records artifact metadata in the same completion transaction.
7. An authorized download request creates a random, hashed, short-lived one-time grant. Consumption rechecks the user, active workspace role, grant state, artifact state, DB size, and private file before marking the grant consumed.
8. Artifact cleanup changes active artifacts and completed runs to `expired`, removes grants, and deletes files after seven days. Report deletion immediately invalidates grants, stops queued/running work, soft-deletes the definition, and removes the artifact.

## Storage And Security Boundary

- Production must explicitly set `FEATURE_PDF_REPORTS=true` and an absolute `REPORT_ARTIFACT_ROOT` outside `public_html`, every application public root, the deployed source tree, filesystem roots, and the shared temporary directory. Startup/preflight rejects an unsafe enabled configuration.
- Storage keys contain only server-generated workspace/run UUIDs and the literal `report.pdf`. Absolute paths, empty segments, dot segments, backslashes, NUL bytes, and traversal are rejected.
- Download filenames are ASCII slugs and never participate in path resolution.
- The renderer contains text and vector graphics only. It loads no remote image, URL, HTML, script, embedded attachment, or user-selected local file.
- Visible text strips control characters and raw `http`, `https`, `ftp`, and `file` URLs. Unsupported glyphs degrade to a printable placeholder rather than a broken PDF glyph.
- Unavailable values render as `Unavailable`, `Not available`, or a provider reason. They are never silently converted to zero.
- Provider metrics remain side by side. Website views, social-video views, followers, subscribers, and provider-specific engagement are not summed into a universal total.

## Enforced Bounds

| Boundary | Default | Hard configuration ceiling |
| --- | ---: | ---: |
| Resources | 20 | 20 |
| Date range | 366 days | 366 days |
| Frozen snapshot | 2 MiB | 4 MiB |
| Content rows per resource | 30 | 50 |
| PDF pages | 80 | 100 |
| Artifact size | 20 MiB | 25 MiB |
| Worker lease | 300 seconds | 900 seconds |
| Attempts | 3 | 5 |
| One-time grant | 120 seconds | 300 seconds |
| Worker invocation | 240 seconds recommended | 900 seconds |
| Artifact retention | 7 days | Fixed at 7 days |

The renderer also receives the invocation deadline and stops between bounded layout operations when the budget has elapsed. Input record counts and snapshot size prevent unbounded in-memory rendering.

## PDF Structure

- Branded cover with title, subtitle, reporting range, timezone, resource count, and generation date.
- Executive summary with source health and explicit attention notes.
- Cross-platform side-by-side summary only when more than one provider is selected.
- One provider/resource section per explicit connection with resource identity, date/data-through boundary, provider-defined KPI cards, independent trend, top content or website paths, and missing-data states.
- Methodology, metric definitions, immutable snapshot boundary, retention, availability, and renderer security notes.
- A4 pages, repeated table headers, bounded row pagination, and page-number footers.

## API Surface

| Method and route | Purpose |
| --- | --- |
| `GET /api/reports/configuration` | Non-sensitive enabled/readiness/limit summary for an authenticated user. |
| `POST /api/workspaces/:workspaceId/reports/preview` | Validates and returns an outline; no artifact. |
| `POST /api/workspaces/:workspaceId/reports` | Freezes stored data and queues an idempotent run. |
| `GET /api/workspaces/:workspaceId/reports` | Lists authorized workspace report statuses. |
| `GET /api/workspaces/:workspaceId/reports/:reportRunId` | Returns one authorized status. |
| `POST /api/workspaces/:workspaceId/reports/:reportRunId/download-grants` | Creates a short-lived one-time grant for a completed active artifact. |
| `GET /api/report-downloads/:token` | Consumes a user-bound one-time grant and serves the checked private file. |
| `DELETE /api/workspaces/:workspaceId/reports/:reportRunId` | Invalidates work/grants and removes the artifact. |

All mutating workspace routes require the existing session and CSRF controls. The artifact route requires the existing session and a matching one-time token.

## Cron And Cleanup

Run both bounded workers. Do not combine them into a long resident process:

```sh
cd /home/CPANEL_USER/apps/social-insights/current
node server/worker.js sync-due --time-budget-seconds 240
node server/worker.js reports-due --time-budget-seconds 240
```

Recommended starting cadence is every five minutes for each command with overlap prevented by MariaDB leases. `reports-due` performs expiry/grant cleanup before claiming report work. `/health/ready` exposes only `ready`, `overdue`, `disabled`, or `configuration_required` queue/configuration states; it exposes no job IDs or paths.

## Deterministic Samples And QA

Generate the ignored local artifacts:

```sh
npm run reports:samples
```

Current sample paths:

- `output/pdf/all-platform-report.pdf`
- `output/pdf/tiktok-no-content-report.pdf`
- `output/pdf/missing-metric-report.pdf`
- `output/pdf/long-title-report.pdf`
- `output/pdf/long-content-report.pdf`
- `output/pdf/tiktok-report.pdf`
- `output/pdf/youtube-report.pdf`
- `output/pdf/facebook-pages-report.pdf`
- `output/pdf/instagram-report.pdf`
- `output/pdf/google-analytics-4-report.pdf`

Independent QA uses pypdf plus pdfplumber and does not trust the renderer's own page counter:

```sh
python3 server/scripts/verify-report-samples.py output/pdf
```

The verifier checks two independent page counts, A4 dimensions, nonblank pages, character bounds, final footers, file/page limits, annotations, embedded files, open actions, raw `null`/`undefined`, uncontrolled URLs, all-provider coverage, no-content and missing-metric states, and repeated headers in the long table fixture. Poppler rendering to `tmp/pdfs/` is still required for visual release QA of every page.

The 2026-07-18 local pass extracted and inspected every page in all ten final samples. It found no blank pages, out-of-page text, annotations, embedded files, raw URLs, raw null/undefined values, or page/file threshold failures. This is deterministic local evidence, not a guarantee about a future deployment environment.
