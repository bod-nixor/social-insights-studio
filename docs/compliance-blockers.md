# Compliance And Submission Blockers

Date: 2026-07-18

These facts must be supplied or verified before provider review submissions or public claims that depend on them.

## Product Owner Inputs

- Legal operating entity name.
- Postal address or legally required contact address.
- Data protection or privacy contact, if different from support.
- Production support SLA or support response expectation.
- Launch countries or jurisdictions, if constrained.
- Infrastructure subprocessors and hosting facts.
- Backup retention and analytics retention policy. Generated PDF artifacts already have a fixed seven-day application expiry and user-triggered early deletion; the owner must approve whether backups exclude them and disclose any backup-erasure timing.
- Account deletion approval workflow and identity verification requirements.
- Demo/reviewer accounts and eligible test resources for every provider.

## External Actions Not Performed In This Repository

- Provider-console setting changes.
- Domain ownership verification.
- OAuth consent-screen submission.
- TikTok, Meta, or Google app review submission.
- Production DNS, deployment, secret rotation, or database migration.
- Live provider smoke tests for new providers.

## Release Recommendation

- TikTok connector and dashboard: source-ready after repository validation passes; live status still depends on production environment verification.
- Instagram: source-ready behind an independent disabled-by-default gate; release remains blocked until the Meta dashboard exposes the exact `instagram_basic`, `instagram_manage_insights`, `pages_show_list`, and `pages_read_engagement` Facebook Login set, and until a linked professional test account, review evidence, legal approval, and live smoke test exist. Meta documents that some Business Manager Page-role paths additionally require ads permissions; any such resource is ineligible and must remain stopped.
- Facebook Pages: source-ready behind a disabled-by-default gate; release remains blocked on exact Facebook Login for Business configuration, permission/access-level confirmation, an eligible Page test resource, review evidence, legal approval, and a live smoke test.
- YouTube: source-ready behind a disabled-by-default gate; release remains blocked on Google OAuth configuration, verified-domain consent-screen setup, an eligible channel test resource, legal approval, and verification evidence.
- Website Analytics: source-ready behind a disabled-by-default exact-scope gate; release remains blocked on a dedicated Google OAuth client, enabled Analytics Admin/Data APIs, verified-domain consent configuration, eligible GA4 test property, legal review, reviewer evidence, and live smoke testing.
- PDF reports: implemented locally behind a production-disabled gate with private storage, one-time downloads, early deletion, and seven-day expiry. Production release remains blocked on an approved private storage path, cron activation, capacity monitoring, backup exclusion/retention approval, and a staged generate/download/delete/expiry smoke test.
