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
- Backup retention, analytics retention, and report artifact retention policy.
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
- Instagram: blocked until implementation, Meta configuration, professional test account, and review evidence exist.
- Facebook Pages: blocked until implementation, Meta configuration, Page test resource, and review evidence exist.
- YouTube: source-ready behind a disabled-by-default gate; release remains blocked on Google OAuth configuration, verified-domain consent-screen setup, an eligible channel test resource, legal approval, and verification evidence.
- Website Analytics: blocked until implementation, Google OAuth configuration, GA4 test property, and verification evidence exist.
