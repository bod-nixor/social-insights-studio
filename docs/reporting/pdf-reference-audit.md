# PDF Reference Audit

Date: 2026-07-17

Reference file: `/home/shahzain/Downloads/Website_Performance_Analytics (1).pdf`

Rendered pages: 5

## Useful Hierarchy To Preserve

- Workspace or resource name at the top of each section.
- Provider title next to the selected date range.
- Profile/channel/property performance KPI band.
- Main trend chart below the KPI band.
- Content performance table or website breakdown tables.
- Provider-specific metrics on provider-specific pages.

## Issues Not To Copy

- Raw `null` values in Facebook tables.
- Large missing-data widgets and broken placeholder boxes.
- TikTok and YouTube pages with copied website labels and missing data sources.
- GA4 labels that confuse users, sessions, views, active users, and subscribers.
- Dense dark/red styling that reduces print legibility.
- Truncated URLs and cramped table text.
- Blank or visually broken provider sections.

## Reporting Requirements For Implementation

- Generate PDFs asynchronously from stored snapshots, never from a long public request.
- Store artifacts outside the public web root and authorize every download.
- Include data-through timestamps, report ID, metric definitions, and unavailable-state notes.
- Use provider-specific metric labels and formulas.
- Render zero only when the provider explicitly returns zero.
- Render `Not available`, `Not granted`, `Delayed`, `Thresholded`, or `No data in range` for non-zero missing states.
- Validate rendered pages as PNGs before release to catch clipping, overlap, black boxes, blank pages, and unreadable text.
