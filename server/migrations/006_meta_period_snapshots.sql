ALTER TABLE meta_account_insight_snapshots
  DROP CONSTRAINT meta_account_insights_kind_check,
  DROP INDEX meta_account_insights_source_run_kind_date_unique,
  ADD COLUMN range_days SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER report_date,
  ADD COLUMN range_start_date DATE NULL AFTER range_days,
  ADD COLUMN range_end_date DATE NULL AFTER range_start_date,
  ADD CONSTRAINT meta_account_insights_kind_check
    CHECK (snapshot_kind IN ('profile', 'daily', 'period')),
  ADD CONSTRAINT meta_account_insights_range_check CHECK (
    (
      snapshot_kind = 'period'
      AND range_days IN (7, 30, 90)
      AND range_start_date IS NOT NULL
      AND range_end_date IS NOT NULL
      AND range_start_date <= range_end_date
    )
    OR
    (
      snapshot_kind IN ('profile', 'daily')
      AND range_days = 0
      AND range_start_date IS NULL
      AND range_end_date IS NULL
    )
  ),
  ADD CONSTRAINT meta_account_insights_source_run_kind_date_range_unique
    UNIQUE (data_source_id, sync_run_id, snapshot_kind, report_date, range_days),
  ADD INDEX meta_account_insights_source_range_idx
    (data_source_id, snapshot_kind, range_days, range_end_date);
