#!/usr/bin/env node
const mariadb = require('mariadb');
const { assertLocalDatabaseUrl, assertNotProductionCommand, getDatabaseUrl } = require('./database-env');

async function main() {
  assertNotProductionCommand('db:seed');
  const databaseUrl = getDatabaseUrl('dev');
  assertLocalDatabaseUrl(databaseUrl);
  const connection = await mariadb.createConnection(databaseUrl);
  try {
    const userId = '00000000-0000-4000-8000-000000000101';
    const workspaceId = '10000000-0000-4000-8000-000000000101';
    const sourceId = '20000000-0000-4000-8000-000000000101';
    const runId = '30000000-0000-4000-8000-000000000101';
    const profileSnapshotId = '40000000-0000-4000-8000-000000000101';
    const contentItemId = '50000000-0000-4000-8000-000000000101';
    const contentSnapshotId = '60000000-0000-4000-8000-000000000101';
    await connection.beginTransaction();
    await connection.query(
      `INSERT INTO users (id, email, display_name)
       VALUES (?, 'demo+local@social-insights.test', 'Local Demo User')
       ON DUPLICATE KEY UPDATE display_name = VALUES(display_name)`,
      [userId]
    );
    await connection.query(
      `INSERT INTO workspaces (id, name, slug, created_by)
       VALUES (?, 'Local Demo Workspace', 'local-demo-workspace', ?)
       ON DUPLICATE KEY UPDATE name = VALUES(name)`,
      [workspaceId, userId]
    );
    await connection.query(
      `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
       VALUES (?, ?, 'owner', 'active')
       ON DUPLICATE KEY UPDATE role = VALUES(role), status = VALUES(status)`,
      [workspaceId, userId]
    );
    await connection.query('DELETE FROM sync_jobs WHERE data_source_id = ?', [sourceId]);
    await connection.query('DELETE FROM sync_runs WHERE data_source_id = ?', [sourceId]);
    await connection.query('DELETE FROM content_items WHERE id = ? AND workspace_id = ?', [contentItemId, workspaceId]);
    await connection.query('DELETE FROM provider_scopes WHERE data_source_id = ?', [sourceId]);
    await connection.query('DELETE FROM oauth_credentials WHERE data_source_id = ?', [sourceId]);
    await connection.query('DELETE FROM provider_accounts WHERE data_source_id = ?', [sourceId]);
    await connection.query(
      `INSERT INTO data_sources (id, workspace_id, provider, status, last_sync_at, last_successful_sync_at, next_sync_at)
       VALUES (?, ?, 'tiktok', 'disconnected', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 6 HOUR))
       ON DUPLICATE KEY UPDATE
         status = VALUES(status),
         last_sync_at = VALUES(last_sync_at),
         last_successful_sync_at = VALUES(last_successful_sync_at),
         next_sync_at = VALUES(next_sync_at),
         updated_at = UTC_TIMESTAMP(3)`,
      [sourceId, workspaceId]
    );
    await connection.query(
      `INSERT INTO sync_runs (id, workspace_id, data_source_id, trigger_type, status, finished_at, profile_count)
       VALUES (?, ?, ?, 'manual', 'success', UTC_TIMESTAMP(3), 1)`,
      [runId, workspaceId, sourceId]
    );
    await connection.query(
      `INSERT INTO profile_snapshots
        (id, workspace_id, data_source_id, sync_run_id, observed_at, follower_count, following_count, likes_count, video_count, provider_metrics)
       VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), 1200, 340, 9800, 42, JSON_OBJECT('fixture', TRUE))`,
      [profileSnapshotId, workspaceId, sourceId, runId]
    );
    await connection.query(
      `INSERT INTO content_items
        (id, workspace_id, data_source_id, provider_content_id, published_at, title, description, share_url, duration_seconds, provider_metadata)
       VALUES
        (?, ?, ?, 'local-demo-video-1', DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 2 DAY),
         'Local demo content: campaign recap with a long provider title for layout testing',
         'Fixture content generated only for local Social Insights Studio QA.',
         'https://www.tiktok.com/@local-demo/video/1', 38, JSON_OBJECT('fixture', TRUE))
       ON DUPLICATE KEY UPDATE
         title = VALUES(title),
         description = VALUES(description),
         share_url = VALUES(share_url),
         provider_metadata = VALUES(provider_metadata),
         last_seen_at = UTC_TIMESTAMP(3)`,
      [contentItemId, workspaceId, sourceId]
    );
    await connection.query(
      `INSERT INTO content_metric_snapshots
        (id, workspace_id, content_item_id, sync_run_id, observed_at, view_count, like_count, comment_count, share_count, provider_metrics)
       VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), 2300, 320, 41, 22, JSON_OBJECT('fixture', TRUE))`,
      [contentSnapshotId, workspaceId, contentItemId, runId]
    );
    await connection.commit();
    console.log('Seeded local demo workspace and labeled fixture snapshot.');
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
