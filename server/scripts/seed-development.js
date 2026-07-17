#!/usr/bin/env node
const mariadb = require('mariadb');
const { assertLocalDatabaseUrl, getDatabaseUrl } = require('./database-env');
const { createId } = require('../platform/security');

async function main() {
  if (process.env.NODE_ENV === 'production') {
    throw new Error('Refusing to seed development fixtures in production.');
  }
  const databaseUrl = getDatabaseUrl('dev');
  assertLocalDatabaseUrl(databaseUrl);
  const connection = await mariadb.createConnection(databaseUrl);
  try {
    const userId = '00000000-0000-4000-8000-000000000101';
    const workspaceId = '10000000-0000-4000-8000-000000000101';
    const sourceId = '20000000-0000-4000-8000-000000000101';
    const runId = createId();
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
    await connection.query(
      `INSERT INTO data_sources (id, workspace_id, provider, status, last_sync_at, last_successful_sync_at, next_sync_at)
       VALUES (?, ?, 'tiktok', 'disconnected', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 6 HOUR))
       ON DUPLICATE KEY UPDATE updated_at = UTC_TIMESTAMP(3)`,
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
      [createId(), workspaceId, sourceId, runId]
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
