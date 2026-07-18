#!/usr/bin/env node
const crypto = require('crypto');
const mariadb = require('mariadb');
const { assertLocalDatabaseUrl, assertNotProductionCommand, getDatabaseUrl } = require('./database-env');

function fixedId(group, index) {
  return `${String(group).padStart(8, '0')}-0000-4000-8000-${String(index).padStart(12, '0')}`;
}

function tokenHash(label) {
  return crypto.createHash('sha256').update(`social-insights-local-demo:${label}`).digest('hex');
}

function utcSql(date) {
  return date.toISOString().slice(0, 23).replace('T', ' ');
}

function makeClock() {
  const anchor = new Date();
  anchor.setUTCMinutes(0, 0, 0);
  return {
    anchor,
    daysAgo(days) {
      return new Date(anchor.getTime() - days * 24 * 60 * 60 * 1000);
    },
    hoursAgo(hours) {
      return new Date(anchor.getTime() - hours * 60 * 60 * 1000);
    },
    daysFromNow(days) {
      return new Date(anchor.getTime() + days * 24 * 60 * 60 * 1000);
    }
  };
}

function dailyRunId(ageDays) {
  return fixedId(30000000, 91 - ageDays);
}

function metricValue(value, nullable) {
  return nullable ? null : value;
}

async function upsertUser(connection, id, email, displayName) {
  const existingRows = await connection.query('SELECT id FROM users WHERE email = ? LIMIT 1', [email]);
  const userId = existingRows[0] ? existingRows[0].id : id;
  if (existingRows[0]) {
    await connection.query(
      `UPDATE users SET display_name = ?, status = 'active', deleted_at = NULL WHERE id = ?`,
      [displayName, userId]
    );
  } else {
    await connection.query(
      `INSERT INTO users (id, email, display_name) VALUES (?, ?, ?)`,
      [userId, email, displayName]
    );
  }
  await connection.query(
    `INSERT INTO user_identities (id, user_id, provider, provider_subject, email)
     VALUES (?, ?, 'email', ?, ?)
     ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), email = VALUES(email)`,
    [crypto.randomUUID(), userId, email, email]
  );
  return userId;
}

async function upsertWorkspace(connection, { id, name, slug, ownerId }) {
  await connection.query(
    `INSERT INTO workspaces (id, name, slug, created_by)
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name), deleted_at = NULL`,
    [id, name, slug, ownerId]
  );
  await connection.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'owner', 'active')
     ON DUPLICATE KEY UPDATE role = VALUES(role), status = VALUES(status)`,
    [id, ownerId]
  );
}

async function resetDemoSource(connection, sourceId, workspaceId) {
  await connection.query('DELETE FROM sync_jobs WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM sync_runs WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM content_items WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM provider_scopes WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM oauth_credentials WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM provider_accounts WHERE data_source_id = ?', [sourceId]);
  await connection.query('DELETE FROM workspace_invitations WHERE workspace_id = ?', [workspaceId]);
}

async function upsertSource(connection, source) {
  await connection.query(
    `INSERT INTO data_sources
       (id, workspace_id, provider, status, reconnect_reason, last_sync_at, last_successful_sync_at, next_sync_at)
     VALUES (?, ?, 'tiktok', ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       status = VALUES(status),
       reconnect_reason = VALUES(reconnect_reason),
       last_sync_at = VALUES(last_sync_at),
       last_successful_sync_at = VALUES(last_successful_sync_at),
       next_sync_at = VALUES(next_sync_at),
       updated_at = UTC_TIMESTAMP(3),
       deleted_at = NULL`,
    [
      source.id,
      source.workspaceId,
      source.status,
      source.reconnectReason || null,
      source.lastSyncAt ? utcSql(source.lastSyncAt) : null,
      source.lastSuccessfulSyncAt ? utcSql(source.lastSuccessfulSyncAt) : null,
      source.nextSyncAt ? utcSql(source.nextSyncAt) : null
    ]
  );
}

async function insertSyncRun(connection, run) {
  await connection.query(
    `INSERT INTO sync_runs
       (id, workspace_id, data_source_id, trigger_type, status, started_at, finished_at,
        duration_ms, attempt, profile_count, content_seen_count, content_snapshot_count, correlation_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      run.id,
      run.workspaceId,
      run.sourceId,
      run.triggerType || 'scheduled',
      run.status,
      utcSql(run.startedAt),
      run.finishedAt ? utcSql(run.finishedAt) : null,
      run.durationMs || null,
      run.attempt || 1,
      run.profileCount || 0,
      run.contentSeenCount || 0,
      run.contentSnapshotCount || 0,
      run.correlationId || 'local-demo-seed'
    ]
  );
  if (run.error) {
    await connection.query(
      `INSERT INTO sync_errors
         (id, sync_run_id, category, provider_code, message, retryable)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        run.error.id,
        run.id,
        run.error.category,
        run.error.providerCode || null,
        run.error.message,
        Boolean(run.error.retryable)
      ]
    );
  }
}

async function seedLocalDashboard(connection, context) {
  const { clock, workspaceId, sourceId } = context;
  for (let age = 90; age >= 0; age -= 1) {
    const trendIndex = 90 - age;
    const runId = dailyRunId(age);
    const startedAt = clock.daysAgo(age);
    await insertSyncRun(connection, {
      id: runId,
      workspaceId,
      sourceId,
      status: 'success',
      startedAt,
      finishedAt: new Date(startedAt.getTime() + 42 * 1000),
      durationMs: 42000,
      profileCount: 1,
      contentSeenCount: age % 3 === 0 ? 2 : 0,
      contentSnapshotCount: age % 3 === 0 ? 2 : 0
    });
    await connection.query(
      `INSERT INTO profile_snapshots
         (id, workspace_id, data_source_id, sync_run_id, observed_at,
          follower_count, following_count, likes_count, video_count, provider_metrics)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, JSON_OBJECT('fixture', TRUE, 'series', 'local-demo'))`,
      [
        fixedId(40000000, 91 - age),
        workspaceId,
        sourceId,
        runId,
        utcSql(startedAt),
        1040 + trendIndex * 7 + (trendIndex % 6),
        metricValue(340 + Math.floor(trendIndex / 8), age % 19 === 0 && age !== 0),
        8200 + trendIndex * 83 + (trendIndex % 5) * 11,
        metricValue(36 + Math.floor(trendIndex / 9), age % 23 === 0 && age !== 0)
      ]
    );
  }

  const examples = [
    { id: fixedId(31000000, 1), status: 'partial', hours: 4, category: 'provider', code: 'partial_profile', retryable: true },
    { id: fixedId(31000000, 2), status: 'failed', hours: 8, category: 'network', code: 'upstream_network', retryable: true },
    { id: fixedId(31000000, 3), status: 'failed', hours: 12, category: 'rate_limit', code: 'too_many_requests', retryable: true },
    { id: fixedId(31000000, 4), status: 'failed', hours: 16, category: 'authentication', code: 'token_expired', retryable: false }
  ];
  for (const [index, example] of examples.entries()) {
    const startedAt = clock.hoursAgo(example.hours);
    await insertSyncRun(connection, {
      id: example.id,
      workspaceId,
      sourceId,
      triggerType: index % 2 === 0 ? 'manual' : 'scheduled',
      status: example.status,
      startedAt,
      finishedAt: new Date(startedAt.getTime() + (index + 1) * 17000),
      durationMs: (index + 1) * 17000,
      attempt: index + 1,
      profileCount: example.status === 'partial' ? 1 : 0,
      contentSeenCount: example.status === 'partial' ? 7 : 0,
      contentSnapshotCount: example.status === 'partial' ? 4 : 0,
      error: {
        id: fixedId(32000000, index + 1),
        category: example.category,
        providerCode: example.code,
        message: example.code,
        retryable: example.retryable
      }
    });
  }

  const contentSnapshotOffsets = [0, 1, 2];
  for (let index = 1; index <= 25; index += 1) {
    const contentId = fixedId(50000000, index);
    const publishedAge = (index * 3) % 89;
    const title = index === 3
      ? null
      : index === 5
        ? 'Local demo content with a deliberately long provider title for responsive table wrapping and chart label truncation'
        : index === 7
          ? '=Formula-looking local demo title sanitized in CSV'
          : `Local demo post ${String(index).padStart(2, '0')}`;
    const description = index === 3
      ? 'Missing-title fixture description used as the accessible fallback label.'
      : index === 11
        ? 'A very long deterministic local-only description that checks how the dashboard handles copy from providers without relying on decorative filler.'
        : `Fixture content generated only for Social Insights Studio local QA item ${index}.`;
    await connection.query(
      `INSERT INTO content_items
         (id, workspace_id, data_source_id, provider_content_id, published_at, title, description,
          share_url, duration_seconds, height, width, provider_metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, JSON_OBJECT('fixture', TRUE, 'thumbnail_state', ?, 'media_type', 'video'))
       ON DUPLICATE KEY UPDATE
         published_at = VALUES(published_at),
         title = VALUES(title),
         description = VALUES(description),
         share_url = VALUES(share_url),
         duration_seconds = VALUES(duration_seconds),
         height = VALUES(height),
         width = VALUES(width),
         provider_metadata = VALUES(provider_metadata),
         deleted_at = NULL,
         last_seen_at = UTC_TIMESTAMP(3)`,
      [
        contentId,
        workspaceId,
        sourceId,
        `local-demo-video-${index}`,
        utcSql(clock.daysAgo(publishedAge)),
        title,
        description,
        index % 4 === 0 ? null : `https://www.tiktok.com/@local-demo/video/${index}`,
        20 + index,
        index % 6 === 0 ? null : 1920,
        index % 6 === 0 ? null : 1080,
        index % 8 === 0 ? 'expired' : index % 5 === 0 ? 'missing' : 'available'
      ]
    );

    const snapshotCount = index <= 8 ? 3 : 1;
    for (let snapshot = 0; snapshot < snapshotCount; snapshot += 1) {
      const observedAge = Math.max(publishedAge - contentSnapshotOffsets[snapshot], 0);
      const views = index === 6 ? 0 : 400 + index * 151 + snapshot * 173;
      await connection.query(
        `INSERT INTO content_metric_snapshots
           (id, workspace_id, content_item_id, sync_run_id, observed_at,
            view_count, like_count, comment_count, share_count, provider_metrics)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, JSON_OBJECT('fixture', TRUE, 'snapshot_index', ?))`,
        [
          fixedId(60000000, index * 10 + snapshot),
          workspaceId,
          contentId,
          dailyRunId(observedAge),
          utcSql(clock.daysAgo(observedAge)),
          metricValue(views, index === 14),
          metricValue(index * 17 + snapshot * 9, index === 9),
          metricValue(index * 3 + snapshot, index === 10),
          metricValue(index * 2 + snapshot, index === 12),
          snapshot
        ]
      );
    }
  }
}

async function seedStateWorkspace(connection, context) {
  const { clock, workspaceId, sourceId, age, status, reconnectReason } = context;
  const runId = fixedId(33000000, Number(workspaceId.slice(-3)));
  const runStatus = status === 'reconnect_required' ? 'failed' : status === 'partial' ? 'partial' : 'success';
  const syncError = status === 'reconnect_required'
    ? {
        id: fixedId(34000000, Number(workspaceId.slice(-3))),
        category: 'authentication',
        providerCode: 'reconnect_required',
        message: reconnectReason,
        retryable: false
      }
    : status === 'partial'
      ? {
          id: fixedId(34000000, Number(workspaceId.slice(-3))),
          category: 'provider',
          providerCode: 'video_list_partial',
          message: 'Local fixture: profile data synced but content listing was partially unavailable.',
          retryable: true
        }
      : null;
  await insertSyncRun(connection, {
    id: runId,
    workspaceId,
    sourceId,
    status: runStatus,
    startedAt: clock.daysAgo(age),
    finishedAt: new Date(clock.daysAgo(age).getTime() + 30000),
    durationMs: 30000,
    profileCount: 1,
    contentSeenCount: status === 'partial' ? 5 : 0,
    contentSnapshotCount: status === 'partial' ? 2 : 0,
    error: syncError
  });
  await connection.query(
    `INSERT INTO profile_snapshots
       (id, workspace_id, data_source_id, sync_run_id, observed_at,
        follower_count, following_count, likes_count, video_count, provider_metrics)
     VALUES (?, ?, ?, ?, ?, 880, 210, 4200, 18, JSON_OBJECT('fixture', TRUE, 'state', ?))`,
    [
      fixedId(41000000, Number(workspaceId.slice(-3))),
      workspaceId,
      sourceId,
      runId,
      utcSql(clock.daysAgo(age)),
      status
    ]
  );
}

async function seedMembersAndInvitations(connection, workspaceId, ownerId) {
  const users = [
    { id: fixedId(0, 201), email: 'admin+local@social-insights.test', name: 'Local Admin User', role: 'admin' },
    { id: fixedId(0, 202), email: 'analyst+local@social-insights.test', name: 'Local Analyst User', role: 'analyst' },
    { id: fixedId(0, 203), email: 'viewer+local@social-insights.test', name: 'Local Viewer User', role: 'viewer' }
  ];
  for (const user of users) {
    const userId = await upsertUser(connection, user.id, user.email, user.name);
    await connection.query(
      `INSERT INTO workspace_memberships (workspace_id, user_id, role, status, invited_by)
       VALUES (?, ?, ?, 'active', ?)
       ON DUPLICATE KEY UPDATE role = VALUES(role), status = 'active', invited_by = VALUES(invited_by)`,
      [workspaceId, userId, user.role, ownerId]
    );
  }

  const clock = makeClock();
  const invitations = [
    { id: fixedId(70000000, 101), email: 'pending.invite+local@social-insights.test', role: 'viewer', expires: clock.daysFromNow(5), accepted: null, revoked: null },
    { id: fixedId(70000000, 102), email: 'expired.invite+local@social-insights.test', role: 'analyst', expires: clock.daysAgo(1), accepted: null, revoked: null },
    { id: fixedId(70000000, 103), email: 'accepted.invite+local@social-insights.test', role: 'admin', expires: clock.daysFromNow(2), accepted: clock.daysAgo(2), revoked: null }
  ];
  for (const invitation of invitations) {
    await connection.query(
      `INSERT INTO workspace_invitations
         (id, workspace_id, email, role, token_hash, invited_by, created_at, expires_at, accepted_at, revoked_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        invitation.id,
        workspaceId,
        invitation.email,
        invitation.role,
        tokenHash(invitation.id),
        ownerId,
        utcSql(clock.daysAgo(3)),
        utcSql(invitation.expires),
        invitation.accepted ? utcSql(invitation.accepted) : null,
        invitation.revoked ? utcSql(invitation.revoked) : null
      ]
    );
  }
}

async function main() {
  assertNotProductionCommand('db:seed');
  const databaseUrl = getDatabaseUrl('dev');
  assertLocalDatabaseUrl(databaseUrl);
  const connection = await mariadb.createConnection(databaseUrl);
  const clock = makeClock();
  try {
    const requestedOwnerId = fixedId(0, 101);
    const workspaces = [
      {
        id: fixedId(10000000, 101),
        sourceId: fixedId(20000000, 101),
        name: 'Local Demo Workspace',
        slug: 'local-demo-workspace',
        status: 'disconnected',
        lastSyncAge: 0,
        nextAge: -1
      },
      {
        id: fixedId(10000000, 102),
        sourceId: fixedId(20000000, 102),
        name: 'Stale Demo Workspace',
        slug: 'stale-demo-workspace',
        status: 'active',
        lastSyncAge: 45,
        nextAge: 44
      },
      {
        id: fixedId(10000000, 103),
        sourceId: fixedId(20000000, 103),
        name: 'Reconnect Required Workspace',
        slug: 'reconnect-required-workspace',
        status: 'reconnect_required',
        reconnectReason: 'Local fixture: TikTok token expired and requires reconnect.',
        lastSyncAge: 2,
        nextAge: 1
      },
      {
        id: fixedId(10000000, 104),
        sourceId: fixedId(20000000, 104),
        name: 'Empty Demo Workspace',
        slug: 'empty-demo-workspace',
        status: 'disconnected',
        lastSyncAge: null,
        nextAge: null
      },
      {
        id: fixedId(10000000, 105),
        sourceId: fixedId(20000000, 105),
        name: 'Partial Sync Demo Workspace',
        slug: 'partial-sync-demo-workspace',
        status: 'active',
        lastSyncAge: 1,
        lastSuccessfulSyncAge: 2,
        nextAge: -1
      }
    ];

    await connection.beginTransaction();
    const ownerId = await upsertUser(
      connection,
      requestedOwnerId,
      'demo+local@social-insights.test',
      'Local Demo User'
    );
    for (const workspace of workspaces) {
      await upsertWorkspace(connection, { ...workspace, ownerId });
      await resetDemoSource(connection, workspace.sourceId, workspace.id);
      await upsertSource(connection, {
        id: workspace.sourceId,
        workspaceId: workspace.id,
        status: workspace.status,
        reconnectReason: workspace.reconnectReason,
        lastSyncAt: workspace.lastSyncAge === null ? null : clock.daysAgo(workspace.lastSyncAge),
        lastSuccessfulSyncAt: workspace.lastSuccessfulSyncAge !== undefined
          ? clock.daysAgo(workspace.lastSuccessfulSyncAge)
          : workspace.lastSyncAge === null || workspace.status === 'reconnect_required'
            ? null
            : clock.daysAgo(workspace.lastSyncAge),
        nextSyncAt: workspace.nextAge === null ? null : clock.daysAgo(workspace.nextAge)
      });
    }

    await seedMembersAndInvitations(connection, workspaces[0].id, ownerId);
    await seedLocalDashboard(connection, {
      clock,
      workspaceId: workspaces[0].id,
      sourceId: workspaces[0].sourceId
    });
    await seedStateWorkspace(connection, {
      clock,
      workspaceId: workspaces[1].id,
      sourceId: workspaces[1].sourceId,
      age: 45,
      status: 'stale'
    });
    await seedStateWorkspace(connection, {
      clock,
      workspaceId: workspaces[2].id,
      sourceId: workspaces[2].sourceId,
      age: 2,
      status: 'reconnect_required',
      reconnectReason: workspaces[2].reconnectReason
    });
    await seedStateWorkspace(connection, {
      clock,
      workspaceId: workspaces[4].id,
      sourceId: workspaces[4].sourceId,
      age: 1,
      status: 'partial'
    });

    await connection.commit();
    console.log('Seeded deterministic local demo workspaces, analytics, members, invitations, and dashboard states.');
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
