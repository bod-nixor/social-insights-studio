#!/usr/bin/env node
const { closePool } = require('./database');
const { runDueSyncs } = require('./platform/sync-service');

function getArgValue(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index === -1 || index + 1 >= process.argv.length) return fallback;
  return process.argv[index + 1];
}

async function main() {
  const command = process.argv[2];
  if (command !== 'sync-due') {
    console.error('Usage: node server/worker.js sync-due --time-budget-seconds 240');
    process.exitCode = 1;
    return;
  }
  const timeBudgetSeconds = Number(getArgValue('--time-budget-seconds', process.env.WORKER_TIME_BUDGET_SECONDS || 240));
  const result = await runDueSyncs({ timeBudgetSeconds });
  console.log(JSON.stringify({
    processed: result.processed,
    results: result.results.map(item => ({
      data_source_id: item.data_source_id,
      sync_run_id: item.sync_run_id,
      status: item.status,
      error_category: item.error && item.error.category
    }))
  }));
}

main()
  .catch(error => {
    console.error(JSON.stringify({ error: error.code || error.message || 'worker_failed' }));
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
