import { Client } from "pg"

export async function withMigrationQueueDatabase<T>(url: string, operation: (db: Client) => Promise<T>, ssl = true): Promise<T> {
  const connectionString = new URL(url)
  connectionString.searchParams.delete("sslmode")
  const client = new Client({ connectionString: connectionString.toString(), ssl: ssl ? true : false, connectionTimeoutMillis: 15_000, keepAlive: true, application_name: "drive-migration-queue-control" })
  await client.connect()
  try { return await operation(client) } finally { await client.end().catch(() => undefined) }
}

export async function ensureMigrationQueueSchema(db: Client) {
  await db.query(`
    create table if not exists drive_migration_queue_meta (
      migration_id uuid not null, generation integer not null, status text not null default 'seeding',
      seed_cursor text, seed_complete boolean not null default false, sync_cursor text,
      execution_config jsonb,
      total_jobs bigint not null default 0, created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
      primary key(migration_id,generation)
    );
    alter table drive_migration_queue_meta add column if not exists sync_cursor text;
    alter table drive_migration_queue_meta add column if not exists execution_config jsonb;
    create table if not exists drive_migration_queue_jobs (
      id uuid primary key, migration_id uuid not null, generation integer not null, status text not null default 'pending',
      payload jsonb not null, progress jsonb not null default '{}'::jsonb, result jsonb not null default '{}'::jsonb,
      summary text, error text, claimed_by text, agent_id uuid, claim_token uuid, attempts integer not null default 0,
      lease_expires_at timestamptz, last_heartbeat_at timestamptz, completed_at timestamptz,
      created_at timestamptz not null default now(), updated_at timestamptz not null default now()
    );
    alter table drive_migration_queue_jobs add column if not exists agent_id uuid;
    create index if not exists drive_migration_queue_jobs_claim_idx on drive_migration_queue_jobs(migration_id,generation,status,created_at);
    create index if not exists drive_migration_queue_jobs_lease_idx on drive_migration_queue_jobs(status,lease_expires_at);
    create index if not exists drive_migration_queue_jobs_sync_idx on drive_migration_queue_jobs(migration_id,generation,updated_at,id);
    create table if not exists drive_migration_queue_workers (
      worker_instance_id text primary key, agent_id uuid, migration_id uuid not null, generation integer not null,
      status text not null default 'online', current_job_id uuid, last_heartbeat_at timestamptz not null default now(),
      runtime_seconds bigint not null default 0,
      metadata jsonb not null default '{}'::jsonb, updated_at timestamptz not null default now()
    );
    alter table drive_migration_queue_workers add column if not exists runtime_seconds bigint not null default 0;
    update drive_migration_queue_workers set
      runtime_seconds=greatest(0,floor(extract(epoch from (
        coalesce(nullif(metadata->>'retiredAt','')::timestamptz,last_heartbeat_at)
        - coalesce(nullif(metadata->>'workerStartedAt','')::timestamptz,last_heartbeat_at)
      )))::bigint),
      metadata=metadata||jsonb_build_object('runtimeAccountingVersion',1)
      where not (metadata ? 'runtimeAccountingVersion');
  `)
}

export async function readMigrationQueueLiveProjection(url: string, ssl: boolean, migrationId: string, requestedGeneration = 0) {
  return withMigrationQueueDatabase(url, async (db) => {
    const meta = (await db.query(`select generation,status,seed_complete,updated_at from drive_migration_queue_meta
      where migration_id=$1 and ($2::int=0 or generation=$2) order by generation desc limit 1`, [migrationId, requestedGeneration])).rows[0]
    if (!meta) return null
    const generation = Number(meta.generation)
    const [itemRows, workerRows, statsRows] = await Promise.all([
      db.query(`select item->>'id' item_id,item->>'sourceBucket' source_bucket,item->>'targetBucket' target_bucket,
        max(case when item->>'sourceObjects' ~ '^[0-9]+$' then (item->>'sourceObjects')::bigint else 0 end)::bigint source_objects,
        count(*)::bigint total_jobs,count(*) filter(where q.status='pending')::bigint queued_jobs,
        count(*) filter(where q.status in('claimed','running'))::bigint running_jobs,
        count(*) filter(where q.status='completed')::bigint completed_jobs,
        count(*) filter(where q.status='failed')::bigint failed_jobs,
        count(*) filter(where q.status='canceled')::bigint canceled_jobs,
        coalesce(sum(case when q.status='completed' then case when q.result->'items'->0->>'transferred' ~ '^[0-9]+$' then (q.result->'items'->0->>'transferred')::bigint else 0 end else greatest(case when q.result->'items'->0->>'transferred' ~ '^[0-9]+$' then (q.result->'items'->0->>'transferred')::bigint else 0 end,case when q.progress->>'transferred' ~ '^[0-9]+$' then (q.progress->>'transferred')::bigint else 0 end) end),0)::bigint transferred_objects,
        coalesce(sum(case when q.result->'items'->0->>'failed' ~ '^[0-9]+$' then (q.result->'items'->0->>'failed')::bigint when q.status='failed' then 1 else 0 end),0)::bigint failed_objects,
        coalesce(sum(case when q.result->'items'->0->>'skipped' ~ '^[0-9]+$' then (q.result->'items'->0->>'skipped')::bigint else 0 end),0)::bigint skipped_objects,
        max(q.updated_at) updated_at,
        (array_agg(q.progress->'currentFile' order by q.updated_at desc) filter(where jsonb_typeof(q.progress->'currentFile')='object'))[1] current_file
        from drive_migration_queue_jobs q cross join lateral jsonb_array_elements(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end) item
        where q.migration_id=$1 and q.generation=$2 and nullif(item->>'id','') is not null
        group by item->>'id',item->>'sourceBucket',item->>'targetBucket'`, [migrationId, generation]),
      db.query(`select w.worker_instance_id,w.agent_id,w.status,w.current_job_id,w.last_heartbeat_at,j.progress->'currentFile' current_file
        from drive_migration_queue_workers w left join drive_migration_queue_jobs j on j.id=w.current_job_id
        where w.migration_id=$1 and w.generation=$2 order by w.last_heartbeat_at desc`, [migrationId, generation]),
      db.query(`select count(*)::bigint total_jobs,count(*) filter(where status='pending')::bigint queued_jobs,
        count(*) filter(where status in('claimed','running'))::bigint running_jobs,
        count(*) filter(where status in('pending','claimed','running'))::bigint remaining_jobs,
        count(*) filter(where status='completed')::bigint completed_jobs,count(*) filter(where status='failed')::bigint failed_jobs,
        count(*) filter(where status='canceled')::bigint canceled_jobs,
        coalesce(sum(case when status='completed' then case when result->'items'->0->>'transferred' ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end else greatest(case when result->'items'->0->>'transferred' ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end,case when progress->>'transferred' ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end) end),0)::bigint transferred,
        max(updated_at) updated_at from drive_migration_queue_jobs where migration_id=$1 and generation=$2`, [migrationId, generation]),
    ])
    const now = Date.now()
    const items = itemRows.rows.map((row) => {
      const live = { status: Number(row.running_jobs) ? "running" : Number(row.queued_jobs) ? "queued" : Number(row.failed_jobs) ? "failed" : Number(row.canceled_jobs) ? "canceled" : "completed", totalJobs: Number(row.total_jobs), queuedJobs: Number(row.queued_jobs), runningJobs: Number(row.running_jobs), completedJobs: Number(row.completed_jobs), failedJobs: Number(row.failed_jobs), canceledJobs: Number(row.canceled_jobs), totalObjects: Number(row.source_objects || row.total_jobs), queuedObjects: Number(row.queued_jobs), transferredObjects: Number(row.transferred_objects), failedObjects: Number(row.failed_objects), skippedObjects: Number(row.skipped_objects), currentFile: row.current_file || null, updatedAt: row.updated_at }
      return { id: row.item_id, sourceBucket: row.source_bucket, targetBucket: row.target_bucket, slurperStatus: live.status, progress: { live: { ...live, workerStage: "migration" } } }
    })
    const workers = workerRows.rows.map((row) => ({ id: row.worker_instance_id, agentId: row.agent_id || "", status: row.status === "online" && now - Date.parse(row.last_heartbeat_at) < 90_000 ? "running" : "completed", online: row.status === "online" && now - Date.parse(row.last_heartbeat_at) < 90_000, instanceId: row.worker_instance_id, jobId: row.current_job_id, currentFile: row.current_file || undefined, currentStatus: row.current_job_id ? "running" : "idle", lastHeartbeatAt: row.last_heartbeat_at, completedFiles: 0, failedFiles: 0, completedBytes: 0, createdAt: row.last_heartbeat_at, updatedAt: row.last_heartbeat_at }))
    const stats = statsRows.rows[0] || {}
    return { generation, migrationStatus: meta.status, seedComplete: meta.seed_complete === true, updatedAt: stats.updated_at || meta.updated_at, items, workerRuns: workers, stats: { totalJobs: Number(stats.total_jobs || 0), queuedJobs: Number(stats.queued_jobs || 0), runningJobs: Number(stats.running_jobs || 0), remainingJobs: Number(stats.remaining_jobs || 0), completedJobs: Number(stats.completed_jobs || 0), failedJobs: Number(stats.failed_jobs || 0), canceledJobs: Number(stats.canceled_jobs || 0), transferred: Number(stats.transferred || 0), onlineWorkers: workers.filter((worker) => worker.online).length } }
  }, ssl)
}
