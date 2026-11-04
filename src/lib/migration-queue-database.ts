import { Client } from "pg"

export async function withMigrationQueueDatabase<T>(url: string, operation: (db: Client) => Promise<T>, ssl = true): Promise<T> {
  const connectionString = new URL(url)
  connectionString.searchParams.delete("sslmode")
  const client = new Client({ connectionString: connectionString.toString(), ssl: ssl ? true : false, connectionTimeoutMillis: 15_000, keepAlive: true, application_name: "drive-migration-queue-control" })
  await client.connect()
  try { return await operation(client) } finally { await client.end().catch(() => undefined) }
}

export async function deleteMigrationQueueData(url: string, migrationId: string, ssl = true) {
  return withMigrationQueueDatabase(url, async (db) => {
    const tables = await db.query(`select to_regclass('public.drive_migration_queue_jobs') jobs,
      to_regclass('public.drive_migration_queue_workers') workers,
      to_regclass('public.drive_migration_queue_meta') meta`)
    if (!tables.rows[0]?.jobs && !tables.rows[0]?.workers && !tables.rows[0]?.meta) return { jobs: 0, workers: 0, meta: 0 }
    await db.query("begin")
    try {
      const jobs = tables.rows[0].jobs ? await db.query("delete from drive_migration_queue_jobs where migration_id=$1", [migrationId]) : { rowCount: 0 }
      const workers = tables.rows[0].workers ? await db.query("delete from drive_migration_queue_workers where migration_id=$1", [migrationId]) : { rowCount: 0 }
      const meta = tables.rows[0].meta ? await db.query("delete from drive_migration_queue_meta where migration_id=$1", [migrationId]) : { rowCount: 0 }
      await db.query("commit")
      return { jobs: jobs.rowCount || 0, workers: workers.rowCount || 0, meta: meta.rowCount || 0 }
    } catch (error) {
      await db.query("rollback").catch(() => undefined)
      throw error
    }
  }, ssl)
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
  return withMigrationQueueDatabase(url, (db) => readMigrationQueueLiveProjectionFromClient(db, migrationId, requestedGeneration), ssl)
}

export async function readMigrationQueueLiveProjectionFromClient(db: Client, migrationId: string, requestedGeneration = 0) {
    const meta = (await db.query(`select generation,status,seed_complete,updated_at from drive_migration_queue_meta
      where migration_id=$1 and ($2::int=0 or generation=$2) order by generation desc limit 1`, [migrationId, requestedGeneration])).rows[0]
    if (!meta) return null
    const generation = Number(meta.generation)
    const [itemRows, workerRows, statsRows] = await Promise.all([
      db.query(`with item_rows as (
          select distinct on (item->>'id') item->>'id' item_id,item->>'sourceBucket' source_bucket,item->>'targetBucket' target_bucket,
            case when item->>'sourceObjects' ~ '^[0-9]+$' then (item->>'sourceObjects')::bigint else 0 end source_objects,
            case when item->>'sourceBytes' ~ '^[0-9]+$' then (item->>'sourceBytes')::bigint else 0 end source_bytes
          from drive_migration_queue_jobs q cross join lateral jsonb_array_elements(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end) item
          where q.migration_id=$1 and q.generation=$2 and nullif(item->>'id','') is not null
          order by item->>'id',q.created_at
        ), job_item_rows as (
        select q.status,q.updated_at,item.item_id,item.source_bucket,item.target_bucket,item.source_objects,item.source_bytes,
            greatest(case when q.result->>'retryCount' ~ '^[0-9]+$' then (q.result->>'retryCount')::int else 0 end,
              case when q.result->>'verificationRetryCount' ~ '^[0-9]+$' then (q.result->>'verificationRetryCount')::int else 0 end) retry_count,
            coalesce(res.transferred,0)::bigint result_transferred,coalesce(res.failed,0)::bigint result_failed,coalesce(res.skipped,0)::bigint result_skipped,
            coalesce(live.transferred,0)::bigint live_transferred,coalesce(live.failed,0)::bigint live_failed,coalesce(live.skipped,0)::bigint live_skipped,
            q.progress->>'currentItemId' current_item_id,q.progress->'currentFile' current_file,
            case when q.progress->>'transferred' ~ '^[0-9]+$' then (q.progress->>'transferred')::bigint else 0 end current_transferred,
            case when q.progress->>'skipped' ~ '^[0-9]+$' then (q.progress->>'skipped')::bigint else 0 end current_skipped
          from drive_migration_queue_jobs q
          cross join lateral jsonb_array_elements(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end) payload_item
          join item_rows item on item.item_id=payload_item->>'id'
          left join lateral (select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end)::bigint transferred,
              sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end)::bigint failed,
              sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end)::bigint skipped
            from jsonb_array_elements(case when jsonb_typeof(q.result->'items')='array' then q.result->'items' else '[]'::jsonb end) x
            where x->>'itemId'=item.item_id or (x->>'itemId' is null and jsonb_array_length(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end)=1)) res on true
          left join lateral (select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end)::bigint transferred,
              sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end)::bigint failed,
              sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end)::bigint skipped
            from jsonb_array_elements(case when jsonb_typeof(q.progress->'itemProgress')='array' then q.progress->'itemProgress' else '[]'::jsonb end) x
            where x->>'itemId'=item.item_id) live on true
          where q.migration_id=$1 and q.generation=$2
        )
        select item_id,source_bucket,target_bucket,max(source_objects)::bigint source_objects,max(source_bytes)::bigint source_bytes,
          count(*)::bigint total_jobs,count(*) filter(where status='pending')::bigint queued_jobs,
          count(*) filter(where status in('claimed','running'))::bigint running_jobs,
          count(*) filter(where status='completed')::bigint completed_jobs,count(*) filter(where status='failed')::bigint failed_jobs,
          count(*) filter(where status='canceled')::bigint canceled_jobs,
          count(*) filter(where status in('pending','claimed','running') or (status='failed' and retry_count<2))::bigint remaining_jobs,
          count(*) filter(where status in('pending','claimed','running') and retry_count>0)::bigint retrying_jobs,
          coalesce(sum(case when status='completed' then result_transferred when status in('claimed','running') then greatest(result_transferred,live_transferred,case when current_item_id=item_id then current_transferred else 0 end) else 0 end),0)::bigint transferred_objects,
          coalesce(sum(case when status='failed' then greatest(1,result_failed,live_failed) when status='completed' then result_failed when status in('claimed','running') then greatest(result_failed,live_failed) else 0 end),0)::bigint failed_objects,
          coalesce(sum(case when status='completed' then result_skipped when status in('claimed','running') then greatest(result_skipped,live_skipped,case when current_item_id=item_id then current_skipped else 0 end) else 0 end),0)::bigint skipped_objects,
          max(updated_at) updated_at,
          (array_agg(current_file order by updated_at desc) filter(where status in('claimed','running') and jsonb_typeof(current_file)='object'))[1] current_file
        from job_item_rows group by item_id,source_bucket,target_bucket`, [migrationId, generation]),
      db.query(`select w.worker_instance_id,w.agent_id,w.status,w.current_job_id,w.last_heartbeat_at,j.progress->'currentFile' current_file
        from drive_migration_queue_workers w left join drive_migration_queue_jobs j on j.id=w.current_job_id
        where w.migration_id=$1 and w.generation=$2 order by w.last_heartbeat_at desc`, [migrationId, generation]),
      db.query(`select count(*)::bigint total_jobs,count(*) filter(where status='pending')::bigint queued_jobs,
        count(*) filter(where status in('claimed','running'))::bigint running_jobs,
        count(*) filter(where status in('pending','claimed','running') or (status='failed' and case when result->>'retryCount' ~ '^[0-9]+$' then (result->>'retryCount')::int else 0 end<2))::bigint remaining_jobs,
        count(*) filter(where status='completed')::bigint completed_jobs,count(*) filter(where status='failed')::bigint failed_jobs,
        count(*) filter(where status='canceled')::bigint canceled_jobs,
        coalesce(sum(case when status='completed' then case when result->'totals'->>'transferred' ~ '^[0-9]+$' then (result->'totals'->>'transferred')::bigint else coalesce((select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end when status in('claimed','running') then greatest(case when progress->'totals'->>'transferred' ~ '^[0-9]+$' then (progress->'totals'->>'transferred')::bigint when progress->>'transferred' ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end,case when result->'totals'->>'transferred' ~ '^[0-9]+$' then (result->'totals'->>'transferred')::bigint else 0 end) else 0 end),0)::bigint transferred,
        coalesce(sum(case when status='completed' then coalesce((select sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) when status in('claimed','running') then coalesce((select sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(progress->'itemProgress')='array' then progress->'itemProgress' else '[]'::jsonb end) x),case when progress->'totals'->>'skipped' ~ '^[0-9]+$' then (progress->'totals'->>'skipped')::bigint else 0 end) else 0 end),0)::bigint skipped,
        coalesce(sum(case when status='failed' then greatest(1,case when result->'totals'->>'failed' ~ '^[0-9]+$' then (result->'totals'->>'failed')::bigint else coalesce((select sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end) when status='completed' then case when result->'totals'->>'failed' ~ '^[0-9]+$' then (result->'totals'->>'failed')::bigint else coalesce((select sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end when status in('claimed','running') then case when progress->'totals'->>'failed' ~ '^[0-9]+$' then (progress->'totals'->>'failed')::bigint else 0 end else 0 end),0)::bigint failed,
        max(updated_at) updated_at from drive_migration_queue_jobs where migration_id=$1 and generation=$2`, [migrationId, generation]),
    ])
    const now = Date.now()
    const items = itemRows.rows.map((row) => {
      const live = { status: Number(row.retrying_jobs) ? "retrying" : Number(row.running_jobs) ? "running" : Number(row.queued_jobs) ? "queued" : Number(row.failed_jobs) ? "failed" : Number(row.canceled_jobs) ? "canceled" : "completed", workerGeneration: generation, totalJobs: Number(row.total_jobs), queuedJobs: Number(row.queued_jobs), runningJobs: Number(row.running_jobs), completedJobs: Number(row.completed_jobs), failedJobs: Number(row.failed_jobs), canceledJobs: Number(row.canceled_jobs), remainingJobs: Number(row.remaining_jobs), totalObjects: Number(row.source_objects || row.total_jobs), sourceBytes: Number(row.source_bytes || 0), queuedObjects: Number(row.queued_jobs), transferredObjects: Number(row.transferred_objects), alreadyPresentObjects: Number(row.already_present_objects), failedObjects: Number(row.failed_objects), skippedObjects: Number(row.skipped_objects), currentFile: row.current_file || null, updatedAt: row.updated_at }
      return { id: row.item_id, sourceBucket: row.source_bucket, targetBucket: row.target_bucket, slurperStatus: live.status, progress: { live: { ...live, workerStage: "migration" } } }
    })
    const workers = workerRows.rows.map((row) => ({ id: row.worker_instance_id, agentId: row.agent_id || "", status: row.status === "online" && now - Date.parse(row.last_heartbeat_at) < 90_000 ? "running" : "completed", online: row.status === "online" && now - Date.parse(row.last_heartbeat_at) < 90_000, instanceId: row.worker_instance_id, jobId: row.current_job_id, currentFile: row.current_file || undefined, currentStatus: row.current_job_id ? "running" : "idle", lastHeartbeatAt: row.last_heartbeat_at, completedFiles: 0, failedFiles: 0, completedBytes: 0, createdAt: row.last_heartbeat_at, updatedAt: row.last_heartbeat_at }))
    const stats = statsRows.rows[0] || {}
    return { generation, queueStatus: meta.status, seedComplete: meta.seed_complete === true, updatedAt: stats.updated_at || meta.updated_at, items, workerRuns: workers, stats: { totalJobs: Number(stats.total_jobs || 0), queuedJobs: Number(stats.queued_jobs || 0), runningJobs: Number(stats.running_jobs || 0), remainingJobs: Number(stats.remaining_jobs || 0), completedJobs: Number(stats.completed_jobs || 0), failedJobs: Number(stats.failed_jobs || 0), canceledJobs: Number(stats.canceled_jobs || 0), transferred: Number(stats.transferred || 0), skipped: Number(stats.skipped || 0), failed: Number(stats.failed || 0), onlineWorkers: workers.filter((worker) => worker.online).length } }
}

export function overlayMigrationQueueItems<T extends { id: string; progress: Record<string, unknown>; slurperStatus?: string; updatedAt?: string }>(
  items: T[],
  projection: { generation: number; items: Array<{ id: string; slurperStatus: string; progress: { live: Record<string, unknown> } }> },
): T[] {
  const liveById = new Map(projection.items.map((item) => [item.id, item]))
  return items.map((item) => {
    const current = liveById.get(item.id)
    if (!current) return item
    const updatedAt = typeof current.progress.live.updatedAt === "string" ? current.progress.live.updatedAt : item.updatedAt
    const migrationQueue = isRecord(item.progress.migrationQueue) ? item.progress.migrationQueue : {}
    const baseline = isRecord(migrationQueue.retryBaseline) ? migrationQueue.retryBaseline : {}
    const numericBaseline = (key: string) => {
      const value = Number(baseline[key] || 0)
      return Number.isFinite(value) ? Math.max(0, value) : 0
    }
    const verification = isRecord(item.progress.fileVerification) ? item.progress.fileVerification : {}
    const verifiedFailures = Math.max(0, Number(verification.missing ?? verification.missingObjects ?? 0) || 0)
      + Math.max(0, Number(verification.mismatched ?? verification.mismatchedObjects ?? 0) || 0)
    const verificationState = "verificationState" in item && isRecord(item.verificationState)
      ? item.verificationState
      : {}
    const verificationStatus = String(verificationState.status || verification.status || "").toLowerCase()
    const verificationMissing = Number(verificationState.missingObjects ?? verification.missing ?? 0)
    const verificationMismatched = Number(verificationState.mismatchedObjects ?? verification.mismatched ?? 0)
    const verificationExtra = Number(verificationState.extraObjects ?? verification.extra ?? 0)
    const verificationPassed = verificationStatus === "completed"
      && verificationMissing === 0
      && verificationMismatched === 0
      && (verificationState.strictDestination !== true || verificationExtra === 0)
    const transferTotalJobs = Number(current.progress.live.totalJobs ?? 0)
    const transferCompletedJobs = Number(current.progress.live.completedJobs ?? 0)
    const transferComplete = transferTotalJobs > 0
      && transferCompletedJobs >= transferTotalJobs
      && Number(current.progress.live.queuedJobs ?? 0) === 0
      && Number(current.progress.live.runningJobs ?? 0) === 0
      && Number(current.progress.live.failedJobs ?? 0) === 0
    const verificationFailed = verificationStatus === "failed"
      || (verificationStatus === "completed" && !verificationPassed)
    const queueLifecycleStatus = transferComplete && !verificationPassed
      ? verificationFailed ? "verification_failed" : verificationStatus === "running" ? "verifying" : "awaiting_verification"
      : current.slurperStatus
    const live = {
      ...current.progress.live,
      ...(transferComplete && !verificationPassed ? { status: queueLifecycleStatus, awaitingVerification: !verificationFailed } : {}),
      totalObjects: Math.max(Number(current.progress.live.totalObjects || 0), numericBaseline("totalObjects")),
      transferredObjects: numericBaseline("transferredObjects") + Number(current.progress.live.transferredObjects || 0),
      skippedObjects: numericBaseline("skippedObjects") + Number(current.progress.live.skippedObjects || 0),
      failedObjects: Number(current.progress.live.failedObjects || 0) + verifiedFailures,
      sourceBytes: Math.max(Number(current.progress.live.sourceBytes || 0), numericBaseline("sourceBytes")),
    }
    return { ...item, slurperStatus: queueLifecycleStatus, progress: { ...item.progress, migrationQueue: { ...migrationQueue, generation: projection.generation }, live }, updatedAt }
  })
}

export function resolveMigrationQueueLifecycleStatus<T extends string>(status: T, projection: { seedComplete: boolean; stats: { failedJobs: number; remainingJobs: number } }): T | "failed" {
  if (projection.seedComplete && projection.stats.failedJobs > 0 && projection.stats.remainingJobs === 0) return "failed"
  return status
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
