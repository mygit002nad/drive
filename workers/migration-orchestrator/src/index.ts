import { Client } from "pg"
import { syncWorkerRepository } from "../../../src/lib/github-worker-sync"

type DispatchMessage = { intentId: string } | { control: "cycle" }
type Env = { POSTGRES_URL?: string; HYPERDRIVE?: { connectionString: string }; MIGRATION_ORCHESTRATOR_SECRET?: string; PANEL_URL?: string; DISABLE_POSTGRES_SSL?: string; GITHUB_WORKER_SOURCE_REPO?: string; GITHUB_DISPATCH_QUEUE: Queue<DispatchMessage> }
// Database rows are dynamically shaped by the PostgreSQL queries below.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Row = Record<string, any>
const BUILD = 36
const MIN_QUEUE_BATCH_SIZE = 500
const DEFAULT_QUEUE_BATCH_SIZE = 2_000
const MAX_QUEUE_BATCH_SIZE = 4_000
const GITHUB_WORKER_MAX_RUNTIME_SECONDS = 21_300
const MAX_SECRET_LENGTH = 512
const TRANSIENT_SCAN_SQL_PATTERN = "(connection terminated unexpectedly|connection reset|connection closed|server closed the connection unexpectedly|client has encountered a connection error|not queryable|socket hang up|econnreset|econnrefused|etimedout|timeout|timed out|eai_again|enotfound|enetunreach|epipe|fetch failed|temporar(y|ily) unavailable|too many (requests|connections|clients)|slow down|throttl|HTTP (408|425|429|500|502|503|504)|57P01|57P03|53300|08[0-9A-Z]{3}|40001|40P01)"
let authCache: { value: string[]; expiresAt: number } | null = null
let adaptiveQueueBatchSize = DEFAULT_QUEUE_BATCH_SIZE
let schemaReady: Promise<void> | null = null

function tuneQueueBatchSize(elapsedMs: number, succeeded: boolean) {
  if (!succeeded) adaptiveQueueBatchSize = Math.max(MIN_QUEUE_BATCH_SIZE, Math.floor(adaptiveQueueBatchSize / 2))
  else if (elapsedMs < 5_000) adaptiveQueueBatchSize = Math.min(MAX_QUEUE_BATCH_SIZE, adaptiveQueueBatchSize + 250)
  else if (elapsedMs > 20_000) adaptiveQueueBatchSize = Math.max(MIN_QUEUE_BATCH_SIZE, adaptiveQueueBatchSize - 250)
}

function isTransientWorkerError(message: string) {
  return /connection terminated unexpectedly|connection reset|connection closed|server closed the connection unexpectedly|client has encountered a connection error|not queryable|socket hang up|econn(?:reset|refused)|etimedout|eai_again|enotfound|enetunreach|epipe|fetch failed|time(?:out|d out)|temporar(?:y|ily) unavailable|too many (?:requests|connections|clients)|slow down|throttl|\bHTTP (?:408|425|429|500|502|503|504)\b|\b(?:57P01|57P03|53300|08000|08001|08003|08004|08006|08007|40001|40P01)\b/i.test(message)
}

function json(value: unknown, status = 200) { return Response.json(value, { status, headers: { "Cache-Control": "no-store, max-age=0" } }) }
function safeEqual(a: string, b: string) {
  if (a.length > MAX_SECRET_LENGTH || b.length > MAX_SECRET_LENGTH) return false
  let different = a.length ^ b.length
  for (let i = 0; i < Math.max(a.length, b.length); i += 1) different |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0)
  return different === 0
}
async function authorized(request: Request, env: Env) {
  const value = request.headers.get("authorization") || ""
  if (!value.toLowerCase().startsWith("bearer ")) return false
  const supplied = value.slice(7).trim()
  if (authCache && authCache.expiresAt > Date.now()) return authCache.value.some((value) => safeEqual(supplied, value))
  return database(env, async (db) => {
    const expected = String(env.MIGRATION_ORCHESTRATOR_SECRET || "")
    const stored = String((await db.query(`select value->>'sharedSecret' secret from drive_app_settings where key='migration-orchestrator' limit 1`)).rows[0]?.secret || "")
    if (!safeEqual(expected, stored)) return false
    if (expected.length >= 24 && expected.length <= MAX_SECRET_LENGTH) authCache = { value: [expected], expiresAt: Date.now() + 30_000 }
    return expected.length >= 24 && expected.length <= MAX_SECRET_LENGTH && safeEqual(supplied, expected)
  }).catch(() => false)
}
async function database<T>(env: Env, operation: (client: Client) => Promise<T>): Promise<T> {
  const hyperdrive = Boolean(env.HYPERDRIVE?.connectionString?.trim())
  const connectionString = hyperdrive ? env.HYPERDRIVE!.connectionString.trim() : String(env.POSTGRES_URL || "").trim()
  if (!connectionString) throw new Error("Neither HYPERDRIVE nor POSTGRES_URL is configured")
  const hostname = new URL(connectionString).hostname
  const sslMode = new URL(connectionString).searchParams.get("sslmode")?.trim().toLowerCase()
  const disableSsl = ["1", "true"].includes(String(env.DISABLE_POSTGRES_SSL || "").toLowerCase()) || sslMode === "disable"
  const client = new Client({ connectionString, ...(hyperdrive ? {} : { ssl: disableSsl || ["localhost", "127.0.0.1"].includes(hostname) ? false : { rejectUnauthorized: false } }), connectionTimeoutMillis: 8_000 })
  await client.connect()
  try { return await operation(client) } finally { await client.end().catch(() => undefined) }
}
async function ensureSchema(db: Client) {
  if (schemaReady) return schemaReady
  const pending = (async () => {
    // Keep each compatibility repair in its own statement. Some transaction
    // poolers do not reliably execute every command in a multi-statement DDL
    // string, which can leave an older table missing a later-added column.
    await db.query(`create table if not exists drive_migration_orchestrator_state (
      id boolean primary key default true check (id), status text not null default 'idle', orchestrator_url text,
      lease_owner text, lease_expires_at timestamptz, last_started_at timestamptz, last_completed_at timestamptz, last_error text,
      last_migration_id uuid references drive_migrations(id) on delete set null,
      last_result jsonb not null default '{}'::jsonb, cycle_count bigint not null default 0, updated_at timestamptz not null default now()
    )`)
    await db.query(`alter table if exists drive_migration_orchestrator_state add column if not exists lease_owner text`)
    await db.query(`alter table if exists drive_migration_orchestrator_state add column if not exists lease_expires_at timestamptz`)
    await db.query(`create table if not exists drive_migration_verification_state (
      migration_item_id uuid primary key references drive_migration_items(id) on delete cascade,
      migration_id uuid not null references drive_migrations(id) on delete cascade, generation integer not null default 1,
      source_scan_id uuid references drive_bucket_scans(id) on delete set null, destination_scan_id uuid references drive_bucket_scans(id) on delete set null,
      phase text not null default 'source', status text not null default 'pending', source_cursor text, destination_cursor text,
      source_objects bigint not null default 0, source_bytes bigint not null default 0, destination_objects bigint not null default 0, destination_bytes bigint not null default 0,
      missing_objects bigint not null default 0, mismatched_objects bigint not null default 0, extra_objects bigint not null default 0,
      attempt_count integer not null default 0, last_error text, lease_owner text, lease_expires_at timestamptz,
      completed_at timestamptz, created_at timestamptz not null default now(), updated_at timestamptz not null default now()
    )`)
    await db.query(`create table if not exists drive_migration_worker_live_state (
      migration_id uuid primary key references drive_migrations(id) on delete cascade,
      snapshot jsonb not null default '{}'::jsonb,
      updated_at timestamptz not null default now()
    )`)
    await db.query(`alter table if exists drive_migration_verification_state add column if not exists attempt_generation integer`)
  })()
  schemaReady = pending
  try { await pending }
  catch (error) {
    if (schemaReady === pending) schemaReady = null
    throw error
  }
}
function opts(row: Row): Row { return row.options && typeof row.options === "object" ? row.options : {} }
async function migrationIsActive(db: Client, migrationId: string) {
  const result = await db.query(`select 1 from drive_migrations where id=$1 and status in('running','verifying') limit 1`, [migrationId])
  return result.rowCount === 1
}
function integer(value: unknown, fallback: number, min: number, max: number) {
  const parsed = Number(value); return Number.isFinite(parsed) ? Math.max(min, Math.min(max, Math.trunc(parsed))) : fallback
}
async function acquire(db: Client, owner: string) {
  const result = await db.query(`
    insert into drive_migration_orchestrator_state(id,status,lease_owner,lease_expires_at,last_started_at,last_error,updated_at)
    values(true,'running',$1,now()+interval '150 seconds',now(),null,now())
    on conflict(id) do update set status='running',lease_owner=$1,lease_expires_at=now()+interval '150 seconds',last_started_at=now(),last_error=null,updated_at=now()
      where drive_migration_orchestrator_state.status<>'running' or drive_migration_orchestrator_state.lease_expires_at is null or drive_migration_orchestrator_state.lease_expires_at<now()
    returning id
  `, [owner])
  return result.rowCount === 1
}
async function renew(db: Client, owner: string) {
  const result = await db.query(`update drive_migration_orchestrator_state set lease_expires_at=now()+interval '150 seconds',updated_at=now() where id=true and status='running' and lease_owner=$1 returning id`, [owner])
  if (result.rowCount !== 1) throw new Error("Migration Orchestrator lease was lost")
}
async function selectMigration(db: Client): Promise<Row | null> {
  const result = await db.query(`select * from drive_migrations m where (
      m.status in ('running','verifying') or (
        m.status='failed' and coalesce(m.options->>'executionMode','super_slurper')='super_slurper' and exists(
          select 1 from drive_migration_items i join drive_bucket_scans s on s.id::text=i.progress->>'sourceScanId'
          where i.migration_id=m.id and i.slurper_job_id is null and i.slurper_status='precheck_failed'
            and s.status='failed' and s.error ~* $1
        )
      ) or (
        m.status='failed' and coalesce(m.options->>'executionMode','super_slurper')='super_slurper'
        and exists(select 1 from drive_migration_items i where i.migration_id=m.id and i.slurper_status='verification_failed')
        and not exists(select 1 from drive_migration_items i where i.migration_id=m.id and i.slurper_status in('failed','aborted','precheck_failed','bucket_create_failed'))
      ) or (
        m.status='verification_failed' and exists(
          select 1 from drive_migration_verification_state v
          where v.migration_id=m.id and v.status='failed'
            and v.last_error ~* $1
        )
      ) or (
        (m.status in('canceled','completed','aborted') or (m.status in('failed','verification_failed') and coalesce(m.options->>'executionMode','')='migration_workers'))
        and exists(select 1 from drive_agent_runs r where r.run_type='github_dispatch' and r.status in('pending','running') and (
          r.payload->>'migrationId'=m.id::text or exists(select 1 from drive_repair_jobs j where j.id::text=r.job_reference and j.migration_id=m.id)
        ))
      )
    ) and coalesce(m.options->>'executionMode','super_slurper') in ('migration_workers','super_slurper')
    order by coalesce(m.last_synced_at,m.created_at),m.created_at limit 1`, [TRANSIENT_SCAN_SQL_PATTERN])
  return result.rows[0] || null
}
async function mapWithConcurrency<T>(items: T[], limit: number, operation: (item: T) => Promise<void>) {
  let cursor = 0
  const workers = Array.from({ length: Math.min(items.length, Math.max(1, limit)) }, async () => {
    while (cursor < items.length) {
      const index = cursor++
      await operation(items[index])
    }
  })
  const outcomes = await Promise.allSettled(workers)
  const failure = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected")
  if (failure) throw failure.reason
}
function nonNegative(value: unknown): number | null {
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : null
}
function slurperProgressValues(payload: Row): { status: string; objects: number | null; transferred: number; skipped: number; failed: number } {
  const result = payload?.result && typeof payload.result === "object" ? payload.result : payload
  const progress = result?.progress && typeof result.progress === "object" ? result.progress : result
  const value = (...keys: string[]) => {
    for (const key of keys) {
      const parsed = nonNegative(progress?.[key])
      if (parsed !== null) return parsed
    }
    return null
  }
  return {
    status: String(progress?.status || result?.status || "running").trim().toLowerCase(),
    objects: value("objects", "totalObjects", "total_objects"),
    transferred: value("transferredObjects", "transferred_objects", "completedObjects", "completed_objects") ?? 0,
    skipped: value("skippedObjects", "skipped_objects") ?? 0,
    failed: value("failedObjects", "failed_objects") ?? 0,
  }
}
function normalizeSlurperStatus(status: string): string {
  if (["completed", "complete", "finished", "success", "succeeded", "copy_completed"].includes(status)) return "completed"
  if (["aborted", "canceled", "cancelled", "copy_aborted"].includes(status)) return "aborted"
  if (status.includes("failed") || status.includes("error")) return "failed"
  if (status === "paused") return "paused"
  if (["queued", "pending", "created"].includes(status)) return "queued"
  return "running"
}
async function refreshSuperSlurperProgress(db: Client, migration: Row) {
  const result = await db.query(`
    select i.id,i.slurper_job_id,i.slurper_status,i.source_objects,i.progress,
      a.cloudflare_account_id,a.api_token
    from drive_migration_items i join drive_accounts a on a.id=$2
    where i.migration_id=$1 and i.slurper_job_id is not null
      and coalesce(i.slurper_status,'') not in('completed','failed','aborted','bucket_create_failed','precheck_failed','verification_failed')
    order by i.updated_at limit 12
  `, [migration.id, migration.target_account_id])
  let refreshed = 0
  await mapWithConcurrency(result.rows, 3, async (item) => {
    let response: Row
    try {
      response = await cloudflare(item, `/slurper/jobs/${encodeURIComponent(item.slurper_job_id)}/progress`, "GET", undefined, false, 8_000)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      await db.query(`update drive_migration_items set
        progress=jsonb_set(coalesce(progress,'{}'::jsonb),'{slurperSync}',$2::jsonb),updated_at=now()
        where id=$1 and coalesce(slurper_status,'') not in('completed','failed','aborted','verification_failed')
          and exists(select 1 from drive_migrations m where m.id=$3 and m.status in('running','verifying'))`, [item.id, JSON.stringify({ status: "retrying", error: message, updatedAt: new Date().toISOString() }), migration.id])
      return
    }
    const values = slurperProgressValues(response)
    const status = normalizeSlurperStatus(values.status)
    const previous = item.progress?.slurperCumulative || item.progress?.slurperNormalized || {}
    const baseline = nonNegative(item.progress?.rerunBaselineTransferred) ?? 0
    const transferred = baseline + values.transferred
    const cumulative = Math.max(nonNegative(previous.transferredObjects) ?? 0, transferred)
    const skipped = Math.max(nonNegative(previous.skippedObjects) ?? 0, values.skipped)
    const failed = Math.max(nonNegative(previous.failedObjects) ?? 0, values.failed)
    const objects = Math.max(values.objects ?? 0, nonNegative(item.source_objects) ?? 0)
    // A retry can report the same object as both copied and skipped. Keep the
    // canonical counters mutually exclusive so copied objects never inflate
    // the skipped column (or vice versa).
    const exclusiveSkipped = Math.max(0, Math.min(objects || skipped, skipped - cumulative))
    const alreadyPresent = opts(migration).overwrite === false ? exclusiveSkipped : 0
    const countedTransferred = Math.min(objects || cumulative, cumulative)
    const liveStatus = status === "completed" ? "verifying" : status
    const normalized = { status, objects, transferredObjects: cumulative, skippedObjects: exclusiveSkipped, failedObjects: failed }
    const live = {
      ...(item.progress?.live && typeof item.progress.live === "object" ? item.progress.live : {}),
      updatedAt: new Date().toISOString(), status: liveStatus, totalObjects: objects,
      transferredObjects: countedTransferred, copiedObjects: Math.min(objects || cumulative, cumulative),
      alreadyPresentObjects: alreadyPresent, skippedObjects: exclusiveSkipped,
      failedObjects: failed, unaccountedObjects: Math.max(0, objects - countedTransferred - failed),
      verifyIssues: 0, slurperJobId: item.slurper_job_id,
    }
    await db.query(`
      update drive_migration_items set slurper_status=$2,
        source_objects=$3::bigint,
        progress=jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  jsonb_set(coalesce(progress,'{}'::jsonb),'{slurperNormalized}',$4::jsonb),
                  '{slurperCumulative}',$5::jsonb),
                '{live}',$6::jsonb),
              '{slurperSync}','{"status":"synced"}'::jsonb),
            '{events}',
            (case when jsonb_typeof(progress->'events')='array' then progress->'events' else '[]'::jsonb end)
              || case when progress->>'slurperEventStatus' is distinct from $2 then
                jsonb_build_array(jsonb_build_object('at',now(),'stage',case when $2='completed' then 'super_slurper_transfer' else 'super_slurper_'||$2 end,'status',$2,
                  'message',case $2 when 'running' then 'Super Slurper is transferring this bucket'
                    when 'verifying' then 'Super Slurper finished; File Scanner verification started'
                    when 'completed' then 'Super Slurper transfer completed; File Scanner verification pending'
                    when 'failed' then 'Super Slurper bucket job failed'
                    when 'aborted' then 'Super Slurper bucket job was aborted'
                    else 'Super Slurper status changed to '||$2 end))
                else '[]'::jsonb end,true),
          '{slurperEventStatus}',to_jsonb($2::text),true),
        last_progress_at=now(),updated_at=now()
      where id=$1 and coalesce(slurper_status,'') not in('completed','failed','aborted','verification_failed')
        and exists(select 1 from drive_migrations m where m.id=$7 and m.status in('running','verifying'))
    `, [item.id, status, objects, JSON.stringify(normalized), JSON.stringify({ ...normalized, status: liveStatus }), JSON.stringify(live), migration.id])
    refreshed += 1
  })
  await db.query("begin")
  let verification: { rowCount: number | null }
  try {
    verification = await db.query(`
    insert into drive_migration_verification_state(
      migration_item_id,migration_id,generation,source_scan_id,source_objects,source_bytes,status,phase
    )
    select i.id,i.migration_id,1,nullif(i.progress->>'sourceScanId','')::uuid,i.source_objects,i.source_bytes,'pending',
      case when nullif(i.progress->>'sourceScanId','') is null then 'source' else 'destination' end
    from drive_migration_items i
    where i.migration_id=$1 and i.slurper_status in('completed','verification_failed')
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
      and coalesce(i.progress->>'stage','')<>'worker_bucket_create_failed'
    on conflict(migration_item_id) do nothing
    `, [migration.id])
    await db.query(`
      update drive_migration_items i set slurper_status='verifying',last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(
        jsonb_set(
          jsonb_set(coalesce(i.progress,'{}'::jsonb),'{live}',
            coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object('status','verifying','updatedAt',now())),
          '{verificationGeneration}','1'::jsonb,true),
        '{verificationAttemptId}',to_jsonb(coalesce(nullif(i.progress->>'verificationAttemptId',''),gen_random_uuid()::text)),true)
    from drive_migration_verification_state v
    where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=1 and v.status in('pending','running')
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
      and i.slurper_status='completed'
    `, [migration.id])
    await db.query("commit")
  } catch (error) {
    await db.query("rollback").catch(() => undefined)
    throw error
  }
  const counts = await db.query(`select
    count(*)::int total,
    count(*) filter(where slurper_status in('completed','verifying','verification_failed','no_files'))::int completed,
    count(*) filter(where slurper_status in('failed','aborted','precheck_failed','bucket_create_failed'))::int failed,
    count(*) filter(where slurper_status='verifying')::int verifying
    from drive_migration_items where migration_id=$1`, [migration.id])
  const state = counts.rows[0] || {}
  const scannerPending = await db.query(`select 1 from drive_migration_verification_state where migration_id=$1 and generation=1 and status in('pending','running') limit 1`, [migration.id])
  const allJobsCompleted = Number(state.total) > 0 && Number(state.completed) === Number(state.total)
  const allJobsTerminal = Number(state.total) > 0 && Number(state.completed) + Number(state.failed) === Number(state.total)
  const nextStatus = scannerPending.rowCount ? "verifying" : allJobsCompleted ? "verifying" : allJobsTerminal && Number(state.failed) ? "failed" : "running"
  await db.query(`update drive_migrations set status=$2,
    sync_status=case when $2='failed' then 'failed' else 'running' end,
    sync_message=case when $2='failed' then 'One or more Super Slurper bucket jobs failed' when $2='verifying' then 'Verifying migrated objects' else 'Refreshing Super Slurper migration progress' end,
    last_synced_at=now(),updated_at=now()
    where id=$1 and status in('running','verifying')`, [migration.id, nextStatus])
  return { refreshed, scannerTasksQueued: verification.rowCount || 0, ...state }
}
async function ensureWorkerTargetBuckets(db: Client, migration: Row, generation: number) {
  const accounts = await db.query(`select id,cloudflare_account_id,api_token from drive_accounts where id=$1 limit 1`, [migration.target_account_id])
  const target = accounts.rows[0]
  if (!target?.cloudflare_account_id || !target.api_token) throw new Error("Destination Cloudflare account or API token is missing")
  const items = await db.query(`select id,target_bucket,source_jurisdiction,source_storage_class,slurper_status,progress
    from drive_migration_items where migration_id=$1 order by created_at`, [migration.id])
  const pending = items.rows.filter((item) =>
    Number(item.progress?.workerTargetBucketGeneration) !== generation || item.slurper_status === "worker_bucket_create_failed"
  )
  if (!pending.length) return { checked: 0, failed: 0 }

  const listBuckets = async () => {
    const payload = await cloudflare(target, "/r2/buckets")
    const buckets = Array.isArray(payload) ? payload : Array.isArray(payload?.buckets) ? payload.buckets : []
    return new Set(buckets.map((bucket: Row) => String(bucket.name || "")))
  }
  const bucketNames = await listBuckets()
  let failed = 0
  for (const item of pending) {
    if (!(await migrationIsActive(db, migration.id))) break
    let errorMessage: string | null = null
    if (!bucketNames.has(item.target_bucket)) {
      try {
        await cloudflare(target, "/r2/buckets", "POST", {
          name: item.target_bucket,
          ...( ["default", "eu", "fedramp"].includes(String(item.source_jurisdiction)) ? { jurisdiction: item.source_jurisdiction } : {}),
          ...(item.source_storage_class ? { storageClass: item.source_storage_class } : {}),
        })
        bucketNames.add(item.target_bucket)
      } catch (error) {
        // Re-list after a create conflict so concurrent orchestrator cycles converge.
        try {
          const refreshed = await listBuckets()
          if (refreshed.has(item.target_bucket)) bucketNames.add(item.target_bucket)
          else errorMessage = error instanceof Error ? error.message : String(error)
        } catch (refreshError) {
          errorMessage = refreshError instanceof Error ? refreshError.message : String(refreshError)
        }
      }
    }

    if (errorMessage) {
      failed += 1
      await db.query(`update drive_migration_items set slurper_status='worker_bucket_create_failed',
        progress=(coalesce(progress,'{}'::jsonb)||jsonb_build_object('stage','worker_bucket_create_failed','error',$3::text,'lastError',$3::text)),
        last_progress_at=now(),updated_at=now()
        where id=$1 and migration_id=$2 and exists(select 1 from drive_migrations where id=$2 and status in('running','verifying'))`,
      [item.id, migration.id, errorMessage])
      continue
    }

    await db.query(`update drive_migration_items set
      slurper_status=case when slurper_status='worker_bucket_create_failed' then 'scanning' else slurper_status end,
      progress=(coalesce(progress,'{}'::jsonb)-'error'-'lastError')||jsonb_build_object('workerTargetBucketGeneration',$3::int),
      last_progress_at=now(),updated_at=now()
      where id=$1 and migration_id=$2 and exists(select 1 from drive_migrations where id=$2 and status in('running','verifying'))`,
    [item.id, migration.id, generation])
  }
  return { checked: pending.length, failed }
}

async function ensureShards(db: Client, migration: Row) {
  const generation = integer(opts(migration).workerGeneration, 1, 1, 1000000)
  const hasItems = await db.query(`select 1 from drive_migration_items where migration_id=$1 limit 1`, [migration.id])
  if (!hasItems.rowCount) {
    return { generation, shardCount: 0, created: 0, inventoryPending: 0, queuePending: 0, terminalFailure: false, noItems: true, targetBuckets: { checked: 0, failed: 0 } }
  }
  const targetBuckets = await ensureWorkerTargetBuckets(db, migration, generation)
  if (targetBuckets.failed > 0) {
    return { generation, shardCount: 0, created: 0, inventoryPending: 0, queuePending: 1, terminalFailure: false, targetBuckets }
  }
  const items = await db.query(`select id,source_bucket,target_bucket,source_objects,progress from drive_migration_items where migration_id=$1 and coalesce(slurper_status,'')<>'worker_bucket_create_failed' order by created_at`, [migration.id])
  if (!items.rowCount) {
    // Every configured bucket failed target preparation. Keep this distinct
    // from a genuinely empty migration: callers finalize empty migrations via
    // noItems before attempting destination bucket preparation.
    await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message='No migration buckets are available for worker processing',last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id])
    return { generation, shardCount: 0, created: 0, inventoryPending: 0, terminalFailure: true, targetBuckets }
  }
  let inventoryPending = 0
  const scansByItem = new Map<string, Row>()
  for (const item of items.rows) {
    const inventory = item.progress?.migrationInventory || {}
    const inventoryGeneration = Number(inventory.generation)
    const adoptLegacyGenerationOne = generation === 1 && inventory.generation == null
    let scanId = inventoryGeneration === generation || adoptLegacyGenerationOne ? String(inventory.sourceScanId || "") : ""
    if (!scanId) {
      // Reuse an in-flight scanner task before creating one. Cron and queued
      // wake cycles can overlap across isolates, so this keeps both paths on
      // one inventory. Completed scans are not reused across repair generations.
      const existing = await db.query(`select id from drive_bucket_scans where account_id=$1 and bucket_name=$2 and migration_id=$3 and migration_item_id=$4 and kind='source' and status in('pending','running') order by updated_at desc limit 1`, [migration.source_account_id, item.source_bucket, migration.id, item.id])
      if (existing.rows[0]?.id) scanId = existing.rows[0].id
      else {
        const scan = await db.query(`insert into drive_bucket_scans(id,account_id,bucket_name,kind,migration_id,migration_item_id,status,updated_at) values(gen_random_uuid(),$1,$2,'source',$3,$4,'pending',now()) returning id`, [migration.source_account_id, item.source_bucket, migration.id, item.id])
        scanId = scan.rows[0].id
      }
      await db.query(`update drive_migration_items set progress=jsonb_set(coalesce(progress,'{}'::jsonb),'{migrationInventory}',$2::jsonb),slurper_status='scanning',updated_at=now() where id=$1`, [item.id, JSON.stringify({ sourceScanId: scanId, generation, status: "scanning" })])
      item.progress = { ...(item.progress || {}), migrationInventory: { sourceScanId: scanId, generation, status: "scanning" } }
    }
    const scan = (await db.query(`select status,error,objects,bytes,completed_at,attempt_count from drive_bucket_scans where id=$1`, [scanId])).rows[0]
    if (scan?.status === "failed") {
      const message = String(scan.error || `Source inventory failed for ${item.source_bucket}`)
      if (isTransientWorkerError(message)) {
        await db.query(`update drive_bucket_scans set status='pending',lease_owner=null,lease_expires_at=null,updated_at=now() where id=$1 and status='failed'`, [scanId])
        const progress = { ...(item.progress || {}), migrationInventory: { ...(item.progress?.migrationInventory || {}), status: "retrying", lastError: message, retryAt: new Date().toISOString() }, events: appendMigrationEvent(item.progress || {}, "source_scan", "retrying", `Transient File Scanner error; resuming the saved cursor after backoff: ${message}`) }
        await db.query(`update drive_migration_items set slurper_status='scanning',progress=$2::jsonb,last_progress_at=now(),updated_at=now() where id=$1 and exists(select 1 from drive_migrations where id=migration_id and status in('running','verifying'))`, [item.id, JSON.stringify(progress)])
        inventoryPending += 1
        scansByItem.set(item.id, { ...scan, status: "pending" })
        continue
      }
      const progress = { ...(item.progress || {}), migrationInventory: { ...(item.progress?.migrationInventory || {}), status: "failed", error: message, failedAt: new Date().toISOString() }, events: appendMigrationEvent(item.progress || {}, "source_scan", "failed", message) }
      await db.query(`update drive_migration_items set slurper_status='precheck_failed',progress=$2::jsonb,last_progress_at=now(),updated_at=now() where id=$1`, [item.id, JSON.stringify(progress)])
      await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, `File Scanner source inventory failed for ${item.source_bucket}: ${message}`])
      return { generation, shardCount: 0, created: 0, inventoryPending: 0, queuePending: 0, terminalFailure: true, targetBuckets }
    }
    if (scan?.status !== "completed") inventoryPending += 1
    scansByItem.set(item.id, scan || {})
  }
  // Mirror scanner counters on every cycle, not only at completion, so live
  // object/byte totals advance while inventory and queueing run concurrently.
  await db.query(`
    update drive_migration_items i set source_objects=s.objects,source_bytes=s.bytes,last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{migrationInventory}',coalesce(i.progress->'migrationInventory','{}'::jsonb)||jsonb_build_object('status',s.status,'objects',s.objects,'bytes',s.bytes,'completedAt',s.completed_at))
    from drive_bucket_scans s where i.migration_id=$1 and s.id=(i.progress->'migrationInventory'->>'sourceScanId')::uuid
  `, [migration.id])
  let created = 0
  let queueItem: Row | undefined
  let queueScan: Row | undefined
  // Wait for a stable, complete inventory before walking it by key. Scanner
  // writes can arrive out of key order, so advancing this cursor during a
  // running scan can permanently skip an object inserted behind lastKey.
  for (const candidate of items.rows) {
    const queueState = candidate.progress?.migrationQueue || {}
    const scanId = String(candidate.progress?.migrationInventory?.sourceScanId || "")
    if (!scanId) continue
    const scan = scansByItem.get(candidate.id) || {}
    if (scan.status !== "completed") continue
    if (Number(queueState.generation) === generation && queueState.status === "completed") {
      const coverage = await db.query(`select count(*)::int missing_count
        from drive_bucket_scan_objects o where o.scan_id=$4 and not o.is_dir_marker
          and not exists(select 1 from drive_repair_jobs j
            where j.migration_id=$1 and j.mode='migration'
              and j.work_key=format('migration:%s:generation:%s:inventory:%s:%s',$1::uuid,$2::int,$3::uuid,encode(convert_to(o.key,'UTF8'),'hex'))
              and j.payload->'itemIds' @> jsonb_build_array($3::text)
              and exists(select 1 from jsonb_array_elements(coalesce(j.payload->'inventoryObjects','[]'::jsonb)) inv where inv->>'key'=o.key))`,
      [migration.id, generation, candidate.id, scanId])
      if (Number(coverage.rows[0]?.missing_count || 0) === 0) continue
      const attempts = Number(queueState.integrityRepairAttempts || 0)
      candidate.progress = { ...(candidate.progress || {}), migrationQueue: {
        ...queueState, generation, status: "materializing", lastKey: "", materializedObjects: 0,
        totalObjects: Number(scan.objects || candidate.source_objects || 0),
        integrityRepairAttempts: attempts + 1, integrityRepairMissing: Number(coverage.rows[0]?.missing_count || 0),
        updatedAt: new Date().toISOString(),
      } }
      await db.query(`update drive_migration_items set progress=jsonb_set(coalesce(progress,'{}'::jsonb),'{migrationQueue}',$2::jsonb),updated_at=now() where id=$1`,
        [candidate.id, JSON.stringify(candidate.progress.migrationQueue)])
    }
    queueItem = candidate
    queueScan = scan
    queueScan.id = scanId
    break
  }
  if (queueItem) {
    const lastKey = Number(queueItem.progress?.migrationQueue?.generation) === generation ? String(queueItem.progress?.migrationQueue?.lastKey || "") : ""
    await db.query("begin")
    try {
      const queueStartedAt = Date.now()
      const materialized = await db.query(`
        with page as materialized (
          select key,size,etag from drive_bucket_scan_objects
          where scan_id=$1 and not is_dir_marker and key>$2
          order by key limit ${adaptiveQueueBatchSize}
        ), inserted as (
        insert into drive_repair_jobs(id,migration_id,status,mode,work_key,payload,progress,result,created_at,updated_at)
        select gen_random_uuid(),$3::uuid,'pending','migration',
          format('migration:%s:generation:%s:inventory:%s:%s',$3::uuid,$4::int,$5::uuid,encode(convert_to(object_row.key,'UTF8'),'hex')),
          jsonb_build_object('source','file_scanner_inventory','kind','migration_inventory_file','workerGeneration',$4::int,'itemIds',jsonb_build_array($5::uuid),'inventoryObjects',jsonb_build_array(jsonb_build_object('key',object_row.key,'size',object_row.size,'etag',object_row.etag))),
          '{}'::jsonb,'{}'::jsonb,now(),now()
        from page object_row
        on conflict(work_key) where work_key is not null do nothing
        returning 1
        )
        select (select count(*)::int from page) page_count,
          (select key from page order by key desc limit 1) next_key,
          (select count(*)::int from inserted) created
      `, [queueScan?.id, lastKey, migration.id, generation, queueItem.id])
      const pageCount = Number(materialized.rows[0]?.page_count || 0)
      created += Number(materialized.rows[0]?.created || 0)
      tuneQueueBatchSize(Date.now() - queueStartedAt, true)
      let missingCount = 0
      if (queueScan?.status === "completed" && pageCount === 0) {
        const coverage = await db.query(`select count(*)::int missing_count
          from drive_bucket_scan_objects o where o.scan_id=$4 and not o.is_dir_marker
            and not exists(select 1 from drive_repair_jobs j
              where j.migration_id=$1 and j.mode='migration'
                and j.work_key=format('migration:%s:generation:%s:inventory:%s:%s',$1::uuid,$2::int,$3::uuid,encode(convert_to(o.key,'UTF8'),'hex'))
                and j.payload->'itemIds' @> jsonb_build_array($3::text)
                and exists(select 1 from jsonb_array_elements(coalesce(j.payload->'inventoryObjects','[]'::jsonb)) inv where inv->>'key'=o.key))`,
        [migration.id, generation, queueItem.id, queueScan.id])
        missingCount = Number(coverage.rows[0]?.missing_count || 0)
      }
      const priorQueue = queueItem.progress?.migrationQueue || {}
      const integrityAttempts = Number(priorQueue.integrityRepairAttempts || 0)
      if (missingCount > 0 && integrityAttempts > 0) {
        const message = `Migration queue integrity check still found ${missingCount} scanned object(s) without a durable file job for ${queueItem.source_bucket}`
        const failedProgress = { ...priorQueue, status: "failed", integrityError: message, integrityRepairMissing: missingCount, updatedAt: new Date().toISOString() }
        await db.query(`update drive_migration_items set slurper_status='precheck_failed',progress=jsonb_set(coalesce(progress,'{}'::jsonb),'{migrationQueue}',$2::jsonb),last_progress_at=now(),updated_at=now() where id=$1`, [queueItem.id, JSON.stringify(failedProgress)])
        await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, message])
        await db.query("commit")
        return { generation, shardCount: 0, created, inventoryPending: 0, queuePending: 0, terminalFailure: true, targetBuckets }
      }
      const retryCoverage = missingCount > 0
      const completed = queueScan?.status === "completed" && pageCount === 0 && missingCount === 0
      const nextKey = retryCoverage ? "" : String(materialized.rows[0]?.next_key || lastKey)
      const priorMaterialized = Number(queueItem.progress?.migrationQueue?.generation) === generation
        ? Number(queueItem.progress?.migrationQueue?.materializedObjects || 0)
        : 0
      await db.query(`update drive_migration_items set progress=jsonb_set(coalesce(progress,'{}'::jsonb),'{migrationQueue}',$2::jsonb),updated_at=now() where id=$1`, [queueItem.id, JSON.stringify({ generation, status: completed ? "completed" : "materializing", lastKey: nextKey, materializedObjects: retryCoverage ? 0 : priorMaterialized + pageCount, totalObjects: Number(queueItem.source_objects || queueScan?.objects || 0), integrityRepairAttempts: integrityAttempts + (retryCoverage ? 1 : 0), integrityRepairMissing: retryCoverage ? missingCount : 0, updatedAt: new Date().toISOString() })])
      await db.query("commit")
    } catch (error) {
      tuneQueueBatchSize(Number.MAX_SAFE_INTEGER, false)
      await db.query("rollback").catch(() => undefined)
      throw error
    }
    return { generation, shardCount: 0, created, inventoryPending, queuePending: 1 }
  }
  if (inventoryPending) return { generation, shardCount: 0, created: 0, inventoryPending, queuePending: 1 }
  // Normalize pending jobs created by an older orchestrator build. Migration
  // mode has the same copy-and-verify guarantees, but keeps full migrations
  // distinct from ad-hoc repair jobs throughout the API and UI.
  await db.query(`update drive_repair_jobs set mode='migration',updated_at=now() where migration_id=$1 and status='pending' and work_key like $2 and mode<>'migration'`, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`])
  const total = await db.query(`select count(*)::int count from drive_repair_jobs where migration_id=$1 and work_key like $2`, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`])
  const shardCount = Number(total.rows[0]?.count || 0)
  if (!shardCount) {
    const inventory = await db.query(`select coalesce(sum(source_objects),0)::bigint objects from drive_migration_items where migration_id=$1`, [migration.id])
    const objects = Number(inventory.rows[0]?.objects || 0)
    if (objects > 0) {
      const message = `File Scanner found ${objects} source object(s), but this worker-pool generation created no durable migration jobs. Retry to rebuild the queue.`
      await db.query(`update drive_migration_items set slurper_status='precheck_failed',progress=coalesce(progress,'{}'::jsonb)||jsonb_build_object('stage','queue_materialization_failed','lastError',$2::text),last_progress_at=now(),updated_at=now() where migration_id=$1 and coalesce(slurper_status,'')<>'worker_bucket_create_failed'`, [migration.id, message])
      await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, message])
      return { generation, shardCount, created, inventoryPending: 0, queuePending: 0, terminalFailure: true, targetBuckets }
    }
    await db.query(`update drive_migration_items set slurper_status='completed',source_objects=0,source_bytes=0,updated_at=now() where migration_id=$1`, [migration.id])
  }
  return { generation, shardCount, created, inventoryPending: 0, queuePending: 0, targetBuckets }
}

async function refreshMigrationSnapshot(db: Client, migrationId: string) {
  const generation = Math.max(1, Number((await db.query(`select coalesce(nullif(options->>'workerGeneration','')::int,1) generation from drive_migrations where id=$1`, [migrationId])).rows[0]?.generation || 1))
  const [workerResult, aggregateResult, itemsResult] = await Promise.all([
    db.query(`select
      count(*) filter(where r.status='running' and a.status='online' and a.last_heartbeat_at>now()-interval '90 seconds')::bigint online_workers,
      count(*) filter(where r.status='running' and a.status='online' and a.last_heartbeat_at>now()-interval '90 seconds' and j.progress ? 'currentFile')::bigint active_transfers
      from drive_agent_runs r
      left join drive_agents a on a.id=r.agent_id
      left join drive_repair_jobs j on j.id::text=r.job_reference
      where r.run_type='github_dispatch' and r.payload->>'migrationId'=$1
        and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=$2`, [migrationId, generation]),
    db.query(`select count(*)::bigint total_jobs,count(*) filter(where status='pending')::bigint queued_jobs,count(*) filter(where status in('claimed','running'))::bigint running_jobs,count(*) filter(where status in('pending','claimed','running') or (status='failed' and case when result->>'retryCount' ~ '^[0-9]+$' then (result->>'retryCount')::int else 0 end<3))::bigint remaining_jobs,count(*) filter(where status='completed')::bigint completed_jobs,count(*) filter(where status='failed')::bigint failed_jobs,count(*) filter(where status='canceled')::bigint canceled_jobs,coalesce(sum(case when (result->'items'->0->>'alreadyPresent') ~ '^[0-9]+$' then (result->'items'->0->>'alreadyPresent')::bigint else greatest(case when (result->'items'->0->>'alreadyPresent') ~ '^[0-9]+$' then (result->'items'->0->>'alreadyPresent')::bigint else 0 end,case when (progress->>'alreadyPresent') ~ '^[0-9]+$' then (progress->>'alreadyPresent')::bigint else 0 end) end),0)::bigint already_present_objects,coalesce(sum(case when status='completed' then case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end else greatest(case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end,case when (progress->>'transferred') ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end) end),0)::bigint transferred_objects,coalesce(sum(case when status='completed' and (result->'items'->0->>'transferred') ~ '^[0-9]+$' then ((result->'items'->0->>'transferred')::bigint>0)::int else (greatest(case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end,case when (progress->>'transferred') ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end)>0)::int end),0)::bigint copied_objects,coalesce(sum(case when status='completed' and (result->'items'->0->>'skipped') ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else greatest(case when (result->'items'->0->>'skipped') ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else 0 end,case when (progress->>'skipped') ~ '^[0-9]+$' then (progress->>'skipped')::bigint else 0 end) end),0)::bigint skipped_objects,coalesce(sum(case when status='completed' and (result->'items'->0->>'failed') ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint else greatest(case when (result->'items'->0->>'failed') ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint else 0 end,case when (progress->>'failed') ~ '^[0-9]+$' then (progress->>'failed')::bigint else 0 end) end),0)::bigint failed_objects,coalesce(sum(case when status='completed' and (result->'items'->0->>'transferred') ~ '^[0-9]+$' and (result->'items'->0->>'transferred')::bigint>0 then coalesce(nullif(payload->'inventoryObjects'->0->>'size','')::bigint,0) else 0 end),0)::bigint completed_bytes from drive_repair_jobs where migration_id=$1 and mode='migration' and work_key like $2`, [migrationId, `migration:${migrationId}:generation:${generation}:inventory:%`]),
    db.query(`select id,source_bucket,target_bucket,source_objects,source_bytes,slurper_status,progress,updated_at from drive_migration_items where migration_id=$1 order by created_at`, [migrationId]),
  ])
  const workers = workerResult.rows[0] || {}
  const aggregate = aggregateResult.rows[0] || {}
  const buckets = itemsResult.rows.map((item) => { const live = item.progress?.live || {}; return { id: item.id, sourceBucket: item.source_bucket, targetBucket: item.target_bucket, status: live.status || item.slurper_status || "pending", totalObjects: Number(live.totalObjects ?? item.source_objects ?? 0), queuedObjects: Number(live.queuedObjects ?? 0), transferredObjects: Number(live.transferredObjects ?? 0), alreadyPresentObjects: Number(live.alreadyPresentObjects ?? 0), copiedObjects: Number(live.copiedObjects ?? 0), skippedObjects: Number(live.skippedObjects ?? 0), failedObjects: Number(live.failedObjects ?? 0), transferredBytes: Number(live.transferredBytes ?? 0), sourceBytes: Number(item.source_bytes ?? 0), updatedAt: item.updated_at } })
  const totalObjects = buckets.reduce((sum, bucket) => sum + bucket.totalObjects, 0)
  const transferredObjects = buckets.reduce((sum, bucket) => sum + bucket.transferredObjects, 0)
  const alreadyPresentObjects = buckets.reduce((sum, bucket) => sum + bucket.alreadyPresentObjects, 0)
  const copiedObjects = buckets.reduce((sum, bucket) => sum + bucket.copiedObjects, 0)
  const failedObjects = buckets.reduce((sum, bucket) => sum + bucket.failedObjects, 0)
  const skippedObjects = buckets.reduce((sum, bucket) => sum + bucket.skippedObjects, 0)
  const totals = { onlineWorkers: Number(workers.online_workers || 0), activeTransfers: Number(workers.active_transfers || 0), totalJobs: Number(aggregate.total_jobs || 0), queuedJobs: Number(aggregate.queued_jobs || 0), runningJobs: Number(aggregate.running_jobs || 0), remainingJobs: Number(aggregate.remaining_jobs || 0), completedJobs: Number(aggregate.completed_jobs || 0), failedJobs: Number(aggregate.failed_jobs || 0), canceledJobs: Number(aggregate.canceled_jobs || 0), totalObjects, transferred: transferredObjects, alreadyPresentObjects, copiedObjects, failed: failedObjects, skipped: skippedObjects, missing: 0, mismatched: 0, processedFiles: Number(aggregate.completed_jobs || 0) + Number(aggregate.failed_jobs || 0) + Number(aggregate.canceled_jobs || 0), totalFiles: Number(aggregate.total_jobs || 0), completedBytes: Number(aggregate.completed_bytes || 0) }
  const snapshot = { migrationId, workerGeneration: generation, ...totals, buckets, updatedAt: new Date().toISOString() }
  await db.query(`insert into drive_migration_worker_live_state(migration_id,snapshot,updated_at) values($1,$2::jsonb,now()) on conflict(migration_id) do update set snapshot=excluded.snapshot,updated_at=now()`, [migrationId, JSON.stringify(snapshot)])
  return { snapshot: { ...snapshot, workerGeneration: generation }, workerGeneration: generation }
}
async function migrationLiveState(db: Client, migrationId: string) {
  const live = await refreshMigrationSnapshot(db, migrationId)
  const [jobsResult, runsResult] = await Promise.all([
    db.query(`select id,status,claimed_by_agent_id,
      (coalesce(progress,'{}'::jsonb)-'logs'-'fileEvents')||jsonb_build_object(
        'logs',coalesce((select jsonb_agg(entry.value order by entry.ordinality) from (select value,ordinality from jsonb_array_elements(case when jsonb_typeof(progress->'logs')='array' then progress->'logs' else '[]'::jsonb end) with ordinality order by ordinality desc limit 3) entry),'[]'::jsonb),
        'fileEvents',coalesce((select jsonb_agg(event.value order by event.ordinality) from (select value,ordinality from jsonb_array_elements(case when jsonb_typeof(progress->'fileEvents')='array' then progress->'fileEvents' else '[]'::jsonb end) with ordinality order by ordinality desc limit 3) event),'[]'::jsonb)
      ) progress,
      (coalesce(result,'{}'::jsonb)-'logs'-'fileEvents')||jsonb_build_object(
        'fileEvents',coalesce((select jsonb_agg(event.value order by event.ordinality) from (select value,ordinality from jsonb_array_elements(case when jsonb_typeof(result->'fileEvents')='array' then result->'fileEvents' else '[]'::jsonb end) with ordinality order by ordinality desc limit 3) event),'[]'::jsonb)
      ) result,
      summary,error,created_at,updated_at,last_heartbeat_at from drive_repair_jobs where migration_id=$1 and mode='migration' and work_key like $2 order by updated_at desc limit 20`, [migrationId, `migration:${migrationId}:generation:${live.workerGeneration}:inventory:%`]),
    db.query(`select r.id,r.status,r.job_reference,r.payload,r.created_at,r.updated_at,a.status agent_status,a.last_heartbeat_at agent_heartbeat from drive_agent_runs r left join drive_agents a on a.id=r.agent_id where r.run_type='github_dispatch' and r.payload->>'migrationId'=$1 and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=$2 order by r.created_at limit 100`, [migrationId, live.workerGeneration]),
  ])
  const jobs = jobsResult.rows
  const runs = runsResult.rows
  const { snapshot, workerGeneration } = live
  return { snapshot, jobs, runs, workerGeneration }
}
async function sha256Hex(value: string) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value))
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("")
}
async function githubWorkerSecretsAreCurrent(orchestration: Row, workerSettings: Row) {
  if (String(workerSettings.secretSyncStatus || "") !== "ready") return false
  if (String(workerSettings.synchronizedServerUrl || "").replace(/\/+$/, "") !== String(orchestration.orchestratorUrl || "").replace(/\/+$/, "")) return false
  const sharedSecret = String(workerSettings.sharedSecret || "")
  if (sharedSecret.length < 24 || sharedSecret.length > MAX_SECRET_LENGTH) return false
  return safeEqual(String(workerSettings.synchronizedSecretHash || ""), await sha256Hex(sharedSecret))
}
async function recoverJobs(db: Client, migrationId: string, generation: number, shardCount: number) {
  const result = await db.query(`
    update drive_repair_jobs set status='pending',claimed_by_agent_id=null,claim_token=null,claimed_at=null,started_at=null,last_heartbeat_at=null,error=null,
      summary='Recovered by Migration Orchestrator',result=jsonb_set(coalesce(result,'{}'::jsonb),'{retryCount}',to_jsonb(coalesce((result->>'retryCount')::int,0)+1)),updated_at=now()
    where migration_id=$1 and work_key like $2 and work_key like $3 and ((status='failed' and coalesce((result->>'retryCount')::int,0)<3) or (status in('claimed','running') and coalesce(last_heartbeat_at,started_at,claimed_at,updated_at)<now()-interval '3 minutes'))
  `, [migrationId, `migration:${migrationId}:generation:${generation}:inventory:%`, `%`])
  return result.rowCount || 0
}
async function refreshWorkerItemProgress(db: Client, migration: Row, generation: number, affectedItemIds?: string[]) {
  await db.query(`
    with relevant_jobs as materialized (
      select id,status,(payload->'itemIds'->>0)::uuid item_id,
        case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end result_transferred,
        case when (progress->>'transferred') ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end progress_transferred,
        case when (result->'items'->0->>'alreadyPresent') ~ '^[0-9]+$' then (result->'items'->0->>'alreadyPresent')::bigint else 0 end result_already_present,
        (result->'items'->0->>'alreadyPresent') ~ '^[0-9]+$' result_already_present_valid,
        case when (progress->>'alreadyPresent') ~ '^[0-9]+$' then (progress->>'alreadyPresent')::bigint else 0 end progress_already_present,
        case when (result->'items'->0->>'skipped') ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else 0 end result_skipped,
        (result->'items'->0->>'skipped') ~ '^[0-9]+$' result_skipped_valid,
        case when (progress->>'skipped') ~ '^[0-9]+$' then (progress->>'skipped')::bigint else 0 end progress_skipped,
        case when (result->'items'->0->>'failed') ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint else 0 end result_failed,
        (result->'items'->0->>'failed') ~ '^[0-9]+$' result_failed_valid,
        case when (progress->>'failed') ~ '^[0-9]+$' then (progress->>'failed')::bigint else 0 end progress_failed,
        case when (payload->'inventoryObjects'->0->>'size') ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end inventory_size
      from drive_repair_jobs
      where migration_id=$1 and work_key like $2
        and ($5::uuid[] is null or (payload->'itemIds'->>0)::uuid=any($5::uuid[]))
    ), aggregate as (
      select i0.id item_id,
        count(j.id) filter(where j.status='pending')::bigint queued_objects,
        count(j.id) filter(where j.status='completed')::bigint completed_objects,
        count(j.id) filter(where j.status in('claimed','running'))::bigint active_objects,
        coalesce(sum(case when j.status='completed' then j.result_transferred else greatest(j.result_transferred,j.progress_transferred) end),0)::bigint transferred_objects,
        coalesce(sum(case when j.status='completed' and j.result_already_present_valid then j.result_already_present else greatest(j.result_already_present,j.progress_already_present) end),0)::bigint already_present_objects,
        coalesce(sum(case when j.status='completed' and j.result_skipped_valid then j.result_skipped else greatest(j.result_skipped,j.progress_skipped) end),0)::bigint skipped_objects,
        coalesce(sum(case when j.status='completed' and j.result_failed_valid then j.result_failed else greatest(j.result_failed,j.progress_failed) end),0)::bigint failed_objects,
        coalesce(sum(case when j.status='completed' and j.result_transferred>0 then 1 when j.status in('claimed','running') and greatest(j.result_transferred,j.progress_transferred)>0 then 1 else 0 end),0)::bigint copied_objects,
        coalesce(sum(j.inventory_size) filter(where j.status='completed' and j.result_transferred>0),0)::bigint completed_bytes
      from drive_migration_items i0
      left join relevant_jobs j on j.item_id=i0.id
      where i0.migration_id=$1 and ($5::uuid[] is null or i0.id=any($5::uuid[]))
      group by i0.id
    )
    update drive_migration_items i set
      slurper_status=case
        when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status='completed' and v.missing_objects=0 and v.mismatched_objects=0 and (v.extra_objects=0 or $4=false)) then 'completed'
        when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('failed','completed') and (v.status='failed' or v.missing_objects>0 or v.mismatched_objects>0 or (v.extra_objects>0 and $4=true))) then 'verification_failed'
        when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('pending','running')) then 'verifying'
        -- Inventory and job materialization are one scanner-owned stage.  Do
        -- not advertise a bucket as queued while the scanner is still
        -- producing pages/jobs; doing so makes the UI look idle and allows a
        -- partially materialized queue to be mistaken for a complete list.
        when coalesce(i.progress->'migrationInventory'->>'status','') <> 'completed'
          or coalesce(i.progress->'migrationQueue'->>'status','') <> 'completed' then 'scanning'
        when coalesce(a.active_objects,0)>0 or coalesce(a.completed_objects,0)>0 then 'running'
        when coalesce(a.queued_objects,0)>0 then 'queued'
        else i.slurper_status
      end,
      last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(coalesce(i.progress,'{}'::jsonb)-'repairWorker'-'repairWorkerStatus','{live}',jsonb_build_object(
        'updatedAt',now(),
        'status',case
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status='completed' and v.missing_objects=0 and v.mismatched_objects=0 and (v.extra_objects=0 or $4=false)) then 'completed'
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('failed','completed') and (v.status='failed' or v.missing_objects>0 or v.mismatched_objects>0 or (v.extra_objects>0 and $4=true))) then 'verification_failed'
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('pending','running')) then 'verifying'
          when coalesce(i.progress->'migrationInventory'->>'status','') <> 'completed'
            or coalesce(i.progress->'migrationQueue'->>'status','') <> 'completed' then 'scanning'
          when coalesce(a.active_objects,0)>0 or coalesce(a.completed_objects,0)>0 then 'running'
          when coalesce(a.queued_objects,0)>0 then 'queued'
          else 'scanning'
        end,
        'transferredObjects',least(coalesce(i.source_objects,0),greatest(0,
          (case when i.progress->'slurperCumulative'->>'transferredObjects' ~ '^[0-9]+$'
            then (i.progress->'slurperCumulative'->>'transferredObjects')::bigint else 0 end)
          + coalesce(a.transferred_objects,0)-least(
            (case when i.progress->'slurperCumulative'->>'transferredObjects' ~ '^[0-9]+$'
              then (i.progress->'slurperCumulative'->>'transferredObjects')::bigint else 0 end)+coalesce(a.transferred_objects,0),
            greatest(case when i.progress->'slurperCumulative'->>'skippedObjects' ~ '^[0-9]+$'
              then (i.progress->'slurperCumulative'->>'skippedObjects')::bigint else 0 end,coalesce(a.skipped_objects,0))))),
        'transferredBytes',coalesce(a.completed_bytes,0),
        'alreadyPresentObjects',coalesce(a.already_present_objects,0),
        'copiedObjects',greatest(0,coalesce(a.transferred_objects,0)-least(coalesce(a.transferred_objects,0),coalesce(a.skipped_objects,0))),
        'skippedObjects',greatest(
          case when i.progress->'slurperCumulative'->>'skippedObjects' ~ '^[0-9]+$'
            then (i.progress->'slurperCumulative'->>'skippedObjects')::bigint else 0 end,
          coalesce(a.skipped_objects,0)),
        'failedObjects',coalesce(a.failed_objects,0),
        'unaccountedObjects',greatest(coalesce(i.source_objects,0)-least(coalesce(i.source_objects,0),
          (case when i.progress->'slurperCumulative'->>'transferredObjects' ~ '^[0-9]+$'
            then (i.progress->'slurperCumulative'->>'transferredObjects')::bigint else 0 end)
          + coalesce(a.transferred_objects,0))-greatest(
          case when i.progress->'slurperCumulative'->>'skippedObjects' ~ '^[0-9]+$'
            then (i.progress->'slurperCumulative'->>'skippedObjects')::bigint else 0 end,
          coalesce(a.skipped_objects,0))-coalesce(a.failed_objects,0),0),
        'verifyIssues',0,
        'totalObjects',coalesce(i.source_objects,0),
        'workerStatus',case
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status='completed' and v.missing_objects=0 and v.mismatched_objects=0 and (v.extra_objects=0 or $4=false)) then 'completed'
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('failed','completed') and (v.status='failed' or v.missing_objects>0 or v.mismatched_objects>0 or (v.extra_objects>0 and $4=true))) then 'verification_failed'
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status in('pending','running')) then 'verifying'
          when coalesce(i.progress->'migrationInventory'->>'status','') <> 'completed'
            or coalesce(i.progress->'migrationQueue'->>'status','') <> 'completed' then 'scanning'
          when coalesce(a.active_objects,0)>0 then 'running' else 'queued' end,
        'workerStage',case
          when exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$3 and v.status<>'blocked') then 'verification'
          when coalesce(i.progress->'migrationInventory'->>'status','') <> 'completed'
            or coalesce(i.progress->'migrationQueue'->>'status','') <> 'completed' then 'scanning'
          else 'migration' end,
        'queuedObjects',coalesce(a.queued_objects,0)
      ))
    from aggregate a where i.id=a.item_id and i.migration_id=$1
      and exists(select 1 from drive_migrations active where active.id=i.migration_id and active.status in('running','verifying'))
  `, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`, generation, opts(migration).verifyStrictDestination === true, affectedItemIds?.length ? affectedItemIds : null])
}
async function recordItemStageEvents(db: Client, migration: Row, generation: number) {
  await db.query(`
    with changed as (
      select i.id,i.source_bucket,i.progress,
        case
          when i.slurper_status in('failed','aborted') then i.slurper_status
          when v.status in('pending','running') then 'verifying'
          when v.status='failed' then 'verification_failed'
          when v.status='completed' and (v.missing_objects>0 or v.mismatched_objects>0 or (v.extra_objects>0 and $3::boolean)) then 'verification_failed'
          when v.status='completed' then 'completed'
          when i.slurper_status='verification_failed' then 'verification_failed'
          when i.slurper_status='completed' then 'verifying'
          else coalesce(i.progress->'live'->>'status',i.slurper_status,'unknown')
        end status,
        coalesce((i.progress->'live'->>'transferredObjects')::bigint,0) transferred,
        coalesce((i.progress->'live'->>'totalObjects')::bigint,i.source_objects,0) total,
        case when i.slurper_status in('verifying','verification_failed','completed') or i.progress->'live'->>'status' in('verifying','verification_failed','completed')
          then coalesce(v.generation,nullif(i.progress->>'verificationGeneration','')::int,$2) end verification_generation,
        case when i.slurper_status in('verifying','verification_failed','completed') or i.progress->'live'->>'status' in('verifying','verification_failed','completed')
          then nullif(i.progress->>'verificationAttemptId','') end verification_attempt_id,
        v.last_error verification_error,v.missing_objects,v.mismatched_objects,v.extra_objects
      from drive_migration_items i
      left join drive_migration_verification_state v on v.migration_item_id=i.id and v.migration_id=i.migration_id and v.generation=$2
      where i.migration_id=$1
    ), pending as (
      select *,case status
        when 'scanning' then 'File Scanner is listing source objects and creating the durable queue'
        when 'queued' then 'Source inventory is queued for migration workers'
        when 'running' then format('Migration workers are transferring files (%s/%s completed)',transferred,total)
        when 'verifying' then 'Transfer completed; File Scanner verification started'
        when 'completed' then 'File Scanner verification passed; bucket migration completed'
        when 'verification_failed' then coalesce(
          'File Scanner verification failed: '||nullif(verification_error,''),
          format('File Scanner verification found %s missing, %s mismatched, %s extra objects',
            coalesce(missing_objects,0),coalesce(mismatched_objects,0),coalesce(extra_objects,0)))
        when 'failed' then coalesce(nullif(verification_error,''),'Bucket migration failed')
        else 'Bucket state changed to '||status end message
      from changed
      where coalesce(progress->>'workerEventStatus','') is distinct from status
        or (verification_generation is not null and (
          coalesce(progress->>'workerEventGeneration','') is distinct from verification_generation::text
          or coalesce(progress->>'workerEventAttemptId','') is distinct from coalesce(verification_attempt_id,'')
        ))
    )
    update drive_migration_items i set progress=jsonb_set(
      jsonb_set(jsonb_set(jsonb_set(coalesce(i.progress,'{}'::jsonb),'{events}',
        coalesce(case when jsonb_typeof(i.progress->'events')='array' then i.progress->'events' else '[]'::jsonb end,'[]'::jsonb)
        || jsonb_build_array(jsonb_build_object('at',now(),
          'stage',case when p.status in('verifying','verification_failed','completed') then 'file_verification_'||p.status else 'worker_'||p.status end,
          'status',p.status,'message',p.message,
          'generation',p.verification_generation,'attemptId',p.verification_attempt_id)),true),
      '{workerEventStatus}',to_jsonb(p.status),true),
      '{workerEventGeneration}',coalesce(to_jsonb(p.verification_generation), 'null'::jsonb),true),
      '{workerEventAttemptId}',coalesce(to_jsonb(p.verification_attempt_id), 'null'::jsonb),true),updated_at=now()
    from pending p where i.id=p.id
  `, [migration.id, generation, opts(migration).verifyStrictDestination === true])
  await db.query(`
    with changed as (
      select id,progress,progress->'orchestratorSettings'->>'status' status,
        progress->'orchestratorSettings'->>'error' error
      from drive_migration_items
      where migration_id=$1 and progress->'orchestratorSettings'->>'status' in('synced','failed')
        and coalesce(progress->>'workerSettingsEventStatus','') is distinct from progress->'orchestratorSettings'->>'status'
    )
    update drive_migration_items i set progress=jsonb_set(
      jsonb_set(coalesce(i.progress,'{}'::jsonb),'{events}',
        coalesce(case when jsonb_typeof(i.progress->'events')='array' then i.progress->'events' else '[]'::jsonb end,'[]'::jsonb)
        || jsonb_build_array(jsonb_build_object('at',now(),'stage',case when c.status='synced' then 'settings_synced' else 'settings_sync_failed' end,
          'status',case when c.status='synced' then 'completed' else 'failed' end,
          'message',case when c.status='synced' then 'Bucket settings synchronized after successful verification' else coalesce(nullif(c.error,''),'Bucket settings synchronization failed') end)),true),
      '{workerSettingsEventStatus}',to_jsonb(c.status),true),updated_at=now()
    from changed c where i.id=c.id
  `, [migration.id])
}
async function ensureBucketVerification(db: Client, migration: Row, generation: number) {
  // Each bucket may verify after its own inventory and transfers finish.
  // Other buckets can keep copying; overall completion still waits for all.
  await db.query("begin")
  try {
    const ready = await db.query(`
      select i.id,coalesce(i.progress->'migrationInventory'->>'status','')='completed'
        and coalesce(i.progress->'migrationQueue'->>'status','')='completed'
        and not exists(select 1 from drive_repair_jobs j where j.migration_id=$1 and j.work_key like $2
          and j.payload->'itemIds'->>0=i.id::text and j.status<>'completed') ready
      from drive_migration_items i where i.migration_id=$1
    `, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`])
    const readyIds = ready.rows.filter((row) => row.ready === true).map((row) => row.id)
    const blockedIds = ready.rows.filter((row) => row.ready !== true).map((row) => row.id)
    if (blockedIds.length > 0) {
      const stale = await db.query(`select 1 from drive_migration_verification_state where migration_id=$1 and generation=$2 and migration_item_id=any($3::uuid[]) and (status not in('pending','blocked') or phase<>'source' or source_objects<>0 or destination_objects<>0 or missing_objects<>0 or mismatched_objects<>0 or extra_objects<>0) limit 1`, [migration.id, generation, blockedIds])
      if (stale.rowCount) {
        await db.query(`update drive_migration_verification_state set status='blocked',lease_owner=null,lease_expires_at=null,updated_at=now() where migration_id=$1 and generation=$2 and migration_item_id=any($3::uuid[])`, [migration.id, generation, blockedIds])
        await db.query(`delete from drive_bucket_verify_diffs d using drive_migration_verification_state v where d.migration_item_id=v.migration_item_id and v.migration_id=$1 and v.generation=$2 and v.migration_item_id=any($3::uuid[])`, [migration.id, generation, blockedIds])
        await db.query(`delete from drive_bucket_scan_objects o using drive_migration_verification_state v,drive_migration_items i where i.id=v.migration_item_id and o.scan_id in(v.source_scan_id,v.destination_scan_id) and o.scan_id is distinct from nullif(i.progress->'migrationInventory'->>'sourceScanId','')::uuid and v.migration_id=$1 and v.generation=$2 and v.migration_item_id=any($3::uuid[])`, [migration.id, generation, blockedIds])
        await db.query(`update drive_bucket_scans s set status='completed',lease_owner=null,lease_expires_at=null,completed_at=coalesce(completed_at,now()),updated_at=now() from drive_migration_verification_state v join drive_migration_items i on i.id=v.migration_item_id where s.id in(v.source_scan_id,v.destination_scan_id) and s.id is distinct from nullif(i.progress->'migrationInventory'->>'sourceScanId','')::uuid and v.migration_id=$1 and v.generation=$2 and v.migration_item_id=any($3::uuid[])`, [migration.id, generation, blockedIds])
        await db.query(`update drive_migration_verification_state set status='blocked',phase='source',source_scan_id=null,destination_scan_id=null,source_cursor=null,destination_cursor=null,source_objects=0,source_bytes=0,destination_objects=0,destination_bytes=0,missing_objects=0,mismatched_objects=0,extra_objects=0,lease_owner=null,lease_expires_at=null,completed_at=null,attempt_count=0,last_error=null,updated_at=now() where migration_id=$1 and generation=$2 and migration_item_id=any($3::uuid[])`, [migration.id, generation, blockedIds])
      }
    }
    await db.query(`update drive_migration_verification_state set status='pending',updated_at=now() where migration_id=$1 and generation=$2 and status='blocked' and migration_item_id=any($3::uuid[])`, [migration.id, generation, readyIds])
    await db.query(`
    insert into drive_migration_verification_state(migration_item_id,migration_id,generation,source_scan_id,status,phase)
    select i.id,i.migration_id,$2,null,'pending','source'
    from drive_migration_items i
    where i.migration_id=$1
      and i.progress->'migrationQueue'->>'status'='completed'
      and i.progress->'migrationInventory'->>'status'='completed'
      and not exists (select 1 from drive_repair_jobs j where j.migration_id=$1 and j.work_key like $3 and j.payload->'itemIds'->>0=i.id::text and j.status<>'completed')
      and not exists (select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$2)
    on conflict (migration_item_id) do update set
      migration_id=excluded.migration_id,generation=excluded.generation,source_scan_id=excluded.source_scan_id,
      destination_scan_id=null,status='pending',phase='source',source_cursor=null,destination_cursor=null,
      source_objects=0,source_bytes=0,destination_objects=0,destination_bytes=0,
      missing_objects=0,mismatched_objects=0,extra_objects=0,attempt_count=0,attempt_generation=null,
      last_error=null,lease_owner=null,lease_expires_at=null,completed_at=null,updated_at=now()
    where drive_migration_verification_state.generation<>excluded.generation
    `, [migration.id, generation, `migration:${migration.id}:generation:${generation}:inventory:%`])
    await db.query(`
    update drive_migration_items i set slurper_status='verifying',last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(
        jsonb_set(
          jsonb_set(coalesce(progress,'{}'::jsonb),'{stage}','"awaiting_independent_verification"'::jsonb),
          '{verificationGeneration}',to_jsonb($2::int),true),
        '{verificationAttemptId}',to_jsonb(case when progress->>'verificationGeneration'=$2::text
          then coalesce(nullif(progress->>'verificationAttemptId',''),gen_random_uuid()::text)
          else gen_random_uuid()::text end),true)
    from drive_migration_verification_state v
    where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=$2 and v.status in('pending','running')
      and coalesce(i.slurper_status,'') not in('completed','worker_bucket_create_failed')
    `, [migration.id, generation])
    await db.query("commit")
  } catch (error) {
    await db.query("rollback").catch(() => undefined)
    throw error
  }
}
async function finalizeVerifiedBuckets(db: Client, migration: Row, generation: number) {
  await db.query(`
    update drive_migration_items i set slurper_status='completed',last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(coalesce(i.progress,'{}'::jsonb),'{repairWorkerStatus}','"completed"'::jsonb),
            '{stage}','"verified"'::jsonb),
          '{live}',coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object('status','completed','verifyIssues',0,'updatedAt',now())),
        '{fileVerification}',jsonb_build_object('status','completed','missing',v.missing_objects,'mismatched',v.mismatched_objects,
          'extra',v.extra_objects,'generation',v.generation,'completedAt',coalesce(v.completed_at,now())))
    from drive_migration_verification_state v
    where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=$2 and v.status='completed'
      and v.missing_objects=0 and v.mismatched_objects=0 and (v.extra_objects=0 or $3=false)
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
  `, [migration.id, generation, opts(migration).verifyStrictDestination === true])
  if (opts(migration).executionMode !== "migration_workers" && opts(migration).overwrite === false) {
    await db.query(`
      update drive_migration_items i set progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{live}',
        coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object(
          'transferredObjects',coalesce(i.source_objects,0),
          'alreadyPresentObjects',least(coalesce(i.source_objects,0),coalesce(nullif(i.progress->'live'->>'skippedObjects','')::bigint,0)),
          'copiedObjects',greatest(0,coalesce(i.source_objects,0)-least(coalesce(i.source_objects,0),coalesce(nullif(i.progress->'live'->>'skippedObjects','')::bigint,0))),
          'updatedAt',now()),true)
      from drive_migration_verification_state v where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=$2
        and v.status='completed' and v.missing_objects=0 and v.mismatched_objects=0 and (v.extra_objects=0 or $3=false)
        and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
    `, [migration.id, generation, opts(migration).verifyStrictDestination === true])
  }
  await db.query(`
    update drive_migration_items i set slurper_status='verification_failed',last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(
        jsonb_set(
          jsonb_set(coalesce(i.progress,'{}'::jsonb),'{live}',
            coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object('status','verification_failed',
              'verifyIssues',v.missing_objects+v.mismatched_objects+case when $3 then v.extra_objects else 0 end,'updatedAt',now())),
          '{stage}','"verification_failed"'::jsonb),
        '{fileVerification}',jsonb_build_object('status','error','missing',v.missing_objects,'mismatched',v.mismatched_objects,
          'extra',v.extra_objects,'generation',v.generation,'completedAt',coalesce(v.completed_at,now())))
    from drive_migration_verification_state v
    where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=$2 and v.status='completed'
      and (v.missing_objects>0 or v.mismatched_objects>0 or (v.extra_objects>0 and $3=true))
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
  `, [migration.id, generation, opts(migration).verifyStrictDestination === true])
  await db.query(`
    update drive_migration_items i set slurper_status='verification_failed',last_progress_at=now(),updated_at=now(),
      progress=jsonb_set(jsonb_set(jsonb_set(coalesce(i.progress,'{}'::jsonb),'{live}',
        coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object('status','verification_failed','updatedAt',now())),
        '{stage}','"verification_failed"'::jsonb),
        '{fileVerification}',jsonb_build_object('status','error','error',coalesce(v.last_error,'File Scanner recorded a non-retryable error'),
          'attemptCount',v.attempt_count,'generation',v.generation,'failedAt',now()))
    from drive_migration_verification_state v
    where v.migration_item_id=i.id and v.migration_id=$1 and v.generation=$2 and v.status='failed'
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
  `, [migration.id, generation])
}
async function finalizeShards(db: Client, migration: Row, generation: number, shardCount: number) {
  const currentGeneration = await db.query(`select 1 from drive_migrations where id=$1 and status in('running','verifying') and greatest(1,coalesce(nullif(options->>'workerGeneration','')::int,1))=$2 limit 1`, [migration.id, generation])
  if (!currentGeneration.rowCount) return { complete: false, superseded: true, jobs: {} }
  const counts = await db.query(`select status,count(*)::int count from drive_repair_jobs where migration_id=$1 and work_key like $2 group by status`, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`])
  const jobs = Object.fromEntries(counts.rows.map((row) => [row.status, Number(row.count)]))
  if ((jobs.completed || 0) !== shardCount) {
    const terminal = (jobs.completed || 0) + (jobs.failed || 0) + (jobs.canceled || 0)
    if (terminal === shardCount && ((jobs.failed || 0) > 0 || (jobs.canceled || 0) > 0)) {
      await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message='Migration worker retries exhausted',last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id])
      return { complete: false, terminalFailure: true, jobs }
    }
    return { complete: false, jobs }
  }
  await db.query("begin")
  try {
    const active = await db.query(`select id from drive_migrations where id=$1 and status in('running','verifying') for update`, [migration.id])
    if (!active.rowCount) { await db.query("commit"); return { complete: false, canceled: true, jobs } }
    await db.query(`update drive_migration_items i set slurper_status='verifying',last_progress_at=now(),updated_at=now(),progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{stage}','"awaiting_independent_verification"'::jsonb)
      where i.migration_id=$1 and coalesce(i.slurper_status,'')<>'worker_bucket_create_failed'
        and not exists(select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.generation=$2 and v.status='completed')`, [migration.id, generation])
    await db.query(`update drive_migrations set status='verifying',sync_status='running',sync_message='File Scanner verification pending',last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id])
    await db.query(`
    insert into drive_migration_verification_state(migration_item_id,migration_id,generation,status,phase)
    select id,migration_id,$2,'pending','source' from drive_migration_items where migration_id=$1
    on conflict(migration_item_id) do update set generation=excluded.generation,
      status=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.status else 'pending' end,
      phase=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.phase else 'source' end,
      source_scan_id=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.source_scan_id else null end,
      destination_scan_id=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.destination_scan_id else null end,
      source_cursor=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.source_cursor else null end,
      destination_cursor=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.destination_cursor else null end,
      source_objects=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.source_objects else 0 end,
      source_bytes=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.source_bytes else 0 end,
      destination_objects=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.destination_objects else 0 end,
      destination_bytes=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.destination_bytes else 0 end,
      missing_objects=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.missing_objects else 0 end,
      mismatched_objects=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.mismatched_objects else 0 end,
      extra_objects=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.extra_objects else 0 end,
      attempt_count=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.attempt_count else 0 end,
      last_error=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.last_error else null end,
      lease_owner=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.lease_owner else null end,
      lease_expires_at=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.lease_expires_at else null end,
      completed_at=case when drive_migration_verification_state.generation=excluded.generation then drive_migration_verification_state.completed_at else null end,
      updated_at=now()
    `, [migration.id, generation])
    await db.query("commit")
  } catch (error) { await db.query("rollback"); throw error }
  return { complete: true, jobs }
}
async function cloudflare(account: Row, path: string, method = "GET", body?: unknown, allow404 = false, timeoutMs = 20_000) {
  if (!account.cloudflare_account_id || !account.api_token) throw new Error("Cloudflare account ID or API token is missing")
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(account.cloudflare_account_id)}${path}`, {
    method, headers: { Authorization: `Bearer ${account.api_token}`, Accept: "application/json", ...(body === undefined ? {} : { "Content-Type": "application/json" }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(timeoutMs),
  })
  if (allow404 && response.status === 404) return null
  const payload = await response.json().catch(() => ({})) as Row
  if (!response.ok || payload.success === false) throw new Error(`${payload.errors?.[0]?.message || "Cloudflare API request failed"} (HTTP ${response.status})`)
  return payload.result ?? payload
}
function slurperJobRows(payload: unknown): Row[] {
  if (Array.isArray(payload)) return payload.filter((row): row is Row => Boolean(row) && typeof row === "object")
  if (!payload || typeof payload !== "object") return []
  const record = payload as Row
  for (const candidate of [record.jobs, record.items, record.result]) {
    if (Array.isArray(candidate)) return candidate.filter((row): row is Row => Boolean(row) && typeof row === "object")
  }
  return []
}
function slurperJobId(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return typeof payload === "string" && payload.trim() ? payload.trim() : null
  const row = payload as Row
  const result = row.result && typeof row.result === "object" ? row.result as Row : {}
  const nestedValue = result.job && typeof result.job === "object" ? result.job : row.job
  const nested = nestedValue && typeof nestedValue === "object" ? nestedValue as Row : {}
  for (const value of [row.id, row.jobId, row.job_id, result.id, result.jobId, result.job_id, nested.id, nested.jobId, nested.job_id]) {
    if (typeof value === "string" && value.trim()) return value.trim()
  }
  if (typeof row.result === "string" && row.result.trim()) return row.result.trim()
  return null
}
function appendMigrationEvent(progress: Row, stage: string, status: string, message: string) {
  const events = Array.isArray(progress.events) ? progress.events : []
  const previous = events.at(-1) as Row | undefined
  if (previous?.stage === stage && previous?.status === status) return events
  return [...events, { at: new Date().toISOString(), stage, status, message }].slice(-100)
}
async function ensureSuperSlurperInventory(db: Client, migration: Row) {
  const items = await db.query(`select id,source_bucket,slurper_job_id,slurper_status,progress from drive_migration_items where migration_id=$1 order by created_at`, [migration.id])
  let pending = 0
  let failed = 0
  for (const item of items.rows) {
    if (!(await migrationIsActive(db, migration.id))) break
    if (item.slurper_job_id || ["completed", "failed", "aborted", "bucket_create_failed", "verification_failed"].includes(String(item.slurper_status || ""))) continue
    const scanId = typeof item.progress?.sourceScanId === "string" ? item.progress.sourceScanId : ""
    let scan = scanId
      ? (await db.query(`select id,status,objects,bytes,error,attempt_count from drive_bucket_scans where id=$1 and migration_id=$2 and migration_item_id=$3 and kind='source' limit 1`, [scanId, migration.id, item.id])).rows[0]
      : null
    if (!scan) {
      const existing = await db.query(`select id,status,objects,bytes,error,attempt_count from drive_bucket_scans where migration_id=$1 and migration_item_id=$2 and kind='source' order by updated_at desc limit 1`, [migration.id, item.id])
      scan = existing.rows[0] || null
    }
    if (!scan) {
      const inserted = await db.query(`
        insert into drive_bucket_scans(id,account_id,bucket_name,kind,migration_id,migration_item_id,prefix,status,updated_at)
        values(gen_random_uuid(),$1,$2,'source',$3,$4,$5,'pending',now()) returning id,status,objects,bytes,error
      `, [migration.source_account_id, item.source_bucket, migration.id, item.id, opts(migration).pathPrefix || null])
      scan = inserted.rows[0]
    }
    const progress = { ...(item.progress || {}) }
    const scanStatus = String(scan.status || "pending")
    if (scanStatus === "failed") {
      const message = String(scan.error || `File Scanner failed for ${item.source_bucket}`)
      const transient = /connection terminated unexpectedly|connection reset|connection closed|socket hang up|econnreset|etimedout|eai_again|fetch failed|temporar(?:y|ily) unavailable|too many requests|\bHTTP (?:408|425|429|500|502|503|504)\b|\b(?:57P01|57P03|08000|08003|08006|08001)\b/i.test(message)
      if (transient) {
        pending += 1
        await db.query(`update drive_bucket_scans set status='pending',lease_owner=null,lease_expires_at=null,updated_at=now() where id=$1 and status='failed'`, [scan.id])
        await db.query(`update drive_migration_items i set slurper_status='scanning',progress=$2::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ ...progress, stage: "scanning_source", sourceScanId: scan.id, sourceScanStatus: "retrying", error: message, lastError: message, events: appendMigrationEvent(progress, "source_scan", "retrying", `Transient File Scanner error; retry ${Number(scan.attempt_count || 0)} will resume from the saved cursor: ${message}`) })])
        continue
      }
      failed += 1
      await db.query(`update drive_migration_items i set slurper_status='precheck_failed',progress=$2::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ ...progress, stage: "scan_failed", sourceScanId: scan.id, sourceScanStatus: "failed", error: message, lastError: message, events: appendMigrationEvent(progress, "source_scan", "failed", message) })])
      continue
    }
    if (scanStatus !== "completed") pending += 1
    const completed = scanStatus === "completed"
    await db.query(`
      update drive_migration_items set
        slurper_status=case when $2 then case when slurper_status='scanning' then 'queued' else coalesce(slurper_status,'queued') end else 'scanning' end,
        source_objects=$3,
        source_bytes=$4,
        progress=$5::jsonb,last_progress_at=now(),updated_at=now()
      where id=$1 and exists(select 1 from drive_migrations m where m.id=drive_migration_items.migration_id and m.status in('running','verifying'))
    `, [item.id, completed, scan.objects || 0, scan.bytes || 0, JSON.stringify({
      ...progress,
      stage: completed ? "scan_completed" : "scanning_source",
      sourceScanId: scan.id,
      sourceScanStatus: scanStatus,
      sourceScanObjects: Number(scan.objects || 0),
      sourceScanBytes: Number(scan.bytes || 0),
      ...(completed ? { error: null, lastError: null } : {}),
      events: appendMigrationEvent(progress, "source_scan", completed ? "completed" : "running", completed ? `File Scanner indexed ${Number(scan.objects || 0).toLocaleString()} source objects` : `File Scanner is scanning ${item.source_bucket}`),
    })])
  }
  return { total: items.rowCount || 0, pending, failed }
}
async function createSuperSlurperJobs(db: Client, migration: Row) {
  const accountsResult = await db.query(`select id,cloudflare_account_id,api_token,r2_access_key_id,r2_secret_access_key from drive_accounts where id in($1,$2)`, [migration.source_account_id, migration.target_account_id])
  const source = accountsResult.rows.find((row) => row.id === migration.source_account_id)
  const target = accountsResult.rows.find((row) => row.id === migration.target_account_id)
  if (!source?.cloudflare_account_id || !source.r2_access_key_id || !source.r2_secret_access_key) throw new Error("Source Cloudflare account or R2 credentials are incomplete")
  if (!target?.cloudflare_account_id || !target.api_token || !target.r2_access_key_id || !target.r2_secret_access_key) throw new Error("Destination Cloudflare account or R2 credentials are incomplete")

  const [bucketPayload, jobsPayload, itemsResult] = await Promise.all([
    cloudflare(target, "/r2/buckets"),
    cloudflare(target, "/slurper/jobs"),
    db.query(`select id,source_bucket,target_bucket,source_jurisdiction,source_storage_class,slurper_job_id,slurper_status,progress from drive_migration_items where migration_id=$1 and slurper_job_id is null order by created_at`, [migration.id]),
  ])
  const bucketList = Array.isArray(bucketPayload) ? bucketPayload : Array.isArray(bucketPayload?.buckets) ? bucketPayload.buckets : []
  const bucketNames = new Set(bucketList.map((bucket: Row) => String(bucket.name || "")))
  const remoteJobs = slurperJobRows(jobsPayload)
  const terminal = new Set(["completed", "complete", "finished", "success", "succeeded", "aborted", "canceled", "cancelled", "failed", "error"])
  let activeCount = remoteJobs.filter((job) => typeof job.status === "string" && !terminal.has(job.status.toLowerCase())).length
  const limit = Math.max(1, Math.min(3, integer(opts(migration).concurrency, 3, 1, 3)))
  let created = 0
  let attached = 0
  let failed = 0

  for (const item of itemsResult.rows) {
    if (!(await migrationIsActive(db, migration.id))) break
    const progress = { ...(item.progress || {}) }
    const scanId = String(progress.sourceScanId || "")
    if (!scanId || progress.sourceScanStatus !== "completed") continue
    const jurisdiction = ["default", "eu", "fedramp"].includes(String(item.source_jurisdiction)) ? item.source_jurisdiction : undefined
    if (activeCount >= limit) {
      await db.query(`update drive_migration_items set slurper_status='queued',progress=$2::jsonb,updated_at=now() where id=$1`, [item.id, JSON.stringify({ ...progress, stage: "cloudflare_job_limit", queueReason: `Waiting for a Super Slurper slot (limit ${limit})` })])
      continue
    }

    const recent = remoteJobs.find((job) => {
      const ageMs = Date.now() - Date.parse(String(job.createdAt || job.created_at || ""))
      const sourceBucket = job.source && typeof job.source === "object" ? String(job.source.bucket || "") : ""
      const targetBucket = job.target && typeof job.target === "object" ? String(job.target.bucket || "") : ""
      return sourceBucket === item.source_bucket && targetBucket === item.target_bucket && Number.isFinite(ageMs) && ageMs >= -60_000 && ageMs <= 10 * 60_000
    })
    if (recent?.id) {
      const linked = await db.query(`update drive_migration_items i set slurper_job_id=$2,slurper_status='running',progress=$3::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and i.slurper_job_id is null and coalesce(i.slurper_status,'') not in('aborted','failed','completed','verification_failed') and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying')) returning i.id`, [item.id, String(recent.id), JSON.stringify({ ...progress, stage: "job_attached", jobName: `drive-migration-${migration.id}-${item.id}`, events: appendMigrationEvent(progress, "super_slurper_job", "running", "Attached existing Cloudflare Super Slurper job") })])
      if (linked.rowCount) {
        attached += 1
        if (typeof recent.status !== "string" || !terminal.has(recent.status.toLowerCase())) activeCount += 1
      }
      continue
    }

    try {
      if (!bucketNames.has(item.target_bucket)) {
        try {
          await cloudflare(target, "/r2/buckets", "POST", {
            name: item.target_bucket,
            ...(jurisdiction ? { jurisdiction } : {}),
            ...(item.source_storage_class ? { storageClass: item.source_storage_class } : {}),
          })
        } catch (error) {
          const listed = await cloudflare(target, "/r2/buckets")
          const current = Array.isArray(listed) ? listed : Array.isArray(listed?.buckets) ? listed.buckets : []
          if (!current.some((bucket: Row) => bucket.name === item.target_bucket)) throw error
        }
        bucketNames.add(item.target_bucket)
      }

      const overwrite = opts(migration).overwrite !== false
      const jobName = `drive-migration-${migration.id}-${item.id}`
      const targetSpec = { vendor: "r2", bucket: item.target_bucket, secret: { accessKeyId: target.r2_access_key_id, secretAccessKey: target.r2_secret_access_key }, ...(jurisdiction ? { jurisdiction } : {}) }
      const sourceS3 = { vendor: "s3", bucket: item.source_bucket, secret: { accessKeyId: source.r2_access_key_id, secretAccessKey: source.r2_secret_access_key }, endpoint: `https://${source.cloudflare_account_id}.r2.cloudflarestorage.com/${encodeURIComponent(item.source_bucket)}`, ...(typeof opts(migration).pathPrefix === "string" ? { pathPrefix: opts(migration).pathPrefix } : {}) }
      const sourceR2 = { vendor: "r2", bucket: item.source_bucket, secret: { accessKeyId: source.r2_access_key_id, secretAccessKey: source.r2_secret_access_key }, ...(jurisdiction ? { jurisdiction } : {}), ...(typeof opts(migration).pathPrefix === "string" ? { pathPrefix: opts(migration).pathPrefix } : {}) }

      await cloudflare(target, "/slurper/target/connectivity-precheck", "PUT", targetSpec)
      let sourceSpec: Row = sourceS3
      try { await cloudflare(target, "/slurper/source/connectivity-precheck", "PUT", sourceS3) }
      catch (s3Error) {
        try { await cloudflare(target, "/slurper/source/connectivity-precheck", "PUT", sourceR2); sourceSpec = sourceR2 }
        catch (r2Error) { throw new Error(`Source precheck failed (S3: ${s3Error instanceof Error ? s3Error.message : String(s3Error)}; R2: ${r2Error instanceof Error ? r2Error.message : String(r2Error)})`) }
      }

      if (!(await migrationIsActive(db, migration.id))) break

      const response = await cloudflare(target, "/slurper/jobs", "POST", {
        overwrite,
        jobName,
        configuration: { overwriteObjects: overwrite },
        source: sourceSpec,
        target: targetSpec,
      })
      let jobId = slurperJobId(response)
      if (!jobId) {
        const createStartedAt = Date.now()
        const refreshed = slurperJobRows(await cloudflare(target, "/slurper/jobs"))
        const match = refreshed.find((job) => {
          const sourceBucket = job.source && typeof job.source === "object" ? String(job.source.bucket || "") : ""
          const targetBucket = job.target && typeof job.target === "object" ? String(job.target.bucket || "") : ""
          const createdAt = Date.parse(String(job.createdAt || job.created_at || ""))
          return sourceBucket === item.source_bucket && targetBucket === item.target_bucket && Number.isFinite(createdAt) && createdAt >= createStartedAt - 60_000 && createdAt <= Date.now() + 60_000
        })
        jobId = typeof match?.id === "string" ? match.id : null
      }
      if (!jobId) {
        await db.query(`update drive_migration_items i set slurper_status='job_id_pending',progress=$2::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ ...progress, stage: "job_id_missing", jobName, lastError: "Cloudflare accepted the job but no job ID was returned", events: appendMigrationEvent(progress, "super_slurper_job", "pending", "Waiting for Cloudflare to return the new job ID") })])
        continue
      }
      const attachedJob = await db.query(`update drive_migration_items i set slurper_job_id=$2,slurper_status='running',progress=$3::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and i.slurper_job_id is null and coalesce(i.slurper_status,'') not in('aborted','failed','completed','verification_failed') and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying')) returning i.id`, [item.id, jobId, JSON.stringify({ ...progress, stage: "job_created", jobName, sourceModeUsed: sourceSpec.vendor, events: appendMigrationEvent(progress, "super_slurper_job", "running", "Cloudflare Super Slurper job started") })])
      if (!attachedJob.rowCount) {
        await cloudflare(target, `/slurper/jobs/${encodeURIComponent(jobId)}/abort`, "PUT").catch(() => undefined)
        continue
      }
      remoteJobs.push({ id: jobId, status: "queued", createdAt: new Date().toISOString(), source: { bucket: item.source_bucket }, target: { bucket: item.target_bucket } })
      activeCount += 1
      created += 1
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      const retryable = /HTTP (?:409|429)|rate.?limit|concurren|job limit/i.test(message)
      const status = retryable ? "queued" : message.toLowerCase().includes("bucket") && !bucketNames.has(item.target_bucket) ? "bucket_create_failed" : "precheck_failed"
      if (!retryable) failed += 1
      await db.query(`update drive_migration_items i set slurper_status=$2,progress=$3::jsonb,last_progress_at=now(),updated_at=now() where i.id=$1 and coalesce(i.slurper_status,'') not in('aborted','completed') and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, status, JSON.stringify({ ...progress, stage: retryable ? "cloudflare_job_limit" : status === "bucket_create_failed" ? "create_target_bucket" : "create_job", error: message, lastError: message, events: appendMigrationEvent(progress, "super_slurper_job", retryable ? "queued" : "failed", message) })])
    }
  }
  return { created, attached, failed, active: activeCount, limit }
}
async function syncNextBucketSettings(db: Client, migration: Row) {
  const pending = await db.query(`select i.* from drive_migration_items i
    join drive_migration_verification_state v on v.migration_item_id=i.id and v.migration_id=i.migration_id
    where i.migration_id=$1 and i.slurper_status='completed' and v.status='completed'
      and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))
      and v.missing_objects=0 and v.mismatched_objects=0
      and (v.extra_objects=0 or $2=false)
      and coalesce(i.progress->'orchestratorSettings'->>'status','')<>'synced'
    order by i.created_at limit 1`, [migration.id, opts(migration).verifyStrictDestination === true])
  const item = pending.rows[0]
  if (!item) return { settings: "synced" }
  const accounts = await db.query(`select id,cloudflare_account_id,api_token from drive_accounts where id in($1,$2)`, [migration.source_account_id, migration.target_account_id])
  const source = accounts.rows.find((row) => row.id === migration.source_account_id); const target = accounts.rows.find((row) => row.id === migration.target_account_id)
  if (!source || !target) throw new Error("Source or target account is missing")
  const attempts = integer(item.progress?.orchestratorSettings?.attempts, 0, 0, 100)
  const sourcePath = `/r2/buckets/${encodeURIComponent(item.source_bucket)}`; const targetPath = `/r2/buckets/${encodeURIComponent(item.target_bucket)}`
  await db.query(`update drive_migration_items i set progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{orchestratorSettings}',$2::jsonb),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ status: "syncing", attempts: attempts + 1, startedAt: new Date().toISOString() })])
  try {
    const [cors, domain] = await Promise.all([
      cloudflare(source, `${sourcePath}/cors`, "GET", undefined, true),
      cloudflare(source, `${sourcePath}/domains/managed`, "GET", undefined, true),
    ])
    const rules = Array.isArray(cors?.rules) ? cors.rules : []
    await cloudflare(target, `${targetPath}/cors`, rules.length ? "PUT" : "DELETE", rules.length ? { rules } : undefined, true)
    await cloudflare(target, `${targetPath}/domains/managed`, "PUT", { enabled: domain?.enabled === true })
    await db.query(`update drive_migration_items i set progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{orchestratorSettings}',$2::jsonb),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ status: "synced", attempts: attempts + 1, syncedAt: new Date().toISOString() })])
    return { settings: "progress", itemId: item.id }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    await db.query(`update drive_migration_items i set progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{orchestratorSettings}',$2::jsonb),updated_at=now() where i.id=$1 and exists(select 1 from drive_migrations m where m.id=i.migration_id and m.status in('running','verifying'))`, [item.id, JSON.stringify({ status: attempts >= 2 ? "failed" : "pending", attempts: attempts + 1, error: message, updatedAt: new Date().toISOString() })])
    if (attempts >= 2) await db.query(`update drive_migrations set status='failed',sync_status='failed',sync_message=$2,updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, `Bucket settings sync failed for ${item.source_bucket}: ${message}`])
    return { settings: attempts >= 2 ? "failed" : "retry", itemId: item.id, error: message }
  }
}
async function wakeBackendOrchestrator(db: Client) {
  const result = await db.query(`select value from drive_app_settings where key='backend-orchestrator' limit 1`)
  const settings = result.rows[0]?.value || {}
  if (!settings.enabled || !settings.orchestratorUrl || !settings.sharedSecret) return "not_configured"
  try {
    const response = await fetch(`${String(settings.orchestratorUrl).replace(/\/+$/, "")}/run`, { method: "POST", headers: { Authorization: `Bearer ${settings.sharedSecret}` }, signal: AbortSignal.timeout(8_000) })
    return response.ok ? "signaled" : `http_${response.status}`
  } catch { return "deferred_to_cron" }
}
async function activateTargetAndCompleteMigration(db: Client, migration: Row) {
  await db.query("begin")
  try {
    const lockedMigration = await db.query(`select id from drive_migrations where id=$1 and status in('running','verifying') for update`, [migration.id])
    if (!lockedMigration.rowCount) { await db.query("commit"); return { activated: false, backendOrchestrator: "migration_not_active" } }
    const previous = await db.query(`select total_buckets,total_objects,total_bytes,last_synced_at
      from drive_accounts where status='active' and id<>$1
      order by last_synced_at desc nulls last limit 1 for update`, [migration.target_account_id])
    // The partial unique index on active accounts is immediate. A single
    // multi-row UPDATE can promote the target before it demotes the source,
    // even though the final state would have only one active account.
    await db.query(`update drive_accounts set status='disabled',updated_at=now()
      where status='active' and id<>$1`, [migration.target_account_id])
    const activated = await db.query(`update drive_accounts set
      status='active',last_migrated=to_char(now(),'YYYY-MM-DD HH24:MI:SS'),
      total_buckets=case when last_synced_at is null then coalesce($2,total_buckets) else total_buckets end,
      total_objects=case when last_synced_at is null then coalesce($3,total_objects) else total_objects end,
      total_bytes=case when last_synced_at is null then coalesce($4,total_bytes) else total_bytes end,
      last_synced_at=case when last_synced_at is null then $5::timestamptz else last_synced_at end,
      sync_status='syncing',sync_message='Awaiting Backend Orchestrator refresh; showing last committed totals',updated_at=now()
      where id=$1 returning id`, [migration.target_account_id, previous.rows[0]?.total_buckets ?? null,
      previous.rows[0]?.total_objects ?? null, previous.rows[0]?.total_bytes ?? null, previous.rows[0]?.last_synced_at ?? null])
    if (!activated.rowCount) throw new Error("Target account was not found during migration completion")
    await db.query(`update drive_migrations set status='completed',completed_at=now(),sync_status='synced',sync_message=NULL,last_synced_at=now(),updated_at=now(),summary_item_count=(select count(*) from drive_migration_items where migration_id=$1),summary_objects=(select coalesce(sum(source_objects),0) from drive_migration_items where migration_id=$1),summary_bytes=(select coalesce(sum(source_bytes),0) from drive_migration_items where migration_id=$1) where id=$1 and status in('running','verifying')`, [migration.id])
    await db.query("commit")
  } catch (error) { await db.query("rollback"); throw error }
  return { activated: true, backendOrchestrator: await wakeBackendOrchestrator(db) }
}
async function finishOrRepair(db: Client, migration: Row, generation: number) {
  const active = await db.query(`select id from drive_migrations where id=$1 and status in('running','verifying')`, [migration.id])
  if (!active.rowCount) return { verification: "canceled" }
  const transfers = await db.query(`
    select not exists(select 1 from drive_migration_items i where i.migration_id=$1
        and (coalesce(i.progress->'migrationInventory'->>'status','')<>'completed'
          or coalesce(i.progress->'migrationQueue'->>'status','')<>'completed'))
      and not exists(select 1 from drive_repair_jobs j where j.migration_id=$1 and j.work_key like $2 and j.status<>'completed') ready
  `, [migration.id, `migration:${migration.id}:generation:${generation}:inventory:%`])
  if (transfers.rows[0]?.ready !== true) return { verification: "waiting_for_transfers" }
  const states = await db.query(`select status,missing_objects,mismatched_objects,extra_objects from drive_migration_verification_state where migration_id=$1 and generation=$2`, [migration.id, generation])
  if (states.rows.some((row) => row.status === "failed")) {
    const failed = await db.query(`select i.source_bucket,v.last_error,v.attempt_count from drive_migration_verification_state v join drive_migration_items i on i.id=v.migration_item_id where v.migration_id=$1 and v.generation=$2 and v.status='failed' order by i.source_bucket`, [migration.id, generation])
    const buckets = failed.rows.map((row) => `${row.source_bucket}: ${row.last_error || `non-retryable scanner error (attempt ${row.attempt_count})`}`)
    const message = `File Scanner verification failed${buckets.length ? ` for ${buckets.join('; ')}` : " because a non-retryable scan error was recorded"}`
    await db.query(`update drive_migrations set status='verification_failed',sync_status='failed',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, message])
    return { verification: "failed", reason: "file_scan_failed" }
  }
  if (!states.rows.length || states.rows.some((row) => row.status !== "completed")) return { verification: "pending" }
  const missing = states.rows.reduce((n, row) => n + Number(row.missing_objects), 0)
  const mismatched = states.rows.reduce((n, row) => n + Number(row.mismatched_objects), 0)
  const extra = states.rows.reduce((n, row) => n + Number(row.extra_objects), 0)
  if (missing || mismatched || (opts(migration).verifyStrictDestination === true && extra)) {
    await db.query(`update drive_migrations set status='verification_failed',sync_status='failed',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, `File Scanner verification found ${missing} missing, ${mismatched} mismatched, ${extra} extra; repair is available`])
    return { verification: "failed", missing, mismatched, extra, repairAvailable: true }
  }
  const settings = await syncNextBucketSettings(db, migration)
  if (settings.settings !== "synced") return { verification: settings.settings === "failed" ? "failed" : "settings_sync", missing, mismatched, extra, ...settings }
  const completion = await activateTargetAndCompleteMigration(db, migration)
  if (!completion.activated) return { verification: "canceled" }
  return { verification: "completed", missing, mismatched, extra, backendOrchestrator: completion.backendOrchestrator }
}
async function releaseWorkerInstanceClaims(db: Client, agentId: string, instanceId: string, reason: string) {
  if (!instanceId) return 0
  const released = await db.query(`
    update drive_repair_jobs set status='pending',claimed_by_agent_id=null,claim_token=null,claimed_at=null,started_at=null,
      last_heartbeat_at=null,error=null,completed_at=null,summary=$3,
      progress=coalesce(progress,'{}'::jsonb)||jsonb_build_object('active',false,'currentFile',null,'rotationRequestedAt',now(),'rotationReason','github_runtime_window'),updated_at=now()
    where mode='migration' and claimed_by_agent_id=$1 and payload->>'claimedWorkerInstanceId'=$2 and status in('claimed','running')
    returning id
  `, [agentId, instanceId, reason.slice(0, 2000)])
  return released.rowCount || 0
}
let endpointHealth: { url: string; expiresAt: number; error: string | null } | null = null
async function workerEndpointAvailable(db: Client, migrationId: string, orchestration: Row) {
  const url = String(orchestration.orchestratorUrl || "").replace(/\/+$/, "")
  if (!endpointHealth || endpointHealth.url !== url || endpointHealth.expiresAt <= Date.now()) {
    let error: string | null = null
    try {
      const response = await fetch(`${url}/health`, { signal: AbortSignal.timeout(10_000) })
      const body = await response.text()
      if (!response.ok) error = response.status === 429 && /1027|reached their plan limits/i.test(body)
        ? "Cloudflare Workers daily request limit reached (Error 1027). Worker dispatch is paused until the endpoint recovers."
        : `Worker endpoint unavailable (HTTP ${response.status}). Worker dispatch is paused until it recovers.`
      else {
        const health = JSON.parse(body)
        if (health?.ok !== true || health.service !== "migration-orchestrator") error = "Worker endpoint returned an unexpected health response. Worker dispatch is paused."
      }
    } catch { error = "Worker endpoint is unreachable. Worker dispatch is paused until it recovers." }
    endpointHealth = { url, expiresAt: Date.now() + 60_000, error }
  }
  if (endpointHealth.error) {
    await db.query(`update drive_migrations set sync_status='error',sync_message=$2,updated_at=now()
      where id=$1 and status in('running','verifying') and sync_message is distinct from $2`, [migrationId, endpointHealth.error])
    return false
  }
  await db.query(`update drive_migrations set sync_status='running',sync_message=null,
    options=jsonb_set(coalesce(options,'{}'::jsonb),'{workerEndpointRecoveryUntil}',to_jsonb(now()+interval '6 minutes')),updated_at=now()
    where id=$1 and status in('running','verifying') and (sync_message like 'Cloudflare Workers daily request limit reached%' or sync_message like 'Worker endpoint %')`, [migrationId])
  return true
}

async function migrationQueueRequest(db: Client, env: Env, action: "seed" | "sync", migrationId: string, generation: number) {
  const configured = await db.query(`select value from drive_app_settings where key='migration-workers' limit 1`)
  const settings = configured.rows[0]?.value || {}
  if (!String(settings.queueDatabaseUrl || "").trim()) return { configured: false, complete: false, inserted: 0, synced: 0 }
  const originRow = await db.query(`select value->>'panelOrigin' panel_origin from drive_app_settings where key='orchestration-panel-origin' limit 1`)
  const base = String(env.PANEL_URL || originRow.rows[0]?.panel_origin || "").trim().replace(/\/+$/, "")
  if (!base) throw new Error("Panel URL is unavailable for migration queue synchronization")
  const response = await fetch(`${base}/api/internal/migration-queue`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${String(env.MIGRATION_ORCHESTRATOR_SECRET || "")}` },
    body: JSON.stringify({ action, migrationId, generation, limit: action === "seed" ? 100_000 : 1_000 }),
    signal: AbortSignal.timeout(45_000),
  })
  const result = await response.json().catch(() => ({})) as Row
  if (!response.ok || result.ok !== true) throw new Error(`Migration queue ${action} failed (HTTP ${response.status}): ${String(result.error || "unknown error")}`)
  return { configured: true, complete: result.complete === true, inserted: Number(result.inserted || 0), synced: Number(result.synced || 0) }
}

async function dispatchWorkers(db: Client, env: Env, migration: Row) {
  const generation = integer(opts(migration).workerGeneration, 1, 1, 1000000)
  const stopped = await db.query(`
    select 1 from drive_repair_jobs j
    where j.migration_id=$1 and j.mode='migration' and j.status='canceled'
      and not exists(select 1 from drive_repair_jobs newer where newer.migration_id=j.migration_id and newer.mode='migration' and (newer.created_at,newer.id)>(j.created_at,j.id))
    limit 1
  `, [migration.id])
  if (stopped.rowCount) return 0
  const configRows = await db.query(`select key,value from drive_app_settings where key in('migration-orchestrator','migration-workers')`)
  const orchestration = configRows.rows.find((row) => row.key === "migration-orchestrator")?.value || {}
  const workerSettings = configRows.rows.find((row) => row.key === "migration-workers")?.value || {}
  if (!(await githubWorkerSecretsAreCurrent(orchestration, workerSettings))) return 0
  // Missing heartbeats during an endpoint outage do not mean the GitHub
  // processes need replacing. Keep their slots and durable file checkpoints.
  const directQueueMode = Boolean(String(workerSettings.queueDatabaseUrl || "").trim())
  if (!directQueueMode && !(await workerEndpointAvailable(db, migration.id, orchestration))) return 0
  // Persist the grace across queue invocations: a worker may still be in its
  // five-minute quota cooldown when the public endpoint first recovers.
  const recovery = await db.query(`select options->>'workerEndpointRecoveryUntil' recovery_until from drive_migrations where id=$1`, [migration.id])
  const endpointRecoveryGrace = Date.parse(String(recovery.rows[0]?.recovery_until || "")) > Date.now()
  const budget = integer(orchestration.maxDispatchesPerCycle, 100, 1, 100)
  const stranded = await db.query(`select id from drive_agent_runs where run_type='github_dispatch' and status='pending' and payload->>'migrationId'=$1 and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=$2 and coalesce(payload->>'phase','created') in('created','queued') order by created_at limit $3`, [migration.id, generation, budget])
  for (const row of stranded.rows) await env.GITHUB_DISPATCH_QUEUE.send({ intentId: row.id }, { contentType: "json" })
  // Every registered GitHub workflow contributes its configured capacity to
  // every active worker-pool migration. Re-read this set on every cycle so a
  // workflow registered mid-migration joins automatically.
  const agents = await db.query(`select id,github_repo_owner,github_repo_name,github_workflow_file,github_ref,github_token,least(5,greatest(1,coalesce(worker_count,1))) worker_count from drive_agents where provider='github_actions' and status<>'disabled' and github_token is not null and github_repo_owner is not null and github_repo_name is not null and github_workflow_file is not null order by created_at,id`)
  let queued = stranded.rowCount || 0
  const githubRunsByWorkflow = new Map<string, Row[]>()
  const activeByWorkflow = new Map<string, Set<string>>()
  const blockedWorkflows = new Set<string>()
  for (const agent of agents.rows) {
    const account = String(agent.github_repo_owner).toLowerCase()
    const workflowKey = `${account}/${String(agent.github_repo_name).toLowerCase()}/${agent.github_workflow_file}/${agent.github_ref || "main"}`
    if (!githubRunsByWorkflow.has(workflowKey) && !blockedWorkflows.has(workflowKey)) {
      try {
        const url = `https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/workflows/${encodeURIComponent(String(agent.github_workflow_file).split("/").pop()!)}/runs?event=repository_dispatch&branch=${encodeURIComponent(agent.github_ref || "main")}&per_page=100`
        const response = await fetch(url, { headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(15_000) })
        if (!response.ok) {
          const failure = await response.json().catch(() => ({})) as Row
          throw new Error(`GitHub workflow capacity check HTTP ${response.status}${typeof failure.message === "string" ? `: ${failure.message.slice(0, 250)}` : ""}`)
        }
        const payload = await response.json() as { workflow_runs?: Row[] }
        if (!Array.isArray(payload.workflow_runs)) throw new Error("GitHub returned an incomplete workflow run list")
        githubRunsByWorkflow.set(workflowKey, payload.workflow_runs)
        await db.query(`update drive_agents set last_error=null,updated_at=now() where id=$1 and last_error like 'GitHub dispatch capacity check failed:%'`, [agent.id])
      } catch (error) {
        blockedWorkflows.add(workflowKey)
        const message = error instanceof Error ? error.message : String(error)
        await db.query(`update drive_agents set last_error=$2,updated_at=now() where id=$1`, [agent.id, `GitHub dispatch capacity check failed: ${message}`])
      }
    }
    if (blockedWorkflows.has(workflowKey)) continue
    const runs = githubRunsByWorkflow.get(workflowKey) || []
    const active = runs.filter((run) => ["queued", "in_progress", "waiting", "requested", "pending"].includes(String(run.status || "").toLowerCase()))
    const workflowRuns = activeByWorkflow.get(workflowKey) || new Set<string>()
    for (const run of active) if (run.id) workflowRuns.add(String(run.id))
    activeByWorkflow.set(workflowKey, workflowRuns)
  }
  for (const agent of agents.rows) {
    const account = String(agent.github_repo_owner).toLowerCase()
    const workflowKey = `${account}/${String(agent.github_repo_name).toLowerCase()}/${agent.github_workflow_file}/${agent.github_ref || "main"}`
    if (blockedWorkflows.has(workflowKey)) continue
    const remoteRuns = githubRunsByWorkflow.get(workflowKey) || []
    const trackedRuns = await db.query(`select id,payload,external_run_id,created_at,updated_at from drive_agent_runs where agent_id=$1 and run_type='github_dispatch' and status in('pending','running') and payload->>'migrationId'=$2 and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=$3`, [agent.id, migration.id, generation])
    let agentOccupancy = 0
    for (const tracked of trackedRuns.rows) {
      const instanceId = String(tracked.payload?.workerInstanceId || "")
      let remote = remoteRuns.find((run) => (tracked.external_run_id && String(run.id) === String(tracked.external_run_id)) || (instanceId && String(run.display_title || "").includes(instanceId)))
      // The workflow listing is bounded and can briefly omit a known run. Ask
      // GitHub for that exact run before deciding whether its slot is free.
      if (!remote && tracked.external_run_id) {
        const lookup = await fetch(`https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/runs/${encodeURIComponent(String(tracked.external_run_id))}`, { headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(10_000) }).catch(() => null)
        if (lookup?.ok) {
          remote = await lookup.json().catch(() => undefined) as Row | undefined
          if (remote?.id && ["queued", "in_progress", "waiting", "requested", "pending"].includes(String(remote.status || "").toLowerCase())) activeByWorkflow.get(workflowKey)?.add(String(remote.id))
        } else if (!lookup || lookup.status !== 404) {
          // Unknown is not stopped. Preserve the slot until GitHub can answer,
          // preventing two workers from owning the same configured capacity.
          agentOccupancy += 1
          continue
        }
      }
      if (remote && String(remote.status).toLowerCase() === "completed") {
        const terminalStatus = String(remote.conclusion).toLowerCase() === "success" ? "completed" : ["cancelled", "canceled"].includes(String(remote.conclusion).toLowerCase()) ? "canceled" : "failed"
        const releasedJobs = await releaseWorkerInstanceClaims(db, agent.id, instanceId, "GitHub worker stopped; file returned to the durable queue")
        await db.query(`update drive_agent_runs set status=$2,summary='GitHub worker run reached a terminal state; capacity released',completed_at=now(),external_run_id=coalesce(external_run_id,$3),payload=payload||$4::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [tracked.id, terminalStatus, String(remote.id || "") || null, JSON.stringify({ githubStatus: remote.status, githubConclusion: remote.conclusion || null, releasedJobs })])
        activeByWorkflow.get(workflowKey)?.delete(String(remote.id || tracked.external_run_id || ""))
        continue
      }
      if (!remote) {
        // A repository_dispatch can take a few seconds to appear in GitHub's
        // run list. This grace applies only before a run id has ever resolved;
        // known runs use their exact GitHub state above and release at once.
        const unresolvedAgeMs = Date.now() - Date.parse(String(tracked.payload?.acceptedAt || tracked.payload?.dispatchStartedAt || tracked.created_at || tracked.updated_at || ""))
        if (Number.isFinite(unresolvedAgeMs) && unresolvedAgeMs < 60_000) {
          agentOccupancy += 1
          continue
        }
        const releasedJobs = await releaseWorkerInstanceClaims(db, agent.id, instanceId, "GitHub no longer reports this worker run; file returned to the durable queue")
        await db.query(`update drive_agent_runs set status='failed',summary='GitHub no longer reports this worker as queued or running; capacity released',completed_at=now(),payload=payload||$2::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [tracked.id, JSON.stringify({ releasedJobs, githubRunMissingAt: new Date().toISOString() })])
        continue
      }
      const remoteStatus = String(remote?.status || "").toLowerCase()
      const remoteStartedAt = Date.parse(String(remote?.run_started_at || remote?.created_at || ""))
      const rotationDue = ["in_progress", "waiting"].includes(remoteStatus)
        && Number.isFinite(remoteStartedAt)
        && Date.now() - remoteStartedAt >= GITHUB_WORKER_MAX_RUNTIME_SECONDS * 1000
      if (remote && rotationDue) {
        agentOccupancy += 1
        if (!tracked.payload?.rotationRequestedAt) {
          const cancel = await fetch(`https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/runs/${encodeURIComponent(String(remote.id))}/cancel`, { method: "POST", headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(15_000) }).catch(() => null)
          if (cancel?.ok || cancel?.status === 202) {
            await db.query(`update drive_agent_runs set summary='Worker runtime window reached; GitHub cancellation requested before replacement',payload=payload||$2::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [tracked.id, JSON.stringify({ rotationRequestedAt: new Date().toISOString(), rotationReason: "github_runtime_window" })])
          }
        }
        continue
      }
      const heartbeatStale = Date.parse(String(tracked.updated_at || "")) < Date.now() - 3 * 60_000
      if (!heartbeatStale) {
        agentOccupancy += 1
        continue
      }
      if (endpointRecoveryGrace) {
        agentOccupancy += 1
        continue
      }
      if (remote && ["queued", "in_progress", "waiting", "requested", "pending"].includes(String(remote.status || "").toLowerCase())) {
        const cancel = await fetch(`https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/runs/${encodeURIComponent(String(remote.id))}/cancel`, { method: "POST", headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(15_000) }).catch(() => null)
        if (cancel?.ok || cancel?.status === 202) {
          const check = await fetch(`https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/runs/${encodeURIComponent(String(remote.id))}`, { headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(10_000) }).catch(() => null)
          const state = check?.ok ? await check.json().catch(() => ({})) as Row : null
          const stopped = String(state?.status || "").toLowerCase() === "completed" && ["cancelled", "canceled"].includes(String(state?.conclusion || "").toLowerCase())
          if (stopped) await releaseWorkerInstanceClaims(db, agent.id, instanceId, "Stale GitHub worker stopped; file returned to the durable queue")
          await db.query(`update drive_agent_runs set status=case when $2 then 'canceled' else status end,summary=$3,completed_at=case when $2 then now() else completed_at end,payload=payload||$4::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [tracked.id, stopped, stopped ? "Stale GitHub worker canceled before replacement dispatch" : "Stale GitHub worker cancellation requested; waiting for its slot to release", JSON.stringify({ githubAbortRequestedAt: new Date().toISOString() })])
          if (!stopped) agentOccupancy += 1
        } else {
          agentOccupancy += 1
        }
      } else {
        agentOccupancy += 1
      }
    }
    const vacancies = Math.max(0, Number(agent.worker_count || 1) - agentOccupancy)
    for (let slot = 0; slot < vacancies && queued < budget; slot += 1) {
      const workerInstanceId = crypto.randomUUID()
      const intent = await db.query(`
        insert into drive_agent_runs(id,agent_id,run_type,status,payload,summary,created_at,updated_at)
        select gen_random_uuid(),$1,'github_dispatch','pending',$2::jsonb,'Durable GitHub dispatch intent queued',now(),now()
        where exists (
          select 1 from drive_migrations m where m.id=$3 and m.status in('running','verifying')
            and greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1))=$4
          for update
        )
        returning id
      `, [agent.id, JSON.stringify({ migrationId: migration.id, workerGeneration: generation, pool: true, workerInstanceId, source: "migration_orchestrator", phase: "created" }), migration.id, generation])
      if (!intent.rows[0]) return queued
      await env.GITHUB_DISPATCH_QUEUE.send({ intentId: intent.rows[0].id }, { contentType: "json" })
      await db.query(`update drive_agent_runs set payload=payload||'{"phase":"queued"}'::jsonb,summary='Queued for independent GitHub dispatch consumer',updated_at=now() where id=$1`, [intent.rows[0].id])
      queued += 1
    }
  }
  return queued
}

async function abortMigrationWorkers(db: Client, migrationId: string, reason: string) {
  const result = await db.query(`
    select r.id,r.status,r.external_run_id,r.payload,a.github_repo_owner,a.github_repo_name,a.github_workflow_file,a.github_ref,a.github_token
    from drive_agent_runs r join drive_agents a on a.id=r.agent_id
    where r.run_type='github_dispatch' and r.status in('pending','running') and (
      r.payload->>'migrationId'=$1::text or exists(select 1 from drive_repair_jobs j where j.id::text=r.job_reference and j.migration_id::text=$1::text)
    )
      and (
        r.payload->>'githubAbortRequestedAt' is not null
        or exists(select 1 from drive_migrations m where m.id::text=$1::text and m.status in('canceled','completed','aborted','failed','verification_failed'))
      )
      and a.provider='github_actions' and a.github_token is not null
  `, [migrationId])
  let canceled = 0
  const warnings: Array<{ runId: string; reason: string }> = []
  for (const run of result.rows) {
    const headers = { Authorization: `Bearer ${run.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }
    let remoteId = String(run.external_run_id || run.payload?.githubRunId || "")
    if (!remoteId) {
      const instanceId = String(run.payload?.workerInstanceId || "")
      if (!instanceId) continue
      const listUrl = `https://api.github.com/repos/${encodeURIComponent(run.github_repo_owner)}/${encodeURIComponent(run.github_repo_name)}/actions/workflows/${encodeURIComponent(String(run.github_workflow_file || "migration-worker.yml").split("/").pop()!)}/runs?event=repository_dispatch&branch=${encodeURIComponent(run.github_ref || "main")}&per_page=100`
      const list = await fetch(listUrl, { headers, signal: AbortSignal.timeout(15_000) }).catch(() => null)
      if (!list?.ok) {
        warnings.push({ runId: String(run.id), reason: `GitHub run lookup failed${list ? ` (HTTP ${list.status})` : " (network error)"}` })
        continue
      }
      const payload = await list.json() as { workflow_runs?: Row[] }
      remoteId = String(payload.workflow_runs?.find((entry) => String(entry.display_title || "").includes(instanceId))?.id || "")
    }
    if (!remoteId) {
      warnings.push({ runId: String(run.id), reason: "Could not resolve the GitHub Actions run" })
      continue
    }
    const cancel = await fetch(`https://api.github.com/repos/${encodeURIComponent(run.github_repo_owner)}/${encodeURIComponent(run.github_repo_name)}/actions/runs/${encodeURIComponent(remoteId)}/cancel`, { method: "POST", headers, signal: AbortSignal.timeout(15_000) }).catch(() => null)
    const abortAt = new Date().toISOString()
    const detail = { githubRunId: remoteId, githubAbortRequestedAt: abortAt }
    const cancellationAccepted = Boolean(cancel?.ok || cancel?.status === 202)
    if (cancellationAccepted) {
      await db.query(`update drive_agent_runs set summary=$2,payload=payload||$3::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [run.id, `${reason}; GitHub cancellation requested`, JSON.stringify(detail)])
    }
    // GitHub can return a conflict when the run finished just before this
    // cancellation. Always inspect the run even when the cancel request was
    // rejected so terminal runs release their durable worker slot.
    const check = await fetch(`https://api.github.com/repos/${encodeURIComponent(run.github_repo_owner)}/${encodeURIComponent(run.github_repo_name)}/actions/runs/${encodeURIComponent(remoteId)}`, { headers, signal: AbortSignal.timeout(10_000) }).catch(() => null)
    const state = check?.ok ? await check.json().catch(() => ({})) as Row : null
    if (String(state?.status || "").toLowerCase() === "completed") {
      const conclusion = String(state?.conclusion || "").toLowerCase()
      const terminalStatus = ["cancelled", "canceled", "cancelled_by_user"].includes(conclusion)
        ? "canceled"
        : conclusion === "success"
          ? "completed"
          : "failed"
      const terminalSummary = terminalStatus === "canceled"
        ? reason
        : `GitHub worker completed as ${conclusion || "unknown"} while cancellation was being reconciled`
      await db.query(`update drive_agent_runs set status=$2,summary=$3,completed_at=now(),external_run_id=$4,payload=payload||$5::jsonb,updated_at=now() where id=$1 and status in('pending','running')`, [run.id, terminalStatus, terminalSummary, remoteId, JSON.stringify({ ...detail, githubStatus: state?.status, githubConclusion: state?.conclusion })])
      if (terminalStatus === "canceled") canceled += 1
      else warnings.push({ runId: String(run.id), reason: terminalSummary })
    } else if (cancellationAccepted) {
      warnings.push({ runId: String(run.id), reason: "GitHub accepted cancellation; waiting for the workflow to stop" })
    } else {
      warnings.push({ runId: String(run.id), reason: `GitHub cancellation failed${cancel ? ` (HTTP ${cancel.status})` : " (network error)"}; run is not yet confirmed terminal` })
    }
  }
  return { matched: result.rowCount || 0, canceled, warnings }
}

async function reconcileCanceledWorkerRepairJobs(db: Client) {
  const jobs = await db.query(`
    select j.id repair_job_id,j.migration_id,greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1)) generation
    from drive_repair_jobs j join drive_migrations m on m.id=j.migration_id
    where j.mode='migration' and j.status='canceled'
      and not exists(select 1 from drive_repair_jobs newer where newer.migration_id=j.migration_id and newer.mode='migration' and (newer.created_at,newer.id)>(j.created_at,j.id))
      and exists(select 1 from drive_agent_runs r where r.run_type='github_dispatch' and r.status in('pending','running')
        and r.payload->>'migrationId'=j.migration_id::text and r.payload->>'pool'='true'
        and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1)))
    order by j.updated_at limit 20
  `)
  const results = []
  for (const job of jobs.rows) {
    await db.query(`
      update drive_agent_runs set payload=payload||jsonb_build_object('githubAbortRequestedAt',coalesce(payload->>'githubAbortRequestedAt',now()::text)),
        summary=case when payload->>'githubAbortRequestedAt' is null then 'GitHub worker stopping; canceled migration worker job' else summary end,updated_at=now()
      where run_type='github_dispatch' and status in('pending','running') and payload->>'migrationId'=$1 and payload->>'pool'='true'
        and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=$2
    `, [job.migration_id, job.generation])
    const queued = await db.query(`
      update drive_agent_runs set status='canceled',summary='Canceled migration worker job; dispatch was not submitted',completed_at=now(),updated_at=now()
      where run_type='github_dispatch' and status='pending' and payload->>'migrationId'=$1 and payload->>'pool'='true'
        and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=$2
        and coalesce(payload->>'phase','created') in('created','queued')
    `, [job.migration_id, job.generation])
    const shutdown = await abortMigrationWorkers(db, String(job.migration_id), `Migration worker job ${job.repair_job_id} was canceled`)
    results.push({ migrationId: job.migration_id, matched: shutdown.matched, canceled: shutdown.canceled, queuedCanceled: queued.rowCount || 0, warnings: shutdown.warnings })
  }
  return results
}

async function reconcileOrphanedGitHubDispatches(db: Client) {
  // Migration rows can be removed after a user deletes a completed/canceled
  // migration. Keep dispatch records independently recoverable: otherwise a
  // GitHub workflow that already stopped can remain `running` forever, and an
  // actually live workflow can keep consuming a worker slot without an owner.
  const orphaned = await db.query(`
    select r.payload->>'migrationId' migration_id,min(r.updated_at) oldest_updated_at
    from drive_agent_runs r
    join drive_agents a on a.id=r.agent_id and a.provider='github_actions' and a.github_token is not null
    where r.run_type='github_dispatch' and r.status in('pending','running')
      and nullif(r.payload->>'migrationId','') is not null
      and not exists(
        select 1 from drive_migrations m
        where m.id::text=r.payload->>'migrationId' and m.status in('running','verifying')
      )
    group by r.payload->>'migrationId'
    order by min(r.updated_at)
    limit 10
  `)
  const results = []
  for (const row of orphaned.rows) {
    const migrationId = String(row.migration_id || "")
    if (!migrationId) continue
    await db.query(`
      update drive_agent_runs
      set payload=payload||jsonb_build_object('githubAbortRequestedAt',coalesce(payload->>'githubAbortRequestedAt',now()::text)),
        summary=case when payload->>'githubAbortRequestedAt' is null then 'Migration is missing or terminal; stopping orphaned GitHub worker' else summary end,
        updated_at=now()
      where run_type='github_dispatch' and status in('pending','running') and payload->>'migrationId'=$1
    `, [migrationId])
    const shutdown = await abortMigrationWorkers(db, migrationId, `Migration ${migrationId} is missing or terminal; stopping orphaned GitHub workers`)
    results.push({ migrationId, ...shutdown })
  }
  return results
}

async function reconcileGitHubIntent(db: Client, intent: Row, agent: Row) {
  const instanceId = String(intent.payload?.workerInstanceId || "")
  if (!instanceId) throw new Error("Dispatch intent is missing workerInstanceId")
  const response = await fetch(`https://api.github.com/repos/${encodeURIComponent(agent.github_repo_owner)}/${encodeURIComponent(agent.github_repo_name)}/actions/workflows/${encodeURIComponent(String(agent.github_workflow_file || "migration-worker.yml").split("/").pop()!)}/runs?event=repository_dispatch&branch=${encodeURIComponent(agent.github_ref || "main")}&per_page=50`, {
    headers: { Authorization: `Bearer ${agent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator" }, signal: AbortSignal.timeout(15_000),
  })
  if (!response.ok) throw new Error(`GitHub reconciliation HTTP ${response.status}`)
  const payload = await response.json() as { workflow_runs?: Array<{ id?: number; display_title?: string; html_url?: string; status?: string; conclusion?: string | null }> }
  const match = payload.workflow_runs?.find((run) => String(run.display_title || "").includes(instanceId))
  if (!match?.id) return false
  const terminal = String(match.status || "").toLowerCase() === "completed"
  const successful = terminal && String(match.conclusion || "").toLowerCase() === "success"
  const runStatus = successful ? "completed" : terminal ? "failed" : "running"
  const summary = successful
    ? "GitHub workflow completed successfully"
    : terminal
      ? `GitHub workflow finished with conclusion ${match.conclusion || "unknown"}`
      : "GitHub workflow reconciled by instance id"
  await db.query(`update drive_agent_runs set external_run_id=$2,status=$3,payload=payload||$4::jsonb,summary=$5,completed_at=case when $3 in('completed','failed') then now() else completed_at end,updated_at=now() where id=$1`, [intent.id, String(match.id), runStatus, JSON.stringify({ phase: "reconciled", htmlUrl: match.html_url || null, workflowStatus: match.status || null, conclusion: match.conclusion || null }), summary])
  return true
}

async function consumeDispatch(env: Env, intentId: string, attempts: number) {
  return database(env, async (db) => {
    await db.query("begin")
    let transactionOpen = true
    try {
    const lock = await db.query(`select pg_try_advisory_lock(hashtext($1)) acquired`, [intentId])
    if (lock.rows[0]?.acquired !== true) { await db.query("commit"); transactionOpen = false; return "awaiting_reconciliation" }
    const result = await db.query(`select r.*,a.github_repo_owner,a.github_repo_name,a.github_workflow_file,a.github_ref,a.github_token,a.status agent_status from drive_agent_runs r join drive_agents a on a.id=r.agent_id where r.id=$1 for update of r`, [intentId])
    const intent = result.rows[0]
    if (!intent || intent.external_run_id || ["completed", "failed", "canceled"].includes(intent.status)) { await db.query("commit"); transactionOpen = false; return "terminal" }
    let endpointConfiguration: Row | null = null
    if (intent.payload?.pool === true) {
      const current = await db.query(`select m.status,greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1)) generation,
        exists(select 1 from drive_repair_jobs j where j.migration_id=m.id and j.mode='migration' and j.status='canceled'
          and not exists(select 1 from drive_repair_jobs newer where newer.migration_id=j.migration_id and newer.mode='migration' and (newer.created_at,newer.id)>(j.created_at,j.id))) pool_stopped
        from drive_migrations m where m.id=$1 for update`, [intent.payload.migrationId])
      const migration = current.rows[0]
      const intentGeneration = integer(intent.payload.workerGeneration, 1, 1, 1000000)
      if (!migration || migration.pool_stopped || !["running", "verifying"].includes(String(migration.status)) || Number(migration.generation) !== intentGeneration) {
        await db.query(`update drive_agent_runs set status='canceled',summary='Superseded migration worker-pool generation; dispatch skipped',completed_at=now(),updated_at=now() where id=$1 and status='pending'`, [intent.id])
        await db.query("commit"); transactionOpen = false
        return "terminal"
      }
      const configuration = await db.query(`select key,value from drive_app_settings where key in('migration-orchestrator','migration-workers')`)
      const orchestration = configuration.rows.find((row) => row.key === "migration-orchestrator")?.value || {}
      endpointConfiguration = orchestration
      const workerSettings = configuration.rows.find((row) => row.key === "migration-workers")?.value || {}
      if (!(await githubWorkerSecretsAreCurrent(orchestration, workerSettings))) {
        await db.query("commit"); transactionOpen = false
        throw new Error("GitHub migration-worker URL or secret is not synchronized to the current configuration; dispatch is paused")
      }
    }
    if (intent.agent_status === "disabled" || !intent.github_token) throw new Error("Registered workflow is disabled or missing its GitHub token")
    if (await reconcileGitHubIntent(db, intent, intent)) { await db.query("commit"); transactionOpen = false; return "reconciled" }
    const phase = String(intent.payload?.phase || "created")
    const dispatchStartedAt = Date.parse(String(intent.payload?.dispatchStartedAt || ""))
    if (phase === "accepted" || (phase === "dispatching" && Number.isFinite(dispatchStartedAt) && Date.now() - dispatchStartedAt < 5 * 60_000)) { await db.query("commit"); transactionOpen = false; return "awaiting_reconciliation" }
    if (endpointConfiguration && !(await workerEndpointAvailable(db, String(intent.payload.migrationId), endpointConfiguration))) {
      await db.query(`update drive_agent_runs set status='failed',summary='Worker endpoint unavailable; dispatch deferred until recovery',completed_at=now(),updated_at=now() where id=$1 and status='pending'`, [intent.id])
      await db.query("commit"); transactionOpen = false
      return "terminal"
    }
    const workerInstanceId = String(intent.payload.workerInstanceId)
    const codeSync = await syncWorkerRepository({ token: intent.github_token, owner: intent.github_repo_owner, repo: intent.github_repo_name, workflow: intent.github_workflow_file || ".github/workflows/migration-worker.yml", sourceRepo: env.GITHUB_WORKER_SOURCE_REPO, activateActions: intent.agent_status === "dispatch_ready" })
    await db.query(`update drive_agents set github_ref=$2,updated_at=now() where id=$1`, [intent.agent_id, codeSync.defaultBranch])
    await db.query(`update drive_agent_runs set payload=payload||$2::jsonb where id=$1`, [intent.id, JSON.stringify({ sourceCommit: codeSync.sourceSha, workerCommit: codeSync.targetSha, ref: codeSync.defaultBranch })])
    await db.query(`update drive_agent_runs set payload=payload||$2::jsonb,summary='Submitting GitHub workflow dispatch',updated_at=now() where id=$1`, [intent.id, JSON.stringify({ phase: "dispatching", dispatchStartedAt: new Date().toISOString(), dispatchAttempt: attempts })])
    const response = await fetch(`https://api.github.com/repos/${encodeURIComponent(intent.github_repo_owner)}/${encodeURIComponent(intent.github_repo_name)}/dispatches`, {
      method: "POST", headers: { Authorization: `Bearer ${intent.github_token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "Drive-Migration-Orchestrator", "Content-Type": "application/json" },
      body: JSON.stringify({ event_type: "drive-migration-worker", client_payload: { migration_id: intent.payload.migrationId, worker_generation: intent.payload.workerGeneration, agent_id: intent.agent_id, worker_instance_id: workerInstanceId, workflow_file: intent.github_workflow_file || ".github/workflows/migration-worker.yml", code_ref: codeSync.targetSha, max_runtime_seconds: GITHUB_WORKER_MAX_RUNTIME_SECONDS } }), signal: AbortSignal.timeout(20_000),
    })
    if (!response.ok) throw new Error(`GitHub dispatch HTTP ${response.status}`)
    await db.query(`update drive_agent_runs set payload=payload||$2::jsonb,summary='GitHub accepted workflow dispatch; awaiting run reconciliation',updated_at=now() where id=$1`, [intent.id, JSON.stringify({ phase: "accepted", acceptedAt: new Date().toISOString() })])
    await db.query("commit"); transactionOpen = false
    return "accepted"
    } catch (error) {
      if (transactionOpen) await db.query("rollback").catch(() => undefined)
      throw error
    }
  })
}
async function wakeFileScanner(db: Client) {
  const result = await db.query(`select value from drive_app_settings where key='migration-orchestrator' limit 1`)
  const settings = result.rows[0]?.value || {}
  if (!settings.fileScannerUrl || !settings.fileScannerSecret) return "not_configured"
  try {
    const base = String(settings.fileScannerUrl).replace(/\/+$/, "")
    const headers = { Authorization: `Bearer ${settings.fileScannerSecret}` }
    const response = await fetch(`${base}/run`, { method: "POST", headers, signal: AbortSignal.timeout(8_000) })
    if (response.ok) return "signaled"
    if (response.status !== 404) return `http_${response.status}`
    const compatibility = await fetch(`${base}/wake`, { method: "POST", headers, signal: AbortSignal.timeout(8_000) })
    return compatibility.ok ? "signaled_compatibility" : `http_${compatibility.status}`
  } catch { return "deferred_to_cron" }
}
async function complete(db: Client, owner: string, migrationId: string | null, result: Row) {
  await db.query(`update drive_migration_orchestrator_state set status='idle',lease_owner=null,lease_expires_at=null,last_completed_at=now(),last_error=null,last_migration_id=$1,last_result=$2::jsonb,cycle_count=cycle_count+1,updated_at=now() where id=true and lease_owner=$3`, [migrationId, JSON.stringify(result), owner])
  return result
}

async function workerAuthorized(db: Client, agentId: string, token: unknown): Promise<Row | null> {
  if (typeof token !== "string" || token.length < 24 || token.length > MAX_SECRET_LENGTH) return null
  const result = await db.query(`select id,name,status,capabilities from drive_agents where id=$1 limit 1`, [agentId])
  const agent = result.rows[0]
  if (!agent || agent.status === "disabled") return null
  const settings = await db.query(`select value from drive_app_settings where key='migration-workers' limit 1`)
  const value = settings.rows[0]?.value || {}
  const expected = String(value.sharedSecret || "")
  const currentMatches = expected.length >= 24 && expected.length <= MAX_SECRET_LENGTH && safeEqual(token, expected)
  const previousMatches = Array.isArray(value.previousSharedSecrets) && value.previousSharedSecrets.some((entry: Row) => {
    const secret = String(entry?.secret || "")
    const expires = Date.parse(String(entry?.expiresAt || ""))
    return secret.length >= 24 && secret.length <= MAX_SECRET_LENGTH && Number.isFinite(expires) && expires > Date.now() && safeEqual(token, secret)
  })
  return currentMatches || previousMatches ? agent : null
}

async function workerPayload(db: Client, job: Row) {
  const migrationResult = await db.query(`select id,source_account_id,target_account_id,options from drive_migrations where id=$1 limit 1`, [job.migration_id])
  const migration = migrationResult.rows[0]
  if (!migration) throw new Error("Migration not found")
  const accounts = await db.query(`select id,cloudflare_account_id,r2_access_key_id,r2_secret_access_key from drive_accounts where id in($1,$2)`, [migration.source_account_id, migration.target_account_id])
  const source = accounts.rows.find((row) => row.id === migration.source_account_id)
  const target = accounts.rows.find((row) => row.id === migration.target_account_id)
  if (!source?.cloudflare_account_id || !target?.cloudflare_account_id) throw new Error("Migration accounts are incomplete")
  const requested = new Set<string>((Array.isArray(job.payload?.itemIds) ? job.payload.itemIds : [])
    .filter((itemId: unknown): itemId is string => typeof itemId === "string" && /^[0-9a-f-]{36}$/i.test(itemId)))
  const items = await db.query(`select id,source_bucket,target_bucket,source_objects,source_bytes,slurper_status,progress
    from drive_migration_items where migration_id=$1 and ($2::uuid[] is null or id=any($2::uuid[])) order by created_at`,
    [migration.id, requested.size ? [...requested] : null])
  const selected = items.rows
  return {
    job: { id: job.id, mode: job.mode, migrationId: migration.id, verifyAllBuckets: true, strictCompletion: true, kind: job.payload?.kind, progress: job.progress || {} },
    ...(job.payload?.workerShard ? { workerShard: job.payload.workerShard } : {}),
    ...(typeof job.payload?.workerGeneration === "number" ? { workerGeneration: job.payload.workerGeneration } : {}),
    ...(Array.isArray(job.payload?.inventoryObjects) ? { inventoryObjects: job.payload.inventoryObjects } : {}),
    migration: { id: migration.id, options: migration.options || {}, pathPrefix: migration.options?.pathPrefix || null },
    source: { accountId: source.cloudflare_account_id, accessKeyId: source.r2_access_key_id, secretAccessKey: source.r2_secret_access_key },
    target: { accountId: target.cloudflare_account_id, accessKeyId: target.r2_access_key_id, secretAccessKey: target.r2_secret_access_key },
    items: selected.map((item) => ({ id: item.id, sourceBucket: item.source_bucket, targetBucket: item.target_bucket, sourceObjects: Number(item.source_objects || 0), sourceBytes: Number(item.source_bytes || 0), slurperStatus: item.slurper_status, progress: item.progress || {} })),
  }
}

async function workerRequest(request: Request, env: Env, path: string) {
  const body = await request.json().catch(() => ({})) as Row
  if (path === "/workers/register") {
    return database(env, async (db) => {
      const token = String(body.token || "").trim()
      const instanceId = String(body.instanceId || "").trim()
      if (!/^[0-9a-f-]{36}$/i.test(instanceId)) return json({ error: "Valid worker instanceId is required" }, 400)
      const settings = await db.query(`select value from drive_app_settings where key='migration-workers' limit 1`)
      const value = settings.rows[0]?.value || {}
      const expected = String(value.sharedSecret || "")
      const currentMatches = expected.length >= 24 && expected.length <= MAX_SECRET_LENGTH && safeEqual(token, expected)
      const previousMatches = Array.isArray(value.previousSharedSecrets) && value.previousSharedSecrets.some((entry: Row) => {
        const secret = String(entry?.secret || "")
        const expires = Date.parse(String(entry?.expiresAt || ""))
        return secret.length >= 24 && secret.length <= MAX_SECRET_LENGTH && Number.isFinite(expires) && expires > Date.now() && safeEqual(token, secret)
      })
      if (!currentMatches && !previousMatches) {
        return json({ error: "Invalid worker secret" }, 401)
      }
      const requestedAgentId = /^[0-9a-f-]{36}$/i.test(String(body.agentId || "")) ? String(body.agentId) : ""
      const existing = await db.query(
        `select id from drive_agents where runtime_instance_id=$1 or ($2::uuid is not null and id=$2::uuid) order by runtime_instance_id=$1 desc limit 1`,
        [instanceId, requestedAgentId || null]
      )
      const agentId = existing.rows[0]?.id || crypto.randomUUID()
      const capabilities = Array.isArray(body.capabilities) ? body.capabilities : []
      if (existing.rows[0]) {
        await db.query(
          `update drive_agents set runtime_instance_id=$2,status='online',last_heartbeat_at=now(),last_seen_host=$3,last_seen_version=$4,capabilities=$5::jsonb,metadata=coalesce(metadata,'{}'::jsonb)||'{"temporaryRuntime":true}'::jsonb,updated_at=now(),last_error=null where id=$1`,
          [agentId, instanceId, String(body.host || "").slice(0, 255) || null, String(body.version || "").slice(0, 80) || null, JSON.stringify(capabilities)]
        )
      } else {
        await db.query(
          `insert into drive_agents(id,name,category,provider,status,capabilities,runtime_instance_id,last_heartbeat_at,last_seen_host,last_seen_version,metadata,created_at,updated_at) values($1,$2,'worker','self_hosted','online',$3::jsonb,$4,now(),$5,$6,'{"temporaryRuntime":true}'::jsonb,now(),now())`,
          [agentId, String(body.name || "Temporary migration worker").slice(0, 255), JSON.stringify(capabilities), instanceId, String(body.host || "").slice(0, 255) || null, String(body.version || "").slice(0, 80) || null]
        )
      }
      return json({ ok: true, agentId })
    })
  }
  const match = /^\/workers\/([0-9a-f-]{36})(?:\/(heartbeat|retire|claim-job|bootstrap|jobs\/([0-9a-f-]{36})))?$/i.exec(path)
  if (!match) return json({ error: "Not found" }, 404)
  return database(env, async (db) => {
    const agent = await workerAuthorized(db, match[1], body.token)
    if (!agent) return json({ error: "Invalid worker secret" }, 401)
    const action = match[2] || "register"
    if (action === "bootstrap") {
      const migrationId = typeof body.migrationId === "string" ? body.migrationId : ""
      const migration = (await db.query(`select id,status,greatest(1,coalesce(nullif(options->>'workerGeneration','')::int,1)) generation from drive_migrations where id=$1 limit 1`, [migrationId])).rows[0]
      if (!migration || !["running", "verifying"].includes(String(migration.status))) return json({ error: "Migration is missing or no longer active" }, 409)
      const requestedGeneration = Number(body.generation || 0)
      if (Number.isInteger(requestedGeneration) && requestedGeneration > 0 && requestedGeneration !== Number(migration.generation)) return json({ error: "Worker generation has been superseded" }, 409)
      if (!Array.isArray(agent.capabilities) || !agent.capabilities.includes("bulk_migrate")) return json({ error: "Worker is not registered for migration pool work" }, 409)
      const settings = (await db.query(`select value from drive_app_settings where key='migration-workers' limit 1`)).rows[0]?.value || {}
      const queueDatabaseUrl = String(settings.queueDatabaseUrl || "").trim()
      const queueDatabaseSsl = settings.queueDatabaseSsl !== false
      let parsed: URL | null = null
      try { parsed = new URL(queueDatabaseUrl) } catch { /* validated below */ }
      if (!parsed || !["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || !parsed.username || !parsed.password) return json({ error: "Migration queue database is not configured or is invalid" }, 503)
      return json({ ok: true, migrationId, generation: Number(migration.generation), queueDatabaseUrl, queueDatabaseSsl }, 200)
    }
    const now = new Date().toISOString()
    if (action === "register" || action === "heartbeat") {
      await db.query(`update drive_agents set status='online',last_heartbeat_at=now(),last_seen_host=$2,last_seen_version=$3,capabilities=case when jsonb_array_length($4::jsonb)>0 then $4::jsonb else capabilities end,metadata=coalesce(metadata,'{}'::jsonb)||$5::jsonb,updated_at=now(),last_error=null where id=$1 and status<>'disabled'`, [agent.id, String(body.host || "").slice(0, 255) || null, String(body.version || "").slice(0, 80) || null, JSON.stringify(Array.isArray(body.capabilities) ? body.capabilities : []), JSON.stringify(body.metadata && typeof body.metadata === "object" ? body.metadata : {})])
      const instanceId = typeof body.metadata?.workerInstanceId === "string" ? body.metadata.workerInstanceId : ""
      if (instanceId) {
        await db.query(`update drive_agent_runs set status='running',updated_at=now() where agent_id=$1 and payload->>'workerInstanceId'=$2 and status in('pending','running')`, [agent.id, instanceId])
      }
      const currentJobId = typeof body.currentJobId === "string" ? body.currentJobId : ""
      const claimToken = typeof body.claimToken === "string" ? body.claimToken : ""
      if (currentJobId && claimToken) {
        const renewed = await db.query(`update drive_repair_jobs set last_heartbeat_at=now(),updated_at=now() where id=$1 and claimed_by_agent_id=$2 and claim_token=$3::uuid and status in('claimed','running') returning id`, [currentJobId, agent.id, claimToken])
        if (!renewed.rowCount) {
          const currentJob = (await db.query(`select status from drive_repair_jobs where id=$1 and claimed_by_agent_id=$2`, [currentJobId, agent.id])).rows[0]
          if (currentJob?.status === "canceled") return json({ ok: true, agentId: agent.id, canceled: true })
          return json({ error: "This job is no longer owned by this worker" }, 409)
        }
      }
      return json({ ok: true, agentId: agent.id })
    }
    if (action === "retire") {
      const instanceId = typeof body.workerInstanceId === "string" ? body.workerInstanceId : ""
      if (!instanceId) return json({ error: "workerInstanceId is required" }, 400)
      const reason = String(body.reason || "worker_exit").slice(0, 120)
      const retired = await db.query(`
        update drive_agent_runs set status='completed',summary=$3,completed_at=now(),
          payload=payload||jsonb_build_object('phase','retired','retiredAt',now(),'retireReason',$3::text),updated_at=now()
        where agent_id=$1 and payload->>'workerInstanceId'=$2 and status in('pending','running')
        returning id
      `, [agent.id, instanceId, `Worker retired cleanly: ${reason}`])
      return json({ ok: true, agentId: agent.id, retiredRuns: retired.rowCount || 0 })
    }
    if (action === "claim-job") {
      const migrationId = typeof body.migrationId === "string" ? body.migrationId : ""
      const requestedJobId = typeof body.jobId === "string" ? body.jobId : ""
      if (!migrationId || (body.pool !== true && !requestedJobId)) return json({ error: "Migration worker claims require a migration id and either pool or job id" }, 409)
      if (!Array.isArray(agent.capabilities) || !agent.capabilities.includes("bulk_migrate")) return json({ error: "Worker is not registered for bulk migrations" }, 409)
      await db.query("begin")
      try {
        const migration = (await db.query(`select options,status from drive_migrations where id=$1 and ($2::boolean=false or coalesce(options->>'executionMode','')='migration_workers') for update`, [migrationId, body.pool === true])).rows[0]
        if (!migration || !["running", "verifying"].includes(String(migration.status))) {
          await db.query("commit")
          return json({ ok: true, job: null, poolComplete: true, poolStatus: migration?.status || "missing" })
        }
        const generation = integer(migration.options?.workerGeneration, 1, 1, 1000000)
        if (body.pool === true) {
          const inventoryReady = await db.query(`
            select not exists(select 1 from drive_migration_items i where i.migration_id=$1
              and (coalesce(i.progress->'migrationInventory'->>'status','')<>'completed'
                or coalesce(i.progress->'migrationQueue'->>'status','')<>'completed')) ready
          `, [migrationId])
          if (inventoryReady.rows[0]?.ready !== true) {
            await db.query("commit")
            return json({ ok: true, job: null, poolComplete: false, poolReason: "inventory_incomplete" })
          }
        }
        const candidate = body.pool === true
          ? await db.query(`select id from drive_repair_jobs where migration_id=$1 and status='pending' and work_key like $2 order by created_at for update skip locked limit 1`, [migrationId, `migration:${migrationId}:generation:${generation}:inventory:%`])
          : await db.query(`select id from drive_repair_jobs where id=$2 and migration_id=$1 and status='pending' for update skip locked`, [migrationId, requestedJobId])
        const job = candidate.rows[0]
        if (!job) {
          const response = body.pool === true
            ? { ok: true, job: null }
            : { ok: true, job: null, poolComplete: true, poolReason: "requested_job_not_claimable" }
          await db.query("commit")
          return json(response)
        }
        const claimed = await db.query(`update drive_repair_jobs set status='running',claimed_by_agent_id=$2,claim_token=gen_random_uuid(),claimed_at=now(),started_at=coalesce(started_at,now()),last_heartbeat_at=now(),summary=$3,payload=coalesce(payload,'{}'::jsonb)||jsonb_build_object('claimedWorkerInstanceId',$4::text),updated_at=now() where id=$1 returning id,migration_id,mode,claim_token,payload,progress`, [job.id, agent.id, `Claimed by ${agent.name}`, typeof body.workerInstanceId === "string" ? body.workerInstanceId : null])
        if (typeof body.workerInstanceId === "string") await db.query(`update drive_agent_runs set job_reference=$2,status='running',updated_at=now() where agent_id=$1 and payload->>'workerInstanceId'=$3 and status in('pending','running')`, [agent.id, job.id, body.workerInstanceId])
        await db.query("commit")
        const claimedJob = claimed.rows[0]
        return json({ ok: true, job: { id: claimedJob.id, migrationId, mode: claimedJob.mode, claimToken: claimedJob.claim_token, payload: claimedJob.payload || {} }, payload: await workerPayload(db, claimedJob) })
      } catch (error) { await db.query("rollback").catch(() => undefined); throw error }
    }
    const jobId = match[3]
    const claimToken = typeof body.claimToken === "string" ? body.claimToken : ""
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(claimToken)) return json({ error: "A valid job claim token is required" }, 409)
    const current = (await db.query(`select id,migration_id,mode,status,payload,progress ? 'currentFile' had_current_file from drive_repair_jobs where id=$1 and claimed_by_agent_id=$2 and claim_token=$3::uuid limit 1`, [jobId, agent.id, claimToken || null])).rows[0]
    if (!current) return json({ error: "This job is no longer owned by this worker" }, 409)
    if (current.status === "canceled") return json({ ok: true, canceled: true, job: current })
    const status = ["pending", "claimed", "running", "completed", "failed", "canceled"].includes(String(body.status)) ? String(body.status) : current.status
    const releaseForRotation = current.mode === "migration" && status === "pending" && current.status !== "pending"
    const updated = await db.query(`update drive_repair_jobs set status=$3,progress=coalesce(progress,'{}'::jsonb)||$4::jsonb,result=coalesce(result,'{}'::jsonb)||$5::jsonb,summary=coalesce($6,summary),error=case when $9 then null else coalesce($7,error) end,last_heartbeat_at=case when $9 then null else now() end,completed_at=case when $3 in ('completed','failed','canceled') then now() when $9 then null else completed_at end,claimed_by_agent_id=case when $9 then null else claimed_by_agent_id end,claim_token=case when $9 then null else claim_token end,claimed_at=case when $9 then null else claimed_at end,started_at=case when $9 then null else started_at end,updated_at=now() where id=$1 and claimed_by_agent_id=$2 and claim_token=$8::uuid returning id,status,claimed_by_agent_id,last_heartbeat_at,updated_at`, [jobId, agent.id, status, JSON.stringify(body.progress && typeof body.progress === "object" ? body.progress : {}), JSON.stringify(body.result && typeof body.result === "object" ? body.result : {}), typeof body.summary === "string" ? body.summary.slice(0, 2000) : null, typeof body.error === "string" ? body.error.slice(0, 4000) : null, claimToken, releaseForRotation])
    if (releaseForRotation && typeof body.workerInstanceId === "string" && body.workerInstanceId) {
      await db.query(`update drive_agent_runs set status='completed',summary='Worker runtime window completed; unfinished file returned to queue',completed_at=now(),payload=payload||jsonb_build_object('phase','retired','retiredAt',now(),'retireReason','github_runtime_window'),updated_at=now() where agent_id=$1 and payload->>'workerInstanceId'=$2 and status in('pending','running')`, [agent.id, body.workerInstanceId])
    }
    if (!updated.rowCount) return json({ error: "This job is no longer owned by this worker" }, 409)
    // Migration item phase and counters are projections of the durable job
    // rows, never of an individual file worker's local scan/copy/verify loop.
    // That loop is per object and concurrent reports used to race while
    // overwriting the shared bucket status and totals.
    if (current.mode === "migration" && !["completed", "failed", "canceled"].includes(current.status) && ["completed", "failed"].includes(status)) {
      const objectSize = Number(current.payload?.inventoryObjects?.[0]?.size || 0)
      await db.query(`update drive_agent_runs set payload=payload||jsonb_build_object(
        'completedFiles',coalesce((payload->>'completedFiles')::bigint,0)+case when $2='completed' then 1 else 0 end,
        'failedFiles',coalesce((payload->>'failedFiles')::bigint,0)+case when $2='failed' then 1 else 0 end,
        'completedBytes',coalesce((payload->>'completedBytes')::bigint,0)+case when $2='completed' then $3::bigint else 0 end
      ),updated_at=now() where job_reference=$1`, [jobId, status, objectSize])
    }
    await db.query(`update drive_agents set last_heartbeat_at=now(),status=case when $2 in ('completed','failed','canceled') then 'offline' else 'online' end,updated_at=now() where id=$1`, [agent.id, status])
    const terminalTransition = !["completed", "failed", "canceled"].includes(current.status) && ["completed", "failed", "canceled"].includes(status)
    const firstActiveFile = current.had_current_file !== true && body.progress && typeof body.progress === "object" && body.progress.currentFile
    const projectionChanged = releaseForRotation || terminalTransition || Boolean(firstActiveFile)
    if (current.mode === "migration" && projectionChanged) {
      const migration = (await db.query(`select id,options from drive_migrations where id=$1`, [current.migration_id])).rows[0]
      if (migration) {
        const reportedItemIds = Array.isArray(current.payload?.itemIds) ? current.payload.itemIds : []
        const affectedItemIds = Array.from(new Set<string>(reportedItemIds.filter((itemId: unknown): itemId is string => typeof itemId === "string" && /^[0-9a-f-]{36}$/i.test(itemId))))
        await refreshWorkerItemProgress(db, migration, integer(opts(migration).workerGeneration, 1, 1, 1000000), affectedItemIds)
      }
    }
    if (current.mode === "migration" && projectionChanged) await refreshMigrationSnapshot(db, current.migration_id)
    return json({ ok: true, job: updated.rows[0] })
  })
}
async function cycle(env: Env) {
  return database(env, async (db) => {
    const owner = crypto.randomUUID()
    if (!(await acquire(db, owner))) return { ok: true, skipped: "cycle_already_running" }
    let migrationId: string | null = null
    try {
      const setting = await db.query(`select value from drive_app_settings where key='migration-orchestrator' limit 1`)
      const canceledWorkerJobs = await reconcileCanceledWorkerRepairJobs(db)
      const orphanedWorkerRuns = await reconcileOrphanedGitHubDispatches(db)
      if (setting.rows[0]?.value?.migrationEnabled !== true && setting.rows[0]?.value?.enabled !== true) return complete(db, owner, null, { ok: true, skipped: "disabled" })
      let migration = await selectMigration(db)
      if (!migration) return complete(db, owner, null, { ok: true, idle: true, canceledWorkerJobs, orphanedWorkerRuns })
      const terminalWorkerMigration = String(migration.options?.executionMode || "super_slurper") === "migration_workers" && ["failed", "verification_failed"].includes(String(migration.status).toLowerCase())
      if (["canceled", "completed", "aborted"].includes(String(migration.status).toLowerCase()) || terminalWorkerMigration) {
        migrationId = migration.id
        const workers = await abortMigrationWorkers(db, migration.id, `Migration is ${migration.status}; stopping remaining GitHub workers`)
        return complete(db, owner, migration.id, { ok: workers.warnings.length === 0, migrationId: migration.id, workerShutdown: workers })
      }
      if (migration.status === "failed" || migration.status === "verification_failed") {
        const reopened = await db.query(`update drive_migrations set status='running',sync_status='running',sync_message='Resuming durable migration state',completed_at=null,last_synced_at=now(),updated_at=now() where id=$1 and status in('failed','verification_failed') returning *`, [migration.id])
        if (!reopened.rowCount) return complete(db, owner, null, { ok: true, skipped: "migration_state_changed" })
        migration = reopened.rows[0] as Row
        await db.query(`update drive_migration_verification_state v set status='pending',lease_owner=null,lease_expires_at=null,updated_at=now() where v.migration_id=$1 and v.status='failed' and v.last_error ~* $2`, [migration.id, TRANSIENT_SCAN_SQL_PATTERN])
        await db.query(`update drive_migration_items i set slurper_status='verifying',progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{stage}','"retrying_verification"'::jsonb,true),last_progress_at=now(),updated_at=now() where i.migration_id=$1 and exists(select 1 from drive_migration_verification_state v where v.migration_item_id=i.id and v.status='pending' and v.last_error ~* $2)`, [migration.id, TRANSIENT_SCAN_SQL_PATTERN])
        await db.query(`update drive_migration_items i set slurper_status='scanning',progress=jsonb_set(coalesce(i.progress,'{}'::jsonb),'{stage}','"retrying_source_scan"'::jsonb),last_progress_at=now(),updated_at=now() where i.migration_id=$1 and i.slurper_job_id is null and i.slurper_status='precheck_failed' and exists(select 1 from drive_bucket_scans s where s.id::text=i.progress->>'sourceScanId' and s.status='failed' and s.error ~* $2)`, [migration.id, TRANSIENT_SCAN_SQL_PATTERN])
      }
      migrationId = migration.id
      if (opts(migration).executionMode !== "migration_workers") {
        const inventory = await ensureSuperSlurperInventory(db, migration)
        if (inventory.total === 0) {
          const completion = await activateTargetAndCompleteMigration(db, migration)
          return complete(db, owner, migration.id, {
            ok: true,
            migrationId,
            executionMode: "super_slurper",
            noItems: true,
            verification: completion.activated ? "completed" : "canceled",
            missing: 0,
            mismatched: 0,
            extra: 0,
            backendOrchestrator: completion.backendOrchestrator,
          })
        }
        if (inventory.pending > 0) {
          const configuration = setting.rows[0]?.value || {}
          const scannerEnabled = configuration.fileScannerEnabled === true || configuration.enabled === true
          const fileScanner = scannerEnabled ? await wakeFileScanner(db) : "disabled"
          const message = scannerEnabled
            ? fileScanner === "signaled"
              ? `File Scanner is scanning source buckets (${inventory.pending} remaining)`
              : `File Scanner wake ${fileScanner}; retrying source scan (${inventory.pending} remaining)`
            : "Waiting for File Scanner to be enabled before starting Super Slurper jobs"
          await db.query(`update drive_migrations set status='running',sync_status='running',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id, message])
          await renew(db, owner)
          return complete(db, owner, migration.id, { ok: true, migrationId, executionMode: "super_slurper", inventory, fileScanner, waitingFor: "source_inventory" })
        }
        const jobs = await createSuperSlurperJobs(db, migration)
        const slurper = await refreshSuperSlurperProgress(db, migration)
        await finalizeVerifiedBuckets(db, migration, 1)
        const verificationQueue = await db.query(`select
          count(*) filter(where status in('pending','running'))::int pending,
          count(*)::int total from drive_migration_verification_state where migration_id=$1 and generation=1`, [migration.id])
        const queue = verificationQueue.rows[0] || { pending: 0, total: 0 }
        const fileScanner = Number(queue.pending) > 0 ? await wakeFileScanner(db) : "not_needed"
        const verification = Number(slurper.total) > 0 && Number(slurper.completed) === Number(slurper.total)
          ? await finishOrRepair(db, migration, 1)
          : { verification: Number(slurper.failed) > 0 ? "failed" : "waiting_for_super_slurper" }
        await recordItemStageEvents(db, migration, 1)
        if (jobs.active >= jobs.limit && slurper.completed < slurper.total && slurper.failed === 0) {
          await db.query(`update drive_migrations set sync_message=$2 where id=$1 and status in('running','verifying')`, [migration.id, `Waiting for a Super Slurper concurrency slot (${jobs.active}/${jobs.limit} active)`])
        }
        await renew(db, owner)
        await db.query(`update drive_migrations set last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id])
        return complete(db, owner, migration.id, { ok: true, migrationId, executionMode: "super_slurper", inventory, jobs, ...slurper, ...verification, fileScanner })
      }
      const shards = await ensureShards(db, migration)
      if (shards.noItems) {
        const completion = await activateTargetAndCompleteMigration(db, migration)
        return complete(db, owner, migration.id, {
          ok: true,
          migrationId,
          executionMode: "migration_workers",
          noItems: true,
          verification: completion.activated ? "completed" : "canceled",
          missing: 0,
          mismatched: 0,
          extra: 0,
          backendOrchestrator: completion.backendOrchestrator,
        })
      }
      // Keep the migration at the scanner-owned stage until every source
      // inventory page and its corresponding durable file jobs are present.
      // This message is intentionally independent of the worker fleet: a
      // queued job is not runnable until materialization has finished.
      if (shards.inventoryPending || shards.queuePending) {
        const syncMessage = shards.targetBuckets?.failed
          ? "Waiting for Migration Orchestrator to prepare destination buckets"
          : shards.inventoryPending
            ? `Scanning source buckets (${shards.inventoryPending} remaining)`
            : "Building migration queue for migration workers"
        await db.query(`update drive_migrations set sync_status='running',sync_message=$2,last_synced_at=now(),updated_at=now() where id=$1 and status='running'`, [migration.id, syncMessage])
      }
      const queueConfiguration = await db.query(`select value->>'queueDatabaseUrl' queue_url from drive_app_settings where key='migration-workers' limit 1`)
      const directQueueMode = Boolean(String(queueConfiguration.rows[0]?.queue_url || "").trim())
      let queueSeed = { configured: false, complete: false, inserted: 0, synced: 0 }
      let queueMirror = { configured: false, complete: false, inserted: 0, synced: 0 }
      if (directQueueMode) {
        if (!shards.inventoryPending && !shards.queuePending && !shards.terminalFailure) {
          queueSeed = await migrationQueueRequest(db, env, "seed", migration.id, shards.generation)
          // Keep the main database quiet while a large scanner inventory is
          // being copied in seed batches. No worker is dispatched until the
          // whole generation is present, so mirror once seeding is complete.
          if (queueSeed.complete) queueMirror = await migrationQueueRequest(db, env, "sync", migration.id, shards.generation)
        }
      }
      await ensureBucketVerification(db, migration, shards.generation)
      await refreshWorkerItemProgress(db, migration, shards.generation)
      await finalizeVerifiedBuckets(db, migration, shards.generation)
      // Settings follow each bucket's successful verification instead of
      // waiting for the entire migration to finish.
      const bucketSettings = await syncNextBucketSettings(db, migration)
      const recovered = shards.inventoryPending || shards.queuePending ? 0 : await recoverJobs(db, migration.id, shards.generation, shards.shardCount)
      await renew(db, owner)
      const finalized = shards.terminalFailure || shards.inventoryPending || shards.queuePending
        ? { complete: false, terminalFailure: Boolean(shards.terminalFailure), jobs: {} }
        : await finalizeShards(db, migration, shards.generation, shards.shardCount)
      const verification = finalized.complete ? await finishOrRepair(db, migration, shards.generation) : { verification: "waiting_for_shards" }
      await renew(db, owner)
      const pendingVerification = await db.query(`select 1 from drive_migration_verification_state where migration_id=$1 and generation=$2 and status in('pending','running') limit 1`, [migration.id, shards.generation])
      const fileScanner = shards.inventoryPending || (pendingVerification.rowCount || 0) > 0 || (finalized.complete && verification.verification === "pending") ? await wakeFileScanner(db) : "not_needed"
      const current = (await db.query(`select * from drive_migrations where id=$1`, [migration.id])).rows[0]
      if (["completed", "failed", "verification_failed", "canceled", "aborted"].includes(String(current?.status || "").toLowerCase())) {
        await abortMigrationWorkers(db, migration.id, `Migration is ${current.status}; stopping remaining GitHub workers`)
        if (directQueueMode) queueMirror = await migrationQueueRequest(db, env, "sync", migration.id, shards.generation)
      }
      // Keep scanning and copying as separate phases. The worker pool starts
      // only after every source inventory is complete and every scanned object
      // has been materialized into a durable migration job. This prevents a
      // partially scanned repair from looking like an active worker run.
      const inventoryReady = !shards.inventoryPending && !shards.queuePending && (!directQueueMode || queueSeed.complete)
      const hasRunnableFiles = shards.shardCount > 0 || shards.created > 0
      const dispatched = current?.status === "running" && inventoryReady && hasRunnableFiles ? await dispatchWorkers(db, env, current) : 0
      await recordItemStageEvents(db, migration, shards.generation)
      if (shards.inventoryPending || shards.queuePending || (directQueueMode && !queueSeed.complete)) await env.GITHUB_DISPATCH_QUEUE.send({ control: "cycle" })
      await db.query(`update drive_migrations set last_synced_at=now(),updated_at=now() where id=$1 and status in('running','verifying')`, [migration.id])
      return complete(db, owner, migration.id, { ok: true, migrationId, ...shards, migrationQueue: directQueueMode ? { seed: queueSeed, mirror: queueMirror } : null, recovered, finalized, ...verification, bucketSettings, fileScanner, dispatched })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      await db.query(`update drive_migration_orchestrator_state set status='error',lease_owner=null,lease_expires_at=null,last_completed_at=now(),last_error=$1,last_migration_id=$2,last_result=$3::jsonb,updated_at=now() where id=true and lease_owner=$4`, [message, migrationId, JSON.stringify({ ok: false, error: message }), owner]).catch(() => undefined)
      throw error
    }
  })
}

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url)
    if (url.pathname.startsWith("/workers/") && request.method === "POST") {
      try { return await workerRequest(request, env, url.pathname) } catch (error) { return json({ error: error instanceof Error ? error.message : String(error) }, 503) }
    }
    if (url.pathname === "/" || url.pathname === "/health") {
      return json({ ok: true, service: "migration-orchestrator", build: BUILD })
    }
    if (!(await authorized(request, env))) return json({ error: "Unauthorized" }, 401)
    if (url.pathname === "/status" && request.method === "GET") return json(await database(env, async (db) => { const row = await db.query(`select * from drive_migration_orchestrator_state where id=true`); return { ok: true, service: "migration-orchestrator", build: BUILD, state: row.rows[0] || null } }))
    const liveMatch = /^\/migrations\/([0-9a-f-]{36})\/live$/i.exec(url.pathname)
    if (liveMatch && request.method === "GET") return json(await database(env, async (db) => { await ensureSchema(db); return { ok: true, ...(await migrationLiveState(db, liveMatch[1])) } }))
    if (url.pathname === "/wake" && request.method === "POST") {
      await env.GITHUB_DISPATCH_QUEUE.send({ control: "cycle" }, { contentType: "json" })
      return json({ ok: true, queued: true }, 202)
    }
    if (url.pathname === "/run" && request.method === "POST") { try { return json(await cycle(env)) } catch (error) { return json({ ok: false, error: error instanceof Error ? error.message : String(error) }, 503) } }
    return json({ error: "Not found" }, 404)
  },
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    // Free-plan cron invocations have a 10 ms CPU budget. Opening PostgreSQL
    // and reconciling a growing fleet here can terminate the invocation after
    // acquiring its lease, starving new workflows indefinitely. Keep the timer
    // lightweight; the durable queue consumer owns the full scheduling cycle.
    ctx.waitUntil(env.GITHUB_DISPATCH_QUEUE.send({ control: "cycle" }, { contentType: "json" }))
  },
  async queue(batch: MessageBatch<DispatchMessage>, env: Env) {
    for (const message of batch.messages) {
      try {
        if ("control" in message.body) {
          const result = await cycle(env)
          if (result?.skipped === "cycle_already_running") message.retry({ delaySeconds: 2 })
          else message.ack()
          continue
        }
        const outcome = await consumeDispatch(env, message.body.intentId, message.attempts)
        if (outcome === "accepted" || outcome === "awaiting_reconciliation") message.retry({ delaySeconds: 30 })
        else message.ack()
      } catch (error) {
        console.error("GitHub dispatch message failed", "intentId" in message.body ? message.body.intentId : message.body.control, error)
        message.retry({ delaySeconds: Math.min(30 * (2 ** Math.min(message.attempts, 10)), 3600) })
      }
    }
  },
}
