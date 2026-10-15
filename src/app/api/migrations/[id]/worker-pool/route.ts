import { NextResponse } from "next/server"

import { ensureDriveSchema, queryDb } from "@/lib/db"
import { requireAdmin } from "@/lib/server-auth"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { withMigrationQueueDatabase } from "@/lib/migration-queue-database"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

type PoolResult = Awaited<ReturnType<typeof readPool>>
const responseCache = new Map<string, { expiresAt: number; promise: Promise<PoolResult> }>()
const RESPONSE_CACHE_TTL_MS = 15_000
let queueSettingsCache: { expiresAt: number; value: Awaited<ReturnType<typeof getMigrationWorkerSettings>> } | null = null
const dispatchedWorkersCache = new Map<string, { expiresAt: number; value: Array<Record<string, unknown>> }>()

async function getDispatchedWorkers(migrationId: string, generation: number) {
  const key = `${migrationId}:${generation}`
  const now = Date.now()
  const cached = dispatchedWorkersCache.get(key)
  if (cached && cached.expiresAt > now) return cached.value
  const { rows } = await queryDb<Record<string, unknown>>(`select distinct on (r.payload->>'workerInstanceId')
      r.id,r.agent_id,r.status,r.external_run_id,r.job_reference,r.payload,r.created_at,r.updated_at
    from drive_agent_runs r
    where r.run_type='github_dispatch' and r.payload->>'migrationId'=$1
      and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=$2
      and nullif(r.payload->>'workerInstanceId','') is not null
      and r.created_at>now()-interval '24 hours'
    order by r.payload->>'workerInstanceId',r.created_at desc,r.updated_at desc limit 100`, [migrationId, generation])
  const value = rows.map((row) => ({
    id: String(row.payload && typeof row.payload === "object" ? (row.payload as Record<string, unknown>).workerInstanceId || row.id : row.id),
    agentId: row.agent_id,
    status: String(row.status || "pending"),
    online: false,
    instanceId: String(row.payload && typeof row.payload === "object" ? (row.payload as Record<string, unknown>).workerInstanceId || "" : ""),
    jobId: row.job_reference || undefined,
    externalRunId: row.external_run_id || undefined,
    currentStatus: String(row.status || "pending") === "pending" ? "dispatching" : String(row.status || "offline"),
    lastHeartbeatAt: row.payload && typeof row.payload === "object" ? (row.payload as Record<string, unknown>).queueHeartbeatAt || (row.payload as Record<string, unknown>).workerHeartbeatAt : undefined,
    completedJobs: 0,
    failedJobs: 0,
    finishedJobs: 0,
    completedFiles: 0,
    failedFiles: 0,
    completedBytes: 0,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }))
  dispatchedWorkersCache.set(key, { expiresAt: now + 20_000, value })
  if (dispatchedWorkersCache.size > 100) {
    for (const [cacheKey, entry] of dispatchedWorkersCache) if (entry.expiresAt <= now) dispatchedWorkersCache.delete(cacheKey)
  }
  return value
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function hasCompleteSnapshot(snapshot: Record<string, unknown>) {
  return Array.isArray(snapshot.buckets) && Number.isFinite(Number(snapshot.totalJobs))
}

async function readPool(id: string, pageIndex: number, pageSize: number, selectedGeneration: number) {
  const now = Date.now()
  let queueReadFailed = false
  if (!queueSettingsCache || queueSettingsCache.expiresAt <= now) {
    queueSettingsCache = { expiresAt: now + 30_000, value: await getMigrationWorkerSettings() }
  }
  const queueSettings = queueSettingsCache.value
  if (queueSettings.queueDatabaseUrl) {
    try {
    const queueProjection = await withMigrationQueueDatabase(queueSettings.queueDatabaseUrl, async (db) => {
      const meta = (await db.query(`select generation,status,seed_complete,updated_at from drive_migration_queue_meta
        where migration_id=$1 and ($2::int=0 or generation=$2) order by generation desc limit 1`, [id, selectedGeneration])).rows[0]
      if (!meta) return null
      const generation = Number(meta.generation)
      const stats = (await db.query(`select count(*)::bigint total_jobs,
        count(*) filter(where status='pending')::bigint queued_jobs,
        count(*) filter(where status in('claimed','running'))::bigint running_jobs,
        count(*) filter(where status in('pending','claimed','running'))::bigint remaining_jobs,
        count(*) filter(where status='completed')::bigint completed_jobs,
        count(*) filter(where status='failed')::bigint failed_jobs,
        count(*) filter(where status='canceled')::bigint canceled_jobs,
        coalesce(sum(case when status='completed' then case when result->'totals'->>'transferred' ~ '^[0-9]+$' then (result->'totals'->>'transferred')::bigint else coalesce((select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end when status in('claimed','running') then greatest(case when result->'totals'->>'transferred' ~ '^[0-9]+$' then (result->'totals'->>'transferred')::bigint else coalesce((select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end,case when progress->'totals'->>'transferred' ~ '^[0-9]+$' then (progress->'totals'->>'transferred')::bigint when progress->>'transferred' ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end) else 0 end),0)::bigint transferred,
        coalesce(sum(case when result->'items'->0->>'skipped' ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint when status in('claimed','running') and progress->>'skipped' ~ '^[0-9]+$' then (progress->>'skipped')::bigint else 0 end),0)::bigint skipped,
        coalesce(sum(case when result->'items'->0->>'failed' ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint when status='failed' then 1 else 0 end),0)::bigint failed,
        coalesce(sum(case when status='completed' and result->'items'->0->>'transferred' ~ '^[0-9]+$' and (result->'items'->0->>'transferred')::bigint>0 and payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end),0)::bigint completed_bytes,
        coalesce(sum(case when status in('claimed','running') and progress->'currentFile'->>'bytesTransferred' ~ '^[0-9]+$' then (progress->'currentFile'->>'bytesTransferred')::bigint else 0 end),0)::bigint in_progress_bytes,
        coalesce((select sum(greatest(w.runtime_seconds,greatest(0,floor(extract(epoch from (
          coalesce(nullif(w.metadata->>'retiredAt','')::timestamptz,
            case when w.status='online' and w.last_heartbeat_at>now()-interval '90 seconds' then now()
              when w.status='online' then w.last_heartbeat_at+interval '90 seconds' else w.last_heartbeat_at end)
          - coalesce(nullif(w.metadata->>'workerStartedAt','')::timestamptz,w.last_heartbeat_at)))::bigint)))
          from drive_migration_queue_workers w where w.migration_id=$1 and w.generation=$2),0)::bigint worker_runtime_seconds,
        max(updated_at) updated_at
        from drive_migration_queue_jobs where migration_id=$1 and generation=$2`, [id, generation])).rows[0]
      const [page, recent, workers, bucketRows, active, generationRows] = await Promise.all([
        db.query(`select id,status,claimed_by,agent_id,summary,error,created_at,updated_at,last_heartbeat_at,completed_at,
          payload->'inventoryObjects'->0->>'key' object_key,
          case when payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end object_size,
          payload->'items'->0->>'sourceBucket' source_bucket,payload->'items'->0->>'targetBucket' target_bucket,
          case when result->'items'->0->>'transferred' ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint when progress->>'transferred' ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end transferred,
          case when result->'items'->0->>'skipped' ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else 0 end skipped,
          case when result->'items'->0->>'failed' ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint else 0 end failed
          from drive_migration_queue_jobs where migration_id=$1 and generation=$2 order by created_at desc,id desc limit $3 offset $4`, [id, generation, pageSize, pageIndex * pageSize]),
        db.query(`select id,status,payload->'job'->>'mode' mode,progress,result from drive_migration_queue_jobs where migration_id=$1 and generation=$2 order by updated_at desc,id desc limit 20`, [id, generation]),
        db.query(`with worker_totals as (
            select claimed_by worker_instance_id,
              count(*) filter(where status='completed')::bigint completed_jobs,
              count(*) filter(where status='failed')::bigint failed_jobs,
              count(*) filter(where status in('completed','failed','canceled'))::bigint finished_jobs,
              coalesce(sum(case when status in('completed','claimed','running') then greatest(
                case when result->'totals'->>'transferred' ~ '^[0-9]+$' then (result->'totals'->>'transferred')::bigint else coalesce((select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end,
                case when progress->'totals'->>'transferred' ~ '^[0-9]+$' then (progress->'totals'->>'transferred')::bigint when progress->>'transferred' ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end) else 0 end),0)::bigint completed_files,
              coalesce(sum(case when status in('completed','claimed','running','failed') then greatest(
                case when result->'totals'->>'failed' ~ '^[0-9]+$' then (result->'totals'->>'failed')::bigint else coalesce((select sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end) from jsonb_array_elements(case when jsonb_typeof(result->'items')='array' then result->'items' else '[]'::jsonb end) x),0) end,
                case when progress->'totals'->>'failed' ~ '^[0-9]+$' then (progress->'totals'->>'failed')::bigint when progress->>'failed' ~ '^[0-9]+$' then (progress->>'failed')::bigint else 0 end) else 0 end),0)::bigint failed_files,
              coalesce(sum(case when status='completed' and result->'totals'->>'transferred' ~ '^[0-9]+$' and (result->'totals'->>'transferred')::bigint>0 and payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end),0)::bigint completed_bytes
            from drive_migration_queue_jobs where migration_id=$1 and generation=$2 and claimed_by is not null group by claimed_by
          ) select w.worker_instance_id,w.agent_id,w.status,w.current_job_id,w.last_heartbeat_at,w.metadata,
              coalesce(t.completed_jobs,0)::bigint completed_jobs,coalesce(t.failed_jobs,0)::bigint failed_jobs,
              coalesce(t.finished_jobs,0)::bigint finished_jobs,coalesce(t.completed_files,0)::bigint completed_files,
              coalesce(t.failed_files,0)::bigint failed_files,coalesce(t.completed_bytes,0)::bigint completed_bytes
            from drive_migration_queue_workers w left join worker_totals t using(worker_instance_id)
            where w.migration_id=$1 and w.generation=$2 and w.last_heartbeat_at>now()-interval '24 hours'
            order by (w.status='online' and w.last_heartbeat_at>now()-interval '90 seconds') desc,w.last_heartbeat_at desc limit 100`, [id, generation]),
        db.query(`with item_rows as (
            select distinct on (item->>'id') item->>'id' item_id,item->>'sourceBucket' source_bucket,item->>'targetBucket' target_bucket,
              case when item->>'sourceObjects' ~ '^[0-9]+$' then (item->>'sourceObjects')::bigint else 0 end source_objects,
              case when item->>'sourceBytes' ~ '^[0-9]+$' then (item->>'sourceBytes')::bigint else 0 end source_bytes
            from drive_migration_queue_jobs q cross join lateral jsonb_array_elements(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end) item
            where q.migration_id=$1 and q.generation=$2 and nullif(item->>'id','') is not null order by item->>'id',q.created_at
          ), item_totals as (
            select source_bucket,target_bucket,sum(source_objects)::bigint total_objects,sum(source_bytes)::bigint source_bytes from item_rows group by 1,2
          ), job_item_rows as (
            select q.id,q.status,q.updated_at,item->>'sourceBucket' source_bucket,item->>'targetBucket' target_bucket,item->>'id' item_id,
              q.progress->>'currentItemId' current_item_id,
              case when q.progress->>'transferred' ~ '^[0-9]+$' then (q.progress->>'transferred')::bigint else 0 end current_transferred,
              case when q.progress->>'skipped' ~ '^[0-9]+$' then (q.progress->>'skipped')::bigint else 0 end current_skipped,
              coalesce(res.transferred,0)::bigint result_transferred,coalesce(res.failed,0)::bigint result_failed,coalesce(res.skipped,0)::bigint result_skipped,
              coalesce(live.transferred,0)::bigint live_transferred,coalesce(live.failed,0)::bigint live_failed,coalesce(live.skipped,0)::bigint live_skipped
            from drive_migration_queue_jobs q
            cross join lateral jsonb_array_elements(case when jsonb_typeof(q.payload->'items')='array' then q.payload->'items' else '[]'::jsonb end) item
            left join lateral (select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end)::bigint transferred,
                sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end)::bigint failed,
                sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end)::bigint skipped
              from jsonb_array_elements(case when jsonb_typeof(q.result->'items')='array' then q.result->'items' else '[]'::jsonb end) x where x->>'itemId'=item->>'id') res on true
            left join lateral (select sum(case when x->>'transferred' ~ '^[0-9]+$' then (x->>'transferred')::bigint else 0 end)::bigint transferred,
                sum(case when x->>'failed' ~ '^[0-9]+$' then (x->>'failed')::bigint else 0 end)::bigint failed,
                sum(case when x->>'skipped' ~ '^[0-9]+$' then (x->>'skipped')::bigint else 0 end)::bigint skipped
              from jsonb_array_elements(case when jsonb_typeof(q.progress->'itemProgress')='array' then q.progress->'itemProgress' else '[]'::jsonb end) x where x->>'itemId'=item->>'id') live on true
            where q.migration_id=$1 and q.generation=$2 and nullif(item->>'id','') is not null
          ), job_totals as (
            select source_bucket,target_bucket,count(*) filter(where status='pending')::bigint queued_objects,
              count(*) filter(where status in('claimed','running'))::bigint running_jobs,
              coalesce(sum(case when status='completed' then result_transferred else greatest(result_transferred,live_transferred,
                case when current_item_id=item_id then current_transferred else 0 end) end),0)::bigint transferred_objects,
              coalesce(sum(case when status='completed' then result_failed else greatest(result_failed,live_failed) end),0)::bigint failed_objects,
              coalesce(sum(case when status='completed' then result_skipped else greatest(result_skipped,live_skipped,
                case when current_item_id=item_id then current_skipped else 0 end) end),0)::bigint skipped_objects,
              max(updated_at) updated_at
            from job_item_rows group by source_bucket,target_bucket
          ) select coalesce(i.source_bucket,j.source_bucket) source_bucket,coalesce(i.target_bucket,j.target_bucket) target_bucket,
              coalesce(i.total_objects,0)::bigint total_objects,coalesce(j.queued_objects,0)::bigint queued_objects,
              coalesce(j.running_jobs,0)::bigint running_objects,coalesce(j.transferred_objects,0)::bigint transferred_objects,
              coalesce(j.failed_objects,0)::bigint failed_objects,coalesce(j.skipped_objects,0)::bigint skipped_objects,
              coalesce(i.source_bytes,0)::bigint source_bytes,j.updated_at
            from item_totals i full outer join job_totals j using(source_bucket,target_bucket)
            order by coalesce(i.source_bucket,j.source_bucket),coalesce(i.target_bucket,j.target_bucket)`, [id, generation]),
        db.query(`select id,claimed_by,last_heartbeat_at,progress,payload->'job'->>'mode' mode from drive_migration_queue_jobs where migration_id=$1 and generation=$2 and status in('claimed','running') and claimed_by is not null and last_heartbeat_at>now()-interval '90 seconds' and jsonb_typeof(progress->'currentFile')='object' and progress->'currentFile'->>'key' is not null order by updated_at desc limit 100`, [id, generation]),
        db.query(`select m.generation,m.status,m.seed_complete,m.created_at,m.updated_at,
            coalesce(j.total_jobs,0)::bigint total_jobs,coalesce(j.queued_jobs,0)::bigint queued_jobs,
            coalesce(j.running_jobs,0)::bigint running_jobs,coalesce(j.remaining_jobs,0)::bigint remaining_jobs,
            coalesce(j.completed_jobs,0)::bigint completed_jobs,coalesce(j.failed_jobs,0)::bigint failed_jobs,
            coalesce(j.canceled_jobs,0)::bigint canceled_jobs,coalesce(w.worker_count,0)::bigint worker_count,
            coalesce(w.online_workers,0)::bigint online_workers
          from drive_migration_queue_meta m
          left join lateral (select count(*)::bigint total_jobs,
              count(*) filter(where status='pending')::bigint queued_jobs,
              count(*) filter(where status in('claimed','running'))::bigint running_jobs,
              count(*) filter(where status in('pending','claimed','running'))::bigint remaining_jobs,
              count(*) filter(where status='completed')::bigint completed_jobs,
              count(*) filter(where status='failed')::bigint failed_jobs,
              count(*) filter(where status='canceled')::bigint canceled_jobs
            from drive_migration_queue_jobs where migration_id=m.migration_id and generation=m.generation) j on true
          left join lateral (select count(*)::bigint worker_count,
              count(*) filter(where status='online' and last_heartbeat_at>now()-interval '90 seconds')::bigint online_workers
            from drive_migration_queue_workers where migration_id=m.migration_id and generation=m.generation) w on true
          where m.migration_id=$1 order by m.generation desc limit 100`, [id]),
      ])
      const total = Number(stats.rows[0]?.total_jobs || 0)
      const onlineWorkers = workers.rows.filter((row) => row.status === "online" && Date.now() - Date.parse(row.last_heartbeat_at) < 90_000).length
      const migrationStatus = String(meta.status || "").toLowerCase()
      const buckets = bucketRows.rows.map((row, index) => ({ id: `${row.source_bucket || ""}:${row.target_bucket || ""}:${index}`, sourceBucket: row.source_bucket || "", targetBucket: row.target_bucket || "", status: Number(row.running_objects) ? "running" : Number(row.queued_objects) ? "dispatching" : Number(row.failed_objects) ? "failed" : Number(row.canceled_jobs) && !Number(row.completed_jobs) ? "canceled" : "completed", totalObjects: Number(row.total_objects), queuedObjects: Number(row.queued_objects), runningObjects: Number(row.running_objects), transferredObjects: Number(row.transferred_objects), skippedObjects: Number(row.skipped_objects), failedObjects: Number(row.failed_objects), sourceBytes: Number(row.source_bytes), updatedAt: row.updated_at }))
      const latestQueueUpdate = stats.rows[0]?.updated_at ? Date.parse(stats.rows[0].updated_at) : 0
      const latestMetaUpdate = meta.updated_at ? Date.parse(meta.updated_at) : 0
      const updatedAt = new Date(Math.max(latestQueueUpdate, latestMetaUpdate) || Date.now()).toISOString()
      const queueAttempts = generationRows.rows.map((row) => {
        const generationStatus = String(row.status || "").toLowerCase()
        const generationOnline = Number(row.online_workers || 0)
        const generationRunning = Number(row.running_jobs || 0)
        const generationRemaining = Number(row.remaining_jobs || 0)
        const failed = ["failed", "verification_failed", "canceled", "cancelled", "aborted"].includes(generationStatus)
        const complete = generationStatus === "completed"
        const poolStatus = failed ? "failed" : complete ? "completed" : generationOnline > 0 || generationRunning > 0 ? "running" : row.seed_complete && generationRemaining > 0 ? "dispatching" : "processing"
        return { generation: Number(row.generation), status: poolStatus, workerCount: Number(row.worker_count || 0), runningWorkers: generationOnline, onlineWorkers: generationOnline, totalJobs: Number(row.total_jobs || 0), queuedJobs: Number(row.queued_jobs || 0), runningJobs: generationRunning, remainingJobs: generationRemaining, completedJobs: Number(row.completed_jobs || 0), failedJobs: Number(row.failed_jobs || 0), canceledJobs: Number(row.canceled_jobs || 0), createdAt: row.created_at, updatedAt: row.updated_at }
      })
      const jobPage = page.rows.map((row) => ({ id: row.id, status: row.status, claimedByAgentId: row.agent_id || row.claimed_by, summary: row.summary, error: row.error, createdAt: row.created_at, updatedAt: row.updated_at, lastHeartbeatAt: row.last_heartbeat_at, completedAt: row.completed_at, objectKey: row.object_key || "", objectSize: Number(row.object_size || 0), sourceBucket: row.source_bucket || "", targetBucket: row.target_bucket || "", transferred: Number(row.transferred || 0), skipped: Number(row.skipped || 0), failed: Number(row.failed || 0) }))
      const workerRuns: Array<Record<string, unknown>> = workers.rows.map((row) => {
        const online = row.status === "online" && Date.now() - Date.parse(row.last_heartbeat_at) < 90_000
        return { id: row.worker_instance_id, agentId: row.agent_id, status: online ? "running" : "offline", online, instanceId: row.worker_instance_id, jobId: row.current_job_id, currentStatus: row.current_job_id ? "running" : online ? "idle" : "offline", startedAt: row.metadata?.workerStartedAt, retiredAt: row.metadata?.retiredAt, lastHeartbeatAt: row.last_heartbeat_at, completedJobs: Number(row.completed_jobs || 0), failedJobs: Number(row.failed_jobs || 0), finishedJobs: Number(row.finished_jobs || 0), completedFiles: Number(row.completed_files || 0), failedFiles: Number(row.failed_files || 0), completedBytes: Number(row.completed_bytes || 0), updatedAt: row.last_heartbeat_at }
      })
      let dispatchedWorkers: Array<Record<string, unknown>> = []
      try { dispatchedWorkers = await getDispatchedWorkers(id, generation) }
      catch (error) { console.error("Migration worker dispatch projection failed", error instanceof Error ? error.message : String(error)) }
      const selectedPool = queueAttempts.find((attempt) => attempt.generation === generation)
      if (selectedPool) selectedPool.workerCount = Math.max(selectedPool.workerCount, dispatchedWorkers.length)
      const queueWorkerIds = new Set(workerRuns.map((worker) => worker.instanceId))
      for (const dispatched of dispatchedWorkers) {
        if (!queueWorkerIds.has(dispatched.instanceId)) workerRuns.push(dispatched)
      }
      return { snapshot: { migrationId: id, workerGeneration: generation, totalJobs: total, queuedJobs: Number(stats.rows[0]?.queued_jobs || 0), runningJobs: Number(stats.rows[0]?.running_jobs || 0), remainingJobs: Number(stats.rows[0]?.remaining_jobs || 0), completedJobs: Number(stats.rows[0]?.completed_jobs || 0), failedJobs: Number(stats.rows[0]?.failed_jobs || 0), canceledJobs: Number(stats.rows[0]?.canceled_jobs || 0), totalObjects: buckets.reduce((sum, bucket) => sum + bucket.totalObjects, 0), transferred: Number(stats.rows[0]?.transferred || 0), inProgressFiles: active.rowCount || 0, completedBytes: Number(stats.rows[0]?.completed_bytes || 0), inProgressBytes: Number(stats.rows[0]?.in_progress_bytes || 0), workerRuntimeSeconds: Number(stats.rows[0]?.worker_runtime_seconds || 0), skipped: Number(stats.rows[0]?.skipped || 0), failed: Number(stats.rows[0]?.failed || 0), onlineWorkers, activeTransfers: active.rowCount || 0, buckets, updatedAt, sampledAt: new Date().toISOString() }, snapshotUpdatedAt: updatedAt, jobs: recent.rows, liveFiles: active.rows.map((row) => ({ ...(row.progress?.currentFile || {}), jobId: row.id, workerId: row.claimed_by, lastHeartbeatAt: row.last_heartbeat_at, operation: row.mode })), jobPage, jobPagination: { pageIndex, pageSize, pageCount: Math.max(1, Math.ceil(total / pageSize)), total }, attempts: queueAttempts, workerRuns, selectedGeneration: generation, migrationStatus: meta.status, dataSource: "queue_database" }
    }, queueSettings.queueDatabaseSsl)
    if (queueProjection) return queueProjection
    } catch (error) {
      queueReadFailed = true
      console.error("Migration worker-pool queue database projection failed", error instanceof Error ? error.message : String(error))
      // Fall back to the saved primary-database projection while the queue DB is unavailable.
    }
  }
  await ensureDriveSchema()
  const { rows } = await queryDb<{
    snapshot: Record<string, unknown> | null
    snapshot_updated_at: string | null
    online_workers: string | number
    active_transfers: string | number
    total_jobs: string | number
    queued_jobs: string | number
    running_jobs: string | number
    remaining_jobs: string | number
    completed_jobs: string | number
    failed_jobs: string | number
    transferred_objects: string | number
    failed_objects: string | number
    skipped_objects: string | number
    canceled_jobs: string | number
    jobs: Array<Record<string, unknown>> | null
    live_files: Array<Record<string, unknown>> | null
    job_page: Array<Record<string, unknown>> | null
    job_total: string | number
    attempts: Array<Record<string, unknown>> | null
    worker_runs: Array<Record<string, unknown>> | null
    selected_generation: string | number
    migration_status: string
    buckets: Array<Record<string, unknown>> | null
    historical_buckets: Array<Record<string, unknown>> | null
    current_generation: string | number
  }>(`
    with state as materialized (
      select snapshot,updated_at
      from public.drive_migration_worker_live_state
      where migration_id=$1
      limit 1
    ), migration_meta as materialized (
      select status,greatest(1,coalesce(nullif(options->>'workerGeneration','')::int,1)) generation
      from public.drive_migrations where id=$1 limit 1
    ), selected_generation as materialized (
      select case when $4::int>0 then $4::int else generation end generation
      from migration_meta
    ), fallback_needed as materialized (
      select not exists(
        select 1 from state
        where jsonb_typeof(snapshot->'buckets')='array'
          and jsonb_typeof(snapshot->'totalJobs')='number'
      ) needs_legacy
    ), job_counts as (
      select count(*)::bigint total_jobs,
        count(*) filter(where status='pending')::bigint queued_jobs,
        count(*) filter(where status in('claimed','running'))::bigint running_jobs,
        count(*) filter(where status in('pending','claimed','running') or (status='failed' and case when result->>'retryCount' ~ '^[0-9]+$' then (result->>'retryCount')::int else 0 end<3))::bigint remaining_jobs,
        count(*) filter(where status='completed')::bigint completed_jobs,
        count(*) filter(where status='failed')::bigint failed_jobs,
        coalesce(sum(case when status='completed' then case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end when status in('claimed','running') then greatest(case when (result->'items'->0->>'transferred') ~ '^[0-9]+$' then (result->'items'->0->>'transferred')::bigint else 0 end,case when (progress->>'transferred') ~ '^[0-9]+$' then (progress->>'transferred')::bigint else 0 end) else 0 end),0)::bigint transferred_objects,
        coalesce(sum(case when (result->'items'->0->>'failed') ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint when status='failed' and jsonb_typeof(result->'items')<>'array' then 1 else 0 end),0)::bigint failed_objects,
        coalesce(sum(case when (result->'items'->0->>'skipped') ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else 0 end),0)::bigint skipped_objects,
        count(*) filter(where status='canceled')::bigint canceled_jobs
      from public.drive_repair_jobs
      where migration_id=$1 and mode='migration'
        and (select needs_legacy from fallback_needed)
    ), all_job_count as materialized (
      select count(*)::bigint job_total
      from public.drive_repair_jobs
      where migration_id=$1 and mode='migration'
        and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
    ), paged_jobs as materialized (
      select j.id,j.status,j.claimed_by_agent_id,j.summary,j.error,
        j.created_at,j.updated_at,j.last_heartbeat_at,j.completed_at,
        coalesce(j.payload->'inventoryObjects'->0->>'key','') object_key,
        case when j.payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$'
          then (j.payload->'inventoryObjects'->0->>'size')::bigint else 0 end object_size,
        coalesce(i.source_bucket,'') source_bucket,
        coalesce(i.target_bucket,'') target_bucket,
        case when j.status='completed' and j.result->'items'->0->>'transferred' ~ '^[0-9]+$' then (j.result->'items'->0->>'transferred')::bigint when j.status in('claimed','running') then greatest(case when j.result->'items'->0->>'transferred' ~ '^[0-9]+$' then (j.result->'items'->0->>'transferred')::bigint else 0 end,case when j.progress->>'transferred' ~ '^[0-9]+$' then (j.progress->>'transferred')::bigint else 0 end) else 0 end transferred,
        case when j.result->'items'->0->>'skipped' ~ '^[0-9]+$' then (j.result->'items'->0->>'skipped')::bigint else 0 end skipped,
        case when j.result->'items'->0->>'failed' ~ '^[0-9]+$' then (j.result->'items'->0->>'failed')::bigint else 0 end failed
      from public.drive_repair_jobs j
      left join public.drive_migration_items i
        on i.id::text=j.payload->'itemIds'->>0 and i.migration_id=j.migration_id
      where j.migration_id=$1 and j.mode='migration'
        and greatest(1,coalesce(nullif(j.payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
      order by j.created_at desc,j.id desc
      limit $2 offset $3
    ), job_page_projection as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'id',id,'status',status,'claimedByAgentId',claimed_by_agent_id,
        'summary',summary,'error',error,'createdAt',created_at,'updatedAt',updated_at,
        'lastHeartbeatAt',last_heartbeat_at,'completedAt',completed_at,
        'objectKey',object_key,'objectSize',object_size,
        'sourceBucket',source_bucket,'targetBucket',target_bucket,
        'transferred',transferred,'skipped',skipped,'failed',failed
      ) order by created_at desc,id desc),'[]'::jsonb) job_page
      from paged_jobs
    ), live_file_projection as (
      select coalesce(jsonb_agg(j.progress->'currentFile' || jsonb_build_object(
        'jobId',j.id,'workerId',j.claimed_by_agent_id,'lastHeartbeatAt',j.last_heartbeat_at,'operation',j.mode
      ) order by j.updated_at desc,j.id),'[]'::jsonb) live_files
      from public.drive_repair_jobs j
      where j.migration_id=$1 and j.mode='migration' and j.status in('claimed','running')
        and j.claimed_by_agent_id is not null
        and j.last_heartbeat_at>now()-interval '90 seconds'
        and greatest(1,coalesce(nullif(j.payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
        and (select generation from selected_generation)=(select generation from migration_meta)
        and (select status from migration_meta) in('running','verifying')
        and jsonb_typeof(j.progress->'currentFile')='object'
        and nullif(j.progress->'currentFile'->>'key','') is not null
        and j.progress->'currentFile'->>'status' in('copying','transferring','running')
        and j.progress->>'active' is distinct from 'false'
    ), recent_jobs as materialized (
      select id,claimed_by_agent_id,status,mode,progress,result,created_at
      from public.drive_repair_jobs
      where migration_id=$1 and mode='migration'
        and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
      order by created_at desc,id desc
      limit 20
    ), telemetry as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'id',id,
        'claimed_by_agent_id',claimed_by_agent_id,
        'mode',mode,
        'status',status,
        'progress',jsonb_build_object(
          'fileEvents',coalesce((
            select jsonb_agg(event.value order by event.ordinality)
            from (
              select value,ordinality
              from jsonb_array_elements(case when jsonb_typeof(progress->'fileEvents')='array' then progress->'fileEvents' else '[]'::jsonb end) with ordinality
              order by ordinality desc limit 3
            ) event
          ),'[]'::jsonb),
          'logs',coalesce((
            select jsonb_agg(entry.value order by entry.ordinality)
            from (
              select value,ordinality
              from jsonb_array_elements(case when jsonb_typeof(progress->'logs')='array' then progress->'logs' else '[]'::jsonb end) with ordinality
              order by ordinality desc limit 3
            ) entry
          ),'[]'::jsonb)
        ),
        'result',jsonb_build_object('fileEvents',coalesce((
          select jsonb_agg(event.value order by event.ordinality)
          from (
            select value,ordinality
            from jsonb_array_elements(case when jsonb_typeof(result->'fileEvents')='array' then result->'fileEvents' else '[]'::jsonb end) with ordinality
            order by ordinality desc limit 3
          ) event
        ),'[]'::jsonb))
      ) order by created_at desc,id desc),'[]'::jsonb) jobs
      from recent_jobs
    ), attempt_generations as materialized (
      select generation from migration_meta
      union
      select greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))
      from public.drive_agent_runs where run_type='github_dispatch' and payload->>'migrationId'=($1::uuid)::text
      union
      select greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))
      from public.drive_repair_jobs where migration_id=$1 and mode='migration'
    ), attempt_projection as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'generation',summary.generation,'status',summary.status,
        'workerCount',summary.worker_count,'runningWorkers',summary.running_workers,
        'onlineWorkers',summary.online_workers,'totalJobs',summary.total_jobs,
        'queuedJobs',summary.queued_jobs,'runningJobs',summary.running_jobs,'remainingJobs',summary.remaining_jobs,
        'completedJobs',summary.completed_jobs,'failedJobs',summary.failed_jobs,
        'canceledJobs',summary.canceled_jobs,'createdAt',summary.created_at,'updatedAt',summary.updated_at
      ) order by summary.generation desc),'[]'::jsonb) attempts
      from (
        select generation.generation,
          case
            when generation.generation=(select generation from migration_meta)
              and (select status from migration_meta) in('canceled','aborted')
              and coalesce(runs.online_workers,0)=0 then 'aborted'
            when coalesce(runs.running_workers,0)>0 then 'running'
            when coalesce(runs.pending_workers,0)>0 then 'deploying'
            when coalesce(jobs.running_jobs,0)>0 then 'running'
            when coalesce(jobs.queued_jobs,0)>0 then 'queued'
            when coalesce(runs.canceled_workers,0)>0 or coalesce(jobs.canceled_jobs,0)>0 then 'aborted'
            when coalesce(runs.failed_workers,0)>0 or coalesce(jobs.failed_jobs,0)>0 then 'failed'
            when coalesce(jobs.completed_jobs,0)>0 or coalesce(runs.completed_workers,0)>0 then 'completed'
            else 'queued'
          end status,
          coalesce(runs.worker_count,0) worker_count,coalesce(runs.running_workers,0) running_workers,
          coalesce(runs.online_workers,0) online_workers,coalesce(jobs.total_jobs,0) total_jobs,
          coalesce(jobs.queued_jobs,0) queued_jobs,coalesce(jobs.running_jobs,0) running_jobs,
          coalesce(jobs.remaining_jobs,0) remaining_jobs,
          coalesce(jobs.completed_jobs,0) completed_jobs,coalesce(jobs.failed_jobs,0) failed_jobs,
          coalesce(jobs.canceled_jobs,0) canceled_jobs,
          least(runs.created_at,jobs.created_at) created_at,
          greatest(runs.updated_at,jobs.updated_at) updated_at
        from attempt_generations generation
        left join lateral (
          select count(*)::bigint worker_count,
            count(*) filter(where r.status='pending')::bigint pending_workers,
            count(*) filter(where r.status='running')::bigint running_workers,
            count(*) filter(where r.status='completed')::bigint completed_workers,
            count(*) filter(where r.status='failed')::bigint failed_workers,
            count(*) filter(where r.status='canceled')::bigint canceled_workers,
            count(*) filter(where r.status='running' and (
              (r.payload->>'workerInstanceId' is not null and coalesce(nullif(r.payload->>'workerHeartbeatAt','')::timestamptz,'-infinity'::timestamptz)>now()-interval '90 seconds')
              or (r.payload->>'workerInstanceId' is null and a.status='online' and a.last_heartbeat_at>now()-interval '90 seconds')
            ))::bigint online_workers,
            min(r.created_at) created_at,max(r.updated_at) updated_at
          from public.drive_agent_runs r left join public.drive_agents a on a.id=r.agent_id
          where r.run_type='github_dispatch' and r.payload->>'migrationId'=($1::uuid)::text
            and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=generation.generation
        ) runs on true
        left join lateral (
          select count(*)::bigint total_jobs,
            count(*) filter(where status='pending')::bigint queued_jobs,
            count(*) filter(where status in('claimed','running'))::bigint running_jobs,
            count(*) filter(where status in('pending','claimed','running') or (status='failed' and case when result->>'retryCount' ~ '^[0-9]+$' then (result->>'retryCount')::int else 0 end<3))::bigint remaining_jobs,
            count(*) filter(where status='completed')::bigint completed_jobs,
            count(*) filter(where status='failed')::bigint failed_jobs,
            count(*) filter(where status='canceled')::bigint canceled_jobs,
            min(created_at) created_at,max(updated_at) updated_at
          from public.drive_repair_jobs
          where migration_id=$1 and mode='migration'
            and greatest(1,coalesce(nullif(payload->>'workerGeneration','')::int,1))=generation.generation
        ) jobs on true
      ) summary
    ), selected_worker_runs as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'id',run.id,'agentId',run.agent_id,'status',case
          when (select status from migration_meta) in('canceled','aborted')
            and run.payload->>'githubAbortRequestedAt' is not null
            and not (run.status='running' and (
              (run.payload->>'workerInstanceId' is not null and coalesce(nullif(run.payload->>'workerHeartbeatAt','')::timestamptz,'-infinity'::timestamptz)>now()-interval '90 seconds')
              or (run.payload->>'workerInstanceId' is null and run.agent_status='online' and run.agent_heartbeat>now()-interval '90 seconds')
            ))
          then 'canceled' else run.status end,
        'online',run.status='running' and (
          (run.payload->>'workerInstanceId' is not null and coalesce(nullif(run.payload->>'workerHeartbeatAt','')::timestamptz,'-infinity'::timestamptz)>now()-interval '90 seconds')
          or (run.payload->>'workerInstanceId' is null and run.agent_status='online' and run.agent_heartbeat>now()-interval '90 seconds')
        ),
        'abortRequested',(run.payload->>'githubAbortRequestedAt') is not null,
        'externalRunId',run.external_run_id,'instanceId',run.payload->>'workerInstanceId',
        'jobId',run.job_reference,'currentStatus',job.status,'lastHeartbeatAt',job.last_heartbeat_at,
        'completedFiles',coalesce((run.payload->>'completedFiles')::bigint,0),
        'failedFiles',coalesce((run.payload->>'failedFiles')::bigint,0),
        'completedBytes',coalesce((run.payload->>'completedBytes')::bigint,0),
        'createdAt',run.created_at,'updatedAt',run.updated_at
      ) order by run.created_at asc),'[]'::jsonb) worker_runs
      from (
        select r.*,a.status agent_status,a.last_heartbeat_at agent_heartbeat
        from public.drive_agent_runs r left join public.drive_agents a on a.id=r.agent_id
        where r.run_type='github_dispatch' and r.payload->>'migrationId'=($1::uuid)::text
          and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
        order by r.created_at asc limit 100
      ) run left join public.drive_repair_jobs job on job.id::text=run.job_reference
    ), bucket_projection as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'id',item.id,
        'sourceBucket',item.source_bucket,
        'targetBucket',item.target_bucket,
        'status',coalesce(nullif(item.progress->'live'->>'status',''),item.slurper_status,'pending'),
        'totalObjects',case when item.progress->'live'->>'totalObjects' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'totalObjects')::numeric else coalesce(item.source_objects,0) end,
        'queuedObjects',case when item.progress->'live'->>'queuedObjects' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'queuedObjects')::numeric else 0 end,
        'transferredObjects',case when item.progress->'live'->>'transferredObjects' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'transferredObjects')::numeric else 0 end,
        'failedObjects',case when item.progress->'live'->>'failedObjects' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'failedObjects')::numeric else 0 end,
        'skippedObjects',case when item.progress->'live'->>'skippedObjects' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'skippedObjects')::numeric else 0 end,
        'transferredBytes',case when item.progress->'live'->>'transferredBytes' ~ '^-?[0-9]+(\\.[0-9]+)?$' then (item.progress->'live'->>'transferredBytes')::numeric else 0 end,
        'sourceBytes',coalesce(item.source_bytes,0),
        'updatedAt',item.updated_at
      ) order by item.source_bucket,item.id),'[]'::jsonb) buckets
      from public.drive_migration_items item
      where item.migration_id=$1 and (select needs_legacy from fallback_needed)
    ), historical_bucket_projection as (
      select coalesce(jsonb_agg(jsonb_build_object(
        'id',i.id,'sourceBucket',i.source_bucket,'targetBucket',i.target_bucket,
        'totalObjects',coalesce(j.total_jobs,0),
        'queuedObjects',coalesce(j.queued_jobs,0),
        'transferredObjects',coalesce(j.transferred_objects,0),
        'failedObjects',coalesce(j.failed_objects,0),
        'skippedObjects',coalesce(j.skipped_objects,0),
        'transferredBytes',coalesce(j.transferred_bytes,0),
        'sourceBytes',coalesce(j.source_bytes,0),
        'status',case when coalesce(j.running_jobs,0)>0 then 'running'
          when coalesce(j.queued_jobs,0)>0 then 'queued'
          when coalesce(j.failed_jobs,0)>0 then 'failed'
          when coalesce(j.canceled_jobs,0)>0 then 'aborted'
          when coalesce(j.total_jobs,0)>0 then 'completed' else coalesce(i.slurper_status,'queued') end,
        'updatedAt',coalesce(j.updated_at,i.updated_at)
      ) order by i.source_bucket,i.id),'[]'::jsonb) buckets
      from public.drive_migration_items i
      left join lateral (
        select count(*)::bigint total_jobs,
          count(*) filter(where status='pending')::bigint queued_jobs,
          count(*) filter(where status in('claimed','running'))::bigint running_jobs,
          count(*) filter(where status='failed')::bigint failed_jobs,
          count(*) filter(where status='canceled')::bigint canceled_jobs,
          coalesce(sum(case when status='completed' and (result->'items'->0->>'transferred') ~ '^[0-9]+$'
            then greatest(0,(result->'items'->0->>'transferred')::bigint-coalesce(nullif(result->'items'->0->>'skipped','')::bigint,0)) else 0 end),0)::bigint transferred_objects,
          coalesce(sum(case when (result->'items'->0->>'skipped') ~ '^[0-9]+$' then (result->'items'->0->>'skipped')::bigint else 0 end),0)::bigint skipped_objects,
          coalesce(sum(case when (result->'items'->0->>'failed') ~ '^[0-9]+$' then (result->'items'->0->>'failed')::bigint else 0 end),0)::bigint failed_objects,
          coalesce(sum(case when status='completed' and (result->'items'->0->>'transferred') ~ '^[0-9]+$'
            and (result->'items'->0->>'transferred')::bigint>0 then case when payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end else 0 end),0)::bigint transferred_bytes,
          coalesce(sum(case when payload->'inventoryObjects'->0->>'size' ~ '^[0-9]+$' then (payload->'inventoryObjects'->0->>'size')::bigint else 0 end),0)::bigint source_bytes,
          max(updated_at) updated_at
        from public.drive_repair_jobs j
        where j.migration_id=i.migration_id and j.mode='migration'
          and j.payload->'itemIds'->>0=i.id::text
          and greatest(1,coalesce(nullif(j.payload->>'workerGeneration','')::int,1))=(select generation from selected_generation)
      ) j on true
      where i.migration_id=$1
    ), worker_counts as (
      select count(*) filter(where r.status='running' and (
          (r.payload->>'workerInstanceId' is not null and coalesce(nullif(r.payload->>'workerHeartbeatAt','')::timestamptz,'-infinity'::timestamptz)>now()-interval '90 seconds')
          or (r.payload->>'workerInstanceId' is null and a.status='online' and a.last_heartbeat_at>now()-interval '90 seconds')
        ))::bigint online_workers,
        count(*) filter(where r.status='running' and j.progress ? 'currentFile'
          and ((r.payload->>'workerInstanceId' is not null and coalesce(nullif(r.payload->>'workerHeartbeatAt','')::timestamptz,'-infinity'::timestamptz)>now()-interval '90 seconds')
            or (r.payload->>'workerInstanceId' is null and a.status='online' and a.last_heartbeat_at>now()-interval '90 seconds')))::bigint active_transfers
      from public.drive_agent_runs r
      join public.drive_agents a on a.id=r.agent_id
      left join public.drive_repair_jobs j on j.id::text=r.job_reference
      where r.run_type='github_dispatch' and r.payload->>'migrationId'=($1::uuid)::text
        and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=(select generation from migration_meta)
    )
    select state.snapshot,state.updated_at snapshot_updated_at,
      worker_counts.online_workers,worker_counts.active_transfers,
      job_counts.total_jobs,job_counts.queued_jobs,job_counts.running_jobs,
      job_counts.completed_jobs,job_counts.failed_jobs,job_counts.transferred_objects,job_counts.failed_objects,job_counts.canceled_jobs,
      telemetry.jobs,live_file_projection.live_files,job_page_projection.job_page,all_job_count.job_total,
      attempt_projection.attempts,selected_worker_runs.worker_runs,selected_generation.generation selected_generation,
      migration_meta.status migration_status,bucket_projection.buckets,
      historical_bucket_projection.buckets historical_buckets,migration_meta.generation current_generation
    from job_counts cross join worker_counts cross join telemetry cross join live_file_projection cross join job_page_projection cross join all_job_count
      cross join attempt_projection cross join selected_worker_runs cross join selected_generation cross join migration_meta cross join bucket_projection cross join historical_bucket_projection
    left join state on true
  `, [id, pageSize, pageIndex * pageSize, selectedGeneration])
  const stateRow = rows[0]
  if (!stateRow) throw new Error("Migration worker pool query returned no row")
  const saved = stateRow.snapshot ?? {}
  const allJobs = stateRow.jobs ?? []
  const liveFiles = stateRow.live_files ?? []
  const jobPage = stateRow.job_page ?? []
  const attempts = stateRow.attempts ?? []
  const workerRuns = stateRow.worker_runs ?? []
  const resolvedGeneration = Number(stateRow.selected_generation || 1)
  const currentGeneration = Number(stateRow.current_generation || 1)
  const migrationStatus = String(stateRow.migration_status || "")
  const jobTotal = Number(stateRow.job_total || 0)
  const jobPagination = {
    pageIndex,
    pageSize,
    pageCount: Math.max(1, Math.ceil(jobTotal / pageSize)),
    total: jobTotal,
  }
  const liveCounts = {
    onlineWorkers: Number(stateRow.online_workers || 0),
    activeTransfers: Number(stateRow.active_transfers || 0),
  }
  const normalizeTerminalBuckets = (snapshot: Record<string, unknown>) => {
    if (!["canceled", "cancelled", "aborted"].includes(migrationStatus.toLowerCase()) || !Array.isArray(snapshot.buckets)) return snapshot
    return { ...snapshot, buckets: snapshot.buckets.map((bucket) => isRecord(bucket) ? { ...bucket, status: "aborted", queuedObjects: 0 } : bucket) }
  }
  if (hasCompleteSnapshot(saved) && resolvedGeneration === currentGeneration) {
    return { snapshot: normalizeTerminalBuckets({ ...saved, ...liveCounts }), snapshotUpdatedAt: stateRow.snapshot_updated_at, jobs: allJobs, liveFiles, jobPage, jobPagination, attempts, workerRuns, selectedGeneration: resolvedGeneration, migrationStatus, dataSource: queueReadFailed ? "primary_database_fallback" : "primary_database", queueReadFailed }
  }

  if (resolvedGeneration !== currentGeneration) {
    const attempt = attempts.find((row) => Number(row.generation) === resolvedGeneration) ?? {}
    const buckets = stateRow.historical_buckets ?? []
    const snapshot = {
      migrationId: id,
      workerGeneration: resolvedGeneration,
      totalJobs: Number(attempt.totalJobs || 0),
      queuedJobs: Number(attempt.queuedJobs || 0),
      runningJobs: Number(attempt.runningJobs || 0),
      remainingJobs: Number(attempt.remainingJobs || 0),
      completedJobs: Number(attempt.completedJobs || 0),
      failedJobs: Number(attempt.failedJobs || 0),
      canceledJobs: Number(attempt.canceledJobs || 0),
      totalObjects: buckets.reduce((sum, bucket) => sum + Number(bucket.totalObjects || 0), 0),
      transferred: buckets.reduce((sum, bucket) => sum + Number(bucket.transferredObjects || 0), 0),
      skipped: buckets.reduce((sum, bucket) => sum + Number(bucket.skippedObjects || 0), 0),
      failed: buckets.reduce((sum, bucket) => sum + Number(bucket.failedObjects || 0), 0),
      onlineWorkers: Number(attempt.onlineWorkers || 0),
      activeTransfers: 0,
      buckets,
      updatedAt: attempt.updatedAt ?? stateRow.snapshot_updated_at,
    }
    return { snapshot: normalizeTerminalBuckets(snapshot), snapshotUpdatedAt: stateRow.snapshot_updated_at, jobs: allJobs, liveFiles, jobPage, jobPagination, attempts, workerRuns, selectedGeneration: resolvedGeneration, migrationStatus, dataSource: queueReadFailed ? "primary_database_fallback" : "primary_database", queueReadFailed }
  }

  // Legacy migrations may not have an orchestrator snapshot yet. This narrow
  // bucket projection is built in the same round trip as the live queue data.
  const buckets = stateRow.buckets ?? []
  const snapshot = {
    ...saved,
    totalJobs: Number(stateRow.total_jobs || 0),
    queuedJobs: Number(stateRow.queued_jobs || 0),
    runningJobs: Number(stateRow.running_jobs || 0),
    remainingJobs: Number(stateRow.remaining_jobs || 0),
    completedJobs: Number(stateRow.completed_jobs || 0),
    failedJobs: Number(stateRow.failed_jobs || 0),
    transferred: Number(stateRow.transferred_objects || 0),
    failed: Number(stateRow.failed_objects || 0),
    skipped: Number(stateRow.skipped_objects || 0),
    canceledJobs: Number(stateRow.canceled_jobs || 0),
    totalObjects: buckets.reduce((sum, bucket) => sum + Number(bucket.totalObjects || 0), 0),
    ...liveCounts,
    buckets,
  }
  return { snapshot: normalizeTerminalBuckets(snapshot), snapshotUpdatedAt: stateRow.snapshot_updated_at, jobs: allJobs, liveFiles, jobPage, jobPagination, attempts, workerRuns, selectedGeneration: resolvedGeneration, migrationStatus, dataSource: queueReadFailed ? "primary_database_fallback" : "primary_database", queueReadFailed }
}

function cachedPool(id: string, pageIndex: number, pageSize: number, selectedGeneration: number) {
  const now = Date.now()
  const key = `${id}:${pageIndex}:${pageSize}:${selectedGeneration}`
  const current = responseCache.get(key)
  if (current && current.expiresAt > now) return current.promise
  const promise = readPool(id, pageIndex, pageSize, selectedGeneration).catch((error) => {
    responseCache.delete(key)
    throw error
  })
  responseCache.set(key, { expiresAt: now + RESPONSE_CACHE_TTL_MS, promise })
  if (responseCache.size > 100) {
    for (const [key, entry] of responseCache) if (entry.expiresAt <= now) responseCache.delete(key)
  }
  return promise
}

export async function GET(request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response
    const { id } = await context.params
    const url = new URL(request.url)
    const pageIndex = Math.max(0, Math.floor(Number(url.searchParams.get("page") || 0) || 0))
    const pageSize = Math.max(10, Math.min(100, Math.floor(Number(url.searchParams.get("pageSize") || 25) || 25)))
    const selectedGeneration = Math.max(0, Math.floor(Number(url.searchParams.get("generation") || 0) || 0))
    const saved = await cachedPool(id, pageIndex, pageSize, selectedGeneration)
    const jobId = url.searchParams.get("jobId")
    let selectedJob: Record<string, unknown> | null = null
    if (jobId && /^[0-9a-f-]{36}$/i.test(jobId)) {
      const queueSettings = queueSettingsCache?.value
      if (queueSettings?.queueDatabaseUrl) {
        selectedJob = await withMigrationQueueDatabase(queueSettings.queueDatabaseUrl, async (db) => {
          const row = (await db.query(`select id,migration_id,generation,status,payload,progress,result,summary,error,claimed_by,agent_id,created_at,updated_at,last_heartbeat_at,completed_at
            from drive_migration_queue_jobs where id=$1 and migration_id=$2 and generation=$3 limit 1`, [jobId, id, saved.selectedGeneration])).rows[0]
          return row ? { id: row.id, migrationId: row.migration_id, status: row.status, mode: row.payload?.job?.mode || "migration", claimedByAgentId: row.agent_id || row.claimed_by, payload: row.payload || {}, progress: row.progress || {}, result: row.result || {}, summary: row.summary, error: row.error, createdAt: row.created_at, updatedAt: row.updated_at, lastHeartbeatAt: row.last_heartbeat_at, completedAt: row.completed_at } : null
        }, queueSettings.queueDatabaseSsl)
      }
      if (!selectedJob) {
        const result = await queryDb(`select id,migration_id,status,mode,claimed_by_agent_id,payload,progress,result,summary,error,created_at,updated_at,last_heartbeat_at,completed_at from drive_repair_jobs where id=$1 and migration_id=$2 limit 1`, [jobId, id])
        const row = result.rows[0]
        if (row) selectedJob = { id: row.id, migrationId: row.migration_id, status: row.status, mode: row.mode, claimedByAgentId: row.claimed_by_agent_id, payload: row.payload || {}, progress: row.progress || {}, result: row.result || {}, summary: row.summary, error: row.error, createdAt: row.created_at, updatedAt: row.updated_at, lastHeartbeatAt: row.last_heartbeat_at, completedAt: row.completed_at }
      }
    }
    return NextResponse.json({ ...saved, ...(jobId ? { job: selectedJob } : {}), source: "database" }, { headers: { "Cache-Control": "no-store, max-age=0" } })
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : String(error) },
      { status: 500, headers: { "Cache-Control": "no-store, max-age=0", "X-Drive-Worker-Failure": "1" } }
    )
  }
}
