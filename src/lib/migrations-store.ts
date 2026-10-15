import crypto from "crypto"
import { compactPreviousMigrationDetails } from "./database-maintenance"
import { queryDb, withDbTransaction } from "./db"
import { ensureActivitySchema, recordActivityInTransaction, type RecordActivityInput } from "./activity-store"
import { mapMigrationWorkerRun, type MigrationWorkerRun, type MigrationWorkerRunRow } from "./migration-worker-runs"

export type MigrationStatus = "draft" | "running" | "verifying" | "completed" | "failed" | "verification_failed" | "canceled"
export type MigrationSyncStatus = "idle" | "syncing" | "ok" | "error"

export type MigrationOptions = {
  /** Cloudflare-managed Super Slurper (default) or the durable worker pool. */
  executionMode?: "super_slurper" | "migration_workers"
  /** Incremented when a worker migration is explicitly retried. */
  workerGeneration?: number
  /** Number of deterministic object shards shared by the worker pool. */
  workerShardCount?: number
  /** GitHub workers enrolled in this pool; the autonomous orchestrator keeps them running. */
  workerAgentIds?: string[]
  /** Automatic repair generations started after independent verification finds drift. */
  workerVerificationRepairAttempts?: number
  /** Permit worker repair generations to replace only files proven hash-mismatched; correct target objects stay untouched. */
  workerRepairMismatchedObjects?: boolean
  /** Require the independent Cloudflare verifier before account activation. */
  requireIndependentVerification?: boolean
  overwrite?: boolean
  concurrency?: number
  includeBuckets?: string[]
  excludeBuckets?: string[]
  pathPrefix?: string | null
  sourceMode?: "r2" | "s3"
  manualCompleted?: boolean
  targetActivatedAt?: string
  workerPoolLiveStats?: Record<string, number>
  historyReadOnlyAt?: string
  historyReadOnlyReason?: string

  // When true (default), after Super Slurper completes a bucket we verify source→destination
  // object listings (key + size) before marking the migration completed.
  verifyAfterCopy?: boolean

  // When true, destination extras are treated as failure (default: false; merge-friendly).
  verifyStrictDestination?: boolean

  // Verification mode. "keys-and-size" verifies object presence + size. "sha256-small"
  // additionally hashes small objects to detect same-size corruption.
  verifyMode?: "keys-and-size" | "sha256-small"

  // Max object size (bytes) to SHA-256 hash when verifyMode="sha256-small".
  verifyHashMaxBytes?: number
}

export interface DriveMigration {
  id: string
  sourceAccountId: string
  targetAccountId: string
  status: MigrationStatus
  options: MigrationOptions
  createdAt: string
  startedAt?: string
  completedAt?: string
  lastSyncedAt?: string
  syncStatus?: MigrationSyncStatus
  syncMessage?: string
  updatedAt?: string
  summaryItemCount: number
  summaryObjects: number
  summaryBytes: number
  workerSummary: Record<string, unknown>
  detailsCompactedAt?: string
}

export interface DriveMigrationItem {
  id: string
  migrationId: string
  sourceBucket: string
  targetBucket: string
  sourceJurisdiction?: string
  sourceStorageClass?: string
  sourceObjects?: number
  sourceBytes?: number
  slurperJobId?: string
  slurperStatus?: string
  verificationState?: {
    generation: number
    status: string
    missingObjects: number
    mismatchedObjects: number
    extraObjects: number
    attemptId?: string
    strictDestination: boolean
    updatedAt?: string
  }
  progress: Record<string, unknown>
  lastProgressAt?: string
  createdAt: string
  updatedAt?: string
}

export type MigrationDashboardBootstrap = {
  migrations: DriveMigration[]
  accounts: Array<{ id: string; label: string; email: string; status: string; cloudflareAccountId?: string }>
  activeAccount: { id: string; cloudflareAccountId?: string } | null
  bucketStats: Array<{ bucketName: string; objects: number; bytes: number; status: string; error?: string; updatedAt?: string }>
  activeItems: DriveMigrationItem[]
}

export type MigrationDetailBootstrap = {
  migration: DriveMigration
  items: DriveMigrationItem[]
  accounts: Array<{ id: string; label: string; email: string; status: string }>
  workerRuns: MigrationWorkerRun[]
}

type DriveMigrationRow = {
  id: string
  source_account_id: string
  target_account_id: string
  status: MigrationStatus
  options: MigrationOptions | null
  created_at: string
  started_at: string | null
  completed_at: string | null
  last_synced_at: string | null
  sync_status: MigrationSyncStatus | null
  sync_message: string | null
  updated_at: string | null
  summary_item_count: number | string | null
  summary_objects: number | string | null
  summary_bytes: number | string | null
  worker_summary: Record<string, unknown> | null
  details_compacted_at: string | null
}

type DriveMigrationItemRow = {
  id: string
  migration_id: string
  source_bucket: string
  target_bucket: string
  source_jurisdiction: string | null
  source_storage_class: string | null
  source_objects: number | string | null
  source_bytes: number | string | null
  slurper_job_id: string | null
  slurper_status: string | null
  verification_state: {
    generation?: number | string
    status?: string
    missing_objects?: number | string
    mismatched_objects?: number | string
    extra_objects?: number | string
    attempt_id?: string
    strict_destination?: boolean
    updated_at?: string
  } | null
  progress: Record<string, unknown> | null
  last_progress_at: string | null
  created_at: string
  updated_at: string | null
}

const MIGRATIONS_TABLE = "drive_migrations"
const MIGRATION_ITEMS_TABLE = "drive_migration_items"

function nonNegativeInteger(value: number | string | null | undefined): number {
  const parsed = typeof value === "number" ? value : Number(value)
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : 0
}

type ProgressEvent = {
  at: string
  stage?: string
  status?: string | null
  message?: string
  data?: Record<string, unknown>
}

function appendProgressEvent(
  progress: Record<string, unknown>,
  event: ProgressEvent
): Record<string, unknown> {
  const existing = Array.isArray(progress.events) ? (progress.events as unknown[]) : []
  const previous = existing.at(-1)
  if (previous && typeof previous === "object") {
    const last = previous as Record<string, unknown>
    const sameData = JSON.stringify(last.data ?? null) === JSON.stringify(event.data ?? null)
    if (
      String(last.stage ?? "") === String(event.stage ?? "") &&
      String(last.status ?? "") === String(event.status ?? "") &&
      String(last.message ?? "") === String(event.message ?? "") &&
      sameData
    ) {
      return { ...progress, events: existing }
    }
  }
  const next = [...existing, event]
  const capped = next.length > 500 ? next.slice(next.length - 500) : next
  return { ...progress, events: capped }
}

function mapMigrationRow(row: DriveMigrationRow): DriveMigration {
  return {
    id: row.id,
    sourceAccountId: row.source_account_id,
    targetAccountId: row.target_account_id,
    status: row.status,
    options: row.options ?? {},
    createdAt: row.created_at,
    startedAt: row.started_at ?? undefined,
    completedAt: row.completed_at ?? undefined,
    lastSyncedAt: row.last_synced_at ?? undefined,
    syncStatus: row.sync_status ?? undefined,
    syncMessage: row.sync_message ?? undefined,
    updatedAt: row.updated_at ?? undefined,
    summaryItemCount: nonNegativeInteger(row.summary_item_count),
    summaryObjects: nonNegativeInteger(row.summary_objects),
    summaryBytes: nonNegativeInteger(row.summary_bytes),
    workerSummary: row.worker_summary ?? {},
    detailsCompactedAt: row.details_compacted_at ?? undefined,
  }
}

function mapMigrationItemRow(row: DriveMigrationItemRow): DriveMigrationItem {
  return {
    id: row.id,
    migrationId: row.migration_id,
    sourceBucket: row.source_bucket,
    targetBucket: row.target_bucket,
    sourceJurisdiction: row.source_jurisdiction ?? undefined,
    sourceStorageClass: row.source_storage_class ?? undefined,
    sourceObjects: row.source_objects === null ? undefined : nonNegativeInteger(row.source_objects),
    sourceBytes: row.source_bytes === null ? undefined : nonNegativeInteger(row.source_bytes),
    slurperJobId: row.slurper_job_id ?? undefined,
    slurperStatus: row.slurper_status ?? undefined,
    verificationState: row.verification_state ? {
      generation: nonNegativeInteger(row.verification_state.generation),
      status: String(row.verification_state.status || ""),
      attemptId: row.verification_state.attempt_id ?? undefined,
      missingObjects: nonNegativeInteger(row.verification_state.missing_objects),
      mismatchedObjects: nonNegativeInteger(row.verification_state.mismatched_objects),
      extraObjects: nonNegativeInteger(row.verification_state.extra_objects),
      strictDestination: row.verification_state.strict_destination === true,
      updatedAt: row.verification_state.updated_at ?? undefined,
    } : undefined,
    progress: row.progress ?? {},
    lastProgressAt: row.last_progress_at ?? undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at ?? undefined,
  }
}

export async function listMigrations(limit = 50): Promise<DriveMigration[]> {
  const boundedLimit = Math.max(1, Math.min(500, Math.floor(limit)))
  const { rows } = await queryDb<DriveMigrationRow>(
    `select * from public.${MIGRATIONS_TABLE} order by created_at desc, id desc limit $1`,
    [boundedLimit]
  )
  return rows.map(mapMigrationRow)
}

function compactedObjectCounts(row: DriveMigrationItemRow) {
  const progress = row.progress && typeof row.progress === "object" ? row.progress as Record<string, unknown> : {}
  const live = progress.live && typeof progress.live === "object" ? progress.live as Record<string, unknown> : null
  const slurperCandidates = [
    progress.slurperCumulative,
    progress.slurperNormalized,
    progress.slurper && typeof progress.slurper === "object" ? (progress.slurper as Record<string, unknown>).result : null,
  ]
  const slurper = slurperCandidates.find((value) => value && typeof value === "object") as Record<string, unknown> | undefined
  const number = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? Math.max(0, Math.floor(value)) : undefined
  return {
    transferred: number(live?.transferredObjects) ?? number(slurper?.transferredObjects) ?? 0,
    skipped: number(live?.skippedObjects) ?? number(slurper?.skippedObjects) ?? 0,
    failed: number(live?.failedObjects) ?? number(slurper?.failedObjects) ?? 0,
    unaccounted: number(live?.unaccountedObjects) ?? 0,
  }
}

/** One database round trip for the migrations page's initial database snapshot. */
export async function getMigrationDashboardBootstrap(limit = 50): Promise<MigrationDashboardBootstrap> {
  const boundedLimit = Math.max(1, Math.min(500, Math.floor(limit)))
  const { rows } = await queryDb<{
    migrations: DriveMigrationRow[] | null
    accounts: Array<{ id: string; label: string; email: string; status: string; cloudflare_account_id: string | null }> | null
    active_account: { id: string; cloudflare_account_id: string | null } | null
    bucket_stats: Array<{ bucket_name: string; objects: number | string; bytes: number | string; status: string; error: string | null; updated_at: string | null }> | null
    active_items: DriveMigrationItemRow[] | null
  }>(`
    with limited_migrations as (
      select * from public.drive_migrations
      order by created_at desc,id desc
      limit $1
    ), current_migration as (
      select * from limited_migrations
      order by case status when 'running' then 0 when 'verifying' then 1 when 'draft' then 2 else 3 end,
        created_at desc,id desc
      limit 1
    ), active_account as (
      select id,cloudflare_account_id
      from public.drive_accounts
      where status='active'
      order by updated_at desc nulls last,created_at desc,id desc
      limit 1
    )
    select
      coalesce((
        select jsonb_agg(
          to_jsonb(m) || jsonb_build_object(
            'summary_item_count',m.summary_item_count::text,
            'summary_objects',m.summary_objects::text,
            'summary_bytes',m.summary_bytes::text
          ) order by m.created_at desc,m.id desc
        ) from limited_migrations m
      ),'[]'::jsonb) as migrations,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'id',account.id,'label',account.label,'email',account.email,'status',account.status,
          'cloudflare_account_id',account.cloudflare_account_id
        ) order by account.updated_at desc nulls last,account.created_at desc,account.id desc)
        from public.drive_accounts account
      ),'[]'::jsonb) as accounts,
      (select to_jsonb(account) from active_account account) as active_account,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'bucket_name',stats.bucket_name,
          'objects',stats.objects::text,
          'bytes',stats.bytes::text,
          'status',stats.status,
          'error',stats.error,
          'updated_at',stats.updated_at
        ) order by stats.bucket_name asc)
        from public.drive_bucket_stats stats
        join active_account account on account.id=stats.account_id
      ),'[]'::jsonb) as bucket_stats,
      coalesce((
        select jsonb_agg(
          to_jsonb(item) || jsonb_build_object(
            'source_objects',item.source_objects::text,
            'source_bytes',item.source_bytes::text,
            'verification_state',(
              select jsonb_build_object(
                'generation',verification.generation,
                'status',verification.status,
                'missing_objects',verification.missing_objects,
                'mismatched_objects',verification.mismatched_objects,
                'extra_objects',verification.extra_objects,
                'attempt_id',item.progress->>'verificationAttemptId',
                'strict_destination',migration.options->>'verifyStrictDestination'='true',
                'updated_at',verification.updated_at
              )
              from public.drive_migration_verification_state verification
              where verification.migration_item_id=item.id
              order by verification.generation desc,verification.updated_at desc
              limit 1
            )
          ) order by item.source_bucket asc,item.id asc
        )
        from public.drive_migration_items item
        join current_migration migration on migration.id=item.migration_id
      ),'[]'::jsonb) as active_items
  `, [boundedLimit])
  const row = rows[0]
  if (!row) throw new Error("Unable to load migration dashboard snapshot")
  const accountRows = Array.isArray(row.accounts) ? row.accounts : []
  const activeAccount = row.active_account ?? null
  return {
    migrations: (Array.isArray(row.migrations) ? row.migrations : []).map(mapMigrationRow),
    accounts: accountRows.map((account) => ({
      id: account.id,
      label: account.label,
      email: account.email,
      status: account.status,
      cloudflareAccountId: account.cloudflare_account_id ?? undefined,
    })),
    activeAccount: activeAccount ? {
      id: activeAccount.id,
      cloudflareAccountId: activeAccount.cloudflare_account_id ?? undefined,
    } : null,
    bucketStats: (Array.isArray(row.bucket_stats) ? row.bucket_stats : []).map((stats) => ({
      bucketName: stats.bucket_name,
      objects: nonNegativeInteger(stats.objects),
      bytes: nonNegativeInteger(stats.bytes),
      status: stats.status,
      error: stats.error ?? undefined,
      updatedAt: stats.updated_at ?? undefined,
    })),
    activeItems: (Array.isArray(row.active_items) ? row.active_items : []).map(mapMigrationItemRow),
  }
}

export async function listMigrationsByAccount(
  accountId: string,
  limit = 200
): Promise<DriveMigration[]> {
  const boundedLimit = Math.max(1, Math.min(500, Math.floor(limit)))
  const { rows } = await queryDb<DriveMigrationRow>(
    `select * from public.${MIGRATIONS_TABLE}
     where source_account_id = $1 or target_account_id = $1
     order by created_at desc, id desc limit $2`,
    [accountId, boundedLimit]
  )
  return rows.map(mapMigrationRow)
}

export async function getMigration(id: string): Promise<DriveMigration | null> {
  const { rows } = await queryDb<DriveMigrationRow>(
    `
      select migration.*,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.item_count,0)::integer else migration.summary_item_count end as resolved_summary_item_count,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_objects,0)::bigint else migration.summary_objects end as resolved_summary_objects,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_bytes,0)::bigint else migration.summary_bytes end as resolved_summary_bytes
      from public.${MIGRATIONS_TABLE} migration
      left join lateral (
        select count(*) as item_count,
          coalesce(sum(source_objects),0) as summary_objects,
          coalesce(sum(source_bytes),0) as summary_bytes
        from public.${MIGRATION_ITEMS_TABLE}
        where migration_id=migration.id
      ) legacy_summary on migration.status='completed' and migration.summary_item_count=0
      where migration.id=$1
      limit 1
    `,
    [id]
  )
  const row = rows[0]
  if (!row) return null
  return mapMigrationRow(row)
}

/** One bounded query for a migration, its selected bucket, and saved failures. */
export async function getMigrationFailureBootstrap(
  migrationId: string,
  itemId: string,
  limit = 150
) {
  const boundedLimit = Math.max(1, Math.min(500, Math.floor(limit)))
  const { rows } = await queryDb<{
    migration: DriveMigrationRow | null
    item: DriveMigrationItemRow | null
    failures: Array<{
      objectKey: string | null
      message: string | null
      occurredAtText: string | null
      rawLog: unknown
      sourceProbe: unknown
      destinationProbe: unknown
      diagnosis: unknown
      download: unknown
      fetchedAt: string | null
    }> | null
  }>(`
    with selected_migration as (
      select migration.*,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.item_count,0)::integer else migration.summary_item_count end as resolved_summary_item_count,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_objects,0)::bigint else migration.summary_objects end as resolved_summary_objects,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_bytes,0)::bigint else migration.summary_bytes end as resolved_summary_bytes
      from public.drive_migrations migration
      left join lateral (
        select count(*) as item_count,
          coalesce(sum(source_objects),0) as summary_objects,
          coalesce(sum(source_bytes),0) as summary_bytes
        from public.drive_migration_items
        where migration_id=migration.id
      ) legacy_summary on migration.status='completed' and migration.summary_item_count=0
      where migration.id=$1
      limit 1
    ), selected_item as (
      select item.* from public.drive_migration_items item
      join selected_migration migration on migration.id=item.migration_id
      where item.id=$2
      limit 1
    ), failure_rows as (
      select failure.object_key,failure.message,failure.occurred_at_text,failure.raw_log,
        failure.source_probe,failure.destination_probe,failure.diagnosis,failure.download,failure.fetched_at
      from public.drive_migration_item_failure_records failure
      join selected_item item on item.id=failure.migration_item_id
      order by failure.occurred_at desc nulls last,failure.fetched_at desc,failure.id desc
      limit $3
    )
    select
      (select (to_jsonb(migration) - 'resolved_summary_item_count' - 'resolved_summary_objects' - 'resolved_summary_bytes') || jsonb_build_object(
        'summary_item_count',migration.resolved_summary_item_count::text,
        'summary_objects',migration.resolved_summary_objects::text,
        'summary_bytes',migration.resolved_summary_bytes::text
      ) from selected_migration migration) as migration,
      (select to_jsonb(item) || jsonb_build_object(
        'source_objects',item.source_objects::text,
        'source_bytes',item.source_bytes::text
      ) from selected_item item) as item,
      coalesce((select jsonb_agg(jsonb_build_object(
        'objectKey',failure.object_key,'message',failure.message,'occurredAtText',failure.occurred_at_text,
        'rawLog',failure.raw_log,'sourceProbe',failure.source_probe,'destinationProbe',failure.destination_probe,
        'diagnosis',failure.diagnosis,'download',failure.download,'fetchedAt',failure.fetched_at
      ) order by failure.occurred_at desc nulls last,failure.fetched_at desc)
      from failure_rows failure),'[]'::jsonb) as failures
  `, [migrationId, itemId, boundedLimit])
  const row = rows[0]
  return {
    migration: row?.migration ? mapMigrationRow(row.migration) : null,
    item: row?.item ? mapMigrationItemRow(row.item) : null,
    failures: row?.failures ?? [],
  }
}

/** One round trip for migration details, account choices, items, and worker-run telemetry. */
export async function getMigrationDetailBootstrap(
  id: string,
  options: { includeAccounts?: boolean } = {}
): Promise<MigrationDetailBootstrap | null> {
  const { rows } = await queryDb<{
    migration: DriveMigrationRow | null
    items: DriveMigrationItemRow[] | null
    accounts: Array<{ id: string; label: string; email: string; status: string }> | null
    worker_runs: MigrationWorkerRunRow[] | null
  }>(`
    with selected_migration as (
      select migration.*,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.item_count,0)::integer else migration.summary_item_count end as resolved_summary_item_count,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_objects,0)::bigint else migration.summary_objects end as resolved_summary_objects,
        case when migration.status='completed' and migration.summary_item_count=0
          then coalesce(legacy_summary.summary_bytes,0)::bigint else migration.summary_bytes end as resolved_summary_bytes
      from public.drive_migrations migration
      left join lateral (
        select count(*) as item_count,
          coalesce(sum(source_objects),0) as summary_objects,
          coalesce(sum(source_bytes),0) as summary_bytes
        from public.drive_migration_items
        where migration_id=migration.id
      ) legacy_summary on migration.status='completed' and migration.summary_item_count=0
      where migration.id=$1
      limit 1
    )
    select
      (select (to_jsonb(migration) - 'resolved_summary_item_count' - 'resolved_summary_objects' - 'resolved_summary_bytes') || jsonb_build_object(
        'summary_item_count',migration.resolved_summary_item_count::text,
        'summary_objects',migration.resolved_summary_objects::text,
        'summary_bytes',migration.resolved_summary_bytes::text
      ) from selected_migration migration) as migration,
      coalesce((
        select jsonb_agg(
          to_jsonb(item) || jsonb_build_object(
            'source_objects',item.source_objects::text,
            'source_bytes',item.source_bytes::text,
            'verification_state',(
              select jsonb_build_object(
                'generation',verification.generation,
                'status',verification.status,
                'missing_objects',verification.missing_objects,
                'mismatched_objects',verification.mismatched_objects,
                'extra_objects',verification.extra_objects,
                'attempt_id',item.progress->>'verificationAttemptId',
                'strict_destination',migration.options->>'verifyStrictDestination'='true',
                'updated_at',verification.updated_at
              )
              from public.drive_migration_verification_state verification
              where verification.migration_item_id=item.id
              order by verification.generation desc,verification.updated_at desc
              limit 1
            )
          ) order by item.source_bucket asc,item.id asc
        )
        from public.drive_migration_items item
        join selected_migration migration on migration.id=item.migration_id
      ),'[]'::jsonb) as items,
      case when $2::boolean then coalesce((
        select jsonb_agg(jsonb_build_object('id',account.id,'label',account.label,'email',account.email,'status',account.status)
          order by account.updated_at desc nulls last,account.created_at desc,account.id desc)
        from public.drive_accounts account
      ),'[]'::jsonb) else '[]'::jsonb end as accounts,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'id',run.id,
          'job_reference',run.job_reference,
          'agent_id',run.agent_id,
          'worker_generation',greatest(1,coalesce(nullif(run.payload->>'workerGeneration','')::int,1)),
          'abort_requested',(run.payload->>'githubAbortRequestedAt') is not null,
          'status',run.status,
          'online',(run.status='running' and run.agent_status='online' and run.agent_last_heartbeat>now()-interval '90 seconds'),
          'external_run_id',run.external_run_id,
          'instance_id',run.payload->>'workerInstanceId',
          'job_status',job.status,
          'job_heartbeat',job.last_heartbeat_at,
          'current_file',case
            when jsonb_typeof(job.progress->'currentFile')='object' then job.progress->'currentFile'
            when jsonb_typeof(job.payload->'inventoryObjects'->0)='object' then job.payload->'inventoryObjects'->0
            else null
          end,
          'completed_files',coalesce((run.payload->>'completedFiles')::bigint,0)::text,
          'failed_files',coalesce((run.payload->>'failedFiles')::bigint,0)::text,
          'completed_bytes',coalesce((run.payload->>'completedBytes')::bigint,0)::text,
          'created_at',run.created_at,
          'updated_at',run.updated_at
        ) order by run.created_at asc)
        from (
          select run.*,agent.status as agent_status,agent.last_heartbeat_at as agent_last_heartbeat
          from public.drive_agent_runs run
          join selected_migration migration on migration.options->>'executionMode'='migration_workers'
            and run.run_type='github_dispatch'
            and run.payload->>'migrationId'=migration.id::text
            and greatest(1,coalesce(nullif(run.payload->>'workerGeneration','')::int,1))=greatest(1,coalesce(nullif(migration.options->>'workerGeneration','')::int,1))
          left join public.drive_agents agent on agent.id=run.agent_id
          order by run.created_at asc
          limit 100
        ) run
        left join public.drive_repair_jobs job on job.id::text=run.job_reference
      ),'[]'::jsonb) as worker_runs
  `, [id, options.includeAccounts !== false])
  const row = rows[0]
  if (!row?.migration) return null
  return {
    migration: mapMigrationRow(row.migration),
    items: (Array.isArray(row.items) ? row.items : []).map(mapMigrationItemRow),
    accounts: Array.isArray(row.accounts) ? row.accounts : [],
    workerRuns: (Array.isArray(row.worker_runs) ? row.worker_runs : []).map(mapMigrationWorkerRun),
  }
}

export async function listMigrationItems(migrationId: string): Promise<DriveMigrationItem[]> {
  const { rows } = await queryDb<DriveMigrationItemRow>(
    `select * from public.${MIGRATION_ITEMS_TABLE}
     where migration_id = $1 order by source_bucket asc, id asc`,
    [migrationId]
  )
  return rows.map(mapMigrationItemRow)
}

export async function getMigrationItem(migrationId: string, itemId: string): Promise<DriveMigrationItem | null> {
  const { rows } = await queryDb<DriveMigrationItemRow>(
    `select * from public.${MIGRATION_ITEMS_TABLE} where migration_id=$1 and id=$2 limit 1`,
    [migrationId, itemId]
  )
  return rows[0] ? mapMigrationItemRow(rows[0]) : null
}

export async function queueMigrationItemVerification(migrationId: string, itemId: string, generation = 1): Promise<string> {
  const result = await queryDb(`
    with active_migration as (
      update public.drive_migrations set status='verifying',completed_at=null,sync_status='syncing',
        sync_message='File Scanner verification requested',last_synced_at=now(),updated_at=now()
      where id=$2 and status in('running','verifying','verification_failed') returning id
    ), queued as (
      insert into public.drive_migration_verification_state
      (migration_item_id,migration_id,generation,status,phase)
      select $1,m.id,$3,'pending','source' from active_migration m
      on conflict(migration_item_id) do update set
        migration_id=excluded.migration_id,
        generation=excluded.generation,
        source_scan_id=null,destination_scan_id=null,phase='source',status='pending',
        source_cursor=null,destination_cursor=null,source_objects=0,source_bytes=0,
        destination_objects=0,destination_bytes=0,missing_objects=0,mismatched_objects=0,
        extra_objects=0,attempt_count=0,attempt_generation=null,last_error=null,
        lease_owner=null,lease_expires_at=null,completed_at=null,updated_at=now()
      where drive_migration_verification_state.status not in('pending','running')
      returning migration_item_id
    ), attempts as (
      select migration_item_id,gen_random_uuid()::text attempt_id from queued
    ), tagged as (
      update public.drive_migration_items i set slurper_status='verifying',
        progress=jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(coalesce(i.progress,'{}'::jsonb),'{verificationAttemptId}',to_jsonb(a.attempt_id),true),
              '{verificationGeneration}',to_jsonb($3::int),true),
            '{stage}','"verification_queued"'::jsonb,true),
          '{live}',coalesce(i.progress->'live','{}'::jsonb)||jsonb_build_object('status','verifying','updatedAt',now()),true)
          || jsonb_build_object('fileVerification',jsonb_build_object('status','pending','requestedAt',now()),
            'events',(case when jsonb_typeof(i.progress->'events')='array' then i.progress->'events' else '[]'::jsonb end)
              || jsonb_build_array(jsonb_build_object('at',now(),'stage','file_verification','status','running',
                'generation',$3::int,'attemptId',a.attempt_id,'message','Manual File Scanner verification requested'))),
        last_progress_at=now(),updated_at=now()
      from attempts a where i.id=a.migration_item_id and i.migration_id=$2
      returning i.id,a.attempt_id
    ) select id,attempt_id from tagged
  `, [itemId, migrationId, generation])
  if (result.rowCount !== 1) throw new Error("File verification is already queued or running for this bucket")
  const attemptId = String((result.rows[0] as { attempt_id?: string } | undefined)?.attempt_id || "")
  if (!attemptId) throw new Error("Unable to persist the File Scanner verification attempt")
  return attemptId
}

export async function deleteMigration(id: string): Promise<void> {
  await queryDb(`delete from public.${MIGRATIONS_TABLE} where id = $1`, [id])
}

export async function createMigration(input: {
  sourceAccountId: string
  targetAccountId: string
  options?: MigrationOptions
  items: Array<{
    sourceBucket: string
    targetBucket: string
    sourceJurisdiction?: string
    sourceStorageClass?: string
    sourceObjects?: number
    sourceBytes?: number
  }>
  activity?: Pick<RecordActivityInput, "actorUserId" | "ipAddress" | "userAgent" | "requestId"> & { sourceLabel: string; targetLabel: string }
}): Promise<{ migration: DriveMigration; items: DriveMigrationItem[] }> {
  if (input.activity) await ensureActivitySchema()
  const now = new Date().toISOString()
  const migrationId = crypto.randomUUID()

  const itemRows = input.items.map((item) => ({
    id: crypto.randomUUID(),
    migration_id: migrationId,
    source_bucket: item.sourceBucket,
    target_bucket: item.targetBucket,
    source_jurisdiction: item.sourceJurisdiction ?? null,
    source_storage_class: item.sourceStorageClass ?? null,
    source_objects: typeof item.sourceObjects === "number" ? item.sourceObjects : null,
    source_bytes: typeof item.sourceBytes === "number" ? item.sourceBytes : null,
    progress: {},
    created_at: now,
    updated_at: now,
  }))
  return withDbTransaction(async (client) => {
    const createdMigration = await client.query<DriveMigrationRow>(
      `insert into public.${MIGRATIONS_TABLE} (
         id, source_account_id, target_account_id, status, options, created_at, updated_at
       ) values ($1,$2,$3,'draft',$4::jsonb,$5,$5) returning *`,
      [migrationId, input.sourceAccountId, input.targetAccountId, JSON.stringify(input.options ?? {}), now]
    )
    let createdItems: DriveMigrationItemRow[] = []
    if (itemRows.length > 0) {
      const values: unknown[] = []
      const tuples = itemRows.map((item) => {
        values.push(item.id, item.migration_id, item.source_bucket, item.target_bucket,
          item.source_jurisdiction, item.source_storage_class, item.source_objects,
          item.source_bytes, JSON.stringify(item.progress), item.created_at, item.updated_at)
        const base = values.length - 11
        return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9}::jsonb,$${base + 10},$${base + 11})`
      })
      const inserted = await client.query<DriveMigrationItemRow>(
        `insert into public.${MIGRATION_ITEMS_TABLE} (
           id, migration_id, source_bucket, target_bucket, source_jurisdiction,
           source_storage_class, source_objects, source_bytes, progress, created_at, updated_at
         ) values ${tuples.join(", ")} returning *`,
        values
      )
      createdItems = inserted.rows
    }
    if (input.activity) {
      await recordActivityInTransaction(client, {
        actorUserId: input.activity.actorUserId,
        ipAddress: input.activity.ipAddress,
        userAgent: input.activity.userAgent,
        requestId: input.activity.requestId,
        action: "migration.created",
        entityType: "migration",
        entityId: migrationId,
        entityLabel: `${input.activity.sourceLabel} → ${input.activity.targetLabel}`,
        summary: `Created migration from ${input.activity.sourceLabel} to ${input.activity.targetLabel}`,
        detail: `${itemRows.length} bucket${itemRows.length === 1 ? "" : "s"} selected.`,
        after: { status: "draft", bucketCount: itemRows.length, executionMode: input.options?.executionMode ?? "super_slurper" },
        undoable: true,
        undoPayload: { type: "delete_draft_migration", migrationId },
      })
    }
    return {
      migration: mapMigrationRow(createdMigration.rows[0]),
      items: createdItems.map(mapMigrationItemRow),
    }
  })
}

export async function updateMigration(
  id: string,
  updates: Partial<Pick<DriveMigration, "status" | "lastSyncedAt" | "syncStatus" | "syncMessage" | "options">> & {
    // Allow explicit null to clear DB fields.
    startedAt?: string | null
    completedAt?: string | null
  }
): Promise<DriveMigration> {
  const dbUpdates: Record<string, unknown> = { updated_at: new Date().toISOString() }
  if (updates.status !== undefined) dbUpdates.status = updates.status
  if (updates.startedAt !== undefined) dbUpdates.started_at = updates.startedAt ?? null
  if (updates.completedAt !== undefined) dbUpdates.completed_at = updates.completedAt ?? null
  if (updates.lastSyncedAt !== undefined) dbUpdates.last_synced_at = updates.lastSyncedAt ?? null
  if (updates.syncStatus !== undefined) dbUpdates.sync_status = updates.syncStatus ?? null
  if (updates.syncMessage !== undefined) dbUpdates.sync_message = updates.syncMessage ?? null
  if (updates.options !== undefined) dbUpdates.options = JSON.stringify(updates.options ?? {})
  const values: unknown[] = [id]
  const assignments = Object.entries(dbUpdates).map(([column, value], index) => {
    values.push(value)
    return `${column} = $${index + 2}${column === "options" ? "::jsonb" : ""}`
  })
  const { rows } = await queryDb<DriveMigrationRow>(
    `update public.${MIGRATIONS_TABLE} set ${assignments.join(", ")} where id = $1 returning *`,
    values
  )
  if (!rows[0]) throw new Error("Migration not found")
  const migration = mapMigrationRow(rows[0])
  if (updates.status === "completed") {
    const { rows: summaryRows } = await queryDb<{
      summary_item_count: string | number
      summary_objects: string | number
      summary_bytes: string | number
    }>(
      `select count(*) as summary_item_count,
         coalesce(sum(source_objects), 0) as summary_objects,
         coalesce(sum(source_bytes), 0) as summary_bytes
       from public.${MIGRATION_ITEMS_TABLE} where migration_id = $1`,
      [id]
    )
    const summaryRow = summaryRows[0]
    const summary = {
      summary_item_count: nonNegativeInteger(summaryRow?.summary_item_count),
      summary_objects: nonNegativeInteger(summaryRow?.summary_objects),
      summary_bytes: nonNegativeInteger(summaryRow?.summary_bytes),
    }
    await queryDb(
      `update public.${MIGRATIONS_TABLE}
       set summary_item_count = $2, summary_objects = $3, summary_bytes = $4
       where id = $1`,
      [id, summary.summary_item_count, summary.summary_objects, summary.summary_bytes]
    )
    migration.summaryItemCount = summary.summary_item_count
    migration.summaryObjects = summary.summary_objects
    migration.summaryBytes = summary.summary_bytes

    // Preserve the object-level accounting before retention can remove the
    // migration items. `completed` means the destination was reconciled; it
    // does not mean every object was copied during this run. Exact matches
    // remain skipped and must never be presented as transferred.
    const { rows: itemRows } = await queryDb<DriveMigrationItemRow>(
      `select * from public.${MIGRATION_ITEMS_TABLE} where migration_id = $1`,
      [id]
    )
    const objectCounts = itemRows.reduce(
      (totals, row) => {
        const counts = compactedObjectCounts(row)
        totals.transferred += counts.transferred
        totals.skipped += counts.skipped
        totals.failed += counts.failed
        totals.unaccounted += counts.unaccounted
        return totals
      },
      { transferred: 0, skipped: 0, failed: 0, unaccounted: 0 }
    )
    await queryDb(
      `update public.${MIGRATIONS_TABLE}
       set worker_summary = jsonb_set(
         coalesce(worker_summary, '{}'::jsonb),
         '{objectCounts}',
         $2::jsonb,
         true
       )
       where id = $1`,
      [id, JSON.stringify(objectCounts)]
    )
    migration.workerSummary = { ...migration.workerSummary, objectCounts }
    await compactPreviousMigrationDetails(id).catch((cleanupError) => {
      console.error("Unable to compact previous migration details:", cleanupError)
    })
  }
  return migration
}

export async function enrollMigrationWorkerAgent(migrationId: string, agentId: string): Promise<void> {
  return enrollMigrationWorkerAgents(migrationId, [agentId])
}

export async function enrollMigrationWorkerAgents(migrationId: string, agentIds: string[]): Promise<void> {
  const uniqueAgentIds = Array.from(new Set(agentIds.filter(Boolean)))
  if (uniqueAgentIds.length === 0) return
  await queryDb(
    `
      update drive_migrations m
      set options = jsonb_set(
        jsonb_set(
          jsonb_set(
            coalesce(m.options, '{}'::jsonb),
            '{workerAgentIds}',
            $2::jsonb,
            true
          ),
          '{workerShardCount}',
          to_jsonb(least(128, greatest(
            coalesce((m.options->>'workerShardCount')::integer, 32),
            coalesce((select sum(least(5, greatest(1, coalesce(a.worker_count, 1))))::integer from drive_agents a where a.id::text in (select jsonb_array_elements_text($2::jsonb)) and a.provider='github_actions' and a.status<>'disabled'), 1)
          ))),
          true
        ),
        '{requireIndependentVerification}',
        coalesce(m.options->'requireIndependentVerification', 'true'::jsonb),
        true
      ), updated_at = now()
      where m.id = $1
    `,
    [migrationId, JSON.stringify(uniqueAgentIds)]
  )
}

export async function claimMigrationSyncLock(input: {
  migrationId: string
  // If the last sync is older than this many ms, allow another sync to take over.
  ttlMs?: number
  message?: string
}): Promise<boolean> {
  const now = new Date()
  const nowIso = now.toISOString()
  const ttlMs = typeof input.ttlMs === "number" && Number.isFinite(input.ttlMs) ? Math.max(500, input.ttlMs) : 12_000
  const cutoff = new Date(now.getTime() - ttlMs).toISOString()
  const { rowCount } = await queryDb(
    `update public.${MIGRATIONS_TABLE}
     set sync_status = 'syncing', sync_message = $3,
         last_synced_at = $4, updated_at = $4
     where id = $1
       and (sync_status is null or sync_status <> 'syncing'
            or last_synced_at is null or last_synced_at < $2)`,
    [input.migrationId, cutoff, input.message ?? "Syncing", nowIso]
  )
  return (rowCount ?? 0) > 0
}

export async function updateMigrationItem(
  id: string,
  updates: Partial<Pick<DriveMigrationItem, "progress" | "lastProgressAt" | "sourceObjects" | "sourceBytes">> & {
    // Allow explicit null to clear DB fields.
    slurperJobId?: string | null
    slurperStatus?: string | null
  }
): Promise<DriveMigrationItem> {
  return withDbTransaction(async (client) => {
    const current = await client.query<DriveMigrationItemRow>(
      `select * from public.${MIGRATION_ITEMS_TABLE} where id = $1 for update`, [id]
    )
    const currentRow = current.rows[0]
    if (!currentRow) throw new Error("Migration item not found")
    const dbUpdates: Record<string, unknown> = { updated_at: new Date().toISOString() }
    if (updates.slurperJobId !== undefined) dbUpdates.slurper_job_id = updates.slurperJobId ?? null
    if (updates.slurperStatus !== undefined) dbUpdates.slurper_status = updates.slurperStatus ?? null
    if (updates.progress !== undefined) {
      const currentProgress = currentRow.progress && typeof currentRow.progress === "object" ? currentRow.progress : {}
      const incomingProgress = updates.progress && typeof updates.progress === "object" ? updates.progress : {}
      const nextProgress: Record<string, unknown> = { ...currentProgress, ...incomingProgress }
      if (updates.slurperStatus !== undefined) nextProgress.slurperStatus = updates.slurperStatus ?? null
      const stage = typeof nextProgress.stage === "string" ? nextProgress.stage : undefined
      const error = typeof nextProgress.error === "string" ? nextProgress.error : undefined
      const lastError = typeof nextProgress.lastError === "string" ? nextProgress.lastError : undefined
      const message = error ?? lastError
      if (stage || message || updates.slurperStatus !== undefined) {
        dbUpdates.progress = appendProgressEvent(nextProgress, {
          at: new Date().toISOString(), stage,
          status: updates.slurperStatus ?? null, message,
        })
      } else dbUpdates.progress = nextProgress
    }
    if (updates.lastProgressAt !== undefined) dbUpdates.last_progress_at = updates.lastProgressAt ?? null
    if (updates.sourceObjects !== undefined) dbUpdates.source_objects = updates.sourceObjects ?? null
    if (updates.sourceBytes !== undefined) dbUpdates.source_bytes = updates.sourceBytes ?? null

    const values: unknown[] = [id]
    const assignments = Object.entries(dbUpdates).map(([column, value], index) => {
      values.push(column === "progress" ? JSON.stringify(value) : value)
      return `${column} = $${index + 2}${column === "progress" ? "::jsonb" : ""}`
    })
    const updated = await client.query<DriveMigrationItemRow>(
      `update public.${MIGRATION_ITEMS_TABLE} set ${assignments.join(", ")} where id = $1 returning *`, values
    )
    return mapMigrationItemRow(updated.rows[0])
  })
}

export async function mergeMigrationItemProgressState(
  id: string,
  patch: Record<string, unknown>,
  lastProgressAt?: string | null
): Promise<DriveMigrationItem> {
  return withDbTransaction(async (client) => {
    const current = await client.query<DriveMigrationItemRow>(
      `select * from public.${MIGRATION_ITEMS_TABLE} where id = $1 for update`, [id]
    )
    const currentRow = current.rows[0]
    if (!currentRow) throw new Error("Migration item not found")
    const currentProgress = currentRow.progress && typeof currentRow.progress === "object" ? currentRow.progress : {}
    const nextProgress = { ...currentProgress, ...patch }
    const updated = await client.query<DriveMigrationItemRow>(
      `update public.${MIGRATION_ITEMS_TABLE}
       set progress = $2::jsonb, updated_at = now(),
           last_progress_at = case when $3::boolean then $4::timestamptz else last_progress_at end
       where id = $1 returning *`,
      [id, JSON.stringify(nextProgress), lastProgressAt !== undefined, lastProgressAt ?? null]
    )
    return mapMigrationItemRow(updated.rows[0])
  })
}

export async function claimMigrationItemJobCreation(input: {
  itemId: string
  progress: Record<string, unknown>
}): Promise<boolean> {
  const progress: Record<string, unknown> =
    input.progress && typeof input.progress === "object" ? (input.progress as Record<string, unknown>) : {}
  progress.slurperStatus = "creating_job"
  const { rowCount } = await queryDb(
    `update public.${MIGRATION_ITEMS_TABLE}
     set slurper_status = 'creating_job', progress = $2::jsonb,
         last_progress_at = now(), updated_at = now()
     where id = $1 and slurper_job_id is null
       and (slurper_status is null or slurper_status <> 'creating_job')
       and (slurper_status is null or slurper_status <> 'job_id_pending')
     returning id`,
    [input.itemId, JSON.stringify(progress)]
  )
  return (rowCount ?? 0) > 0
}
