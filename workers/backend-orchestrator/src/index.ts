import { Client } from "pg"
import { AwsClient } from "aws4fetch"

type Env = {
  PANEL_URL: string
  BACKEND_ORCHESTRATOR_SECRET: string
  POSTGRES_URL?: string
  HYPERDRIVE?: Hyperdrive
  SYNC_INTERVAL_MINUTES: string
  API_EVENTS_RETENTION_DAYS: string
  OBJECT_CHANGES_RETENTION_DAYS: string
  SCAN_DETAILS_RETENTION_DAYS: string
  DISABLE_POSTGRES_SSL: string
}

type RuntimeConfig = {
  version: number
  postgresUrl: string
  syncIntervalMinutes: number
  retention: { apiEventsDays: number; objectChangesDays: number; scanDetailsDays: number }
  disablePostgresSsl: boolean
  hyperdrive: boolean
}

type Hyperdrive = { connectionString: string }

type AccountRow = {
  id: string
  label: string
  email: string
  api_token: string
  cloudflare_account_id: string | null
  r2_access_key_id: string | null
  r2_secret_access_key: string | null
  status: string
  last_synced_at: string | null
}

type BucketInfo = {
  name: string
  creationDate: string | null
  jurisdiction: string
  location: string | null
  storageClass: string
}
type BucketMetric = { bucket: string; objects: number; bytes: number; observedAt: string }
type PublishedAccountMetrics = { objects: number; bytes: number }
type BucketCorsRule = {
  id?: string
  allowedOrigins: string[]
  allowedMethods: string[]
  allowedHeaders: string[]
  exposeHeaders: string[]
  maxAgeSeconds?: number
}
type BucketSettings = {
  publicAccess: { enabled: boolean; domain: string | null; bucketId: string | null }
  corsRules: BucketCorsRule[]
}

// Each bucket settings refresh uses two Cloudflare API requests. Keep the batch
// bounded so one invocation stays below Workers subrequest limits and can
// persist progress after every bucket.
const BUCKET_BATCH_SIZE = 10
const EXTERNAL_REQUEST_TIMEOUT_MS = 8_000
const METRICS_CACHE_TTL_MS = 10_000
const WORKER_BUILD = 25
const REQUIRED_SCHEMA_VERSION = 2026091303
const RETENTION_BATCH_SIZE = 250
const metricsCache = new Map<string, { expiresAt: number; metrics: BucketMetric[] }>()

function json(value: unknown, status = 200) {
  return Response.json(value, { status, headers: { "Cache-Control": "no-store" } })
}

async function fetchWithTimeout(input: RequestInfo | URL, init: RequestInit = {}, timeoutMs = EXTERNAL_REQUEST_TIMEOUT_MS) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(input, { ...init, signal: controller.signal })
  } catch (error) {
    if (controller.signal.aborted) throw new Error(`External request timed out after ${timeoutMs}ms`)
    throw error
  } finally {
    clearTimeout(timeout)
  }
}

function panelUrl(env: Env, path: string) {
  return `${env.PANEL_URL.trim().replace(/\/$/, "")}${path}`
}

function authorized(request: Request, env: Env) {
  const header = request.headers.get("authorization") ?? ""
  return header.startsWith("Bearer ") && header.slice(7).trim() === env.BACKEND_ORCHESTRATOR_SECRET.trim()
}

function boundedInteger(value: string, fallback: number, minimum: number, maximum: number) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? Math.max(minimum, Math.min(maximum, Math.floor(parsed))) : fallback
}

function envFlag(value: string | undefined) {
  const normalized = String(value ?? "").trim().toLowerCase()
  return normalized === "1" || normalized === "true"
}

function runtimeConfig(env: Env): RuntimeConfig {
  const hyperdrive = Boolean(env.HYPERDRIVE?.connectionString?.trim())
  const postgresUrl = hyperdrive ? env.HYPERDRIVE!.connectionString.trim() : env.POSTGRES_URL?.trim()
  if (!postgresUrl) throw new Error("Neither HYPERDRIVE nor POSTGRES_URL was injected during deployment")
  return {
    version: 2,
    postgresUrl,
    syncIntervalMinutes: boundedInteger(env.SYNC_INTERVAL_MINUTES, 1, 1, 60),
    retention: {
      apiEventsDays: boundedInteger(env.API_EVENTS_RETENTION_DAYS, 7, 1, 365),
      objectChangesDays: boundedInteger(env.OBJECT_CHANGES_RETENTION_DAYS, 7, 1, 365),
      scanDetailsDays: boundedInteger(env.SCAN_DETAILS_RETENTION_DAYS, 7, 1, 365),
    },
    disablePostgresSsl: !hyperdrive && envFlag(env.DISABLE_POSTGRES_SSL),
    hyperdrive,
  }
}

function dbClient(connectionString: string, disablePostgresSsl: boolean, hyperdrive = false) {
  const sslMode = new URL(connectionString).searchParams.get("sslmode")?.trim().toLowerCase()
  return new Client({
    connectionString,
    ...(hyperdrive ? {} : { ssl: disablePostgresSsl || sslMode === "disable" ? false : { rejectUnauthorized: false } }),
    connectionTimeoutMillis: 10_000,
    query_timeout: 20_000,
    statement_timeout: 20_000,
    keepAlive: true,
    keepAliveInitialDelayMillis: 5_000,
  })
}

async function ensureSchema(db: Client) {
  const result = await db.query<{ version: string | number }>(`select version from drive_schema_meta where id=true limit 1`)
  const version = Number(result.rows[0]?.version ?? 0)
  if (version < REQUIRED_SCHEMA_VERSION) {
    throw new Error(`Database schema ${version || "unknown"} is behind Backend Orchestrator requirement ${REQUIRED_SCHEMA_VERSION}; deploy the panel schema first`)
  }
}

async function legacyEnsureSchema(db: Client) {
  await db.query(`
    create table if not exists drive_backend_orchestrator_state (
      id boolean primary key default true check (id),
      status text not null default 'idle',
      orchestrator_url text,
      last_started_at timestamptz,
      last_completed_at timestamptz,
      last_error text,
      last_result jsonb not null default '{}'::jsonb,
      updated_at timestamptz not null default now()
    )
  `)
  await db.query(`
    create table if not exists drive_storage_stats_history (
      id bigint generated always as identity primary key,
      account_id uuid not null,
      account_label text,
      account_email text,
      bucket_name text not null,
      previous_objects bigint,
      objects bigint not null default 0,
      object_delta bigint not null default 0,
      previous_bytes bigint,
      bytes bigint not null default 0,
      byte_delta bigint not null default 0,
      change_type text not null,
      changed_at timestamptz not null default now()
    )
  `)
  await db.query(`create index if not exists drive_storage_stats_history_bucket_time_idx on drive_storage_stats_history (account_id, bucket_name, changed_at desc)`)
  await db.query(`create index if not exists drive_storage_stats_history_time_idx on drive_storage_stats_history (changed_at desc)`)
  await db.query(`
    create table if not exists drive_bucket_settings_snapshots (
      account_id uuid not null references drive_accounts(id) on delete cascade,
      bucket_name text not null,
      bucket_created_at timestamptz,
      jurisdiction text not null default 'default',
      location text,
      storage_class text not null default 'Standard',
      public_access jsonb not null default '{"enabled":false,"domain":null,"bucketId":null}'::jsonb,
      cors_rules jsonb not null default '[]'::jsonb,
      settings_status text not null default 'pending',
      settings_error text,
      settings_last_attempted_at timestamptz,
      settings_last_synced_at timestamptz,
      inventory_synced_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (account_id, bucket_name)
    )
  `)
  await db.query(`create index if not exists drive_bucket_settings_snapshots_account_idx on drive_bucket_settings_snapshots (account_id)`)
  await db.query(`
    create table if not exists drive_analytics_bucket_snapshots (
      account_id uuid not null,
      account_label text,
      account_email text,
      bucket_name text not null,
      objects bigint not null default 0,
      bytes bigint not null default 0,
      status text,
      source_updated_at timestamptz,
      captured_at timestamptz not null default now(),
      primary key (account_id, bucket_name)
    )
  `)
  await db.query(`
    create table if not exists drive_analytics_active_account_snapshots (
      captured_day date primary key,
      account_id uuid not null,
      account_label text,
      account_email text,
      buckets integer not null default 0,
      objects bigint not null default 0,
      bytes bigint not null default 0,
      bucket_fingerprint text,
      captured_at timestamptz not null default now()
    )
  `)
  await db.query(`alter table drive_analytics_active_account_snapshots add column if not exists bucket_fingerprint text`)
  await db.query(`
    with ranked as (
      select ctid, row_number() over (
        partition by captured_day
        order by captured_at desc, account_id desc
      ) position
      from drive_analytics_active_account_snapshots
    )
    delete from drive_analytics_active_account_snapshots snapshots
    using ranked
    where snapshots.ctid = ranked.ctid and ranked.position > 1
  `)
  await db.query(`create unique index if not exists drive_analytics_active_account_snapshots_one_per_day_idx on drive_analytics_active_account_snapshots (captured_day)`)
  await db.query(`create index if not exists drive_analytics_active_account_snapshots_day_idx on drive_analytics_active_account_snapshots (captured_day desc)`)
  await db.query(`
    create table if not exists drive_backend_orchestrator_metric_candidates (
      account_id uuid not null,
      bucket_name text not null,
      objects bigint not null,
      bytes bigint not null,
      observed_at timestamptz not null,
      confirmations integer not null default 1,
      first_observed_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      primary key (account_id, bucket_name)
    )
  `)
  await db.query(`
    create table if not exists drive_maintenance_state (
      task_name text primary key,
      last_run_at timestamptz not null default now(),
      last_result jsonb not null default '{}'::jsonb
    )
  `)
  await db.query(`
    create table if not exists drive_backend_orchestrator_progress (
      id boolean primary key default true check (id),
      account_id uuid not null,
      bucket_names jsonb not null default '[]'::jsonb,
      bucket_offset integer not null default 0 check (bucket_offset >= 0),
      reconciled boolean not null default false,
      metrics_incomplete boolean not null default false,
      pending_decreases boolean not null default false,
      started_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `)
}

async function legacyEnsureProgressSchema(db: Client) {
  await db.query(`do $$ begin
    if to_regclass('public.drive_bucket_stat_history') is not null
       and to_regclass('public.drive_storage_stats_history') is null then
      alter table public.drive_bucket_stat_history rename to drive_storage_stats_history;
    end if;
  end $$`)
  await db.query(`
    create table if not exists drive_backend_orchestrator_progress (
      id boolean primary key default true check (id),
      account_id uuid not null,
      bucket_names jsonb not null default '[]'::jsonb,
      bucket_offset integer not null default 0 check (bucket_offset >= 0),
      reconciled boolean not null default false,
      metrics_incomplete boolean not null default false,
      pending_decreases boolean not null default false,
      started_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    )
  `)
  await db.query(`alter table drive_backend_orchestrator_progress add column if not exists reconciled boolean not null default false`)
  await db.query(`alter table drive_backend_orchestrator_progress add column if not exists metrics_incomplete boolean not null default false`)
  await db.query(`alter table drive_backend_orchestrator_progress add column if not exists pending_decreases boolean not null default false`)
}

async function setState(db: Client, input: { status: string; orchestratorUrl?: string; error?: string | null; result?: unknown; completed?: boolean }) {
  await db.query(
    `
      insert into drive_backend_orchestrator_state
        (id, status, orchestrator_url, last_started_at, last_completed_at, last_error, last_result, updated_at)
      values (true, $1, $2, now(), case when $5::boolean then now() else null end, $3, $4::jsonb, now())
      on conflict (id) do update set
        status = excluded.status,
        orchestrator_url = coalesce(excluded.orchestrator_url, drive_backend_orchestrator_state.orchestrator_url),
        last_started_at = case when excluded.status = 'running' then now() else drive_backend_orchestrator_state.last_started_at end,
        last_completed_at = case when $5::boolean then now() else drive_backend_orchestrator_state.last_completed_at end,
        last_error = excluded.last_error,
        last_result = excluded.last_result,
        updated_at = now()
    `,
    [input.status, input.orchestratorUrl ?? null, input.error ?? null, JSON.stringify(input.result ?? {}), input.completed === true]
  )
}

async function resolveCloudflareAccount(account: AccountRow) {
  const response = await fetchWithTimeout("https://api.cloudflare.com/client/v4/accounts", {
    headers: { Authorization: `Bearer ${account.api_token}` },
  })
  const payload = await response.json().catch(() => ({})) as { result?: unknown; errors?: Array<{ message?: string }> }
  if (!response.ok) throw new Error(payload.errors?.[0]?.message || `Cloudflare account lookup failed (${response.status})`)
  const result = Array.isArray(payload.result) ? payload.result[0] : payload.result
  if (!result || typeof result !== "object" || !("id" in result)) throw new Error(`No Cloudflare account found for ${account.label}`)
  return { id: String((result as { id: unknown }).id), name: String((result as { name?: unknown }).name ?? account.label) }
}

async function listBuckets(account: AccountRow) {
  if (!account.cloudflare_account_id) throw new Error(`Account ${account.label} has no Cloudflare account ID`)
  const response = await fetchWithTimeout(`https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(account.cloudflare_account_id)}/r2/buckets`, {
    headers: { Authorization: `Bearer ${account.api_token}` },
  })
  const payload = await response.json().catch(() => ({})) as { success?: unknown; result?: unknown; errors?: Array<{ message?: string }> }
  if (!response.ok || payload.success !== true) throw new Error(payload.errors?.[0]?.message || `R2 bucket listing failed (${response.status})`)
  const result = payload.result
  const values = Array.isArray(result) ? result : result && typeof result === "object" && Array.isArray((result as { buckets?: unknown }).buckets) ? (result as { buckets: unknown[] }).buckets : null
  if (!values) throw new Error("R2 bucket listing returned an invalid Cloudflare API result")
  return values
    .map((value): BucketInfo => ({
      name: String((value as { name?: unknown })?.name ?? ""),
      creationDate: typeof (value as { creation_date?: unknown })?.creation_date === "string"
        ? String((value as { creation_date: unknown }).creation_date)
        : null,
      jurisdiction: String((value as { jurisdiction?: unknown })?.jurisdiction ?? "default"),
      location: typeof (value as { location?: unknown })?.location === "string"
        ? String((value as { location: unknown }).location)
        : null,
      storageClass: typeof (value as { storage_class?: unknown })?.storage_class === "string"
        ? String((value as { storage_class: unknown }).storage_class)
        : "Standard",
    }))
    .filter((value) => value.name.length > 0)
}

async function cloudflareBucketSubresource(
  account: AccountRow,
  bucket: BucketInfo,
  suffix: string
) {
  if (!account.cloudflare_account_id) throw new Error(`Account ${account.label} has no Cloudflare account ID`)
  const response = await fetchWithTimeout(
    `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(account.cloudflare_account_id)}/r2/buckets/${encodeURIComponent(bucket.name)}/${suffix}`,
    {
      headers: {
        Authorization: `Bearer ${account.api_token}`,
        ...(bucket.jurisdiction !== "default" ? { "cf-r2-jurisdiction": bucket.jurisdiction } : {}),
      },
    }
  )
  const payload = await response.json().catch(() => ({})) as {
    result?: unknown
    errors?: Array<{ message?: string }>
  }
  if (!response.ok) {
    throw new Error(payload.errors?.[0]?.message || `${suffix} lookup failed (${response.status})`)
  }
  return payload.result
}

function stringArray(value: unknown) {
  return Array.isArray(value)
    ? Array.from(new Set(value.filter((item): item is string => typeof item === "string")))
    : []
}

async function readBucketSettings(account: AccountRow, bucket: BucketInfo): Promise<BucketSettings> {
  const [managedResult, corsResult] = await Promise.all([
    cloudflareBucketSubresource(account, bucket, "domains/managed"),
    cloudflareBucketSubresource(account, bucket, "cors"),
  ])
  const managed = managedResult && typeof managedResult === "object"
    ? managedResult as Record<string, unknown>
    : {}
  const cors = corsResult && typeof corsResult === "object"
    ? corsResult as { rules?: unknown }
    : {}
  const corsRules = Array.isArray(cors.rules)
    ? cors.rules.flatMap((value): BucketCorsRule[] => {
        if (!value || typeof value !== "object") return []
        const rule = value as Record<string, unknown>
        const allowed = rule.allowed && typeof rule.allowed === "object"
          ? rule.allowed as Record<string, unknown>
          : {}
        const allowedOrigins = stringArray(allowed.origins)
        const allowedMethods = stringArray(allowed.methods)
        if (allowedOrigins.length === 0 || allowedMethods.length === 0) return []
        return [{
          ...(typeof rule.id === "string" ? { id: rule.id } : {}),
          allowedOrigins,
          allowedMethods,
          allowedHeaders: stringArray(allowed.headers),
          exposeHeaders: stringArray(rule.exposeHeaders),
          ...(typeof rule.maxAgeSeconds === "number" ? { maxAgeSeconds: rule.maxAgeSeconds } : {}),
        }]
      })
    : []
  return {
    publicAccess: {
      enabled: managed.enabled === true,
      domain: typeof managed.domain === "string" && managed.domain ? managed.domain : null,
      bucketId: typeof managed.bucketId === "string" && managed.bucketId ? managed.bucketId : null,
    },
    corsRules,
  }
}

async function getBucketMetrics(account: AccountRow, buckets: BucketInfo[]): Promise<BucketMetric[]> {
  if (!account.cloudflare_account_id) throw new Error(`Account ${account.label} has no Cloudflare account ID`)
  const cacheKey = `${account.id}:${buckets.map((bucket) => `${bucket.jurisdiction}:${bucket.name}`).join("|")}`
  const cached = metricsCache.get(cacheKey)
  if (cached && cached.expiresAt > Date.now()) return cached.metrics
  if (cached) metricsCache.delete(cacheKey)
  const query = `
    query R2Storage($accountTag: string!, $startDate: Time!, $endDate: Time!, $bucketNames: [string!]!) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          r2StorageAdaptiveGroups(
            limit: 10000
            filter: { datetime_geq: $startDate, datetime_leq: $endDate, bucketName_in: $bucketNames }
            orderBy: [datetime_DESC]
          ) {
            max { objectCount payloadSize }
            dimensions { bucketName datetime }
          }
        }
      }
    }
  `
  const response = await fetchWithTimeout("https://api.cloudflare.com/client/v4/graphql", {
    method: "POST",
    headers: { Authorization: `Bearer ${account.api_token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      query,
      variables: {
        accountTag: account.cloudflare_account_id,
        startDate: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
        endDate: new Date().toISOString(),
        bucketNames: buckets.map((bucket) => bucket.jurisdiction === "default" ? bucket.name : `${bucket.jurisdiction}_${bucket.name}`),
      },
    }),
  })
  const payload = await response.json().catch(() => ({})) as {
    data?: { viewer?: { accounts?: Array<{ r2StorageAdaptiveGroups?: Array<{
      dimensions?: { bucketName?: string; datetime?: string }
      max?: { objectCount?: number | string; payloadSize?: number | string }
    }> }> } }
    errors?: Array<{ message?: string }>
  }
  if (!response.ok || payload.errors?.length) {
    throw new Error(payload.errors?.[0]?.message || `R2 analytics query failed (${response.status})`)
  }
  const groups = payload.data?.viewer?.accounts?.[0]?.r2StorageAdaptiveGroups ?? []
  const latestByAnalyticsName = new Map<string, BucketMetric>()
  for (const group of groups) {
    const bucket = group.dimensions?.bucketName
    const observedAt = group.dimensions?.datetime
    const objectCount = group.max?.objectCount
    const payloadSize = group.max?.payloadSize
    if (!bucket || !observedAt || latestByAnalyticsName.has(bucket)) continue
    const objects = typeof objectCount === "number" ? objectCount : Number(objectCount)
    const bytes = typeof payloadSize === "number" ? payloadSize : Number(payloadSize)
    if (!Number.isFinite(objects) || !Number.isFinite(bytes)) continue
    latestByAnalyticsName.set(bucket, {
      bucket,
      objects: Math.max(0, Math.trunc(objects)),
      bytes: Math.max(0, Math.trunc(bytes)),
      observedAt,
    })
  }
  const metrics = buckets.flatMap((bucket) => {
    const analyticsName = bucket.jurisdiction === "default" ? bucket.name : `${bucket.jurisdiction}_${bucket.name}`
    const metric = latestByAnalyticsName.get(analyticsName)
    return metric ? [{ ...metric, bucket: bucket.name }] : []
  })
  metricsCache.set(cacheKey, { expiresAt: Date.now() + METRICS_CACHE_TTL_MS, metrics })
  while (metricsCache.size > 64) metricsCache.delete(metricsCache.keys().next().value as string)
  return metrics
}

async function getBucketMetricsFromR2(account: AccountRow, buckets: BucketInfo[]): Promise<BucketMetric[]> {
  if (!account.cloudflare_account_id || !account.r2_access_key_id || !account.r2_secret_access_key) {
    throw new Error(`R2 S3 credentials are required to calculate per-bucket usage for ${account.label}`)
  }
  const signer = new AwsClient({
    accessKeyId: account.r2_access_key_id,
    secretAccessKey: account.r2_secret_access_key,
    service: "s3",
    region: "auto",
    retries: 2,
  })
  const readXmlTag = (xml: string, tag: string) => {
    const escapedTag = tag.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
    const match = xml.match(new RegExp(`<${escapedTag}>([\\s\\S]*?)</${escapedTag}>`))
    return match?.[1]
      ?.replace(/&#x([\da-f]+);/gi, (_entity, value: string) => String.fromCodePoint(Number.parseInt(value, 16)))
      .replace(/&#(\d+);/g, (_entity, value: string) => String.fromCodePoint(Number.parseInt(value, 10)))
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&apos;/g, "'")
      .replace(/&amp;/g, "&")
  }
  return mapWithConcurrency(buckets, 3, async (bucket) => {
    let continuationToken: string | undefined
    let objects = 0
    let bytes = 0
    do {
      const url = new URL(`https://${account.cloudflare_account_id}.r2.cloudflarestorage.com/${bucket.name}`)
      url.searchParams.set("list-type", "2")
      url.searchParams.set("max-keys", "1000")
      if (continuationToken) url.searchParams.set("continuation-token", continuationToken)
      const response = await signer.fetch(url, { signal: AbortSignal.timeout(25_000) })
      const xml = await response.text()
      if (!response.ok) {
        throw new Error(`R2 object listing failed for ${bucket.name} (${response.status}): ${readXmlTag(xml, "Message") || "unknown S3 error"}`)
      }
      for (const entry of xml.matchAll(/<Contents(?:\s[^>]*)?>([\s\S]*?)<\/Contents>/g)) {
        const size = readXmlTag(entry[1], "Size")
        if (size === undefined || !/^\d+$/.test(size)) throw new Error(`R2 returned an invalid object size for ${bucket.name}`)
        objects += 1
        bytes += Number(size)
      }
      const truncated = readXmlTag(xml, "IsTruncated") === "true"
      continuationToken = truncated ? readXmlTag(xml, "NextContinuationToken") : undefined
      if (truncated && !continuationToken) {
        throw new Error(`R2 object listing for ${bucket.name} was truncated without a continuation token`)
      }
    } while (continuationToken)
    return { bucket: bucket.name, objects, bytes, observedAt: new Date().toISOString() }
  })
}

function isPerBucketAnalyticsAuthorizationError(error: unknown) {
  return /not authorized for that account/i.test(error instanceof Error ? error.message : String(error))
}

async function getPublishedAccountMetrics(account: AccountRow): Promise<PublishedAccountMetrics> {
  if (!account.cloudflare_account_id) throw new Error(`Account ${account.label} has no Cloudflare account ID`)
  const response = await fetchWithTimeout(`https://api.cloudflare.com/client/v4/accounts/${account.cloudflare_account_id}/r2/metrics`, {
    headers: { Authorization: `Bearer ${account.api_token}` },
  })
  const payload = await response.json().catch(() => ({})) as {
    result?: Record<string, { published?: { objects?: number | string; payloadSize?: number | string } }>
    errors?: Array<{ message?: string }>
  }
  if (!response.ok || payload.errors?.length) {
    throw new Error(payload.errors?.[0]?.message || `Published R2 metrics query failed (${response.status})`)
  }
  return Object.values(payload.result ?? {}).reduce(
    (totals, storageClass) => {
      totals.objects += Math.max(0, Math.trunc(Number(storageClass.published?.objects) || 0))
      totals.bytes += Math.max(0, Math.trunc(Number(storageClass.published?.payloadSize) || 0))
      return totals
    },
    { objects: 0, bytes: 0 }
  )
}

async function recordBucketHistory(db: Client, input: { accountId: string; bucket: string; objects: number; bytes: number; deleted: boolean }) {
  await db.query(
    `
      with latest as (
      select objects, bytes, change_type from drive_storage_stats_history
        where account_id = $1 and bucket_name = $2 order by changed_at desc, id desc limit 1
      ), account as (
        select label, email from drive_accounts where id = $1
      ), inserted as (
        insert into drive_storage_stats_history (
          account_id, account_label, account_email, bucket_name,
          previous_objects, objects, object_delta, previous_bytes, bytes, byte_delta, change_type
        )
        select $1, account.label, account.email, $2,
               latest.objects, $3, $3 - coalesce(latest.objects, 0),
               latest.bytes, $4, $4 - coalesce(latest.bytes, 0),
               case when $5 then 'deleted' when latest.objects is null then 'created' else 'changed' end
        from account left join latest on true
        where latest.objects is null or latest.objects is distinct from $3 or latest.bytes is distinct from $4
           or ($5::boolean and latest.change_type <> 'deleted')
        returning account_id, account_label, account_email, bucket_name, objects, bytes, change_type, changed_at
      )
      insert into drive_analytics_bucket_snapshots
        (account_id, account_label, account_email, bucket_name, objects, bytes, status, source_updated_at, captured_at)
      select account_id, account_label, account_email, bucket_name, objects, bytes,
             case when change_type='deleted' then 'deleted' else 'completed' end, changed_at, changed_at
      from inserted
      on conflict (account_id, bucket_name) do update set
        account_label=excluded.account_label, account_email=excluded.account_email,
        objects=excluded.objects, bytes=excluded.bytes, status=excluded.status,
        source_updated_at=excluded.source_updated_at, captured_at=excluded.captured_at
    `,
    [input.accountId, input.bucket, input.objects, input.bytes, input.deleted]
  )
}

async function reconcileBuckets(db: Client, account: AccountRow, buckets: BucketInfo[]) {
  const bucketNames = buckets.map((bucket) => bucket.name)
  const current = await db.query<{ id: string; bucket_name: string; objects: string; bytes: string }>(
    `select id, bucket_name, objects, bytes from drive_bucket_stats where account_id=$1`, [account.id]
  )
  const names = new Set(bucketNames)
  for (const row of current.rows.filter((row) => !names.has(row.bucket_name))) {
    await recordBucketHistory(db, { accountId: account.id, bucket: row.bucket_name, objects: 0, bytes: 0, deleted: true })
    await db.query(`delete from drive_bucket_stats where id=$1`, [row.id])
  }
  await db.query(
    `delete from drive_bucket_settings_snapshots where account_id=$1 and not (bucket_name=any($2::text[]))`,
    [account.id, bucketNames]
  )
  for (const bucket of buckets) {
    await db.query(
      `insert into drive_bucket_stats (id,account_id,bucket_name,objects,bytes,status,updated_at)
       values (gen_random_uuid(),$1,$2,0,0,'pending',now())
       on conflict (account_id,bucket_name) do nothing`,
      [account.id, bucket.name]
    )
    await db.query(
      `insert into drive_bucket_settings_snapshots
        (account_id,bucket_name,bucket_created_at,jurisdiction,location,storage_class,
         settings_status,inventory_synced_at,updated_at)
       values ($1,$2,$3,$4,$5,$6,'syncing',now(),now())
       on conflict(account_id,bucket_name) do update set
         bucket_created_at=excluded.bucket_created_at,
         jurisdiction=excluded.jurisdiction,
         location=excluded.location,
         storage_class=excluded.storage_class,
         settings_status='syncing',
         inventory_synced_at=now(),
         updated_at=now()`,
      [account.id, bucket.name, bucket.creationDate, bucket.jurisdiction, bucket.location, bucket.storageClass]
    )
  }
}

async function mapWithConcurrency<T, R>(items: T[], concurrency: number, worker: (item: T, index: number) => Promise<R>): Promise<R[]> {
  if (items.length === 0) return []
  const output = new Array<R>(items.length)
  let nextIndex = 0
  const run = async () => {
    while (true) {
      const index = nextIndex
      nextIndex += 1
      if (index >= items.length) return
      output[index] = await worker(items[index], index)
    }
  }
  await Promise.all(Array.from({ length: Math.min(Math.max(1, concurrency), items.length) }, () => run()))
  return output
}

async function syncBucketSettingsBatch(db: Client, account: AccountRow, buckets: BucketInfo[]) {
  // readBucketSettings performs two provider fetches. Keep at most three
  // buckets in flight so the six-connection free-tier limit is not exceeded.
  const results = await mapWithConcurrency(buckets, 3, async (bucket) => {
    try {
      const settings = await readBucketSettings(account, bucket)
      return {
        bucket: bucket.name,
        ok: true,
        publicAccess: settings.publicAccess,
        corsRules: settings.corsRules,
        error: null,
      }
    } catch (error) {
      return {
        bucket: bucket.name,
        ok: false,
        publicAccess: null,
        corsRules: null,
        error: (error instanceof Error ? error.message : String(error)).slice(0, 1000),
      }
    }
  })
  if (results.length > 0) {
    // One atomic, idempotent write is substantially cheaper than one query per
    // bucket. Failed rows update only status metadata and retain verified JSON.
    await db.query(`
      with input as (
        select bucket,ok,public_access,cors_rules,error
        from jsonb_to_recordset($2::jsonb) as value(
          bucket text, ok boolean, public_access jsonb, cors_rules jsonb, error text
        )
      )
      update drive_bucket_settings_snapshots snapshots set
        public_access=case when input.ok then input.public_access else snapshots.public_access end,
        cors_rules=case when input.ok then input.cors_rules else snapshots.cors_rules end,
        settings_status=case when input.ok then 'completed' else 'error' end,
        settings_error=input.error,
        settings_last_attempted_at=now(),
        settings_last_synced_at=case when input.ok then now() else snapshots.settings_last_synced_at end,
        updated_at=now()
      from input
      where snapshots.account_id=$1 and snapshots.bucket_name=input.bucket
    `, [account.id, JSON.stringify(results.map((result) => ({
      bucket: result.bucket,
      ok: result.ok,
      public_access: result.publicAccess,
      cors_rules: result.corsRules,
      error: result.error,
    })))])
  }
  return results.flatMap((result) => result.error ? [{ bucket: result.bucket, error: result.error }] : [])
}

async function selectNextAccount(db: Client, syncIntervalMinutes: number) {
  const account = await db.query<AccountRow>(`
    select id,label,email,api_token,cloudflare_account_id,r2_access_key_id,r2_secret_access_key,status,last_synced_at
    from drive_accounts
    where api_token<>''
      and status in('active','available')
      and (last_synced_at is null or last_synced_at < now() - ($1::text || ' minutes')::interval)
      -- An incomplete sync keeps last_synced_at at its last successful value.
      -- Cool down recently attempted accounts so an older available account
      -- with missing analytics cannot starve the active account every minute.
      and (coalesce(sync_status,'')<>'syncing' or updated_at < now()-interval '5 minutes')
    order by last_synced_at asc nulls first,
             created_at asc limit 1
  `, [syncIntervalMinutes])
  return account.rows[0] ?? null
}

async function loadSyncProgress(db: Client) {
  const result = await db.query<{ account_id: string; bucket_names: unknown; bucket_offset: number; reconciled: boolean; metrics_incomplete: boolean; pending_decreases: boolean }>(
    `select account_id, bucket_names, bucket_offset, reconciled, metrics_incomplete, pending_decreases from drive_backend_orchestrator_progress where id=true limit 1`
  )
  const row = result.rows[0]
  if (!row || !Array.isArray(row.bucket_names)) return null
  return {
    accountId: row.account_id,
    buckets: row.bucket_names.flatMap((value): BucketInfo[] => {
      if (typeof value === "string" && value.length > 0) return [{
        name: value,
        creationDate: null,
        jurisdiction: "default",
        location: null,
        storageClass: "Standard",
      }]
      if (!value || typeof value !== "object") return []
      const bucket = value as {
        name?: unknown
        creationDate?: unknown
        jurisdiction?: unknown
        location?: unknown
        storageClass?: unknown
      }
      return typeof bucket.name === "string" && bucket.name.length > 0
        ? [{
            name: bucket.name,
            creationDate: typeof bucket.creationDate === "string" ? bucket.creationDate : null,
            jurisdiction: typeof bucket.jurisdiction === "string" ? bucket.jurisdiction : "default",
            location: typeof bucket.location === "string" ? bucket.location : null,
            storageClass: typeof bucket.storageClass === "string" ? bucket.storageClass : "Standard",
          }]
        : []
    }),
    bucketOffset: Math.max(0, Math.trunc(Number(row.bucket_offset) || 0)),
    reconciled: row.reconciled === true,
    metricsIncomplete: row.metrics_incomplete === true,
    pendingDecreases: row.pending_decreases === true,
  }
}

async function selectAccountById(db: Client, accountId: string) {
  const result = await db.query<AccountRow>(`
    select id,label,email,api_token,cloudflare_account_id,r2_access_key_id,r2_secret_access_key,status,last_synced_at
    from drive_accounts where id=$1 and api_token<>'' and status in('active','available') limit 1
  `, [accountId])
  return result.rows[0] ?? null
}

async function saveSyncProgress(db: Client, input: { accountId: string; buckets: BucketInfo[]; bucketOffset: number; reconciled?: boolean; metricsIncomplete?: boolean; pendingDecreases?: boolean; resetMetricFlags?: boolean }) {
  await db.query(`
    insert into drive_backend_orchestrator_progress (id,account_id,bucket_names,bucket_offset,reconciled,metrics_incomplete,pending_decreases,started_at,updated_at)
    values (true,$1,$2::jsonb,$3,$4::boolean,$5::boolean,$6::boolean,now(),now())
    on conflict (id) do update set
      account_id=excluded.account_id, bucket_names=excluded.bucket_names,
      bucket_offset=excluded.bucket_offset, reconciled=excluded.reconciled,
      metrics_incomplete=case when $7::boolean or drive_backend_orchestrator_progress.account_id is distinct from excluded.account_id then excluded.metrics_incomplete else drive_backend_orchestrator_progress.metrics_incomplete or excluded.metrics_incomplete end,
      pending_decreases=case when $7::boolean or drive_backend_orchestrator_progress.account_id is distinct from excluded.account_id then excluded.pending_decreases else drive_backend_orchestrator_progress.pending_decreases or excluded.pending_decreases end,
      updated_at=now()
  `, [input.accountId, JSON.stringify(input.buckets), input.bucketOffset, input.reconciled === true, input.metricsIncomplete === true, input.pendingDecreases === true, input.resetMetricFlags === true])
}

async function clearSyncProgress(db: Client) {
  await db.query(`delete from drive_backend_orchestrator_progress where id=true`)
}

async function recordDailyAccountSnapshot(db: Client, accountId: string) {
  await db.query(`
    with current_state as (
      select
        a.id account_id,
        a.label account_label,
        a.email account_email,
        a.total_buckets buckets,
        a.total_objects objects,
        a.total_bytes bytes,
        md5(coalesce((
          select string_agg(
            stats.bucket_name || ':' || stats.objects::text || ':' || stats.bytes::text,
            '|' order by stats.bucket_name
          )
          from drive_bucket_stats stats
          where stats.account_id = a.id
        ), '')) bucket_fingerprint
      from drive_accounts a
      where a.id = $1 and a.status = 'active'
    ), previous_state as (
      select bucket_fingerprint
      from drive_analytics_active_account_snapshots
      where captured_day < current_date
      order by captured_day desc
      limit 1
    ), remove_reverted_day as (
      delete from drive_analytics_active_account_snapshots snapshots
      using current_state current
      where snapshots.captured_day = current_date
        and exists (
          select 1 from previous_state previous
          where previous.bucket_fingerprint is not null
            and previous.bucket_fingerprint = current.bucket_fingerprint
        )
      returning snapshots.captured_day
    )
    insert into drive_analytics_active_account_snapshots
      (captured_day, account_id, account_label, account_email, buckets, objects, bytes, bucket_fingerprint, captured_at)
    select current_date, current.account_id, current.account_label, current.account_email,
           current.buckets, current.objects, current.bytes, current.bucket_fingerprint, now()
    from current_state current
    where not exists (
      select 1 from previous_state previous
      where previous.bucket_fingerprint is not null
        and previous.bucket_fingerprint = current.bucket_fingerprint
    )
    on conflict (captured_day) do update set
      account_id = excluded.account_id,
      account_label = excluded.account_label,
      account_email = excluded.account_email,
      buckets = excluded.buckets,
      objects = excluded.objects,
      bytes = excluded.bytes,
      bucket_fingerprint = excluded.bucket_fingerprint,
      captured_at = excluded.captured_at
  `, [accountId])
}

async function pendingMetricDecreases(db: Client, accountId: string, metrics: BucketMetric[]) {
  if (metrics.length === 0) return [] as Array<{ bucketName: string; confirmations: number }>
  const result = await db.query<{ bucket_name: string; confirmations: number }>(`
    with input as (
      select bucket, objects, bytes, observed_at
      from jsonb_to_recordset($2::jsonb)
        as value(bucket text, objects bigint, bytes bigint, observed_at timestamptz)
    ), decreases as (
      select input.*
      from input
      join drive_bucket_stats stats
        on stats.account_id = $1 and stats.bucket_name = input.bucket
      where input.objects < stats.objects or input.bytes < stats.bytes
    ), candidates as (
      insert into drive_backend_orchestrator_metric_candidates
        (account_id, bucket_name, objects, bytes, observed_at, confirmations, first_observed_at, updated_at)
      select $1, bucket, objects, bytes, observed_at, 1, now(), now()
      from decreases
      on conflict (account_id, bucket_name) do update set
        confirmations = case
          when drive_backend_orchestrator_metric_candidates.objects = excluded.objects
           and drive_backend_orchestrator_metric_candidates.bytes = excluded.bytes
            then drive_backend_orchestrator_metric_candidates.confirmations + 1
          else 1
        end,
        first_observed_at = case
          when drive_backend_orchestrator_metric_candidates.objects = excluded.objects
           and drive_backend_orchestrator_metric_candidates.bytes = excluded.bytes
            then drive_backend_orchestrator_metric_candidates.first_observed_at
          else now()
        end,
        objects = excluded.objects,
        bytes = excluded.bytes,
        observed_at = greatest(drive_backend_orchestrator_metric_candidates.observed_at, excluded.observed_at),
        updated_at = now()
      returning bucket_name, confirmations
    )
    select bucket_name, confirmations from candidates where confirmations < 2
  `, [accountId, JSON.stringify(metrics.map((metric) => ({
    bucket: metric.bucket,
    objects: metric.objects,
    bytes: metric.bytes,
    observed_at: metric.observedAt,
  })))])
  if (result.rows.length === 0) {
    await db.query(
      `delete from drive_backend_orchestrator_metric_candidates where account_id=$1 and bucket_name=any($2::text[])`,
      [accountId, metrics.map((metric) => metric.bucket)]
    )
  }
  return result.rows.map((row) => ({ bucketName: row.bucket_name, confirmations: row.confirmations }))
}

async function applyBucketMetrics(db: Client, account: AccountRow, metrics: BucketMetric[]) {
  if (metrics.length === 0) return
  await db.query(
    `
      with input as (
        select bucket, objects, bytes
        from jsonb_to_recordset($2::jsonb) as value(bucket text, objects bigint, bytes bigint)
      ), updated_stats as (
        update drive_bucket_stats stats set
          objects=input.objects, bytes=input.bytes, continuation_token=null,
          status='completed', error=null, updated_at=now()
        from input where stats.account_id=$1 and stats.bucket_name=input.bucket
        returning stats.bucket_name
      ), latest as (
        select input.*,
               history.objects previous_objects,
               history.bytes previous_bytes
        from input
        left join lateral (
          select objects, bytes from drive_storage_stats_history
          where account_id=$1 and bucket_name=input.bucket
          order by changed_at desc, id desc limit 1
        ) history on true
      ), inserted as (
        insert into drive_storage_stats_history (
          account_id, account_label, account_email, bucket_name,
          previous_objects, objects, object_delta, previous_bytes, bytes, byte_delta, change_type
        )
        select $1, $3, $4, bucket,
               previous_objects, objects, objects-coalesce(previous_objects,0),
               previous_bytes, bytes, bytes-coalesce(previous_bytes,0),
               case when previous_objects is null then 'created' else 'changed' end
        from latest
        where previous_objects is null or previous_objects is distinct from objects or previous_bytes is distinct from bytes
        returning account_id, account_label, account_email, bucket_name, objects, bytes, changed_at
      )
      insert into drive_analytics_bucket_snapshots
        (account_id,account_label,account_email,bucket_name,objects,bytes,status,source_updated_at,captured_at)
      select account_id,account_label,account_email,bucket_name,objects,bytes,'completed',changed_at,changed_at
      from inserted
      on conflict(account_id,bucket_name) do update set
        account_label=excluded.account_label, account_email=excluded.account_email,
        objects=excluded.objects, bytes=excluded.bytes, status=excluded.status,
        source_updated_at=excluded.source_updated_at, captured_at=excluded.captured_at
    `,
    [account.id, JSON.stringify(metrics), account.label, account.email]
  )
}

async function syncNextAccount(db: Client, config: RuntimeConfig) {
  let progress = await loadSyncProgress(db)
  let account = progress
    ? await selectAccountById(db, progress.accountId)
    : await selectNextAccount(db, config.syncIntervalMinutes)
  if (progress && !account) {
    await clearSyncProgress(db)
    progress = null
    account = await selectNextAccount(db, config.syncIntervalMinutes)
  }
  if (!account) return { status: "idle", message: "No account is due for synchronization" }
  try {
    if (!account.cloudflare_account_id) {
      const resolved = await resolveCloudflareAccount(account)
      account.cloudflare_account_id = resolved.id
      await db.query(`update drive_accounts set cloudflare_account_id=$2,cloudflare_account_name=$3,updated_at=now() where id=$1`, [account.id, resolved.id, resolved.name])
    }
    await db.query(`update drive_accounts set sync_status='syncing',sync_message='Backend Orchestrator metrics sync running',updated_at=now() where id=$1`, [account.id])
    const buckets = progress ? progress.buckets : await listBuckets(account)
    const bucketNames = buckets.map((bucket) => bucket.name)
    const bucketOffset = progress?.accountId === account.id ? Math.min(progress.bucketOffset, buckets.length) : 0
    if (!progress || progress.accountId !== account.id) {
      // Persist the account and bucket list before doing reconciliation. If the
      // invocation is killed at the CPU limit, the next cron run can resume
      // from this durable checkpoint instead of starting from account selection.
      await saveSyncProgress(db, { accountId: account.id, buckets, bucketOffset: 0, reconciled: false })
      await reconcileBuckets(db, account, buckets)
      await saveSyncProgress(db, { accountId: account.id, buckets, bucketOffset: 0, reconciled: true })
    } else if (!progress.reconciled) {
      await reconcileBuckets(db, account, buckets)
      await saveSyncProgress(db, { accountId: account.id, buckets, bucketOffset, reconciled: true })
    }
    if (buckets.length === 0) {
      await clearSyncProgress(db)
      await db.query(`update drive_accounts set total_buckets=0,total_objects=0,total_bytes=0,sync_status='ok',sync_message='No R2 buckets',last_synced_at=now(),updated_at=now() where id=$1`, [account.id])
      await recordDailyAccountSnapshot(db, account.id)
      return { account: account.label, status: "completed", buckets: 0 }
    }
    const batch = buckets.slice(bucketOffset, bucketOffset + BUCKET_BATCH_SIZE)
    let analyticsAuthorizationError: string | null = null
    let metricsSource: "analytics" | "r2_s3_listing" = "analytics"
    let metrics: BucketMetric[] = []
    try {
      metrics = await getBucketMetrics(account, batch)
    } catch (error) {
      if (!isPerBucketAnalyticsAuthorizationError(error)) throw error
      // The token used to manage the R2 account may lack the separate
      // per-bucket Analytics permission. List objects through the account's
      // S3 credentials so bucket rows still receive exact counts and sizes.
      analyticsAuthorizationError = error instanceof Error ? error.message : String(error)
      metrics = await getBucketMetricsFromR2(account, batch)
      metricsSource = "r2_s3_listing"
    }
    const settingsErrors = await syncBucketSettingsBatch(db, account, batch)
    const pendingDecreases = await pendingMetricDecreases(db, account.id, metrics)
    const missingMetrics = Math.max(0, batch.length - metrics.length)
    const nextMetricsIncomplete = Boolean(progress?.metricsIncomplete || missingMetrics > 0)
    const nextPendingDecreases = Boolean(progress?.pendingDecreases || pendingDecreases.length > 0)
    if (missingMetrics === 0 && pendingDecreases.length === 0) await applyBucketMetrics(db, account, metrics)
    const nextOffset = bucketOffset + batch.length
    await saveSyncProgress(db, {
      accountId: account.id,
      buckets,
      bucketOffset: nextOffset,
      reconciled: true,
      metricsIncomplete: nextMetricsIncomplete,
      pendingDecreases: nextPendingDecreases,
    })
    if (nextOffset < buckets.length) {
      return { account: account.label, status: "in_progress", buckets: buckets.length, processedBuckets: nextOffset, remainingBuckets: buckets.length - nextOffset }
    }
    if (nextMetricsIncomplete || nextPendingDecreases) {
      await clearSyncProgress(db)
      if (analyticsAuthorizationError && !nextPendingDecreases) {
        const publishedMetrics = await getPublishedAccountMetrics(account)
        const message = `R2 published totals and settings synced for ${buckets.length} buckets; per-bucket metrics read from R2 S3 because Analytics permission is unavailable`
        await db.query(`
          update drive_accounts set total_buckets=$2,total_objects=$3,total_bytes=$4,
            sync_status='ok',sync_message=$5,last_synced_at=now(),updated_at=now()
          where id=$1
        `, [account.id, buckets.length, publishedMetrics.objects, publishedMetrics.bytes, message])
        await recordDailyAccountSnapshot(db, account.id)
        return {
          account: account.label,
          status: "completed",
          warning: "per_bucket_analytics_unavailable",
          metricsSource,
          buckets: buckets.length,
          publishedMetrics,
          refreshedSettings: buckets.length,
          settingsErrors,
        }
      }
      const messages = [
        nextMetricsIncomplete ? "R2 analytics missing for one or more buckets" : null,
        nextPendingDecreases ? "R2 decreases are awaiting a second provider observation" : null,
      ].filter((message): message is string => Boolean(message))
      await db.query(`update drive_accounts set sync_status='syncing',sync_message=$2,updated_at=now() where id=$1`, [account.id, `${messages.join("; ")}; retaining last known totals`])
      return {
        account: account.label,
        status: "incomplete",
        buckets: buckets.length,
        missingMetrics: nextMetricsIncomplete,
        pendingDecreases: nextPendingDecreases,
        refreshedSettings: buckets.length,
        settingsErrors,
      }
    }
    await clearSyncProgress(db)
    const staleSettings = await db.query<{ count: string }>(`
      select count(*)::text count
      from drive_bucket_settings_snapshots
      where account_id=$1 and settings_status<>'completed'
    `, [account.id])
    const staleSettingsCount = Number(staleSettings.rows[0]?.count ?? 0)
    const publishedMetrics = await getPublishedAccountMetrics(account)
    await db.query(`
      update drive_accounts a set
        total_buckets=(select count(*) from drive_bucket_stats s where s.account_id=a.id),
        total_objects=$2,total_bytes=$3,
        sync_status='ok',sync_message=$4,last_synced_at=now(),updated_at=now()
      where a.id=$1
    `, [
      account.id,
      publishedMetrics.objects,
      publishedMetrics.bytes,
        staleSettingsCount > 0
        ? `R2 metrics synced${metricsSource === "r2_s3_listing" ? " from S3 object listings" : ""}; settings need retry for ${staleSettingsCount} bucket(s)`
        : `R2 ${metricsSource === "r2_s3_listing" ? "S3 object listings" : "published metrics"} and settings synced for ${buckets.length} buckets`,
    ])
    await recordDailyAccountSnapshot(db, account.id)
    return {
      account: account.label,
      status: "completed",
      buckets: buckets.length,
      metrics: metrics.length,
      metricsSource,
      missingMetrics: buckets.length - metrics.length,
      staleSettings: staleSettingsCount,
      latestObservedAt: metrics.reduce<string | null>((latest, metric) => !latest || metric.observedAt > latest ? metric.observedAt : latest, null),
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    await db.query(`update drive_accounts set sync_status='error',sync_message=$2,updated_at=now() where id=$1`, [account.id, message]).catch(() => undefined)
    return { account: account.label, status: "error", error: message }
  }
}

async function deleteRetentionBatch(db: Client, table: string, condition: string, params: unknown[] = [], batchSize = RETENTION_BATCH_SIZE) {
  try {
    const result = await db.query(
      `with doomed as (select ctid from ${table} where ${condition} limit ${Math.max(1, Math.floor(batchSize))})
       delete from ${table} where ctid in (select ctid from doomed)`,
      params
    )
    return { deleted: result.rowCount ?? 0 }
  } catch (error) {
    return { deleted: 0, error: error instanceof Error ? error.message : String(error) }
  }
}

async function runRetention(db: Client, config: RuntimeConfig) {
  const claim = await db.query(`
    insert into drive_maintenance_state(task_name,last_run_at) values('backend-orchestrator-retention',now())
    on conflict(task_name) do update set last_run_at=excluded.last_run_at
    where drive_maintenance_state.last_run_at<now()-interval '6 hours' returning task_name
  `)
  if (claim.rowCount === 0) return false
  const tasks: Array<[string, string, string, unknown[]]> = [
    ["apiEvents", "drive_project_api_events", "occurred_at<now()-($1::text||' days')::interval", [config.retention.apiEventsDays]],
    ["objectChanges", "drive_object_change_events", "occurred_at<now()-($1::text||' days')::interval", [config.retention.objectChangesDays]],
    ["scanObjects", "drive_bucket_scan_objects", `created_at<now()-($1::text||' days')::interval and not exists (select 1 from drive_bucket_scans s join drive_migrations m on m.id=s.migration_id where s.id=drive_bucket_scan_objects.scan_id and m.status in('running','verifying'))`, [config.retention.scanDetailsDays]],
    ["verifyDiffs", "drive_bucket_verify_diffs", `created_at<now()-($1::text||' days')::interval and not exists (select 1 from drive_migration_items i join drive_migrations m on m.id=i.migration_id where i.id=drive_bucket_verify_diffs.migration_item_id and m.status in('running','verifying'))`, [config.retention.scanDetailsDays]],
    ["bucketScans", "drive_bucket_scans", `status in('completed','failed') and coalesce(completed_at,updated_at)<now()-($1::text||' days')::interval and not exists (select 1 from drive_migrations m where m.id=drive_bucket_scans.migration_id and m.status in('running','verifying'))`, [config.retention.scanDetailsDays]],
    ["syncRuns", "drive_object_sync_runs", "status<>'running' and coalesce(completed_at,started_at)<now()-($1::text||' days')::interval", [config.retention.scanDetailsDays]],
    ["operationJobs", "drive_project_operation_jobs", "status in('completed','failed','canceled') and coalesce(completed_at,updated_at)<now()-($1::text||' days')::interval", [30]],
    ["activityEvents", "drive_activity_events", "occurred_at<now()-($1::text||' days')::interval", [90]],
    ["expiredLocks", "drive_project_object_locks", "expires_at is not null and expires_at<now()", []],
    ["emailTokens", "drive_email_verification_tokens", "(expires_at<now() or consumed_at is not null) and created_at<now()-interval '1 day'", []],
    ["smsTokens", "drive_sms_verification_tokens", "(expires_at<now() or consumed_at is not null) and created_at<now()-interval '1 day'", []],
  ]
  const deleted: Record<string, number> = {}
  const errors: Record<string, string> = {}
  let incomplete = false
  for (const [name, table, condition, params] of tasks) {
    const result = await deleteRetentionBatch(db, table, condition, params)
    deleted[name] = result.deleted
    if (result.error) errors[name] = result.error
    if (result.deleted >= RETENTION_BATCH_SIZE) incomplete = true
  }
  await db.query(
    `update drive_maintenance_state set
       last_run_at=case when $2::boolean then now()-interval '6 hours' else now() end,
       last_result=$3::jsonb
     where task_name=$1`,
    ["backend-orchestrator-retention", incomplete, JSON.stringify({ deleted, errors, incomplete, completedAt: new Date().toISOString() })]
  )
  return { ran: true, deleted, errors, incomplete }
}

async function reconcilePanel(env: Env) {
  try {
    const response = await fetchWithTimeout(panelUrl(env, "/api/internal/backend-orchestrator/reconcile"), {
      method: "POST",
      headers: { Authorization: `Bearer ${env.BACKEND_ORCHESTRATOR_SECRET}` },
    }, 25_000)
    if (response.status === 403) return { ok: false, disabled: true }
    if (!response.ok) return { ok: false, error: `Panel reconciliation failed (${response.status})` }
    return response.json()
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) }
  }
}

async function claimCycle(db: Client, orchestratorUrl?: string) {
  const result = await db.query(`
    insert into drive_backend_orchestrator_state
      (id,status,orchestrator_url,last_started_at,last_error,last_result,updated_at)
    values (true,'running',$1,now(),null,'{}'::jsonb,now())
    on conflict (id) do update set
      status='running', orchestrator_url=coalesce(excluded.orchestrator_url, drive_backend_orchestrator_state.orchestrator_url),
      last_started_at=now(), last_error=null, last_result='{}'::jsonb, updated_at=now()
    where drive_backend_orchestrator_state.status <> 'running'
       or drive_backend_orchestrator_state.last_started_at < now() - interval '2 minutes'
    returning id
  `, [orchestratorUrl ?? null])
  return result.rowCount === 1
}

async function runCycle(env: Env, orchestratorUrl?: string) {
  const config = runtimeConfig(env)
  const db = dbClient(config.postgresUrl, config.disablePostgresSsl, config.hyperdrive)
  await db.connect()
  let claimed = false
  try {
    await ensureSchema(db)
    claimed = await claimCycle(db, orchestratorUrl)
    if (!claimed) return { ok: true, skipped: "Another Backend Orchestrator cycle is active" }
    // This panel reconciliation also checks a durable batch of project/bucket
    // delivery policies. The panel writes the single managed R2 CORS rule only
    // when its verified provider state differs from the database policy.
        const panel = await reconcilePanel(env)
    if (panel && typeof panel === "object" && "disabled" in panel && panel.disabled === true) {
      await setState(db, { status: "idle", result: { skipped: "Backend Orchestrator is disabled in the panel" }, completed: true })
      return { ok: true, skipped: "Backend Orchestrator is disabled in the panel" }
    }
    await db.query(`
      update drive_bucket_scans set status='failed',error='Superseded by R2 analytics metrics sync',completed_at=now(),updated_at=now()
      where kind='orchestrator' and status='running'
    `)
        const sync = await syncNextAccount(db, config)
        const panelDegraded = Boolean(
          panel && typeof panel === "object" && "ok" in panel && panel.ok === false
        )
        const panelError = panelDegraded ? "Panel delivery reconciliation reported errors" : null
    // Publish the synchronization result before optional retention work. If a
    // Worker invocation is terminated during maintenance, a successfully
    // synced account must not remain displayed as pending behind a stale lock.
        await setState(db, {
          status: panelDegraded ? "error" : "idle",
          error: panelError,
          result: { sync, maintenance: { status: "pending" }, panel },
      completed: true,
    })
    const maintenance = await runRetention(db, config)
    const result = { sync, maintenance, panel }
        await setState(db, { status: panelDegraded ? "error" : "idle", error: panelError, result, completed: true })
    return { ok: true, result }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    await setState(db, { status: "error", error: message, result: { error: message }, completed: true }).catch(() => undefined)
    throw error
  } finally {
    await db.end().catch(() => undefined)
  }
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === "/health") {
      return json({ ok: true, service: "backend-orchestrator", build: WORKER_BUILD })
    }
    if (!authorized(request, env)) return json({ error: "Unauthorized" }, 401)
    if (url.pathname === "/run" && request.method === "POST") {
      try { return json(await runCycle(env, url.origin)) } catch (error) { return json({ error: error instanceof Error ? error.message : String(error) }, 500) }
    }
    if (url.pathname === "/status" && request.method === "GET") {
      let db: Client | null = null
      try {
        const config = runtimeConfig(env)
        db = dbClient(config.postgresUrl, config.disablePostgresSsl, config.hyperdrive)
        await db.connect()
        const state = await db.query(`select * from drive_backend_orchestrator_state where id=true limit 1`)
        return json({ ok: true, build: WORKER_BUILD, state: state.rows[0] ?? null })
      } catch (error) {
        return json({ error: error instanceof Error ? error.message : String(error) }, 500)
      } finally {
        await db?.end().catch(() => undefined)
      }
    }
    void ctx
    return json({ error: "Not found" }, 404)
  },
  async scheduled(_controller, env, ctx): Promise<void> {
    // Use Cloudflare's automatic loopback binding instead of a public self-URL.
    // The scheduled event only dispatches; /run owns the database work and
    // checkpoints every bucket so a terminated run resumes on the next tick.
    const loopback = (ctx.exports as unknown as { default: { fetch(request: Request): Promise<Response> } }).default
    ctx.waitUntil(
      loopback.fetch(new Request("https://internal-backend-orchestrator/run", {
        method: "POST",
      headers: { Authorization: `Bearer ${env.BACKEND_ORCHESTRATOR_SECRET.trim()}` },
      })).then(async (response) => {
        if (!response.ok) console.error(`Scheduled Backend Orchestrator dispatch failed (${response.status})`)
      }).catch((error) => {
        console.error("Scheduled Backend Orchestrator dispatch failed:", error instanceof Error ? error.message : String(error))
      })
    )
  },
} satisfies ExportedHandler<Env>
