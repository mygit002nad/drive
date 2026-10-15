import {
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListPartsCommand,
  ListObjectsV2Command,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { createHash, randomUUID } from "crypto"
import pg from "pg"
import { mkdir, readFile, rename, writeFile } from "fs/promises"
import os from "os"
import path from "path"
import { Transform } from "stream"

class WorkerAuthenticationError extends Error {}
class WorkerBootstrapTerminalError extends Error {}

function getArg(name, fallback = "") {
  const index = process.argv.indexOf(`--${name}`)
  if (index >= 0 && index + 1 < process.argv.length) return process.argv[index + 1]
  return process.env[name.toUpperCase().replace(/-/g, "_")] || fallback
}

let SERVER_URL = String(getArg("server-url", "")).replace(/\/+$/, "")
let QUEUE_DATABASE_URL = String(process.env.DRIVE_MIGRATION_QUEUE_DATABASE_URL || "").trim()
let QUEUE_DATABASE_SSL = String(process.env.DRIVE_MIGRATION_QUEUE_SSL || "true").toLowerCase() !== "false"
let QUEUE_GENERATION = Math.max(0, Math.trunc(Number(process.env.DRIVE_WORKER_GENERATION) || 0))
let AGENT_ID = String(getArg("agent-id", "")).trim()
let AGENT_TOKEN = String(getArg("token", ""))
const MIGRATION_ID = String(getArg("migration-id", process.env.DRIVE_MIGRATION_ID || process.env.MIGRATION_ID || ""))
const REPAIR_JOB_ID = String(getArg("repair-job-id", process.env.DRIVE_REPAIR_JOB_ID || ""))
const POOL_MODE = Boolean(MIGRATION_ID && !REPAIR_JOB_ID)
const GITHUB_RUN_ID = String(process.env.GITHUB_RUN_ID || "")
const WORKER_INSTANCE_ID = String(process.env.WORKER_INSTANCE_ID || GITHUB_RUN_ID || "").trim()
const WORKER_STARTED_AT = new Date().toISOString()
// GitHub-hosted jobs have a hard six-hour ceiling. Retire five minutes early
// so the current file can be durably released and the run can close cleanly.
const DEFAULT_MAX_RUNTIME_SECONDS = process.env.GITHUB_ACTIONS === "true" ? "21300" : "0"
const MAX_RUNTIME_SECONDS = Math.max(0, Number(getArg("max-runtime-seconds", DEFAULT_MAX_RUNTIME_SECONDS)) || 0)
const RUNTIME_DEADLINE = MAX_RUNTIME_SECONDS > 0 ? Date.now() + MAX_RUNTIME_SECONDS * 1000 : Number.POSITIVE_INFINITY
// Completed files immediately hand off to the next claim. Idle workers back
// off instead of spending the account's request allowance every second.
const POLL_MS = Math.max(5_000, Number(getArg("poll-ms", "10000")) || 10_000)
let idleClaimCount = 0
let apiBlockedUntil = 0
const HEARTBEAT_MS = Math.max(10_000, Number(getArg("heartbeat-ms", "20000")) || 20_000)
const LIVE_PROGRESS_SYNC_MS = Math.max(5_000, Number(getArg("live-progress-sync-ms", "10000")) || 10_000)
const TELEMETRY_LOG_LIMIT = 100
const TELEMETRY_FILE_EVENT_LIMIT = 100
const MAX_OBJECTS = Math.max(1, Math.min(10_000_000, Number(getArg("max-objects", "2000000")) || 2_000_000))
const API_TIMEOUT_MS = Math.max(5_000, Number(getArg("api-timeout-ms", "30000")) || 30_000)
const API_RETRIES = Math.max(1, Math.min(6, Number(getArg("api-retries", "3")) || 3))
const S3_RETRIES = Math.max(1, Math.min(6, Number(getArg("s3-retries", "3")) || 3))
const COPY_CONCURRENCY = Math.max(1, Math.min(64, Number(getArg("copy-concurrency", "8")) || 8))
const UPLOAD_QUEUE_SIZE = Math.max(1, Math.min(16, Number(getArg("upload-queue-size", "4")) || 4))
const UPLOAD_PART_SIZE = Math.max(
  5 * 1024 * 1024,
  Math.min(128 * 1024 * 1024, (Number(getArg("upload-part-size-mb", "16")) || 16) * 1024 * 1024)
)
const RANGE_COPY_THRESHOLD_MB = Number(getArg("range-copy-threshold-mb", "64"))
const RANGE_COPY_THRESHOLD = Math.max(
  0,
  Math.min(1024 * 1024 * 1024, (Number.isFinite(RANGE_COPY_THRESHOLD_MB) ? RANGE_COPY_THRESHOLD_MB : 64) * 1024 * 1024)
)
// Large inventory objects are copied as independent ranged parts. GitHub
// workers get a little more per-object parallelism so multi-GB files do not
// sit behind a small four-part window; keep the part size unchanged to bound
// memory and preserve resumable multipart checkpoints.
const DEFAULT_RANGE_COPY_CONCURRENCY = process.env.GITHUB_ACTIONS === "true" ? "8" : String(UPLOAD_QUEUE_SIZE)
const RANGE_COPY_CONCURRENCY = Math.max(1, Math.min(16, Number(getArg("range-copy-concurrency", DEFAULT_RANGE_COPY_CONCURRENCY)) || UPLOAD_QUEUE_SIZE))
// Keep one heartbeat cycle well below the panel's stale-lease window. A
// 60-second timeout with five retries can block for several minutes, causing
// the orchestrator to requeue a live worker while it is still copying.
const DEFAULT_EXIT_AFTER_JOB = POOL_MODE ? "false" : process.env.GITHUB_ACTIONS === "true" ? "true" : "false"
const EXIT_AFTER_JOB = ["1", "true", "yes"].includes(
  String(getArg("exit-after-job", DEFAULT_EXIT_AFTER_JOB)).toLowerCase()
)
const jobAbortControllers = new Map()
const jobUpdateQueues = new Map()
const jobClaimTokens = new Map()
let runtimeConfigurationLoadedAt = 0
let fatalAuthenticationError = false
function queueClientOptions() {
  const connectionString = new URL(QUEUE_DATABASE_URL)
  connectionString.searchParams.delete("sslmode")
  return { connectionString: connectionString.toString(), ssl: QUEUE_DATABASE_SSL, connectionTimeoutMillis: 15_000, query_timeout: 15_000, keepAlive: true, application_name: "drive-migration-worker" }
}
let queueClient = QUEUE_DATABASE_URL ? new pg.Client(queueClientOptions()) : null
let queueReady = false
let queueOperation = Promise.resolve()

async function withQueue(operation) {
  if (!queueClient) throw new Error("Migration queue database is not configured")
  const run = queueOperation.then(async () => {
    if (!queueReady) {
      await queueClient.end().catch(() => undefined)
      queueClient = new pg.Client(queueClientOptions())
      await queueClient.connect()
      queueReady = true
    }
    try {
      return await operation(queueClient)
    } catch (error) {
      queueReady = false
      await queueClient.end().catch(() => undefined)
      queueClient = new pg.Client(queueClientOptions())
      throw error
    }
  })
  queueOperation = run.catch(() => undefined)
  return run
}

async function bootstrapQueueDatabase() {
  if (queueClient || !SERVER_URL || !AGENT_ID || AGENT_TOKEN.length < 24 || !MIGRATION_ID) return
  // Spread simultaneous GitHub starts across a short window so bootstrap and
  // PostgreSQL connections do not arrive as a single burst.
  await sleep(Math.floor(Math.random() * 5_000))
  const response = await fetch(`${SERVER_URL}/workers/${encodeURIComponent(AGENT_ID)}/bootstrap`, {
    method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ token: AGENT_TOKEN, migrationId: MIGRATION_ID, generation: QUEUE_GENERATION || undefined, workerInstanceId: WORKER_INSTANCE_ID }),
    signal: AbortSignal.timeout(API_TIMEOUT_MS),
  })
  const payload = await response.json().catch(() => ({}))
  if (response.status === 401) throw new WorkerAuthenticationError(payload?.error || "Migration queue bootstrap authentication failed")
  if (response.status === 409) throw new WorkerBootstrapTerminalError(payload?.error || "Migration is no longer active")
  if (!response.ok || typeof payload?.queueDatabaseUrl !== "string") throw new Error(payload?.error || `Migration queue bootstrap failed with HTTP ${response.status}`)
  const parsed = new URL(payload.queueDatabaseUrl)
  if (!["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || !parsed.username || !parsed.password) throw new Error("Migration Orchestrator returned an invalid queue database URI")
  QUEUE_DATABASE_URL = payload.queueDatabaseUrl
  QUEUE_DATABASE_SSL = payload.queueDatabaseSsl !== false
  QUEUE_GENERATION = Math.max(1, Math.trunc(Number(payload.generation) || 1))
  queueClient = new pg.Client(queueClientOptions())
}
let runtimeRotationRequested = false
let runtimeRotationTimer = null
const WORKER_STATE_DIR = path.resolve(String(getArg("state-dir", path.join(process.cwd(), ".drive-worker"))))
const WORKER_IDENTITY_PATH = path.join(WORKER_STATE_DIR, "identity.json")

if ((!QUEUE_DATABASE_URL && (!SERVER_URL || AGENT_TOKEN.length < 24)) || (QUEUE_DATABASE_URL && !MIGRATION_ID)) {
  console.error("Missing required configuration. Provide the migration queue database URL and migration id, or the legacy Orchestrator URL and token.")
  process.exit(1)
}

async function loadRuntimeConfiguration(force = false) {
  if (!force && runtimeConfigurationLoadedAt > Date.now() - 60_000) return
  if (QUEUE_DATABASE_URL) return
  if (!SERVER_URL || AGENT_TOKEN.length < 24) throw new Error("SERVER_URL and the common Migration Worker secret are required")
  runtimeConfigurationLoadedAt = Date.now()
}

function validUuid(value) {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

async function readIdentity() {
  try {
    const parsed = JSON.parse(await readFile(WORKER_IDENTITY_PATH, "utf8"))
    if (!validUuid(parsed?.instanceId)) return null
    return { instanceId: parsed.instanceId, agentId: validUuid(parsed.agentId) ? parsed.agentId : "" }
  } catch {
    return null
  }
}

async function getOrCreateIdentity() {
  const existing = await readIdentity()
  if (existing) return existing
  await mkdir(WORKER_STATE_DIR, { recursive: true, mode: 0o700 })
  const identity = { instanceId: randomUUID(), agentId: "" }
  try {
    // Exclusive creation makes concurrent starts in one state directory share
    // one durable instance identity instead of creating duplicate workers.
    await writeFile(WORKER_IDENTITY_PATH, `${JSON.stringify(identity, null, 2)}\n`, { flag: "wx", mode: 0o600 })
    return identity
  } catch (error) {
    if (error?.code !== "EEXIST") throw error
    const winner = await readIdentity()
    if (!winner) throw new Error(`Worker identity at ${WORKER_IDENTITY_PATH} is invalid`)
    return winner
  }
}

async function persistIdentity(identity) {
  await mkdir(WORKER_STATE_DIR, { recursive: true, mode: 0o700 })
  const temporary = `${WORKER_IDENTITY_PATH}.${process.pid}.${randomUUID()}.tmp`
  await writeFile(temporary, `${JSON.stringify(identity, null, 2)}\n`, { mode: 0o600 })
  await rename(temporary, WORKER_IDENTITY_PATH)
}

async function ensureWorkerIdentity() {
  if (AGENT_ID) return
  const identity = await getOrCreateIdentity()
  const response = await withRetries(
    "register worker instance",
    () => fetch(`${SERVER_URL}/workers/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        token: AGENT_TOKEN,
        instanceId: identity.instanceId,
        agentId: identity.agentId || undefined,
        name: `${os.hostname()} migration worker`,
        host: os.hostname(),
        version: "worker-v3",
        capabilities: ["scan", "verify", "repair", "bulk_migrate", "diagnostics"],
      }),
      signal: AbortSignal.timeout(API_TIMEOUT_MS),
    }),
    API_RETRIES
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok || !validUuid(payload?.agentId)) {
    throw new Error(payload?.error || `Worker registration failed with HTTP ${response.status}`)
  }
  AGENT_ID = payload.agentId
  if (identity.agentId !== AGENT_ID) await persistIdentity({ ...identity, agentId: AGENT_ID })
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function isRecord(value) {
  return typeof value === "object" && value !== null
}

function normalizeWorkerShard(value) {
  if (!isRecord(value)) return null
  const index = Number(value.index)
  const count = Number(value.count)
  if (!Number.isInteger(index) || !Number.isInteger(count) || count < 1 || index < 0 || index >= count) return null
  return { index, count }
}

function requiresWorkerShard(payload) {
  return payload?.job?.kind === "migration_shard"
}

// FNV-1a gives every object a stable owner shard. The namespace includes the
// source bucket so identical keys in different buckets remain independent.
function objectBelongsToShard(object, namespace, shard) {
  if (!shard || shard.count <= 1) return true
  const value = `${String(namespace || "")}\u0000${String(object?.key || "")}`
  let hash = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0) % shard.count === shard.index
}

function filterObjectsForShard(objects, namespace, shard) {
  if (!shard || shard.count <= 1) return objects
  return objects.filter((object) => objectBelongsToShard(object, namespace, shard))
}

function closeBodyStream(body) {
  if (body && typeof body.destroy === "function") {
    try {
      body.destroy()
    } catch {}
  }
}

function isRetryableError(error) {
  const name = typeof error?.name === "string" ? error.name.toLowerCase() : ""
  const message = error instanceof Error ? error.message.toLowerCase() : String(error || "").toLowerCase()
  const status = Number(error?.$metadata?.httpStatusCode || error?.status || 0)
  return (
    name === "aborterror" ||
    name === "timeouterror" ||
    message.includes("timeout") ||
    message.includes("timed out") ||
    message.includes("aborted") ||
    message.includes("aborterror") ||
    message.includes("request aborted") ||
    message.includes("stream closed") ||
    message.includes("premature close") ||
    message.includes("ecanceled") ||
    message.includes("econnreset") ||
    message.includes("socket hang up") ||
    message.includes("network") ||
    message.includes("fetch failed") ||
    message.includes("slow down") ||
    message.includes("thrott") ||
    message.includes("internalerror") ||
    message.includes("service unavailable") ||
    status === 408 ||
    status === 429 ||
    status >= 500 ||
    message.includes("530") ||
    message.includes("503") ||
    message.includes("502") ||
    message.includes("500")
  )
}

function isObjectNotFoundError(error) {
  const code = typeof error?.name === "string" ? error.name.toLowerCase() : ""
  const status = Number(error?.$metadata?.httpStatusCode || 0)
  const message = error instanceof Error ? error.message.toLowerCase() : String(error || "").toLowerCase()
  return (
    code === "notfound" ||
    code === "nosuchkey" ||
    code === "nosuchbucket" ||
    status === 404 ||
    message.includes("not found") ||
    message.includes("nosuchkey")
  )
}

async function withRetries(label, fn, retries = 3) {
  let lastError = null
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      return await fn(attempt)
    } catch (error) {
      lastError = error
      if (error instanceof JobAbortedError) throw error
      if (attempt >= retries || !isRetryableError(error)) throw error
      await sleep(Math.min(5000, 400 * 2 ** (attempt - 1)))
    }
  }
  throw lastError || new Error(`${label} failed`)
}

async function runConcurrent(items, concurrency, worker) {
  let nextIndex = 0
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex
      nextIndex += 1
      await worker(items[index], index)
    }
  })
  // Wait for every in-flight task before propagating a failure. Promise.all
  // rejects immediately and would otherwise let copies from this job continue
  // in the background while the lease is being finalized or requeued.
  const settled = await Promise.allSettled(workers)
  const failure = settled.find((entry) => entry.status === "rejected")
  if (failure?.status === "rejected") throw failure.reason
}

class JobAbortedError extends Error {
  constructor(message = "Worker job aborted by user") {
    super(message)
    this.name = "JobAbortedError"
  }
}

function markJobAborted(jobId) {
  const controller = jobAbortControllers.get(jobId)
  if (controller && !controller.signal.aborted) controller.abort()
}

function getJobAbortSignal(jobId) {
  return jobAbortControllers.get(jobId)?.signal
}

function throwIfJobAborted(jobId) {
  if (getJobAbortSignal(jobId)?.aborted) throw new JobAbortedError()
}

function createClient(config) {
  return new S3Client({
    region: "auto",
    endpoint: `https://${config.accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  })
}

async function api(path, body, options = {}) {
  const timeoutMs =
    typeof options.timeoutMs === "number" && Number.isFinite(options.timeoutMs) && options.timeoutMs > 0
      ? options.timeoutMs
      : API_TIMEOUT_MS
  const retries =
    typeof options.retries === "number" && Number.isFinite(options.retries) && options.retries > 0
      ? Math.trunc(options.retries)
      : API_RETRIES
  return withRetries(
    `api ${path}`,
    async () => {
      while (Date.now() < apiBlockedUntil) await sleep(Math.min(30_000, apiBlockedUntil - Date.now()))
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), timeoutMs)
      try {
        const response = await fetch(`${SERVER_URL}${path}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          signal: controller.signal,
        })
        const responseText = await response.text()
        let json = {}
        try { json = JSON.parse(responseText) } catch { /* Provider errors can be HTML. */ }
        if (!response.ok) {
          const quotaExceeded = response.status === 429 && /(?:Error\s*1027|error-code[^>]*>\s*1027|reached their plan limits)/i.test(responseText)
          if (response.status === 429 || response.status >= 500) {
            const retryAfter = response.headers.get("retry-after")
            const retryAfterMs = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : retryAfter ? Date.parse(retryAfter) - Date.now() : 0
            // Share the cooldown across claims, heartbeats and progress updates.
            apiBlockedUntil = Math.max(apiBlockedUntil, Date.now() + Math.max(quotaExceeded ? 300_000 : 30_000, Number.isFinite(retryAfterMs) ? retryAfterMs : 0))
          }
          const message = quotaExceeded
            ? "Cloudflare Workers daily request limit reached (Error 1027); requests paused for five minutes"
            : typeof json?.error === "string" ? json.error : `Request failed: ${response.status}`
          if (response.status === 401 || response.status === 403) throw new WorkerAuthenticationError(`Migration Orchestrator rejected the worker identity/secret (HTTP ${response.status})`)
          const requestError = new Error(message)
          requestError.status = response.status
          throw requestError
        }
        if (!json || typeof json !== "object" || Array.isArray(json) || json.ok !== true) {
          apiBlockedUntil = Math.max(apiBlockedUntil, Date.now() + 30_000)
          const requestError = new Error("Migration Orchestrator returned an invalid API response")
          requestError.status = 502
          throw requestError
        }
        return json
      } finally {
        clearTimeout(timeout)
      }
    },
    retries
  )
}

async function heartbeat(extra = {}) {
  if (queueClient) return withQueue(async (db) => {
    const migrationId = MIGRATION_ID
    if (!migrationId || !WORKER_INSTANCE_ID) throw new Error("Queue worker identity or migration scope is missing")
    const meta = (await db.query(`select generation,status from drive_migration_queue_meta where migration_id=$1 and ($2::int=0 or generation=$2) order by generation desc limit 1`, [migrationId, QUEUE_GENERATION])).rows[0]
    if (!meta) throw new Error("Migration queue has not been initialized")
    if (meta.status === "superseded") {
      if (currentJobId) markJobAborted(currentJobId)
      throw new JobAbortedError()
    }
    QUEUE_GENERATION = Number(meta.generation)
    await db.query(`insert into drive_migration_queue_workers(worker_instance_id,agent_id,migration_id,generation,status,current_job_id,metadata,last_heartbeat_at,updated_at)
      values($1,$2,$3,$4,'online',$5,jsonb_build_object('workerStartedAt',$7::timestamptz,'runtimeAccountingVersion',1)||$6::jsonb,now(),now())
      on conflict(worker_instance_id) do update set status='online',current_job_id=excluded.current_job_id,
        runtime_seconds=drive_migration_queue_workers.runtime_seconds+case when drive_migration_queue_workers.status='online' then greatest(0,floor(extract(epoch from(now()-drive_migration_queue_workers.last_heartbeat_at)))::bigint) else 0 end,
        metadata=case when drive_migration_queue_workers.metadata ? 'workerStartedAt' then drive_migration_queue_workers.metadata||(excluded.metadata-'workerStartedAt')
          else jsonb_build_object('workerStartedAt',$7::timestamptz)||drive_migration_queue_workers.metadata||(excluded.metadata-'workerStartedAt') end,
        last_heartbeat_at=now(),updated_at=now()`,
      [WORKER_INSTANCE_ID, AGENT_ID || null, migrationId, meta.generation, currentJobId, JSON.stringify(extra), WORKER_STARTED_AT])
    if (currentJobId) {
      const claimToken = jobClaimTokens.get(currentJobId)
      const renewed = await db.query(`update drive_migration_queue_jobs set lease_expires_at=now()+interval '3 minutes',last_heartbeat_at=now(),updated_at=now()
        where id=$1 and claimed_by=$2 and claim_token=$3::uuid and status in('claimed','running') returning id`, [currentJobId, WORKER_INSTANCE_ID, claimToken || null])
      if (!renewed.rowCount) {
        const row = (await db.query(`select status from drive_migration_queue_jobs where id=$1`, [currentJobId])).rows[0]
        if (row?.status === "canceled") {
          markJobAborted(currentJobId)
          throw new JobAbortedError()
        }
        throw new Error("This migration queue job is no longer owned by this worker")
      }
    }
    return { ok: true }
  })
  await loadRuntimeConfiguration()
  const result = await api(`/workers/${encodeURIComponent(AGENT_ID)}/heartbeat`, {
    token: AGENT_TOKEN,
    host: os.hostname(),
    version: "worker-v3",
    capabilities: ["scan", "verify", "repair", "bulk_migrate", "diagnostics"],
    metadata: { ...extra, workerInstanceId: WORKER_INSTANCE_ID || null },
    ...(currentJobId ? { currentJobId, claimToken: jobClaimTokens.get(currentJobId) || null } : {}),
  })
  if (result?.canceled && currentJobId) {
    markJobAborted(currentJobId)
    throw new JobAbortedError()
  }
  return result
}

async function retireWorker(reason) {
  if (!WORKER_INSTANCE_ID) return
  if (queueClient) return withQueue((db) => db.query(`update drive_migration_queue_workers set
    runtime_seconds=runtime_seconds+case when status='online' then greatest(0,floor(extract(epoch from(now()-last_heartbeat_at)))::bigint) else 0 end,
    status='offline',current_job_id=null,metadata=metadata||$2::jsonb,updated_at=now() where worker_instance_id=$1`, [WORKER_INSTANCE_ID, JSON.stringify({ retireReason: reason, retiredAt: new Date().toISOString(), runtimeAccountingVersion: 1 })]))
  await api(`/workers/${encodeURIComponent(AGENT_ID)}/retire`, {
    token: AGENT_TOKEN,
    workerInstanceId: WORKER_INSTANCE_ID,
    reason,
  })
}

async function releaseJobForRotation(jobId) {
  try {
    await updateJob(jobId, {
      status: "pending",
      workerInstanceId: WORKER_INSTANCE_ID || undefined,
      summary: "Worker runtime window ended; returned to the durable queue for the replacement worker",
      progress: {
        active: false,
        currentFile: null,
        rotationRequestedAt: new Date().toISOString(),
        rotationReason: "github_runtime_window",
      },
    })
    jobClaimTokens.delete(jobId)
  } catch (error) {
    console.error(`Unable to release job ${jobId} during worker rotation:`, error instanceof Error ? error.message : String(error))
  }
}

async function claimJob() {
  if (queueClient) return withQueue(async (db) => {
    if (!MIGRATION_ID || !WORKER_INSTANCE_ID) throw new Error("Migration queue claims require migration id and worker instance id")
    await db.query("begin")
    try {
      const meta = (await db.query(`select generation,status,seed_complete,execution_config from drive_migration_queue_meta where migration_id=$1 and ($2::int=0 or generation=$2) order by generation desc limit 1 for share`, [MIGRATION_ID, QUEUE_GENERATION])).rows[0]
      if (!meta || ["canceled", "completed", "failed", "verification_failed", "aborted", "superseded"].includes(String(meta.status))) {
        await db.query("commit")
        return { ok: true, job: null, poolComplete: true, poolStatus: meta?.status || "missing" }
      }
      QUEUE_GENERATION = Number(meta.generation)
      const candidate = await db.query(`select id,payload,progress,result,status from drive_migration_queue_jobs
        where migration_id=$1 and generation=$2 and (status='pending' or (status in('claimed','running') and lease_expires_at<now()))
        order by created_at,id for update skip locked limit 1`, [MIGRATION_ID, meta.generation])
      if (!candidate.rowCount) {
        const counts = await db.query(`select count(*) filter(where status in('pending','claimed','running')) remaining from drive_migration_queue_jobs where migration_id=$1 and generation=$2`, [MIGRATION_ID, meta.generation])
        const complete = meta.seed_complete === true && Number(counts.rows[0]?.remaining || 0) === 0
        await db.query("commit")
        return { ok: true, job: null, poolComplete: complete, poolReason: complete ? "terminal" : "queue_empty_or_seeding" }
      }
      const selected = candidate.rows[0]
      const claimToken = randomUUID()
      const claimed = await db.query(`update drive_migration_queue_jobs set status='running',claimed_by=$2,agent_id=$4::uuid,claim_token=$3::uuid,attempts=attempts+1,
        lease_expires_at=now()+interval '3 minutes',last_heartbeat_at=now(),updated_at=now()
        where id=$1 returning id,migration_id,generation,payload,progress,result,claim_token,status`, [selected.id, WORKER_INSTANCE_ID, claimToken, validUuid(AGENT_ID) ? AGENT_ID : null])
      await db.query(`insert into drive_migration_queue_workers(worker_instance_id,agent_id,migration_id,generation,status,current_job_id,metadata,last_heartbeat_at,updated_at)
        values($1,$2,$3,$4,'online',$5,jsonb_build_object('workerStartedAt',$6::timestamptz,'runtimeAccountingVersion',1),now(),now())
        on conflict(worker_instance_id) do update set status='online',current_job_id=excluded.current_job_id,
          runtime_seconds=drive_migration_queue_workers.runtime_seconds+case when drive_migration_queue_workers.status='online' then greatest(0,floor(extract(epoch from(now()-drive_migration_queue_workers.last_heartbeat_at)))::bigint) else 0 end,
          metadata=case when drive_migration_queue_workers.metadata ? 'workerStartedAt' then drive_migration_queue_workers.metadata
            else drive_migration_queue_workers.metadata||excluded.metadata end,last_heartbeat_at=now(),updated_at=now()`,
        [WORKER_INSTANCE_ID, AGENT_ID || null, MIGRATION_ID, meta.generation, selected.id, WORKER_STARTED_AT])
      await db.query("commit")
      const row = claimed.rows[0]
      jobClaimTokens.set(row.id, row.claim_token)
      const payload = { ...row.payload, ...(meta.execution_config || {}) }
      return { ok: true, job: { id: row.id, migrationId: row.migration_id, mode: "migration", claimToken: row.claim_token, status: row.status }, payload }
    } catch (error) { await db.query("rollback").catch(() => undefined); throw error }
  })
  await loadRuntimeConfiguration()
  if (!MIGRATION_ID) throw new Error("Migration Orchestrator claim requires migrationId")
  const claimed = await api(`/workers/${encodeURIComponent(AGENT_ID)}/claim-job`, {
    token: AGENT_TOKEN,
    migrationId: MIGRATION_ID,
    pool: POOL_MODE,
    ...(REPAIR_JOB_ID ? { jobId: REPAIR_JOB_ID } : {}),
    ...(GITHUB_RUN_ID ? { githubRunId: GITHUB_RUN_ID } : {}),
    ...(WORKER_INSTANCE_ID ? { workerInstanceId: WORKER_INSTANCE_ID } : {}),
  })
  if (claimed?.job?.id && typeof claimed.job.claimToken === "string") jobClaimTokens.set(claimed.job.id, claimed.job.claimToken)
  return claimed
}

async function updateJob(jobId, body, options = {}) {
  if (queueClient) return withQueue(async (db) => {
    const claimToken = jobClaimTokens.get(jobId)
    const status = String(body.status || "running")
    const released = status === "pending"
    const terminal = ["completed", "failed", "canceled"].includes(status)
    const result = await db.query(`update drive_migration_queue_jobs set status=$4,progress=coalesce(progress,'{}'::jsonb)||$5::jsonb,
      result=coalesce(result,'{}'::jsonb)||$6::jsonb,summary=coalesce($7,summary),error=case when $8 then null else coalesce($9,error) end,
      claimed_by=case when $10 then null else claimed_by end,agent_id=case when $10 then null else agent_id end,claim_token=case when $10 then null else claim_token end,
      lease_expires_at=case when $10 then null when $11 then null else now()+interval '3 minutes' end,
      last_heartbeat_at=case when $10 then null else now() end,completed_at=case when $11 then now() when $10 then null else completed_at end,updated_at=now()
      where id=$1 and claimed_by=$2 and claim_token=$3::uuid and status in('claimed','running','pending') returning id,status`,
      [jobId, WORKER_INSTANCE_ID, claimToken || null, status, JSON.stringify(body.progress || {}), JSON.stringify(body.result || {}), typeof body.summary === "string" ? body.summary.slice(0, 2000) : null, released, typeof body.error === "string" ? body.error.slice(0, 4000) : null, released, terminal])
    if (!result.rowCount) {
      const current = (await db.query(`select status from drive_migration_queue_jobs where id=$1`, [jobId])).rows[0]
      if (current?.status === "canceled") return { ok: true, canceled: true }
      throw new Error("This job is no longer owned by this worker")
    }
    if (terminal || released) jobClaimTokens.delete(jobId)
    return { ok: true, job: result.rows[0] }
  })
  const allowOffline = options?.allowOffline === true
  let response
  try {
    response = await api(`/workers/${encodeURIComponent(AGENT_ID)}/jobs/${encodeURIComponent(jobId)}`, {
      token: AGENT_TOKEN,
      ...body,
      claimToken: jobClaimTokens.get(jobId),
    })
  } catch (error) {
    if (allowOffline && !(error instanceof JobAbortedError) && isRetryableError(error)) {
      return { offline: true, error: error instanceof Error ? error.message : String(error) }
    }
    throw error
  }
  if (response?.canceled || response?.job?.status === "canceled") {
    throw new JobAbortedError()
  }
  if (["completed", "failed", "canceled"].includes(String(body.status || ""))) jobClaimTokens.delete(jobId)
  return response
}

async function safeUpdateJob(jobId, body) {
  // Progress is best-effort. Keep at most one request in flight per job and
  // replace stale queued telemetry with the newest snapshot. Without this,
  // a panel outage creates dozens of concurrent retries and can delay the
  // terminal update long after the object work has finished.
  const existing = jobUpdateQueues.get(jobId)
  if (existing) {
    existing.pending = body
    return existing.promise
  }

  const state = { pending: body, promise: null }
  state.promise = (async () => {
    let response = null
    while (state.pending) {
      const nextBody = state.pending
      state.pending = null
      try {
        response = await updateJob(jobId, nextBody, { allowOffline: true })
        if (response?.offline) {
          console.error(`Job sync deferred for ${jobId}: ${response.error}`)
          // Drop stale telemetry after an outage. The next heartbeat/progress
          // tick will start one fresh bounded attempt if connectivity returns.
          state.pending = null
          break
        }
      } catch (error) {
        if (error instanceof JobAbortedError) {
          markJobAborted(jobId)
          return { canceled: true }
        }
        if (error instanceof Error && /no longer owned|claimed by another worker/i.test(error.message)) {
          // The orchestrator reclaimed this lease. Fence the old process before it
          // can report progress or intentionally retry the same object set.
          markJobAborted(jobId)
          return { canceled: true, fenced: true }
        }
        const message = error instanceof Error ? error.message : String(error)
        console.error(`Job sync failed for ${jobId}:`, message)
        return { offline: true, error: message }
      }
    }
    return response
  })()
  jobUpdateQueues.set(jobId, state)
  try {
    return await state.promise
  } finally {
    if (jobUpdateQueues.get(jobId) === state) jobUpdateQueues.delete(jobId)
  }
}

async function flushJobUpdates(jobId) {
  const state = jobUpdateQueues.get(jobId)
  if (state) await state.promise.catch(() => undefined)
}

async function finalizeJobUpdate(jobId, body) {
  await flushJobUpdates(jobId)
  try {
    const response = await updateJob(jobId, body, { allowOffline: true })
    if (response?.offline) {
      console.error(`Final job update deferred for ${jobId}: ${response.error}`)
    }
    return response
  } catch (error) {
    if (error instanceof JobAbortedError) return { canceled: true }
    if (error instanceof Error && /no longer owned|claimed by another worker/i.test(error.message)) {
      markJobAborted(jobId)
      return { canceled: true, fenced: true }
    }
    console.error(`Final job update failed for ${jobId}:`, error instanceof Error ? error.message : String(error))
    return { offline: true, error: error instanceof Error ? error.message : String(error) }
  }
}

async function tryClaimJob() {
  try {
    return await claimJob()
  } catch (error) {
    if (isRetryableError(error)) {
      console.error(`Claim job failed:`, error instanceof Error ? error.message : String(error))
      return null
    }
    throw error
  }
}

async function listAllObjects(client, bucket, prefix, onProgress) {
  const objects = []
  const seenKeys = new Set()
  let continuationToken = undefined
  const seenTokens = new Set()
  while (true) {
    const tokenKey = continuationToken || "__first__"
    if (seenTokens.has(tokenKey)) throw new Error(`ListObjectsV2 pagination loop detected for ${bucket}`)
    seenTokens.add(tokenKey)
    const page = await withRetries(
      `list objects ${bucket}`,
      () =>
        client.send(
          new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix || undefined,
            ContinuationToken: continuationToken,
            MaxKeys: 1000,
          })
        ),
      S3_RETRIES
    )
    const contents = Array.isArray(page.Contents) ? page.Contents : []
    for (const object of contents) {
      const key = typeof object?.Key === "string" ? object.Key : ""
      if (!key) continue
      if (seenKeys.has(key)) continue
      seenKeys.add(key)
      const size = typeof object?.Size === "number" ? object.Size : 0
      objects.push({ key, size, etag: typeof object?.ETag === "string" ? object.ETag.replace(/^\"|\"$/g, "") : null })
      if (typeof onProgress === "function") onProgress({ count: objects.length, key, size })
      if (objects.length > MAX_OBJECTS) {
        throw new Error(
          `Object inventory for ${bucket} exceeds MAX_OBJECTS=${MAX_OBJECTS}. Increase MAX_OBJECTS and retry; refusing to mark a truncated migration complete.`
        )
      }
    }
    const nextContinuationToken = typeof page.NextContinuationToken === "string" ? page.NextContinuationToken : undefined
    if (page.IsTruncated === true && !nextContinuationToken) {
      throw new Error(`Object inventory pagination ended without a continuation token for ${bucket}`)
    }
    continuationToken = nextContinuationToken
    if (objects.length >= MAX_OBJECTS && continuationToken) {
      throw new Error(
        `Object inventory for ${bucket} exceeds MAX_OBJECTS=${MAX_OBJECTS}. Increase MAX_OBJECTS and retry; refusing to mark a truncated migration complete.`
      )
    }
    if (!continuationToken) return objects
  }
}

function diffObjects(sourceObjects, destObjects) {
  const destinationMap = new Map(destObjects.map((object) => [object.key, object]))
  const missing = []
  const mismatched = []
  for (const sourceObject of sourceObjects) {
    const destination = destinationMap.get(sourceObject.key)
    if (!destination) {
      missing.push(sourceObject)
    } else if (
      destination.size !== sourceObject.size ||
      (sourceObject.etag && destination.etag && !sourceObject.etag.includes("-") && !destination.etag.includes("-") && sourceObject.etag !== destination.etag)
    ) {
      mismatched.push({ ...sourceObject, destinationSize: destination.size, destinationEtag: destination.etag })
    }
  }
  return { missing, mismatched }
}

async function getTargetObjectSize(targetClient, bucket, key) {
  try {
    const head = await withRetries(
      `head target ${bucket}/${key}`,
      () => targetClient.send(new HeadObjectCommand({ Bucket: bucket, Key: key })),
      S3_RETRIES
    )
    return typeof head.ContentLength === "number" ? head.ContentLength : 0
  } catch (error) {
    if (isObjectNotFoundError(error)) return null
    throw error
  }
}

async function hashObject(client, bucket, key, abortSignal) {
  const response = await withRetries(
    `hash ${bucket}/${key}`,
    () => client.send(new GetObjectCommand({ Bucket: bucket, Key: key }), abortSignal ? { abortSignal } : undefined),
    S3_RETRIES
  )
  const body = response.Body
  if (!body) throw new Error(`Object body missing while hashing ${bucket}/${key}`)
  const hash = createHash("sha256")
  try {
    for await (const chunk of body) {
      if (abortSignal?.aborted) throw new JobAbortedError()
      hash.update(chunk)
    }
    return hash.digest("hex")
  } finally { closeBodyStream(body) }
}

async function inspectAssignedObjects(sourceClient, targetClient, sourceBucket, targetBucket, objects, abortSignal) {
  const found = []
  await runConcurrent(objects, Math.min(2, COPY_CONCURRENCY), async (object) => {
    let targetHead
    try {
      targetHead = await withRetries(`head target ${targetBucket}/${object.key}`, () => targetClient.send(new HeadObjectCommand({ Bucket: targetBucket, Key: object.key })), S3_RETRIES)
    } catch (error) {
      if (isObjectNotFoundError(error)) return
      throw error
    }
    const actualSize = typeof targetHead.ContentLength === "number" ? targetHead.ContentLength : -1
    if (actualSize !== Number(object.size)) { found.push({ key: object.key, size: actualSize }); return }
    const [sourceSha256, destinationSha256] = await Promise.all([
      hashObject(sourceClient, sourceBucket, object.key, abortSignal),
      hashObject(targetClient, targetBucket, object.key, abortSignal),
    ])
    found.push({
      key: object.key,
      size: sourceSha256 === destinationSha256 ? actualSize : -1,
      destinationSize: actualSize,
      destinationEtag: typeof targetHead.ETag === "string" ? targetHead.ETag.replace(/^"|"$/g, "") : null,
      sourceSha256,
      destinationSha256,
      integrityVerified: sourceSha256 === destinationSha256,
    })
  })
  return found
}

function diffObjectsByListing(sourceObjects, destObjects, onProgress) {
  const destinationMap = new Map(destObjects.map((object) => [object.key, object.size]))
  const missing = []
  const mismatched = []
  let checked = 0

  for (const sourceObject of sourceObjects) {
    const destSize = destinationMap.get(sourceObject.key)
    checked += 1
    if (typeof destSize === "undefined") {
      missing.push(sourceObject)
    } else if (destSize !== sourceObject.size) {
      mismatched.push({ ...sourceObject, destinationSize: destSize })
    }
    if (typeof onProgress === "function") {
      onProgress({
        checked,
        key: sourceObject.key,
        size: sourceObject.size,
        missing: missing.length,
        mismatched: mismatched.length,
      })
    }
  }

  return { missing, mismatched }
}

function buildLiveTotals(resultItems, current = {}) {
  const completedItems = Array.isArray(resultItems) ? resultItems : []
  const totals = {
    transferred: 0,
    failed: 0,
    skipped: 0,
    missing: 0,
    mismatched: 0,
    completedItems: 0,
    failedItems: 0,
  }

  for (const item of completedItems) {
    totals.transferred += Number(item?.transferred || 0)
    totals.failed += Number(item?.failed || 0)
    totals.skipped += Number(item?.skipped || 0)
    totals.missing += Number(item?.finalMissing || 0)
    totals.mismatched += Number(item?.finalMismatched || 0)
    if (item?.completed) totals.completedItems += 1
    else totals.failedItems += 1
  }

  totals.transferred += Number(current.transferred || 0)
  totals.failed += Number(current.failed || 0)
  totals.skipped += Number(current.skipped || 0)
  totals.missing += Number(current.missing || 0)
  totals.mismatched += Number(current.mismatched || 0)

  return totals
}

function createJobTelemetry(payload) {
  return {
    startedAt: new Date().toISOString(),
    logs: [],
    itemProgress: [],
    fileEvents: [],
    currentFile: null,
    stats: {
      totalBuckets: Array.isArray(payload?.items) ? payload.items.length : 0,
      completedBuckets: 0,
      failedBuckets: 0,
      scannedSourceObjects: 0,
      scannedDestinationObjects: 0,
      repairCandidates: 0,
      verifiedObjects: 0,
    },
  }
}

function pushLog(state, message, extra = {}) {
  const line = {
    at: new Date().toISOString(),
    message: String(message),
    ...extra,
  }
  state.logs = [...state.logs.slice(-(TELEMETRY_LOG_LIMIT - 1)), line]
}

function upsertItemProgress(state, patch) {
  const itemId = typeof patch?.itemId === "string" ? patch.itemId : ""
  if (!itemId) return
  const next = {
    updatedAt: new Date().toISOString(),
    ...patch,
  }
  const index = state.itemProgress.findIndex((entry) => entry?.itemId === itemId)
  if (index < 0) state.itemProgress = [...state.itemProgress, next]
  else state.itemProgress = [...state.itemProgress.slice(0, index), { ...state.itemProgress[index], ...next }, ...state.itemProgress.slice(index + 1)]
}

function upsertFileEvent(state, patch) {
  const itemId = typeof patch?.itemId === "string" ? patch.itemId : ""
  const key = typeof patch?.key === "string" ? patch.key : ""
  if (!itemId || !key) return
  const next = {
    updatedAt: new Date().toISOString(),
    ...patch,
  }
  const index = state.fileEvents.findIndex((entry) => entry?.itemId === itemId && entry?.key === key)
  if (index < 0) state.fileEvents = [...state.fileEvents.slice(-(TELEMETRY_FILE_EVENT_LIMIT - 1)), next]
  else state.fileEvents = [...state.fileEvents.slice(0, index), { ...state.fileEvents[index], ...next }, ...state.fileEvents.slice(index + 1)]
}

function buildTelemetryProgress(state, current = {}) {
  return {
    startedAt: state.startedAt,
    logs: state.logs,
    itemProgress: state.itemProgress,
    fileEvents: state.fileEvents,
    currentFile: state.currentFile,
    stats: state.stats,
    ...current,
  }
}

function buildObjectMetadataParams(sourceHead) {
  return {
    ...(typeof sourceHead.ContentType === "string" ? { ContentType: sourceHead.ContentType } : {}),
    ...(typeof sourceHead.CacheControl === "string" ? { CacheControl: sourceHead.CacheControl } : {}),
    ...(sourceHead.Metadata ? { Metadata: sourceHead.Metadata } : {}),
  }
}

function createProgressTransform(onChunk) {
  return new Transform({
    transform(chunk, encoding, callback) {
      if (Buffer.isBuffer(chunk) || typeof chunk.length === "number") onChunk(chunk.length)
      callback(null, chunk)
    },
  })
}

async function copyObjectWithRangedMultipart(sourceClient, targetClient, sourceBucket, targetBucket, key, sourceHead, options = {}) {
  const sourceSize = typeof sourceHead.ContentLength === "number" ? sourceHead.ContentLength : 0
  const checkpoint = async (state) => { options.multipartState = state; await options.onMultipartCheckpoint?.(state) }
  const partCount = Math.ceil(sourceSize / UPLOAD_PART_SIZE)
  const parts = Array.from({ length: partCount }, (_, index) => {
    const start = index * UPLOAD_PART_SIZE
    const end = Math.min(sourceSize - 1, start + UPLOAD_PART_SIZE - 1)
    return { partNumber: index + 1, start, end, size: end - start + 1 }
  })
  let uploadId = options.multipartState?.key === key && Number(options.multipartState?.sourceSize) === sourceSize
    ? String(options.multipartState?.uploadId || "")
    : ""
  const uploadedParts = []
  const partProgress = new Map()
  let reportedLoaded = 0

  if (uploadId) {
    try {
      let marker
      do {
        const listed = await targetClient.send(new ListPartsCommand({ Bucket: targetBucket, Key: key, UploadId: uploadId, PartNumberMarker: marker }))
        for (const part of listed.Parts || []) {
          if (part.PartNumber && part.ETag) uploadedParts.push({ PartNumber: part.PartNumber, ETag: part.ETag, Size: Number(part.Size || 0) })
        }
        marker = listed.IsTruncated ? listed.NextPartNumberMarker : undefined
      } while (marker)
    } catch { uploadId = ""; uploadedParts.length = 0 }
  }
  if (!uploadId) {
    const createResult = await targetClient.send(new CreateMultipartUploadCommand({ Bucket: targetBucket, Key: key, ...buildObjectMetadataParams(sourceHead) }), options.abortSignal ? { abortSignal: options.abortSignal } : undefined)
    uploadId = String(createResult.UploadId || "")
    if (!uploadId) throw new Error(`Multipart upload id missing for ${key}`)
  }
  await checkpoint({ uploadId, key, sourceBucket, targetBucket, sourceSize, completedParts: uploadedParts.length })
  const completedParts = new Map(uploadedParts.map((part) => [part.PartNumber, part]))
  for (const part of parts) {
    const existing = completedParts.get(part.partNumber)
    if (existing && Number(existing.Size) === part.size) { partProgress.set(part.partNumber, part.size); reportedLoaded += part.size }
  }

  try {
    await runConcurrent(parts.filter((part) => !completedParts.has(part.partNumber) || Number(completedParts.get(part.partNumber)?.Size) !== part.size), RANGE_COPY_CONCURRENCY, async (part) => {
      const uploaded = await withRetries(
        `range copy ${sourceBucket}/${key} part ${part.partNumber}`,
        async () => {
          if (options.abortSignal?.aborted) throw new JobAbortedError()
          const sourceResponse = await sourceClient.send(
            new GetObjectCommand({
              Bucket: sourceBucket,
              Key: key,
              Range: `bytes=${part.start}-${part.end}`,
            }),
            options.abortSignal ? { abortSignal: options.abortSignal } : undefined
          )
          const body = sourceResponse.Body
          if (!body) throw new Error(`Source object body missing for ${key} part ${part.partNumber}`)

          let partLoaded = 0
          const uploadBody =
            typeof options.onProgress === "function" && typeof body.pipe === "function"
              ? body.pipe(
                  createProgressTransform((chunkLength) => {
                    partLoaded += chunkLength
                    const previous = partProgress.get(part.partNumber) || 0
                    const next = Math.min(part.size, Math.max(previous, partLoaded))
                    if (next > previous) {
                      partProgress.set(part.partNumber, next)
                      reportedLoaded += next - previous
                      options.onProgress({ loaded: reportedLoaded, total: sourceSize })
                    }
                  })
                )
              : body

          try {
            return await targetClient.send(
              new UploadPartCommand({
                Bucket: targetBucket,
                Key: key,
                UploadId: uploadId,
                PartNumber: part.partNumber,
                Body: uploadBody,
                ContentLength: part.size,
              }),
              options.abortSignal ? { abortSignal: options.abortSignal } : undefined
            )
          } finally {
            closeBodyStream(body)
          }
        },
        S3_RETRIES
      )

      if (!uploaded.ETag) throw new Error(`Multipart upload ETag missing for ${key} part ${part.partNumber}`)
      uploadedParts.push({ PartNumber: part.partNumber, ETag: uploaded.ETag })
      await checkpoint({ uploadId, key, sourceBucket, targetBucket, sourceSize, completedParts: uploadedParts.length, lastPartNumber: part.partNumber })
    })

    await targetClient.send(
      new CompleteMultipartUploadCommand({
        Bucket: targetBucket,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: uploadedParts.sort((a, b) => a.PartNumber - b.PartNumber).map(({ PartNumber, ETag }) => ({ PartNumber, ETag })),
        },
      }),
      options.abortSignal ? { abortSignal: options.abortSignal } : undefined
    )

    // CompleteMultipartUpload can succeed while the committed object has a
    // provider-side size discrepancy. Verify the committed object before the
    // part is reported as copied; the final migration listing is a second
    // independent check.
    const targetHead = await withRetries(
      `verify target head ${targetBucket}/${key}`,
      () => targetClient.send(new HeadObjectCommand({ Bucket: targetBucket, Key: key })),
      S3_RETRIES
    )
    const targetSize = typeof targetHead.ContentLength === "number" ? targetHead.ContentLength : -1
    if (targetSize !== sourceSize) {
      throw new Error(`Size mismatch after multipart copy for ${key}: source=${sourceSize} target=${targetSize}`)
    }
    await checkpoint(null)
  } catch (error) {
    // Leave the provider-side multipart upload intact. The durable upload ID
    // and ListParts reconciliation let a replacement worker resume it.
    throw error
  }
}

async function copyObject(sourceClient, targetClient, sourceBucket, targetBucket, key, options = {}) {
  await withRetries(
    `copy ${sourceBucket}/${key}`,
    async () => {
      if (options.abortSignal?.aborted) throw new JobAbortedError()
      const requestOptions = options.abortSignal ? { abortSignal: options.abortSignal } : undefined
      const sourceHead = await sourceClient.send(new HeadObjectCommand({ Bucket: sourceBucket, Key: key }), requestOptions)
      const sourceSize = typeof sourceHead.ContentLength === "number" ? sourceHead.ContentLength : 0
      if (sourceSize > 0 && sourceSize >= RANGE_COPY_THRESHOLD) {
        await copyObjectWithRangedMultipart(sourceClient, targetClient, sourceBucket, targetBucket, key, sourceHead, options)
      } else {
        const sourceResponse = await sourceClient.send(new GetObjectCommand({ Bucket: sourceBucket, Key: key }), requestOptions)
      const body = sourceResponse.Body
      if (!body) throw new Error(`Source object body missing for ${key}`)

      const upload = new Upload({
        client: targetClient,
        params: {
          Bucket: targetBucket,
          Key: key,
          Body: body,
          ...buildObjectMetadataParams(sourceHead),
        },
        queueSize: UPLOAD_QUEUE_SIZE,
        partSize: UPLOAD_PART_SIZE,
        leavePartsOnError: false,
      })
      if (typeof options.onProgress === "function") {
        upload.on("httpUploadProgress", (progress) => {
          options.onProgress({
            loaded: typeof progress?.loaded === "number" ? progress.loaded : 0,
            total:
              typeof progress?.total === "number"
                ? progress.total
                : typeof sourceHead.ContentLength === "number"
                  ? sourceHead.ContentLength
                  : 0,
          })
        })
      }

      try {
        const abortUpload = () => void upload.abort().catch(() => undefined)
        options.abortSignal?.addEventListener("abort", abortUpload, { once: true })
        await upload.done()
        options.abortSignal?.removeEventListener("abort", abortUpload)
        const targetHead = await withRetries(
          `verify target head ${targetBucket}/${key}`,
          () => targetClient.send(new HeadObjectCommand({ Bucket: targetBucket, Key: key })),
          S3_RETRIES
        )
        const sourceSize = typeof sourceHead.ContentLength === "number" ? sourceHead.ContentLength : 0
        const targetSize = typeof targetHead.ContentLength === "number" ? targetHead.ContentLength : -1
        if (targetSize !== sourceSize) {
          throw new Error(`Size mismatch after copy for ${key}: source=${sourceSize} target=${targetSize}`)
        }
        return { sourceSize, targetSize }
      } finally {
        closeBodyStream(body)
      }
      }
      if (typeof options.onProgress === "function") options.onProgress({ loaded: sourceSize, total: sourceSize })
      const targetHead = await withRetries(
        `verify target head ${targetBucket}/${key}`,
        () => targetClient.send(new HeadObjectCommand({ Bucket: targetBucket, Key: key })),
        S3_RETRIES
      )
      const targetSize = typeof targetHead.ContentLength === "number" ? targetHead.ContentLength : -1
      if (targetSize !== sourceSize) {
        throw new Error(`Size mismatch after copy for ${key}: source=${sourceSize} target=${targetSize}`)
      }
      return { sourceSize, targetSize }
    },
    S3_RETRIES
  )
}

async function processItem(jobId, payload, item, completedResults, state) {
  const prefix = payload.migration?.pathPrefix || null
  // Overwrite is literal for migration work: when enabled, every assigned
  // source object is copied even if the destination already matches. When it
  // is disabled, verified matches remain separate skipped objects and proven
  // mismatches are not replaced.
  const overwrite = payload.migration?.options?.overwrite !== false || payload.migration?.options?.workerRepairMismatchedObjects === true
  const workerShard = normalizeWorkerShard(payload.workerShard)
  const isSharded = Boolean(workerShard && workerShard.count > 1)
  const shardLabel = isSharded ? ` shard ${workerShard.index + 1}/${workerShard.count}` : ""
  const sourceClient = createClient(payload.source)
  const targetClient = createClient(payload.target)
  let stage = "repair_scan"
  let transferred = 0
  let failed = 0
  let skipped = 0
  let alreadyPresent = 0
  let sourceObjectCount = 0
  let initialMissing = 0
  let initialMismatched = 0
  const failureSamples = []
  let sourceScanLastCount = 0
  let destinationScanLastCount = 0
  let currentStageStartedAt = new Date().toISOString()
  let lastLiveProgressSyncAt = 0

  const syncLiveProgress = (extra = {}) => {
    const nowTs = Date.now()
    if (nowTs - lastLiveProgressSyncAt < LIVE_PROGRESS_SYNC_MS) return
    lastLiveProgressSyncAt = nowTs
    void safeUpdateJob(jobId, {
      status: "running",
      progress: {
        ...buildTelemetryProgress(state, {
          currentItemId: item.id,
          currentBucket: item.sourceBucket,
          stage,
          transferred,
          failed,
          skipped,
          totals: buildLiveTotals(completedResults, {
            transferred,
            failed,
            skipped,
            ...(typeof extra.missing === "number" ? { missing: extra.missing } : {}),
            ...(typeof extra.mismatched === "number" ? { mismatched: extra.mismatched } : {}),
          }),
          ...extra,
        }),
      },
    })
  }

  const forceSyncLiveProgress = (extra = {}) => {
    lastLiveProgressSyncAt = Date.now()
    void safeUpdateJob(jobId, {
      status: "running",
      progress: {
        ...buildTelemetryProgress(state, {
          currentItemId: item.id,
          currentBucket: item.sourceBucket,
          stage,
          transferred,
          failed,
          skipped,
          totals: buildLiveTotals(completedResults, {
            transferred,
            failed,
            skipped,
            ...(typeof extra.missing === "number" ? { missing: extra.missing } : {}),
            ...(typeof extra.mismatched === "number" ? { mismatched: extra.mismatched } : {}),
          }),
          ...extra,
        }),
      },
    })
  }

  try {
    pushLog(state, `Scanning ${item.sourceBucket} -> ${item.targetBucket}`, {
      itemId: item.id,
      stage,
      bucket: item.sourceBucket,
    })
    upsertItemProgress(state, {
      itemId: item.id,
      sourceBucket: item.sourceBucket,
      targetBucket: item.targetBucket,
      stage,
      status: "running",
      transferred,
      failed,
      skipped,
      processedFiles: 0,
      totalFiles: 0,
      summary: `Scanning ${item.sourceBucket} -> ${item.targetBucket}`,
    })
    const startSync = await safeUpdateJob(jobId, {
      status: "running",
      items: [
        {
          itemId: item.id,
          stage,
          status: "running",
          summary: `Scanning ${item.sourceBucket} -> ${item.targetBucket}`,
        },
      ],
      progress: {
        ...buildTelemetryProgress(state, {
          currentItemId: item.id,
          currentBucket: item.sourceBucket,
          stage,
          totals: buildLiveTotals(completedResults),
        }),
      },
    })
    if (startSync?.canceled) throw new JobAbortedError()

    const assignedInventory = Array.isArray(payload.inventoryObjects) ? payload.inventoryObjects : null
    const allSourceObjects = assignedInventory || await listAllObjects(sourceClient, item.sourceBucket, prefix, ({ count, key, size }) => {
      const delta = Math.max(0, count - sourceScanLastCount)
      sourceScanLastCount = count
      state.stats.scannedSourceObjects += delta
      state.currentFile = {
        itemId: item.id,
        bucket: item.sourceBucket,
        key: typeof key === "string" ? key : "",
        size: typeof size === "number" ? size : 0,
        stage,
        status: "scanning",
        startedAt: currentStageStartedAt,
        scanPhase: "source",
        scannedObjects: count,
        updatedAt: new Date().toISOString(),
      }
      upsertItemProgress(state, {
        itemId: item.id,
        stage,
        status: "running",
        scanSourceCount: count,
        summary: `Scanning ${item.sourceBucket}: ${count} source files found`,
      })
      syncLiveProgress()
    })
    const allDestinationObjects = assignedInventory
      ? await inspectAssignedObjects(sourceClient, targetClient, item.sourceBucket, item.targetBucket, assignedInventory, getJobAbortSignal(jobId))
      : await listAllObjects(targetClient, item.targetBucket, prefix, ({ count, key, size }) => {
      const delta = Math.max(0, count - destinationScanLastCount)
      destinationScanLastCount = count
      state.stats.scannedDestinationObjects += delta
      state.currentFile = {
        itemId: item.id,
        bucket: item.targetBucket,
        key: typeof key === "string" ? key : "",
        size: typeof size === "number" ? size : 0,
        stage,
        status: "scanning",
        startedAt: currentStageStartedAt,
        scanPhase: "destination",
        scannedObjects: count,
        updatedAt: new Date().toISOString(),
      }
      upsertItemProgress(state, {
        itemId: item.id,
        stage,
        status: "running",
        scanDestinationCount: count,
        summary: `Scanning ${item.targetBucket}: ${count} destination files found`,
      })
      syncLiveProgress()
    })
    // Inventory batches come from the File Scanner and are already disjoint.
    // Legacy manual shard jobs retain deterministic filtering compatibility.
    const sourceObjects = filterObjectsForShard(allSourceObjects, item.sourceBucket, workerShard)
    const destinationObjects = filterObjectsForShard(allDestinationObjects, item.sourceBucket, workerShard)
    const sourceBytes = allSourceObjects.reduce((sum, object) => sum + Number(object?.size || 0), 0)
    const shardSourceBytes = sourceObjects.reduce((sum, object) => sum + Number(object?.size || 0), 0)
    sourceObjectCount = allSourceObjects.length
    const shardObjectCount = sourceObjects.length
    const initialDiff = diffObjectsByListing(sourceObjects, destinationObjects, ({ checked, key, size, missing, mismatched }) => {
      state.currentFile = {
        itemId: item.id,
        bucket: item.sourceBucket,
        key: typeof key === "string" ? key : "",
        size: typeof size === "number" ? size : 0,
        stage,
        status: "verifying",
        startedAt: currentStageStartedAt,
        checkedObjects: checked,
        totalObjects: shardObjectCount,
        missing,
        mismatched,
        updatedAt: new Date().toISOString(),
      }
      upsertItemProgress(state, {
        itemId: item.id,
        stage,
        status: "running",
        verifyCheckedCount: checked,
        initialMissing: missing,
        initialMismatched: mismatched,
        summary: `Comparing ${item.sourceBucket}${shardLabel}: ${checked}/${shardObjectCount} files checked`,
      })
      syncLiveProgress({ verifyCheckedCount: checked, missing, mismatched })
    })
    initialMissing = initialDiff.missing.length
    initialMismatched = initialDiff.mismatched.length

    const diffCandidates = [...initialDiff.missing, ...initialDiff.mismatched]
    const destinationByKey = new Map(destinationObjects.map((object) => [object.key, object]))
    const toRepair = overwrite && payload.job.mode !== "verify_only"
      ? sourceObjects.map((object) => {
          const destination = destinationByKey.get(object.key)
          return destination
            ? { ...object, destinationSize: destination.destinationSize ?? destination.size, destinationEtag: destination.destinationEtag ?? destination.etag }
            : object
        })
      : diffCandidates
    alreadyPresent = overwrite ? 0 : Math.max(0, sourceObjects.length - diffCandidates.length)
    // Every source object that was not copied must be visible in the
    // accounting.  In particular, an exact destination match is a skipped
    // object when this is a repair/no-overwrite run; otherwise the UI reports
    // fewer processed objects than the source scan found.  Keep this count
    // separate from `transferred`, which means an actual copy.
    skipped = alreadyPresent
    state.stats.repairCandidates += toRepair.length
    upsertItemProgress(state, {
      itemId: item.id,
      stage,
      status: "running",
      alreadyPresent,
      initialMissing,
      initialMismatched,
      totalFiles: sourceObjectCount,
      processedFiles: transferred + failed + skipped,
      summary: `Scan complete for ${item.sourceBucket}${shardLabel}: ${alreadyPresent} already verified, ${initialMissing} missing, ${initialMismatched} mismatched`,
    })
    pushLog(state, `Scan complete for ${item.sourceBucket}`, {
      itemId: item.id,
      stage,
      alreadyPresent,
      initialMissing,
      initialMismatched,
      sourceCount: sourceObjectCount,
      shardSourceCount: shardObjectCount,
      destinationCount: destinationObjects.length,
    })

    if (payload.job.mode !== "verify_only") {
      stage = "repair_copy"
      currentStageStartedAt = new Date().toISOString()
      await runConcurrent(toRepair, COPY_CONCURRENCY, async (object) => {
        throwIfJobAborted(jobId)
        const isMismatch = typeof object?.destinationSize === "number"
        const objectSize = typeof object?.size === "number" ? object.size : 0
        if (isMismatch && !overwrite) {
          skipped += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: "mismatched",
            stage,
            status: "skipped",
            startedAt: new Date().toISOString(),
            completedAt: new Date().toISOString(),
            bytesTransferred: 0,
            bytesTotal: objectSize,
            reason: "overwrite_disabled",
          })
          upsertItemProgress(state, {
            itemId: item.id,
            stage,
            status: "running",
            transferred,
            failed,
            skipped,
            processedFiles: transferred + failed + skipped,
            totalFiles: sourceObjectCount,
            summary: `Skipping mismatched ${item.sourceBucket} object because overwrite is disabled`,
          })
          return
        }
        const latestTargetSize = await getTargetObjectSize(targetClient, item.targetBucket, object.key)
        if (!overwrite && !isMismatch && latestTargetSize === objectSize) {
          skipped += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "skipped",
            startedAt: new Date().toISOString(),
            completedAt: new Date().toISOString(),
            bytesTransferred: objectSize,
            bytesTotal: objectSize,
          })
          upsertItemProgress(state, {
            itemId: item.id,
            stage,
            status: "running",
            transferred,
            failed,
            skipped,
            processedFiles: transferred + failed + skipped,
            totalFiles: sourceObjectCount,
            summary: `Repairing ${item.sourceBucket}: ${transferred} copied, ${failed} failed, ${skipped} skipped`,
          })
          return
        }
        const startedAt = new Date().toISOString()
        state.currentFile = {
          itemId: item.id,
          bucket: item.sourceBucket,
          key: object.key,
          size: objectSize,
          stage,
          status: "copying",
          startedAt,
          bytesTransferred: 0,
          bytesTotal: objectSize,
        }
        upsertFileEvent(state, {
          itemId: item.id,
          bucket: item.sourceBucket,
          key: object.key,
          size: objectSize,
          kind: isMismatch ? "mismatched" : "missing",
          stage,
          status: "copying",
          startedAt,
          bytesTransferred: 0,
          bytesTotal: objectSize,
        })
        forceSyncLiveProgress()
        pushLog(state, `Copying ${object.key}`, {
          itemId: item.id,
          stage,
          key: object.key,
          size: objectSize,
          kind: isMismatch ? "mismatched" : "missing",
        })
        let lastProgressAt = 0
        try {
          await copyObject(sourceClient, targetClient, item.sourceBucket, item.targetBucket, object.key, {
            abortSignal: getJobAbortSignal(jobId),
            multipartState: payload.job?.progress?.multipart,
            onMultipartCheckpoint: (multipart) => updateJob(jobId, { progress: { multipart } }, { allowOffline: true }),
            onProgress: ({ loaded, total }) => {
              const now = Date.now()
              if (now - lastProgressAt < 800 && loaded < total) return
              lastProgressAt = now
              state.currentFile = {
                itemId: item.id,
                bucket: item.sourceBucket,
                key: object.key,
                size: objectSize,
                stage,
                status: "copying",
                startedAt,
                bytesTransferred: loaded,
                bytesTotal: total || objectSize,
                updatedAt: new Date().toISOString(),
              }
              upsertFileEvent(state, {
                itemId: item.id,
                bucket: item.sourceBucket,
                key: object.key,
                size: objectSize,
                kind: isMismatch ? "mismatched" : "missing",
                stage,
                status: "copying",
                startedAt,
                bytesTransferred: loaded,
                bytesTotal: total || objectSize,
              })
              syncLiveProgress()
            },
          })
          transferred += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "copied",
            startedAt,
            completedAt: new Date().toISOString(),
            bytesTransferred: objectSize,
            bytesTotal: objectSize,
          })
          forceSyncLiveProgress()
        } catch (error) {
          if (getJobAbortSignal(jobId)?.aborted) throw new JobAbortedError()
          failed += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "failed",
            startedAt,
            completedAt: new Date().toISOString(),
            error: error instanceof Error ? error.message : String(error),
          })
          if (failureSamples.length < 25) {
            failureSamples.push({
              key: object.key,
              error: error instanceof Error ? error.message : String(error),
            })
          }
          pushLog(state, `Failed to copy ${object.key}`, {
            itemId: item.id,
            stage,
            key: object.key,
            error: error instanceof Error ? error.message : String(error),
          })
          forceSyncLiveProgress()
        }
        state.currentFile = null
        upsertItemProgress(state, {
          itemId: item.id,
          stage,
          status: "running",
          transferred,
          failed,
          skipped,
          processedFiles: transferred + failed + skipped,
          totalFiles: sourceObjectCount,
          summary: `Repairing ${item.sourceBucket}: ${transferred} copied, ${failed} failed, ${skipped} skipped`,
        })

        syncLiveProgress()
      })
      const copyPhaseSync = await safeUpdateJob(jobId, {
        status: "running",
        items: [
          {
            itemId: item.id,
            stage,
            status: "running",
            summary: `Repairing ${item.sourceBucket}: ${transferred} copied, ${failed} failed, ${skipped} skipped`,
            transferred,
            failed,
            skipped,
            details: {
              initialMissing,
              initialMismatched,
              attempted: transferred + failed + skipped,
              remaining: Math.max(0, toRepair.length - transferred - failed - skipped),
            },
          },
        ],
        progress: {
          ...buildTelemetryProgress(state, {
            currentItemId: item.id,
            stage,
            currentBucket: item.sourceBucket,
            transferred,
            failed,
            skipped,
            totals: buildLiveTotals(completedResults, {
              transferred,
              failed,
              skipped,
            }),
          }),
        },
      })
      if (copyPhaseSync?.canceled) throw new JobAbortedError()
    } else {
      // Verify-only jobs do not copy anything. Every source object is
      // therefore accounted for as skipped, including exact matches found in
      // the initial source/destination diff.
      skipped = sourceObjectCount
      upsertItemProgress(state, {
        itemId: item.id,
        stage: "repair_verify",
        status: "running",
        transferred,
        failed,
        skipped,
        processedFiles: sourceObjectCount,
        totalFiles: sourceObjectCount,
        summary: `Verify-only mode for ${item.sourceBucket}${shardLabel}: ${toRepair.length} files queued for verification`,
      })
    }

    stage = "repair_verify"
    currentStageStartedAt = new Date().toISOString()
    pushLog(state, `Verifying ${item.sourceBucket}`, {
      itemId: item.id,
      stage,
      sourceCount: shardObjectCount,
    })
    let finalDestinationObjects = assignedInventory
      ? await inspectAssignedObjects(sourceClient, targetClient, item.sourceBucket, item.targetBucket, sourceObjects, getJobAbortSignal(jobId))
      : filterObjectsForShard(await listAllObjects(targetClient, item.targetBucket, prefix), item.sourceBucket, workerShard)
    let finalDiff = diffObjectsByListing(sourceObjects, finalDestinationObjects, ({ checked, key, size, missing, mismatched }) => {
      state.currentFile = {
        itemId: item.id,
        bucket: item.sourceBucket,
        key: typeof key === "string" ? key : "",
        size: typeof size === "number" ? size : 0,
        stage,
        status: "verifying",
        startedAt: currentStageStartedAt,
        checkedObjects: checked,
        totalObjects: shardObjectCount,
        missing,
        mismatched,
        updatedAt: new Date().toISOString(),
      }
      upsertItemProgress(state, {
        itemId: item.id,
        stage,
        status: "running",
        verifyCheckedCount: checked,
        finalMissing: missing,
        finalMismatched: mismatched,
        summary: `Verifying ${item.sourceBucket}${shardLabel}: ${checked}/${shardObjectCount} files checked`,
      })
      syncLiveProgress({ verifyCheckedCount: checked, missing, mismatched })
    })
    let finalMissing = finalDiff.missing.length
    let finalMismatched = finalDiff.mismatched.length

    if ((finalMissing > 0 || (overwrite && finalMismatched > 0)) && payload.job.mode !== "verify_only") {
      stage = "repair_reconcile"
      currentStageStartedAt = new Date().toISOString()
      const remainingToRepair = overwrite ? [...finalDiff.missing, ...finalDiff.mismatched] : finalDiff.missing
      pushLog(state, `Final verify found remaining issues in ${item.sourceBucket}; retrying ${remainingToRepair.length} object(s)`, {
        itemId: item.id,
        stage,
        finalMissing,
        finalMismatched,
      })

      await runConcurrent(remainingToRepair, COPY_CONCURRENCY, async (object) => {
        throwIfJobAborted(jobId)
        const isMismatch = typeof object?.destinationSize === "number"
        const objectSize = typeof object?.size === "number" ? object.size : 0
        if (isMismatch && !overwrite) {
          skipped += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: "mismatched",
            stage,
            status: "skipped",
            startedAt: new Date().toISOString(),
            completedAt: new Date().toISOString(),
            bytesTransferred: 0,
            bytesTotal: objectSize,
            reason: "overwrite_disabled",
          })
          return
        }
        const latestTargetSize = await getTargetObjectSize(targetClient, item.targetBucket, object.key)
        if (!overwrite && latestTargetSize === objectSize) {
          skipped += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "skipped",
            startedAt: new Date().toISOString(),
            completedAt: new Date().toISOString(),
            bytesTransferred: objectSize,
            bytesTotal: objectSize,
          })
          return
        }
        const startedAt = new Date().toISOString()
        state.currentFile = {
          itemId: item.id,
          bucket: item.sourceBucket,
          key: object.key,
          size: objectSize,
          stage,
          status: "copying",
          startedAt,
          bytesTransferred: 0,
          bytesTotal: objectSize,
        }
        upsertFileEvent(state, {
          itemId: item.id,
          bucket: item.sourceBucket,
          key: object.key,
          size: objectSize,
          kind: isMismatch ? "mismatched" : "missing",
          stage,
          status: "copying",
          startedAt,
          bytesTransferred: 0,
          bytesTotal: objectSize,
        })
        forceSyncLiveProgress()

        try {
          await copyObject(sourceClient, targetClient, item.sourceBucket, item.targetBucket, object.key, {
            abortSignal: getJobAbortSignal(jobId),
            multipartState: payload.job?.progress?.multipart,
            onMultipartCheckpoint: (multipart) => updateJob(jobId, { progress: { multipart } }, { allowOffline: true }),
            onProgress: ({ loaded, total }) => {
              state.currentFile = {
                itemId: item.id,
                bucket: item.sourceBucket,
                key: object.key,
                size: objectSize,
                stage,
                status: "copying",
                startedAt,
                bytesTransferred: loaded,
                bytesTotal: total || objectSize,
                updatedAt: new Date().toISOString(),
              }
              upsertFileEvent(state, {
                itemId: item.id,
                bucket: item.sourceBucket,
                key: object.key,
                size: objectSize,
                kind: isMismatch ? "mismatched" : "missing",
                stage,
                status: "copying",
                startedAt,
                bytesTransferred: loaded,
                bytesTotal: total || objectSize,
              })
              syncLiveProgress()
            },
          })
          transferred += 1
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "copied",
            startedAt,
            completedAt: new Date().toISOString(),
            bytesTransferred: objectSize,
            bytesTotal: objectSize,
          })
          forceSyncLiveProgress()
        } catch (error) {
          if (getJobAbortSignal(jobId)?.aborted) throw new JobAbortedError()
          failed += 1
          if (failureSamples.length < 25) {
            failureSamples.push({
              key: object.key,
              error: error instanceof Error ? error.message : String(error),
            })
          }
          upsertFileEvent(state, {
            itemId: item.id,
            bucket: item.sourceBucket,
            key: object.key,
            size: objectSize,
            kind: isMismatch ? "mismatched" : "missing",
            stage,
            status: "failed",
            startedAt,
            completedAt: new Date().toISOString(),
            error: error instanceof Error ? error.message : String(error),
          })
          forceSyncLiveProgress()
        }
      })

      state.currentFile = null
      finalDestinationObjects = assignedInventory
        ? await inspectAssignedObjects(sourceClient, targetClient, item.sourceBucket, item.targetBucket, sourceObjects, getJobAbortSignal(jobId))
        : filterObjectsForShard(await listAllObjects(targetClient, item.targetBucket, prefix), item.sourceBucket, workerShard)
      finalDiff = diffObjectsByListing(sourceObjects, finalDestinationObjects)
      finalMissing = finalDiff.missing.length
      finalMismatched = finalDiff.mismatched.length
    }

    const completed = finalMissing === 0 && (!overwrite || finalMismatched === 0)
    const resolvedAllObjects = !isSharded && completed && finalDestinationObjects.length >= sourceObjects.length
    state.stats.verifiedObjects += shardObjectCount
    const itemStatus = isSharded || assignedInventory ? "running" : completed ? "completed" : "failed"
    const completionSummary = isSharded
      ? `Shard ${workerShard.index + 1}/${workerShard.count} verified for ${item.sourceBucket}`
      : completed
        ? overwrite
          ? `Repair verified for ${item.sourceBucket}`
          : `Repair completed for ${item.sourceBucket}; existing objects were preserved`
        : `Repair incomplete for ${item.sourceBucket}: ${finalMissing} missing, ${finalMismatched} mismatched`
    state.currentFile = null
    upsertItemProgress(state, {
      itemId: item.id,
      stage,
      status: itemStatus,
      transferred,
      failed,
      skipped,
      processedFiles: Math.max(toRepair.length, transferred + failed + skipped),
      totalFiles: sourceObjectCount,
      initialMissing,
      initialMismatched,
      finalMissing,
      finalMismatched,
      summary: completionSummary,
      ...(isSharded
        ? {
            shardIndex: workerShard.index,
            shardCount: workerShard.count,
            shardObjectCount,
          }
        : {}),
    })
    pushLog(
      state,
      completed
        ? isSharded
          ? `Shard ${workerShard.index + 1}/${workerShard.count} verified for ${item.sourceBucket}`
          : `Repair verified for ${item.sourceBucket}`
        : `Repair incomplete for ${item.sourceBucket}`,
      {
        itemId: item.id,
        stage,
        finalMissing,
        finalMismatched,
      }
    )
    if (completed) state.stats.completedBuckets += 1
    else state.stats.failedBuckets += 1

    const itemCompleteSync = await safeUpdateJob(jobId, {
        status: "running",
        items: [
          {
            itemId: item.id,
            stage,
            status: itemStatus,
            summary: completionSummary,
            transferred,
            failed,
            skipped,
            details: {
              initialMissing,
              initialMismatched,
              alreadyPresent,
              sourceObjectCount,
              shardObjectCount,
              ...(isSharded
                ? {
                    shardComplete: completed,
                    shardIndex: workerShard.index,
                    shardCount: workerShard.count,
                  }
                : {}),
              sourceBytes,
              shardSourceBytes,
              destinationObjectCountBefore: destinationObjects.length,
              destinationObjectCountAfter: finalDestinationObjects.length,
              finalMissing,
              finalMismatched,
              resolvedAllObjects,
              failureSamples,
            },
          },
        ],
        progress: {
          ...buildTelemetryProgress(state, {
            currentItemId: item.id,
            stage,
            currentBucket: item.sourceBucket,
            transferred,
            failed,
            skipped,
            finalMissing,
            finalMismatched,
            totals: buildLiveTotals(completedResults, {
              transferred,
              failed,
              skipped,
              missing: finalMissing,
              mismatched: finalMismatched,
            }),
        }),
      },
    })
    if (itemCompleteSync?.canceled) throw new JobAbortedError()

    return {
      itemId: item.id,
      sourceBucket: item.sourceBucket,
      targetBucket: item.targetBucket,
      initialMissing,
      initialMismatched,
      alreadyPresent,
      sourceObjectCount,
      shardObjectCount,
      sourceBytes,
      shardSourceBytes,
      destinationObjectCountBefore: destinationObjects.length,
      destinationObjectCountAfter: finalDestinationObjects.length,
      transferred,
      failed,
      skipped,
      finalMissing,
      finalMismatched,
      completed,
      resolvedAllObjects,
      shardComplete: isSharded ? completed : undefined,
      shardIndex: isSharded ? workerShard.index : undefined,
      shardCount: isSharded ? workerShard.count : undefined,
      failureSamples,
      integrityProofs: assignedInventory ? finalDestinationObjects.map((object) => ({ key: object.key, size: object.destinationSize ?? object.size, destinationEtag: object.destinationEtag ?? null, sha256: object.sourceSha256 ?? null, verified: object.integrityVerified === true })) : undefined,
    }
  } catch (error) {
    if (error instanceof JobAbortedError || getJobAbortSignal(jobId)?.aborted) throw new JobAbortedError()
    state.stats.failedBuckets += 1
    pushLog(state, `Worker ${stage.replace("repair_", "")} failed for ${item.sourceBucket}`, {
      itemId: item.id,
      stage,
      error: error instanceof Error ? error.message : String(error),
    })
    upsertItemProgress(state, {
      itemId: item.id,
      sourceBucket: item.sourceBucket,
      targetBucket: item.targetBucket,
      stage,
      status: "failed",
      transferred,
      failed,
      skipped,
      processedFiles: transferred + failed + skipped,
      totalFiles: sourceObjectCount,
      summary: `Worker ${stage.replace("repair_", "")} failed for ${item.sourceBucket}`,
      error: error instanceof Error ? error.message : String(error),
    })
    if (state.currentFile?.itemId === item.id) {
      upsertFileEvent(state, {
        ...state.currentFile,
        itemId: item.id,
        key: state.currentFile.key,
        stage,
        status: "failed",
        completedAt: new Date().toISOString(),
        error: error instanceof Error ? error.message : String(error),
      })
      state.currentFile = null
    }
    const itemFailedSync = await safeUpdateJob(jobId, {
      status: "running",
      items: [
        {
          itemId: item.id,
          stage,
          status: "failed",
          summary: `Worker ${stage.replace("repair_", "")} failed for ${item.sourceBucket}`,
          transferred,
          failed,
          skipped,
          details: {
            initialMissing,
            initialMismatched,
            failureSamples,
            error: error instanceof Error ? error.message : String(error),
          },
        },
      ],
      progress: {
        ...buildTelemetryProgress(state, {
          currentItemId: item.id,
          currentBucket: item.sourceBucket,
          stage,
          transferred,
          failed,
          skipped,
          totals: buildLiveTotals(completedResults, {
            transferred,
            failed,
            skipped,
          }),
        }),
      },
    }).catch(() => {})
    if (itemFailedSync?.canceled) throw new JobAbortedError()
    throw error
  }
}

async function runJob(job, payload) {
  const state = createJobTelemetry(payload)
  const workerShard = normalizeWorkerShard(payload?.workerShard)
  if ((requiresWorkerShard(payload) || payload?.workerShard !== undefined) && !workerShard) {
    throw new Error("Migration shard job is missing a valid worker shard assignment; refusing to process the full migration")
  }
  const isSharded = Boolean(workerShard && workerShard.count > 1)
  const bucketCount = Array.isArray(payload?.items) ? payload.items.length : 0
  pushLog(
    state,
    isSharded
      ? `Worker shard ${workerShard.index + 1}/${workerShard.count} enabled across ${bucketCount} bucket(s)`
      : `Strict worker verification enabled across ${bucketCount} bucket(s)`,
    {
    stage: "start",
    mode: payload?.job?.mode || "repair_and_verify",
    verifyAllBuckets: payload?.job?.verifyAllBuckets === true,
    strictCompletion: payload?.job?.strictCompletion === true,
      ...(isSharded ? { shardIndex: workerShard.index, shardCount: workerShard.count } : {}),
    }
  )
  const results = []
  for (const item of Array.isArray(payload.items) ? payload.items : []) {
    results.push(await processItem(job.id, payload, item, results, state))
  }

  const totalMissing = results.reduce((sum, item) => sum + item.finalMissing, 0)
  const totalMismatched = results.reduce((sum, item) => sum + item.finalMismatched, 0)
  const totalTransferred = results.reduce((sum, item) => sum + item.transferred, 0)
  const totalFailed = results.reduce((sum, item) => sum + item.failed, 0)
  const totalVerifiedObjects = isSharded
    ? results.reduce((sum, item) => sum + Number(item.shardObjectCount || 0), 0)
    : results.reduce((sum, item) => sum + Number(item.sourceObjectCount || 0), 0)
  const overwrite = payload.migration?.options?.overwrite !== false || payload.migration?.options?.workerRepairMismatchedObjects === true
  const completed = totalMissing === 0 && (!overwrite || totalMismatched === 0) && totalFailed === 0
  const completionSummary = isSharded
    ? completed
      ? `Worker shard ${workerShard.index + 1}/${workerShard.count} completed across ${bucketCount} bucket(s); ${totalVerifiedObjects} objects verified, ${totalTransferred} repaired`
      : `Worker shard ${workerShard.index + 1}/${workerShard.count} incomplete: ${totalMissing} missing, ${totalMismatched} mismatched, ${totalFailed} copy failures`
    : completed
      ? `Worker reconciliation completed: destination matches source across ${bucketCount} bucket(s); ${totalVerifiedObjects} objects verified, ${totalTransferred} repaired`
      : `Worker reconciliation incomplete: ${totalMissing} missing, ${totalMismatched} mismatched, ${totalFailed} copy failures`
  pushLog(
    state,
    completionSummary,
    {
      stage: "completed",
      transferred: totalTransferred,
      failed: totalFailed,
      missing: totalMissing,
      mismatched: totalMismatched,
      verifiedObjects: totalVerifiedObjects,
    }
  )

  const finalSync = await finalizeJobUpdate(job.id, {
    status: completed ? "completed" : "failed",
    summary: completionSummary,
    error: completed ? null : "One or more items still have missing/mismatched files after worker repair",
    result: {
      items: results,
      logs: state.logs,
      fileEvents: state.fileEvents,
      itemProgress: state.itemProgress,
      totals: {
        transferred: totalTransferred,
        failed: totalFailed,
        skipped: results.reduce((sum, item) => sum + item.skipped, 0),
        missing: totalMissing,
        mismatched: totalMismatched,
        verifiedObjects: totalVerifiedObjects,
      },
    },
    progress: {
      ...buildTelemetryProgress(state, {
        stage: "completed",
        active: false,
        currentFile: null,
        totals: {
          transferred: totalTransferred,
          failed: totalFailed,
          skipped: results.reduce((sum, item) => sum + item.skipped, 0),
          missing: totalMissing,
          mismatched: totalMismatched,
          verifiedObjects: totalVerifiedObjects,
          completedItems: results.filter((item) => item.completed).length,
          failedItems: results.filter((item) => !item.completed).length,
        },
      }),
    },
  })
  if (finalSync?.canceled) return
}

let currentJobId = null
let heartbeatLoopStarted = false
let heartbeatLoopStopped = false
let currentLeaseConfirmedAt = 0

async function startHeartbeatLoop() {
  while (!heartbeatLoopStopped) {
    try {
      await heartbeat({ currentJobId: currentJobId ?? null })
      if (currentJobId) currentLeaseConfirmedAt = Date.now()
    } catch (error) {
      if (error instanceof WorkerAuthenticationError) {
        fatalAuthenticationError = true
        console.error("Worker authentication failed:", error.message)
        stopHeartbeatLoop()
      }
      if (currentJobId && Date.now() - currentLeaseConfirmedAt > 60_000) {
        console.error(`Queue lease could not be renewed for ${currentJobId}; aborting work before the lease can expire`)
        markJobAborted(currentJobId)
      }
      console.error("Heartbeat failed:", error instanceof Error ? error.message : String(error))
    }
    if (!heartbeatLoopStopped) await sleep(HEARTBEAT_MS)
  }
}

function stopHeartbeatLoop() {
  heartbeatLoopStopped = true
}

async function main() {
  console.log(`Worker starting for agent ${AGENT_ID} using ${queueClient ? "the migration queue database" : SERVER_URL}`)
  console.log(
    `Copy tuning: ${COPY_CONCURRENCY} object(s) in parallel, ${RANGE_COPY_CONCURRENCY} ranged part(s) per large object, ${UPLOAD_QUEUE_SIZE} upload part(s) per streamed object, ${Math.round(UPLOAD_PART_SIZE / 1024 / 1024)} MB parts`
  )
  if (!heartbeatLoopStarted) {
    heartbeatLoopStarted = true
    void startHeartbeatLoop()
  }
  if (Number.isFinite(RUNTIME_DEADLINE) && !runtimeRotationTimer) {
    runtimeRotationTimer = setTimeout(() => {
      runtimeRotationRequested = true
      if (currentJobId) markJobAborted(currentJobId)
    }, Math.max(0, RUNTIME_DEADLINE - Date.now()))
    runtimeRotationTimer.unref?.()
  }

  while (!fatalAuthenticationError) {
    try {
      if (runtimeRotationRequested || Date.now() >= RUNTIME_DEADLINE) {
        runtimeRotationRequested = true
        await retireWorker("runtime_window_complete").catch((error) => console.error("Worker retirement sync failed:", error instanceof Error ? error.message : String(error)))
        console.log("Worker runtime window completed; replacement capacity is now available")
        stopHeartbeatLoop()
        return
      }
      const claimed = await tryClaimJob()
      if (claimed?.poolComplete === true) {
        console.log(`Worker pool is complete (${claimed.poolReason || "terminal"}); stopping worker cleanly`)
        await retireWorker("pool_complete").catch((error) => console.error("Worker retirement sync failed:", error instanceof Error ? error.message : String(error)))
        stopHeartbeatLoop()
        return
      }
      if (!claimed?.job || !claimed?.payload) {
        idleClaimCount += 1
        await sleep(Math.min(60_000, POLL_MS * 2 ** Math.min(idleClaimCount - 1, 3)) + Math.floor(Math.random() * 1_000))
        continue
      }

      idleClaimCount = 0
      currentJobId = claimed.job.id
      currentLeaseConfirmedAt = Date.now()
      jobAbortControllers.set(claimed.job.id, new AbortController())
      console.log(`Claimed job ${claimed.job.id} for migration ${claimed.payload?.migration?.id || "-"}`)
      await runJob(claimed.job, claimed.payload)
      console.log(`Finished job ${claimed.job.id}`)
      currentJobId = null
      currentLeaseConfirmedAt = 0
      jobAbortControllers.delete(claimed.job.id)
      if (runtimeRotationRequested || Date.now() >= RUNTIME_DEADLINE) {
        runtimeRotationRequested = true
        await retireWorker("runtime_window_complete").catch((error) => console.error("Worker retirement sync failed:", error instanceof Error ? error.message : String(error)))
        console.log("Worker runtime window completed after the current file; replacement capacity is now available")
        stopHeartbeatLoop()
        return
      }
      if (EXIT_AFTER_JOB) {
        console.log(`Exit-after-job enabled; stopping worker after job ${claimed.job.id}`)
        await retireWorker("exit_after_job").catch((error) => console.error("Worker retirement sync failed:", error instanceof Error ? error.message : String(error)))
        stopHeartbeatLoop()
        return
      }
    } catch (error) {
      if (error instanceof WorkerAuthenticationError) {
        fatalAuthenticationError = true
        console.error("Worker authentication failed:", error.message)
        stopHeartbeatLoop()
        process.exitCode = 1
        return
      }
      console.error("Worker loop error:", error instanceof Error ? error.message : String(error))
      const failedJobId = currentJobId
      if (runtimeRotationRequested) {
        if (failedJobId) await releaseJobForRotation(failedJobId)
        currentJobId = null
        if (failedJobId) jobAbortControllers.delete(failedJobId)
        await retireWorker("runtime_window_complete").catch((retireError) => console.error("Worker retirement sync failed:", retireError instanceof Error ? retireError.message : String(retireError)))
        console.log("Worker runtime window completed; unfinished work was returned to the durable queue")
        stopHeartbeatLoop()
        return
      }
      if (currentJobId) {
        if (error instanceof JobAbortedError) {
          console.log(`Job ${currentJobId} aborted by user`)
        } else {
          try {
            await finalizeJobUpdate(currentJobId, {
              status: "failed",
              error: error instanceof Error ? error.message : String(error),
              summary: "Worker crashed while processing repair job",
            })
          } catch {}
        }
      }
      currentJobId = null
      currentLeaseConfirmedAt = 0
      if (failedJobId) jobAbortControllers.delete(failedJobId)
      if (EXIT_AFTER_JOB && failedJobId) {
        console.log(`Exit-after-job enabled; stopping worker after terminal job ${failedJobId}`)
        stopHeartbeatLoop()
        return
      }
      await sleep(POLL_MS)
    }
  }
}

async function runWorkerForever() {
  while (true) {
    try {
      await loadRuntimeConfiguration()
      await ensureWorkerIdentity()
      await bootstrapQueueDatabase()
      await main()
      return
    } catch (error) {
      console.error("Worker fatal error:", error instanceof Error ? error.stack || error.message : String(error))
      if (error instanceof WorkerAuthenticationError) {
        fatalAuthenticationError = true
        stopHeartbeatLoop()
        process.exitCode = 1
        return
      }
      if (error instanceof WorkerBootstrapTerminalError) {
        console.log(`Worker pool bootstrap finished: ${error.message}`)
        stopHeartbeatLoop()
        return
      }
      currentJobId = null
      currentLeaseConfirmedAt = 0
      if (Date.now() >= RUNTIME_DEADLINE) {
        console.error("Worker startup retries reached the configured runtime window")
        stopHeartbeatLoop()
        return
      }
      if (EXIT_AFTER_JOB) {
        stopHeartbeatLoop()
        return
      }
      await sleep(POLL_MS)
    }
  }
}

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason instanceof Error ? reason.stack || reason.message : String(reason))
})

process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error instanceof Error ? error.stack || error.message : String(error))
})

void runWorkerForever().finally(async () => {
  if (queueClient && queueReady) await queueClient.end().catch(() => undefined)
})
