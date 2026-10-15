import { createCipheriv, createDecipheriv, createHash, createHmac, randomBytes, randomUUID } from "node:crypto"

import { queryDb, withDbAdvisoryLock, withDbTransaction } from "@/lib/db"
import { getBackendOrchestratorSettings } from "@/lib/backend-orchestrator-settings-store"
import { getMigrationOrchestratorSettings } from "@/lib/migration-orchestrator-settings-store"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { syncAllGitHubWorkerSecretsWithinInstallerLock } from "@/lib/github-worker-secrets"
import { runtimeHealthError } from "./cloudflare-worker-health.cjs"

export type HostedWorker = "backend" | "scanner" | "migration"
export type InstallMode = "single" | "separate"

type TokenMap = Record<HostedWorker, string>
type Account = { id: string; name: string }
type Artifact = { url: string; sha256: string; compatibilityDate: string; compatibilityFlags?: string[] }
type Manifest = { version: string; workers: Record<HostedWorker, Artifact> }
type WorkerState = { accountId?: string; accountName?: string; scriptName: string; url?: string; deployed?: boolean; verified?: boolean; phase?: "queued" | "uploading" | "configuring" | "deployed" | "deleting" | "verifying" | "verified" | "failed"; deployedAt?: string; verifiedAt?: string; lastCheckedAt?: string; latencyMs?: number; build?: string | number; releaseVersion?: string; artifactSha256?: string; error?: string }
type HyperdriveState = { id: string; name: string; accountId: string; originFingerprint: string }
type InstallState = {
  id: string
  mode: InstallMode
  releaseVersion?: string
  status: "pending" | "running" | "ready" | "failed"
  step: string
  secrets: Record<HostedWorker, string>
  encryptedTokens?: Partial<TokenMap>
  hyperdrives?: Record<string, HyperdriveState>
  workers: Record<HostedWorker, WorkerState>
  error?: string
  lastReconciledAt?: string
  updatedAt: string
}
type RuntimeSnapshot = {
  backend: { enabled: boolean; orchestratorUrl: string; sharedSecret: string; syncIntervalMinutes: number }
  migration: { enabled: boolean; migrationEnabled: boolean; fileScannerEnabled: boolean; orchestratorUrl: string; fileScannerUrl: string; sharedSecret: string; fileScannerSecret: string }
}
type HostingPreference = { mode: "automatic" }

const API = "https://api.cloudflare.com/client/v4"
const ORDER: HostedWorker[] = ["backend", "scanner", "migration"]

function postgresEncryptionMaterial() {
  return String(process.env.POSTGRES_URL || "").trim()
}

function encryptionMaterials() {
  const configured = [process.env.CLOUDFLARE_TOKEN_ENCRYPTION_KEY, process.env.AUTH_SECRET, process.env.NEXTAUTH_SECRET]
  const database = postgresEncryptionMaterial()
  return [...configured, database]
    .map((value) => String(value || "").trim())
    .filter((value, index, values) => value.length >= 24 && values.indexOf(value) === index)
}

function primaryEncryptionKey() {
  const material = encryptionMaterials()[0]
  if (!material) throw new Error("Configure PostgreSQL before securely saving Cloudflare tokens")
  return createHash("sha256").update(`drive-cloudflare-token:${material}`).digest()
}

function encryptionKeys() {
  const materials = encryptionMaterials()
  if (!materials.length) throw new Error("No Cloudflare token decryption key is configured")
  return materials.map((material) => createHash("sha256").update(`drive-cloudflare-token:${material}`).digest())
}

function encryptToken(token: string, installationId: string, worker: HostedWorker) {
  const iv = randomBytes(12)
  const cipher = createCipheriv("aes-256-gcm", primaryEncryptionKey(), iv)
  cipher.setAAD(Buffer.from(`${installationId}:${worker}`))
  const encrypted = Buffer.concat([cipher.update(token, "utf8"), cipher.final()])
  return `v1.${iv.toString("base64url")}.${cipher.getAuthTag().toString("base64url")}.${encrypted.toString("base64url")}`
}

function decryptToken(value: string, installationId: string, worker: HostedWorker) {
  const [version, iv, tag, encrypted] = String(value).split(".")
  if (version !== "v1" || !iv || !tag || !encrypted) throw new Error("Saved Cloudflare token is invalid; replace it")
  for (const key of encryptionKeys()) {
    try {
      return decryptTokenWithKey(value, installationId, worker, key)
    } catch { /* Try legacy key material so saved tokens survive a safe key migration. */ }
  }
  throw new Error("Saved Cloudflare token cannot be decrypted; replace it")
}

function decryptTokenWithKey(value: string, installationId: string, worker: HostedWorker, key: Buffer) {
  const [version, iv, tag, encrypted] = String(value).split(".")
  if (version !== "v1" || !iv || !tag || !encrypted) throw new Error("Saved Cloudflare token is invalid; replace it")
  const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(iv, "base64url"))
  decipher.setAAD(Buffer.from(`${installationId}:${worker}`)); decipher.setAuthTag(Buffer.from(tag, "base64url"))
  return Buffer.concat([decipher.update(Buffer.from(encrypted, "base64url")), decipher.final()]).toString("utf8")
}

function panelUrl() {
  const configured = String(process.env.APP_URL || process.env.NEXT_PUBLIC_APP_URL || process.env.AUTH_URL || process.env.NEXTAUTH_URL || "").trim()
  const vercel = String(process.env.VERCEL_PROJECT_PRODUCTION_URL || process.env.VERCEL_URL || "").trim()
  const value = configured || (vercel ? `https://${vercel}` : "")
  return value.replace(/\/+$/, "")
}

function installationSuffix() {
  return createHash("sha256").update((panelUrl() || "drive").toLowerCase()).digest("hex").slice(0, 10)
}

function resourceNames() {
  const suffix = installationSuffix()
  return {
    scripts: { backend: `drive-backend-${suffix}`, scanner: `drive-file-scanner-${suffix}`, migration: `drive-migration-${suffix}` } satisfies Record<HostedWorker, string>,
    scannerQueue: `drive-file-scan-${suffix}`,
    scannerDlq: `drive-file-scan-dlq-${suffix}`,
    migrationQueue: `drive-github-dispatch-${suffix}`,
    migrationDlq: `drive-github-dispatch-dlq-${suffix}`,
    hyperdrive: `drive-postgres-${suffix}`,
  }
}

function jsonState(row: unknown): InstallState | null {
  if (!row || typeof row !== "object") return null
  return row as InstallState
}

async function ensureTable() {
  await queryDb(`create table if not exists drive_cloudflare_worker_installations (
    id uuid primary key, state jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(), updated_at timestamptz not null default now()
  )`)
}

async function loadState(): Promise<InstallState | null> {
  await ensureTable()
  const result = await queryDb<{ state: unknown }>(`select state from drive_cloudflare_worker_installations order by updated_at desc limit 1`)
  return jsonState(result.rows[0]?.state)
}

async function saveState(state: InstallState) {
  state.updatedAt = new Date().toISOString()
  await queryDb(`insert into drive_cloudflare_worker_installations(id,state,updated_at) values($1,$2::jsonb,now())
    on conflict(id) do update set state=excluded.state,updated_at=now()`, [state.id, JSON.stringify(state)])
}

async function rotateSavedTokenEncryption(state: InstallState): Promise<InstallState> {
  let primary: Buffer
  try { primary = primaryEncryptionKey() } catch { return state }
  if (!state.encryptedTokens || !ORDER.every((worker) => state.encryptedTokens?.[worker])) return state
  let changed = false
  const encryptedTokens = {} as TokenMap
  for (const worker of ORDER) {
    const encrypted = state.encryptedTokens[worker]!
    try {
      decryptTokenWithKey(encrypted, state.id, worker, primary)
      encryptedTokens[worker] = encrypted
    } catch {
      encryptedTokens[worker] = encryptToken(decryptToken(encrypted, state.id, worker), state.id, worker)
      changed = true
    }
  }
  if (!changed) return state
  const result = await queryDb<{ state: unknown }>(`
    update public.drive_cloudflare_worker_installations
    set state=jsonb_set(state,'{encryptedTokens}',$2::jsonb),updated_at=now()
    where id=$1 and state->'encryptedTokens'=$3::jsonb
    returning state
  `, [state.id, JSON.stringify(encryptedTokens), JSON.stringify(state.encryptedTokens)])
  return result.rows[0] ? (jsonState(result.rows[0].state) ?? state) : (await loadState() ?? state)
}

async function saveRuntimeConfiguration(state: InstallState, enabled: boolean) {
  const backend = await getBackendOrchestratorSettings()
  const backendValue = {
    enabled,
    orchestratorUrl: state.workers.backend.url || "",
    sharedSecret: state.secrets.backend,
    syncIntervalMinutes: backend.syncIntervalMinutes,
  }
  const migrationValue = {
    enabled,
    migrationEnabled: enabled,
    fileScannerEnabled: enabled,
    orchestratorUrl: state.workers.migration.url || "",
    fileScannerUrl: state.workers.scanner.url || "",
    sharedSecret: state.secrets.migration,
    fileScannerSecret: state.secrets.scanner,
  }
  await withDbTransaction(async (client) => {
    await client.query(`insert into drive_app_settings(key,value,updated_at) values('backend-orchestrator',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify(backendValue)])
    await client.query(`insert into drive_app_settings(key,value,updated_at) values('migration-orchestrator',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify(migrationValue)])
  })
}

async function currentRuntimeSnapshot(): Promise<RuntimeSnapshot> {
  const [backend, migration] = await Promise.all([getBackendOrchestratorSettings(), getMigrationOrchestratorSettings()])
  return {
    backend: { enabled: backend.enabled, orchestratorUrl: backend.orchestratorUrl, sharedSecret: backend.sharedSecret, syncIntervalMinutes: backend.syncIntervalMinutes },
    migration: { enabled: migration.enabled, migrationEnabled: migration.migrationEnabled, fileScannerEnabled: migration.fileScannerEnabled, orchestratorUrl: migration.orchestratorUrl, fileScannerUrl: migration.fileScannerUrl, sharedSecret: migration.sharedSecret, fileScannerSecret: migration.fileScannerSecret },
  }
}

async function adoptExistingWorkers(state: InstallState, tokens: TokenMap, accounts: Record<HostedWorker, Account>) {
  const snapshot = await currentRuntimeSnapshot()
  const candidates: Record<HostedWorker, { url: string; secret: string }> = {
    backend: { url: snapshot.backend.orchestratorUrl, secret: snapshot.backend.sharedSecret },
    scanner: { url: snapshot.migration.fileScannerUrl, secret: snapshot.migration.fileScannerSecret },
    migration: { url: snapshot.migration.orchestratorUrl, secret: snapshot.migration.sharedSecret },
  }
  await Promise.all(ORDER.map(async (worker) => {
    const current = state.workers[worker]
    const candidate = candidates[worker]
    if (current.deployed || !candidate.url || candidate.secret.length < 24) return
    try {
      if (!(await scriptExists(tokens[worker], accounts[worker].id, current.scriptName))) return
      const inspected = await inspectWorker(candidate.url, candidate.secret)
      const checkedAt = new Date().toISOString()
      state.secrets[worker] = candidate.secret; current.url = candidate.url; current.deployed = true; current.verified = true; current.phase = "verified"
      current.deployedAt = checkedAt; current.verifiedAt = checkedAt; current.lastCheckedAt = checkedAt; current.latencyMs = inspected.latencyMs; current.build = inspected.build
    } catch { /* Existing configuration is not authoritative; the installer will repair it. */ }
  }))
}

async function writeRuntimeSnapshot(snapshot: RuntimeSnapshot) {
  await withDbTransaction(async (client) => {
    await client.query(`insert into drive_app_settings(key,value,updated_at) values('backend-orchestrator',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify(snapshot.backend)])
    await client.query(`insert into drive_app_settings(key,value,updated_at) values('migration-orchestrator',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify(snapshot.migration)])
  })
}

export async function getCloudflareHostingPreference(): Promise<HostingPreference> {
  await queryDb(`select value from drive_app_settings where key='cloudflare-worker-hosting' limit 1`)
  return { mode: "automatic" }
}

export async function setCloudflareHostingMode(mode: "automatic") {
  const installation = await loadState()
  if (installation?.status === "ready") await saveRuntimeConfiguration(installation, true)
  else {
    const disabled = await currentRuntimeSnapshot()
    disabled.backend.enabled = false
    disabled.migration.enabled = false
    disabled.migration.migrationEnabled = false
    disabled.migration.fileScannerEnabled = false
    await writeRuntimeSnapshot(disabled)
  }
  const preference: HostingPreference = { mode }
  await queryDb(`insert into drive_app_settings(key,value,updated_at) values('cloudflare-worker-hosting',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify(preference)])
  return { mode }
}

function freshState(mode: InstallMode): InstallState {
  const names = resourceNames()
  return {
    id: randomUUID(), mode, status: "pending", step: "created",
    secrets: { backend: randomBytes(32).toString("base64url"), scanner: randomBytes(32).toString("base64url"), migration: randomBytes(32).toString("base64url") },
    workers: {
      backend: { scriptName: names.scripts.backend, phase: "queued" }, scanner: { scriptName: names.scripts.scanner, phase: "queued" }, migration: { scriptName: names.scripts.migration, phase: "queued" },
    },
    updatedAt: new Date().toISOString(),
  }
}

async function cf<T>(token: string, path: string, init: RequestInit = {}): Promise<T> {
  const isForm = typeof FormData !== "undefined" && init.body instanceof FormData
  let lastError: unknown
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      const response = await fetch(`${API}${path}`, {
        ...init,
        headers: { Authorization: `Bearer ${token}`, ...(init.body && !isForm ? { "Content-Type": "application/json" } : {}), ...(init.headers || {}) },
        signal: AbortSignal.timeout(25_000),
      })
      const payload = await response.json().catch(() => ({})) as { success?: boolean; result?: T; errors?: Array<{ message?: string }> }
      if (!response.ok || payload.success === false) {
        const error = new Error(payload.errors?.[0]?.message || `Cloudflare request failed (${response.status})`)
        if (![408, 425, 429, 500, 502, 503, 504].includes(response.status)) throw error
        throw error
      }
      return payload.result as T
    } catch (error) {
      lastError = error
      const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase()
      const retryable = message.includes("timeout") || message.includes("timed out") || message.includes("aborted") || message.includes("fetch failed") || message.includes("connect") || message.includes("socket")
      if (!retryable || attempt === 3) throw error
      await new Promise((resolve) => setTimeout(resolve, 750 * 2 ** attempt))
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Cloudflare request failed")
}

async function resolveAccount(token: string): Promise<Account> {
  const accounts = await cf<Account[]>(token, "/accounts?per_page=50")
  if (accounts.length !== 1) throw new Error(accounts.length ? "Token can access multiple accounts; create an account-scoped token" : "Token cannot access a Cloudflare account")
  return accounts[0]
}

async function fetchWithRetry(url: string, init: RequestInit, attempts = 4, retryNotFound = false) {
  let lastError: unknown
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(url, init)
      const retryableStatus = [408, 425, 429, 500, 502, 503, 504].includes(response.status) || (retryNotFound && response.status === 404)
      if (response.ok || !retryableStatus || attempt === attempts - 1) return response
      lastError = new Error(`Request failed (${response.status})`)
    } catch (error) {
      lastError = error
      const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase()
      const retryable = message.includes("timeout") || message.includes("timed out") || message.includes("aborted") || message.includes("fetch failed") || message.includes("connect") || message.includes("socket")
      if (!retryable || attempt === attempts - 1) throw error
    }
    await new Promise((resolve) => setTimeout(resolve, 750 * 2 ** attempt))
  }
  throw lastError instanceof Error ? lastError : new Error("Request failed")
}

async function getManifest(): Promise<Manifest> {
  const sourceRepository = String(process.env.GITHUB_WORKER_SOURCE_REPO || "iamnadith/Drive").trim()
  const defaultUrl = `https://github.com/${sourceRepository}/releases/latest/download/manifest.json`
  const url = String(process.env.CLOUDFLARE_WORKER_MANIFEST_URL || defaultUrl).trim()
  if (!/^https:\/\//i.test(url)) throw new Error("CLOUDFLARE_WORKER_MANIFEST_URL is not configured")
  // GitHub may resolve a just-published "latest" release before its manifest
  // is visible at every edge. Retry 404s briefly instead of failing setup.
  const response = await fetchWithRetry(url, { cache: "no-store", signal: AbortSignal.timeout(20_000) }, 6, true)
  if (!response.ok) throw new Error(`Unable to fetch Worker release manifest (${response.status})`)
  const manifest = await response.json() as Manifest
  if (!manifest.version || !ORDER.every((worker) => manifest.workers?.[worker]?.url && /^[a-f0-9]{64}$/i.test(manifest.workers[worker].sha256))) throw new Error("Worker release manifest is invalid")
  return manifest
}

async function artifact(entry: Artifact): Promise<Uint8Array> {
  // Release assets can lag the manifest briefly during GitHub publication or
  // CDN propagation. A 404 is retryable here; checksum validation still makes
  // the accepted bytes immutable and exact.
  const response = await fetchWithRetry(entry.url, { cache: "no-store", signal: AbortSignal.timeout(30_000) }, 6, true)
  if (!response.ok) throw new Error(`Unable to fetch Worker artifact (${response.status})`)
  const bytes = new Uint8Array(await response.arrayBuffer())
  const digest = createHash("sha256").update(bytes).digest("hex")
  if (digest !== entry.sha256.toLowerCase()) throw new Error("Worker artifact checksum mismatch")
  return bytes
}

async function ensureQueue(token: string, accountId: string, name: string) {
  const queues = await cf<Array<{ queue_id: string; queue_name: string }>>(token, `/accounts/${accountId}/queues?per_page=100`)
  const existing = queues.find((queue) => queue.queue_name === name)
  return existing || cf<{ queue_id: string; queue_name: string }>(token, `/accounts/${accountId}/queues`, { method: "POST", body: JSON.stringify({ queue_name: name }) })
}

async function removeWorkerQueueConsumer(token: string, accountId: string, worker: HostedWorker, scriptName: string) {
  if (worker === "backend") return
  // Find consumers by the actual script binding, not by the currently
  // configured queue name. The installation suffix can change if APP_URL
  // changes, and Cloudflare refuses script deletion while any Queue points at
  // it. Queue listings are paginated, so inspect every page.
  const queues: Array<{ queue_id: string; queue_name: string }> = []
  for (let page = 1; ; page += 1) {
    const pageQueues = await cf<Array<{ queue_id: string; queue_name: string }>>(token, `/accounts/${accountId}/queues?per_page=100&page=${page}`)
    queues.push(...pageQueues)
    if (pageQueues.length < 100) break
  }

  for (const queue of queues) {
    const consumers = queueConsumers(await cf<QueueConsumer[] | { consumers?: QueueConsumer[] }>(token, `/accounts/${accountId}/queues/${queue.queue_id}/consumers`))
    for (const consumer of consumers.filter((candidate) => candidate.script_name === scriptName)) {
      if (!consumer.consumer_id) throw new Error(`Queue ${queue.queue_name} has a consumer for ${scriptName} without an identifier`)
      await cf(token, `/accounts/${accountId}/queues/${queue.queue_id}/consumers/${consumer.consumer_id}`, { method: "DELETE" })
    }
  }

  const stillAttached: string[] = []
  for (const queue of queues) {
    const consumers = queueConsumers(await cf<QueueConsumer[] | { consumers?: QueueConsumer[] }>(token, `/accounts/${accountId}/queues/${queue.queue_id}/consumers`))
    if (consumers.some((consumer) => consumer.script_name === scriptName)) stillAttached.push(queue.queue_name)
  }
  if (stillAttached.length) throw new Error(`Cannot delete ${scriptName}; it remains attached to Queue consumer(s): ${stillAttached.join(", ")}`)
}

async function ensureHyperdrive(token: string, accountId: string, state: InstallState) {
  const postgresUrl = String(process.env.POSTGRES_URL || "").trim()
  let parsed: URL
  try { parsed = new URL(postgresUrl) } catch { throw new Error("POSTGRES_URL is not a valid PostgreSQL connection URL") }
  if (!["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || !parsed.pathname.slice(1)) throw new Error("POSTGRES_URL must include a PostgreSQL host and database")
  const origin = {
    database: decodeURIComponent(parsed.pathname.slice(1)),
    host: parsed.hostname,
    password: decodeURIComponent(parsed.password),
    port: Number(parsed.port || 5432),
    scheme: "postgres",
    user: decodeURIComponent(parsed.username),
  }
  if (!origin.user || !origin.password || !Number.isInteger(origin.port)) throw new Error("POSTGRES_URL must include valid database credentials")
  const fingerprint = createHmac("sha256", primaryEncryptionKey()).update(JSON.stringify(origin)).digest("hex")
  const name = resourceNames().hyperdrive
  const saved = state.hyperdrives?.[accountId]
  const configs: Array<{ id: string; name: string; origin_connection_limit?: number; origin?: { host?: string; database?: string; user?: string; port?: number; scheme?: string } }> = []
  for (let page = 1; page <= 100; page += 1) {
    const batch = await cf<Array<{ id: string; name: string; origin_connection_limit?: number; origin?: { host?: string; database?: string; user?: string; port?: number; scheme?: string } }>>(token, `/accounts/${accountId}/hyperdrive/configs?per_page=100&page=${page}`)
    configs.push(...batch)
    if (batch.length < 100) break
  }
  const existing = configs.find((config) => config.name === name) || configs.find((config) => config.id === saved?.id)
  let config: { id: string; name: string }
  if (existing) {
    config = existing
    const originMatches = existing.origin?.host === origin.host
      && existing.origin.database === origin.database
      && existing.origin.user === origin.user
      && Number(existing.origin.port || 5432) === origin.port
      && ["postgres", "postgresql"].includes(String(existing.origin.scheme || "postgres"))
    const needsUpdate = saved?.id !== existing.id || saved.originFingerprint !== fingerprint || !originMatches || existing.origin_connection_limit !== 5
    if (needsUpdate) {
      await cf(token, `/accounts/${accountId}/hyperdrive/configs/${existing.id}`, {
        method: "PATCH",
        body: JSON.stringify({ name, origin, origin_connection_limit: 5, caching: { disabled: true }, mtls: { sslmode: "require" } }),
      })
      await cf(token, `/accounts/${accountId}/hyperdrive/configs/${existing.id}/restart`, { method: "POST" })
    }
  } else {
    config = await cf<{ id: string; name: string }>(token, `/accounts/${accountId}/hyperdrive/configs`, {
      method: "POST",
      body: JSON.stringify({ name, origin, origin_connection_limit: 5, caching: { disabled: true }, mtls: { sslmode: "require" } }),
    })
  }
  if (!config?.id) throw new Error("Cloudflare did not return a Hyperdrive configuration ID")
  state.hyperdrives = { ...(state.hyperdrives || {}), [accountId]: { id: config.id, name, accountId, originFingerprint: fingerprint } }
  return config.id
}

async function uploadWorker(input: { worker: HostedWorker; token: string; accountId: string; hyperdriveId: string; entry: Artifact; code: Uint8Array; state: InstallState }) {
  const { worker, token, accountId, entry, code, state } = input
  const publicPanelUrl = panelUrl()
  const postgresUrl = String(process.env.POSTGRES_URL || "").trim()
  if (!/^https:\/\//i.test(publicPanelUrl) || !postgresUrl || !input.hyperdriveId) throw new Error("Panel URL, PostgreSQL URL, or Hyperdrive binding is not configured")
  const postgresSsl = String(process.env.POSTGRES_SSL || "").trim().toLowerCase()
  let sslMode = ""
  try { sslMode = new URL(postgresUrl).searchParams.get("sslmode")?.trim().toLowerCase() || "" } catch { /* POSTGRES_URL validation is handled by the database connection. */ }
  const disablePostgresSsl = postgresSsl === "0" || postgresSsl === "false" || ["1", "true"].includes(String(process.env.DISABLE_POSTGRES_SSL || "").trim().toLowerCase()) || sslMode === "disable"
  const bindings: Array<Record<string, unknown>> = [
    { type: "hyperdrive", name: "HYPERDRIVE", id: input.hyperdriveId },
    { type: "secret_text", name: worker === "backend" ? "BACKEND_ORCHESTRATOR_SECRET" : worker === "scanner" ? "FILE_SCANNER_SECRET" : "MIGRATION_ORCHESTRATOR_SECRET", text: state.secrets[worker] },
    { type: "plain_text", name: "PANEL_URL", text: publicPanelUrl },
    { type: "plain_text", name: "DISABLE_POSTGRES_SSL", text: disablePostgresSsl ? "1" : "0" },
    { type: "plain_text", name: "GITHUB_WORKER_SOURCE_REPO", text: process.env.GITHUB_WORKER_SOURCE_REPO?.trim() || "iamnadith/Drive" },
  ]
  const names = resourceNames()
  if (worker === "scanner") bindings.push({ type: "queue", name: "FILE_SCAN_QUEUE", queue_name: names.scannerQueue })
  if (worker === "migration") bindings.push({ type: "queue", name: "GITHUB_DISPATCH_QUEUE", queue_name: names.migrationQueue })
  const form = new FormData()
  form.set("metadata", JSON.stringify({ main_module: "index.js", compatibility_date: entry.compatibilityDate, compatibility_flags: entry.compatibilityFlags || ["nodejs_compat_v2"], bindings, observability: { enabled: true } }))
  const uploadBytes = code.buffer.slice(code.byteOffset, code.byteOffset + code.byteLength) as ArrayBuffer
  form.set("index.js", new Blob([uploadBytes], { type: "application/javascript+module" }), "index.js")
  const scriptName = state.workers[worker].scriptName
  await cf(token, `/accounts/${accountId}/workers/scripts/${scriptName}`, { method: "PUT", body: form, headers: {} })
  await cf(token, `/accounts/${accountId}/workers/scripts/${scriptName}/subdomain`, { method: "POST", body: JSON.stringify({ enabled: true, previews_enabled: false }) })
}

async function workerSchedules(token: string, accountId: string, scriptName: string) {
  const result = await cf<{ schedules?: Array<{ cron?: string }> } | Array<{ cron?: string }>>(token, `/accounts/${accountId}/workers/scripts/${scriptName}/schedules`)
  return Array.isArray(result) ? result : Array.isArray(result?.schedules) ? result.schedules : []
}

async function replaceWorkerSchedules(token: string, accountId: string, scriptName: string, schedules: Array<{ cron?: string }>) {
  await cf(token, `/accounts/${accountId}/workers/scripts/${scriptName}/schedules`, {
    method: "PUT",
    body: JSON.stringify(schedules.filter((schedule): schedule is { cron: string } => typeof schedule.cron === "string").map(({ cron }) => ({ cron }))),
  })
}

function isCronTriggerLimitError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error)
  return /workers free limit of 5 cron triggers per account|cron triggers?.{0,80}(?:limit|quota)|(?:limit|quota).{0,80}cron triggers?/i.test(message)
}

async function ensureSchedule(token: string, accountId: string, scriptName: string, replacedScriptName?: string) {
  const schedules = await workerSchedules(token, accountId, scriptName)
  const hasRequiredSchedule = schedules.some((schedule) => schedule.cron === "* * * * *")
  const previousScript = replacedScriptName && replacedScriptName !== scriptName ? replacedScriptName : ""
  const previousExists = previousScript ? await scriptExists(token, accountId, previousScript) : false
  const previousSchedules = previousExists ? await workerSchedules(token, accountId, previousScript) : []
  const previousRequired = previousSchedules.some((schedule) => schedule.cron === "* * * * *")

  if (hasRequiredSchedule) {
    if (previousRequired) await replaceWorkerSchedules(token, accountId, previousScript, previousSchedules.filter((schedule) => schedule.cron !== "* * * * *"))
    return
  }

  try {
    await replaceWorkerSchedules(token, accountId, scriptName, [...schedules, { cron: "* * * * *" }])
  } catch (error) {
    // A fresh installation may use a new script suffix while the prior
    // Drive-owned Worker still holds the account's cron slot. Move just this
    // install's minutely trigger, then restore it if the replacement fails.
    if (!isCronTriggerLimitError(error)) throw error
    if (!previousRequired) {
      throw new Error("Cloudflare account cron capacity is full (Workers Free allows five triggers per account). Drive needs one trigger for each scheduled Worker. Free an unused cron slot or select an account with capacity; unrelated triggers were left untouched.")
    }
    await replaceWorkerSchedules(token, accountId, previousScript, previousSchedules.filter((schedule) => schedule.cron !== "* * * * *"))
    try {
      await replaceWorkerSchedules(token, accountId, scriptName, [...schedules, { cron: "* * * * *" }])
    } catch (replacementError) {
      await replaceWorkerSchedules(token, accountId, previousScript, previousSchedules).catch(() => undefined)
      throw replacementError
    }
    return
  }

  if (previousRequired) await replaceWorkerSchedules(token, accountId, previousScript, previousSchedules.filter((schedule) => schedule.cron !== "* * * * *"))
}

type QueueConsumer = { consumer_id?: string; script_name?: string }

function queueConsumers(result: QueueConsumer[] | { consumers?: QueueConsumer[] }) {
  if (Array.isArray(result)) return result
  return Array.isArray(result?.consumers) ? result.consumers : []
}

async function configureConsumer(token: string, accountId: string, queueName: string, dlqName: string, scriptName: string, retryDelay: number) {
  const queue = await ensureQueue(token, accountId, queueName)
  await ensureQueue(token, accountId, dlqName)
  const consumers = queueConsumers(await cf<QueueConsumer[] | { consumers?: QueueConsumer[] }>(token, `/accounts/${accountId}/queues/${queue.queue_id}/consumers`))
  const body = JSON.stringify({ type: "worker", script_name: scriptName, dead_letter_queue: dlqName, settings: { batch_size: 1, max_wait_time_ms: 1000, max_retries: 20, retry_delay: retryDelay } })
  // Cloudflare permits one push consumer for this Queue. Repoint an existing
  // consumer to the expected script instead of attempting a duplicate POST.
  const existing = consumers.find((consumer) => consumer.script_name === scriptName) || consumers[0]
  if (existing && !existing.consumer_id) throw new Error(`Existing consumer for ${queueName} has no identifier`)
  await cf(token, existing
    ? `/accounts/${accountId}/queues/${queue.queue_id}/consumers/${existing.consumer_id}`
    : `/accounts/${accountId}/queues/${queue.queue_id}/consumers`, { method: existing ? "PUT" : "POST", body })
}

async function ensureConsumer(token: string, accountId: string, queueName: string, dlqName: string, scriptName: string, retryDelay: number) {
  const queue = await ensureQueue(token, accountId, queueName)
  await ensureQueue(token, accountId, dlqName)
  const consumers = queueConsumers(await cf<QueueConsumer[] | { consumers?: QueueConsumer[] }>(token, `/accounts/${accountId}/queues/${queue.queue_id}/consumers`))
  if (!consumers.some((consumer) => consumer.script_name === scriptName)) await configureConsumer(token, accountId, queueName, dlqName, scriptName, retryDelay)
}

async function workersDevUrl(token: string, accountId: string, scriptName: string) {
  const accountSubdomain = await cf<{ subdomain: string }>(token, `/accounts/${accountId}/workers/subdomain`)
  return `https://${scriptName}.${accountSubdomain.subdomain}.workers.dev`
}

async function verify(url: string, secret: string) {
  let lastError = "Worker did not become ready"
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      const health = await fetch(`${url}/health`, { cache: "no-store", signal: AbortSignal.timeout(10_000) })
      if (!health.ok) throw new Error(`Health verification failed (${health.status})`)
      const status = await fetch(`${url}/status`, { cache: "no-store", headers: { Authorization: `Bearer ${secret}` }, signal: AbortSignal.timeout(15_000) })
      const payload = await status.json().catch(() => null)
      if (!status.ok) {
        const detail = payload && typeof payload === "object" && typeof (payload as { error?: unknown }).error === "string"
          ? `: ${(payload as { error: string }).error}`
          : ""
        throw new Error(`Authenticated verification failed (${status.status})${detail}`)
      }
      const healthError = runtimeHealthError(payload)
      if (healthError) throw new Error(`Runtime verification failed: ${healthError}`)
      return
    } catch (error) {
      lastError = error instanceof Error ? error.message : lastError
      if (attempt < 4) await new Promise((resolve) => setTimeout(resolve, 750 * 2 ** attempt))
    }
  }
  throw new Error(lastError)
}

async function scriptExists(token: string, accountId: string, scriptName: string) {
  const response = await fetch(`${API}/accounts/${accountId}/workers/scripts/${scriptName}/settings`, {
    headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(15_000), cache: "no-store",
  })
  if (response.status === 404) return false
  const payload = await response.json().catch(() => ({})) as { success?: boolean; errors?: Array<{ message?: string }> }
  if (!response.ok || payload.success === false) throw new Error(payload.errors?.[0]?.message || `Unable to inspect ${scriptName} (${response.status})`)
  return true
}

async function deleteWorkerScript(token: string, accountId: string, scriptName: string) {
  try { await cf(token, `/accounts/${accountId}/workers/scripts/${scriptName}`, { method: "DELETE" }) }
  catch (error) { if (!(error instanceof Error && /\(404\)$/.test(error.message))) throw error }
}

async function inspectWorkerOnce(url: string, secret: string) {
  const started = Date.now()
  const [health, status] = await Promise.all([
    fetch(`${url}/health`, { cache: "no-store", signal: AbortSignal.timeout(10_000) }),
    fetch(`${url}/status`, { cache: "no-store", headers: { Authorization: `Bearer ${secret}` }, signal: AbortSignal.timeout(10_000) }),
  ])
  if (!health.ok) throw new Error(`Health check failed (${health.status})`)
  if (!status.ok) throw new Error(`Authenticated status check failed (${status.status})`)
  const payload = await status.json().catch(() => null) as { build?: string | number } | null
  const healthError = runtimeHealthError(payload)
  if (healthError) throw new Error(`Runtime verification failed: ${healthError}`)
  return { latencyMs: Date.now() - started, build: payload?.build }
}

async function inspectWorker(url: string, secret: string) {
  try { return await inspectWorkerOnce(url, secret) }
  catch (firstError) {
    await new Promise((resolve) => setTimeout(resolve, 750))
    try { return await inspectWorkerOnce(url, secret) }
    catch { throw firstError }
  }
}

export async function reconcileCloudflareWorker(worker: HostedWorker, force = false) {
  return withDbAdvisoryLock("cloudflare-worker-install", "singleton", async () => {
    const state = await loadState()
    if (!state || !state.encryptedTokens) return getCloudflareInstallation()
    // A running or never-finished installation must be resumed by the installer.
    // Reconciling it cannot work without URLs and used to erase the actionable
    // deployment error with a generic "metadata is incomplete" message.
    if (state.status === "running" || (state.status === "failed" && !ORDER.some((key) => Boolean(state.workers[key].url)))) return getCloudflareInstallation()
    if (!force && state.workers[worker].lastCheckedAt && Date.now() - new Date(state.workers[worker].lastCheckedAt).getTime() < 60_000) return getCloudflareInstallation()

    const current = state.workers[worker]
    current.phase = "verifying"
    current.error = undefined
    state.step = `${worker}_verifying`
    await saveState(state)
    let scriptPresent = false
    try {
      if (!current.accountId || !current.url || !state.encryptedTokens[worker]) throw new Error("Deployment metadata is incomplete")
      const token = decryptToken(state.encryptedTokens[worker]!, state.id, worker)
      scriptPresent = await scriptExists(token, current.accountId, current.scriptName)
      if (!scriptPresent) throw new Error("Worker script was not found in Cloudflare")
      const inspected = await inspectWorker(current.url, state.secrets[worker])
      const names = resourceNames()
      if (worker === "scanner") await ensureConsumer(token, current.accountId, names.scannerQueue, names.scannerDlq, current.scriptName, 15)
      if (worker === "migration") await ensureConsumer(token, current.accountId, names.migrationQueue, names.migrationDlq, current.scriptName, 30)
      await ensureSchedule(token, current.accountId, current.scriptName)
      current.deployed = true; current.verified = true; current.phase = "verified"; current.error = undefined
      current.lastCheckedAt = new Date().toISOString(); current.latencyMs = inspected.latencyMs; current.build = inspected.build
    } catch (error) {
      current.deployed = scriptPresent; current.verified = false; current.phase = "failed"
      current.error = error instanceof Error ? error.message : "Worker reconciliation failed"; current.lastCheckedAt = new Date().toISOString(); current.latencyMs = undefined; current.build = undefined
    }
    state.lastReconciledAt = new Date().toISOString()
    const failed = ORDER.filter((key) => !state.workers[key].verified)
    if (failed.length) { state.status = "failed"; state.step = "reconciliation_failed"; state.error = `${failed.length} Worker${failed.length === 1 ? "" : "s"} require repair` }
    else { state.status = "ready"; state.step = "reconciled"; state.error = undefined }
    await saveState(state)
    return getCloudflareInstallation()
  }, { wait: false })
}

export async function reconcileCloudflareWorkers(force = false) {
  for (const worker of ORDER) await reconcileCloudflareWorker(worker, force)
  return getCloudflareInstallation()
}

export async function deleteCloudflareWorkers() {
  return withDbAdvisoryLock("cloudflare-worker-install", "singleton", async () => {
    const state = await loadState()
    if (!state) return null
    state.status = "running"; state.step = "deleting_workers"; state.error = undefined
    await saveState(state)
    try {
      for (const worker of ORDER) {
        const current = state.workers[worker]
        current.phase = "deleting"; state.step = `${worker}_deleting`; await saveState(state)
        if (current.accountId && state.encryptedTokens?.[worker]) {
          const token = decryptToken(state.encryptedTokens[worker]!, state.id, worker)
          await removeWorkerQueueConsumer(token, current.accountId, worker, current.scriptName)
          await deleteWorkerScript(token, current.accountId, current.scriptName)
        }
        state.workers[worker] = { scriptName: current.scriptName, phase: "queued" }
        await saveState(state)
      }
      await setCloudflareHostingMode("automatic")
      await queryDb(`delete from public.drive_cloudflare_worker_installations where id=$1`, [state.id])
      return null
    } catch (error) {
      state.status = "failed"; state.step = "deletion_failed"; state.error = error instanceof Error ? error.message : "Worker deletion failed"
      await saveState(state)
      throw error
    }
  }, { wait: false })
}

export async function reconcileAndRepairCloudflareWorkers(force = false) {
  const installation = await reconcileCloudflareWorkers(force)
  if (!installation || !installation.tokensSaved) return installation
  const abandonedRunning = installation.status === "running" && Date.now() - new Date(installation.updatedAt).getTime() >= 60_000
  if (installation.status !== "failed" && !abandonedRunning) return installation
  return installCloudflareWorkers({ mode: installation.mode, tokens: {} })
}

export async function getCloudflareInstallation() {
  const loaded = await loadState()
  const state = loaded ? await rotateSavedTokenEncryption(loaded) : null
  if (!state) return null
  const { encryptedTokens, ...safe } = state
  const tokensSaved = ORDER.every((worker) => Boolean(encryptedTokens?.[worker]))
  const workers = Object.fromEntries(ORDER.map((worker) => {
    const current = safe.workers[worker]
    return [worker, tokensSaved ? current : { scriptName: current.scriptName, phase: "queued" as const }]
  })) as Record<HostedWorker, WorkerState>
  return { ...safe, workers, secrets: { backend: "", scanner: "", migration: "" }, tokensSaved }
}

export function cloudflareInstallationReady(installation: Awaited<ReturnType<typeof getCloudflareInstallation>>) {
  if (!installation || installation.status !== "ready" || installation.tokensSaved !== true) return false
  return ORDER.every((worker) => {
    const current = installation.workers[worker]
    const checkedAt = current.lastCheckedAt ? new Date(current.lastCheckedAt).getTime() : 0
    return current.deployed === true && current.verified === true && Boolean(current.url && current.deployedAt && current.verifiedAt) && Number.isFinite(checkedAt) && checkedAt > 0
  })
}

export async function revealCloudflareTokens() {
  const state = await loadState()
  if (!state?.encryptedTokens || !ORDER.every((worker) => state.encryptedTokens?.[worker])) throw new Error("No saved Cloudflare token is available; enter it once and deploy")
  const tokens = Object.fromEntries(ORDER.map((worker) => [worker, decryptToken(state.encryptedTokens![worker]!, state.id, worker)])) as TokenMap
  return state.mode === "single" ? { mode: state.mode, token: tokens.backend } : { mode: state.mode, backendToken: tokens.backend, scannerToken: tokens.scanner, migrationToken: tokens.migration }
}

export async function replaceCloudflareTokens(input: { mode: InstallMode; tokens: Partial<TokenMap> }) {
  return withDbAdvisoryLock("cloudflare-worker-install", "singleton", async () => {
    const state = await loadState()
    if (!state || state.status !== "ready") throw new Error("A verified installation is required before replacing its tokens")
    const shared = String(input.tokens.backend || "").trim()
    const tokens: TokenMap = input.mode === "single" ? { backend: shared, scanner: shared, migration: shared } : {
      backend: shared, scanner: String(input.tokens.scanner || "").trim(), migration: String(input.tokens.migration || "").trim(),
    }
    if (ORDER.some((worker) => tokens[worker].length < 20)) throw new Error("Enter every replacement token")
    for (const worker of ORDER) {
      const account = await resolveAccount(tokens[worker])
      if (account.id !== state.workers[worker].accountId) throw new Error(`${worker} replacement token does not belong to its deployed account`)
    }
    state.mode = input.mode
    state.encryptedTokens = Object.fromEntries(ORDER.map((worker) => [worker, encryptToken(tokens[worker], state.id, worker)]))
    await saveState(state)
    return getCloudflareInstallation()
  }, { wait: false })
}

export async function installCloudflareWorkers(input: { mode: InstallMode; tokens: Partial<TokenMap>; restart?: boolean; checkForUpdates?: boolean; forceRedeploy?: boolean }) {
  return withDbAdvisoryLock("cloudflare-worker-install", "singleton", async () => {
    const previous = await loadState()
    const previousRuntime = await currentRuntimeSnapshot()
    let githubSecretsSynchronized = false
    let workerSharedSecretForSync = ""
    const hasSuppliedToken = ORDER.some((worker) => Boolean(String(input.tokens[worker] || "").trim()))
    if (previous?.status === "ready" && !input.restart && !input.checkForUpdates && !input.forceRedeploy && !hasSuppliedToken) return getCloudflareInstallation()
    const supplied = input.mode === "single"
      ? { backend: String(input.tokens.backend || "").trim(), scanner: String(input.tokens.backend || "").trim(), migration: String(input.tokens.backend || "").trim() }
      : { backend: String(input.tokens.backend || "").trim(), scanner: String(input.tokens.scanner || "").trim(), migration: String(input.tokens.migration || "").trim() }
    const tokens = {} as TokenMap
    for (const worker of ORDER) tokens[worker] = supplied[worker] || (previous?.mode === input.mode && previous.encryptedTokens?.[worker] ? decryptToken(previous.encryptedTokens[worker]!, previous.id, worker) : "")
    if (ORDER.some((worker) => tokens[worker].length < 20)) throw new Error("Enter every Cloudflare token once; saved tokens can then be reused")
    let state = input.restart ? null : previous
    if (!state || (state.status === "ready" && !input.checkForUpdates) || state.mode !== input.mode) {
      state = freshState(input.mode)
      const configuredSecrets = {
        backend: previousRuntime.backend.sharedSecret,
        scanner: previousRuntime.migration.fileScannerSecret,
        migration: previousRuntime.migration.sharedSecret,
      }
      const previousSecrets = previous?.secrets
      for (const worker of ORDER) {
        const previousSecret = previousSecrets?.[worker] || ""
        state.secrets[worker] = configuredSecrets[worker].length >= 24
          ? configuredSecrets[worker]
          : previousSecret.length >= 24
            ? previousSecret
            : state.secrets[worker]
      }
    }
    state.encryptedTokens = Object.fromEntries(ORDER.map((worker) => [worker, encryptToken(tokens[worker], state!.id, worker)]))
    state.status = "running"; state.error = undefined; await saveState(state)
    try {
      const names = resourceNames()
      const accounts = {} as Record<HostedWorker, Account>
      for (const worker of ORDER) {
        accounts[worker] = await resolveAccount(tokens[worker])
        if (state.workers[worker].accountId && state.workers[worker].accountId !== accounts[worker].id) {
          throw new Error(`${worker} token belongs to a different account than this resumable installation; choose Start fresh`)
        }
        state.workers[worker].accountId = accounts[worker].id
        state.workers[worker].accountName = accounts[worker].name
      }
      state.step = "accounts_validated"; await saveState(state)
      const accountTokens = new Map<string, string>()
      for (const worker of ORDER) if (!accountTokens.has(accounts[worker].id)) accountTokens.set(accounts[worker].id, tokens[worker])
      for (const [accountId, token] of accountTokens) await ensureHyperdrive(token, accountId, state)
      state.step = "hyperdrive_ready"; await saveState(state)
      await adoptExistingWorkers(state, tokens, accounts); state.step = "existing_workers_checked"; await saveState(state)
      const manifest = await getManifest(); state.releaseVersion = manifest.version
      await saveState(state)
      // Download and checksum the complete immutable release before touching
      // any Worker. A partially published release must never leave a healthy
      // Worker marked undeployed just because a later bundle is unavailable.
      state.step = "release_artifacts_fetching"; await saveState(state)
      const releaseArtifacts = Object.fromEntries(
        await Promise.all(ORDER.map(async (worker) => [worker, await artifact(manifest.workers[worker])]))
      ) as Record<HostedWorker, Uint8Array>
      await ensureQueue(tokens.scanner, accounts.scanner.id, names.scannerQueue)
      await ensureQueue(tokens.scanner, accounts.scanner.id, names.scannerDlq)
      await ensureQueue(tokens.migration, accounts.migration.id, names.migrationQueue)
      await ensureQueue(tokens.migration, accounts.migration.id, names.migrationDlq)
      state.step = "queues_ready"; await saveState(state)
      for (const worker of ORDER) {
        const current = state.workers[worker]
        const artifactEntry = manifest.workers[worker]
        const artifactMatches = current.releaseVersion === manifest.version && current.artifactSha256 === artifactEntry.sha256.toLowerCase()
        // A failed runtime verification can mean the Worker bindings drifted
        // from the dashboard's authoritative settings even when the script
        // checksum is unchanged. Repair must re-upload the script so its
        // generated secret, database URL, SSL flag, and panel URL are synced.
        if (input.forceRedeploy || !current.deployed || !current.verified || !artifactMatches) {
          current.deployed = false; current.verified = false; current.verifiedAt = undefined; current.latencyMs = undefined; current.build = undefined
          state.workers[worker].phase = "uploading"; state.workers[worker].error = undefined; state.step = `${worker}_uploading`; await saveState(state)
          await uploadWorker({ worker, token: tokens[worker], accountId: accounts[worker].id, hyperdriveId: state.hyperdrives![accounts[worker].id].id, entry: artifactEntry, code: releaseArtifacts[worker], state })
          state.workers[worker].phase = "configuring"; state.step = `${worker}_configuring`; await saveState(state)
          state.workers[worker].url = await workersDevUrl(tokens[worker], accounts[worker].id, state.workers[worker].scriptName)
          state.workers[worker].deployed = true; state.workers[worker].phase = "deployed"; state.workers[worker].deployedAt = new Date().toISOString()
          state.workers[worker].releaseVersion = manifest.version; state.workers[worker].artifactSha256 = artifactEntry.sha256.toLowerCase()
          state.step = `${worker}_deployed`; await saveState(state)
        }
      }
      await configureConsumer(tokens.scanner, accounts.scanner.id, names.scannerQueue, names.scannerDlq, state.workers.scanner.scriptName, 15)
      await configureConsumer(tokens.migration, accounts.migration.id, names.migrationQueue, names.migrationDlq, state.workers.migration.scriptName, 30)
      state.step = "consumers_ready"; await saveState(state)
      await saveRuntimeConfiguration(state, false)
      state.step = "configuration_saved"; await saveState(state)
      for (const worker of ORDER) {
        state.workers[worker].phase = "verifying"; state.step = `${worker}_verifying`; await saveState(state)
        await verify(state.workers[worker].url!, state.secrets[worker])
        const inspected = await inspectWorker(state.workers[worker].url!, state.secrets[worker])
        const checkedAt = new Date().toISOString()
        state.workers[worker].verified = true; state.workers[worker].phase = "verified"; state.workers[worker].verifiedAt = checkedAt; state.workers[worker].lastCheckedAt = checkedAt; state.workers[worker].latencyMs = inspected.latencyMs; state.workers[worker].build = inspected.build; await saveState(state)
      }
      state.step = "schedules_configuring"; await saveState(state)
      for (const worker of ORDER) {
        const previousWorker = previous?.workers[worker]
        const replacedScriptName = previousWorker?.accountId === accounts[worker].id ? previousWorker.scriptName : undefined
        await ensureSchedule(tokens[worker], accounts[worker].id, state.workers[worker].scriptName, replacedScriptName)
      }
      state.step = "schedules_ready"; await saveState(state)
      const workerSettings = await getMigrationWorkerSettings()
      workerSharedSecretForSync = workerSettings.sharedSecret
      const migrationUrlChanged = previousRuntime.migration.orchestratorUrl !== state.workers.migration.url
      const synchronizedConfigurationMatches = workerSettings.synchronizedServerUrl === state.workers.migration.url
        && workerSettings.synchronizedSecretHash === createHash("sha256").update(workerSettings.sharedSecret).digest("hex")
      if (migrationUrlChanged || workerSettings.secretSyncStatus !== "ready" || !synchronizedConfigurationMatches) {
        state.step = "github_worker_secrets_syncing"; await saveState(state)
        await syncAllGitHubWorkerSecretsWithinInstallerLock({
          serverUrl: state.workers.migration.url || "",
          sharedSecret: workerSettings.sharedSecret,
        })
        githubSecretsSynchronized = true
        state.step = "github_worker_secrets_ready"; await saveState(state)
      }
      await saveRuntimeConfiguration(state, true)
      await queryDb(`insert into drive_app_settings(key,value,updated_at) values('cloudflare-worker-hosting',$1::jsonb,now()) on conflict(key) do update set value=excluded.value,updated_at=now()`, [JSON.stringify({ mode: "automatic" })])
      state.status = "ready"; state.step = "enabled"; await saveState(state)
      return getCloudflareInstallation()
    } catch (error) {
      // Restore the runtime configuration that existed before this attempt,
      // including when a prior deployment already has a failed install record.
      // Verification may fail after saveRuntimeConfiguration(false), but that
      // must not leave unrelated running migrations switched off indefinitely.
      let runtimeRestoreError: string | null = null
      if (githubSecretsSynchronized && previousRuntime.migration.orchestratorUrl) {
        await syncAllGitHubWorkerSecretsWithinInstallerLock({
          serverUrl: previousRuntime.migration.orchestratorUrl,
          sharedSecret: workerSharedSecretForSync,
        }).catch(() => undefined)
      }
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          await writeRuntimeSnapshot(previousRuntime)
          runtimeRestoreError = null
          break
        } catch (restoreError) {
          runtimeRestoreError = restoreError instanceof Error ? restoreError.message : String(restoreError)
          if (attempt < 2) await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** attempt))
        }
      }
      state.status = "failed"; state.error = `${state.step}: ${error instanceof Error ? error.message : "Installation failed"}`
      if (runtimeRestoreError) state.error += `; previous runtime settings could not be restored: ${runtimeRestoreError}`
      const active = ORDER.find((worker) => state.step.startsWith(`${worker}_`) && !state.workers[worker].verified)
      if (active) { state.workers[active].phase = "failed"; state.workers[active].error = state.error }
      await saveState(state); throw error
    }
  }, { wait: false })
}
