import { ensureDriveSchema, queryDb } from "./db"

const SETTINGS_KEY = "migration-workers"
const MIN_SECRET_LENGTH = 24
const MAX_SECRET_LENGTH = 512

export type MigrationWorkerSettings = {
  sharedSecret: string
  queueDatabaseUrl: string
  queueDatabaseSsl: boolean
  previousSharedSecrets: Array<{ secret: string; expiresAt: string }>
  updatedAt?: string
  secretSyncStatus: "ready" | "syncing" | "failed" | "unverified"
  secretSyncError?: string
  secretSyncAt?: string
  synchronizedServerUrl?: string
  synchronizedSecretHash?: string
}

type SettingsRow = {
  value: unknown
  updated_at: string | null
}

function normalizeSecret(value: unknown): string {
  return typeof value === "string" ? value.trim() : ""
}

function normalize(value: unknown, updatedAt?: string | null): MigrationWorkerSettings {
  const row = value && typeof value === "object" ? (value as Record<string, unknown>) : {}
  const previousSharedSecrets = Array.isArray(row.previousSharedSecrets)
    ? row.previousSharedSecrets.flatMap((entry) => {
        if (!entry || typeof entry !== "object") return []
        const previous = entry as Record<string, unknown>
        const secret = normalizeSecret(previous.secret)
        const expiresAt = typeof previous.expiresAt === "string" ? previous.expiresAt : ""
        return secret && Number.isFinite(Date.parse(expiresAt)) && Date.parse(expiresAt) > Date.now() ? [{ secret, expiresAt }] : []
      }).slice(-20)
    : []
  return {
    sharedSecret: normalizeSecret(row.sharedSecret),
    queueDatabaseUrl: normalizeSecret(row.queueDatabaseUrl),
    queueDatabaseSsl: row.queueDatabaseSsl !== false,
    previousSharedSecrets,
    updatedAt: updatedAt ?? undefined,
    secretSyncStatus: row.secretSyncStatus === "ready" || row.secretSyncStatus === "syncing" || row.secretSyncStatus === "failed"
      ? row.secretSyncStatus
      : "unverified",
    secretSyncError: typeof row.secretSyncError === "string" ? row.secretSyncError : undefined,
    secretSyncAt: typeof row.secretSyncAt === "string" ? row.secretSyncAt : undefined,
    synchronizedServerUrl: typeof row.synchronizedServerUrl === "string" ? row.synchronizedServerUrl : undefined,
    synchronizedSecretHash: typeof row.synchronizedSecretHash === "string" ? row.synchronizedSecretHash : undefined,
  }
}

function envSecret(): string {
  return normalizeSecret(process.env.MIGRATION_WORKER_SHARED_SECRET)
}

export async function getMigrationWorkerSettings(): Promise<MigrationWorkerSettings> {
  await ensureDriveSchema()
  const { rows } = await queryDb<SettingsRow>(
    `select value, updated_at from drive_app_settings where key = $1 limit 1`,
    [SETTINGS_KEY]
  )
  const saved = rows[0] ? normalize(rows[0].value, rows[0].updated_at) : normalize({})
  if (saved.sharedSecret.length >= MIN_SECRET_LENGTH && saved.sharedSecret.length <= MAX_SECRET_LENGTH) return saved
  const fallback = envSecret()
  return fallback.length >= MIN_SECRET_LENGTH && fallback.length <= MAX_SECRET_LENGTH
    ? { ...saved, sharedSecret: fallback }
    : saved
}

export async function getMigrationWorkerSharedSecret(): Promise<string> {
  return (await getMigrationWorkerSettings()).sharedSecret
}

export async function saveMigrationWorkerSettings(input: { sharedSecret?: unknown; queueDatabaseUrl?: unknown; queueDatabaseSsl?: unknown }): Promise<MigrationWorkerSettings> {
  const current = await getMigrationWorkerSettings()
  const requested = normalizeSecret(input.sharedSecret)
  const nextSecret = requested || current.sharedSecret
  if (nextSecret && nextSecret.length < MIN_SECRET_LENGTH) {
    throw new Error(`Migration worker shared secret must be at least ${MIN_SECRET_LENGTH} characters`)
  }
  if (nextSecret.length > MAX_SECRET_LENGTH) {
    throw new Error(`Migration worker shared secret must be at most ${MAX_SECRET_LENGTH} characters`)
  }

  const requestedQueueUrl = typeof input.queueDatabaseUrl === "string" ? input.queueDatabaseUrl.trim() : ""
  const queueDatabaseUrl = requestedQueueUrl || current.queueDatabaseUrl
  const queueDatabaseSsl = typeof input.queueDatabaseSsl === "boolean" ? input.queueDatabaseSsl : current.queueDatabaseSsl
  if (queueDatabaseUrl) {
    let parsed: URL
    try { parsed = new URL(queueDatabaseUrl) } catch { throw new Error("Migration queue database URL must be a valid PostgreSQL connection URL") }
    if (!["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || !parsed.username || !parsed.password) {
      throw new Error("Migration queue database URL must include PostgreSQL scheme, host, username, and password")
    }
    if (queueDatabaseUrl.length > 4096) throw new Error("Migration queue database URL must be at most 4096 characters")
  }

  const previousSharedSecrets = current.previousSharedSecrets.filter((entry) => Date.parse(entry.expiresAt) > Date.now())
  if (current.sharedSecret && current.sharedSecret !== nextSecret) {
    previousSharedSecrets.push({ secret: current.sharedSecret, expiresAt: new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString() })
  }

  const { rows } = await queryDb<SettingsRow>(
    `
      insert into drive_app_settings (key, value, updated_at)
      values ($1, $2::jsonb, now())
      on conflict (key) do update set value = coalesce(drive_app_settings.value,'{}'::jsonb)||excluded.value, updated_at = now()
      returning value, updated_at
    `,
    [SETTINGS_KEY, JSON.stringify({ sharedSecret: nextSecret, previousSharedSecrets, ...(requestedQueueUrl ? { queueDatabaseUrl } : {}), queueDatabaseSsl })]
  )
  return normalize(rows[0]?.value, rows[0]?.updated_at)
}

export async function setMigrationWorkerSecretSyncStatus(
  status: "ready" | "syncing" | "failed",
  error?: string,
  synchronized?: { serverUrl: string; secretHash: string }
) {
  await ensureDriveSchema()
  await queryDb(`
    insert into drive_app_settings(key,value,updated_at)
    values($1::text,jsonb_build_object(
      'secretSyncStatus',$2::text,
      'secretSyncError',$3::text,
      'secretSyncAt',now(),
      'synchronizedServerUrl',$4::text,
      'synchronizedSecretHash',$5::text
    ),now())
    on conflict(key) do update set
      value=coalesce(drive_app_settings.value,'{}'::jsonb)||excluded.value,
      updated_at=now()
  `, [SETTINGS_KEY, status, error?.slice(0, 800) || null, synchronized?.serverUrl || null, synchronized?.secretHash || null])
}

export function publicMigrationWorkerSettings(settings: MigrationWorkerSettings) {
  return {
    sharedSecret: settings.sharedSecret,
    secretConfigured: settings.sharedSecret.length >= MIN_SECRET_LENGTH && settings.sharedSecret.length <= MAX_SECRET_LENGTH,
    queueDatabaseConfigured: Boolean(settings.queueDatabaseUrl),
    queueDatabaseSsl: settings.queueDatabaseSsl,
    updatedAt: settings.updatedAt,
    secretSyncStatus: settings.secretSyncStatus,
    secretSyncError: settings.secretSyncError,
    secretSyncAt: settings.secretSyncAt,
  }
}
