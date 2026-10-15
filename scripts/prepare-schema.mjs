import { readFile } from "node:fs/promises"
import { dirname, resolve } from "node:path"
import { fileURLToPath } from "node:url"

import nextEnv from "@next/env"
import pg from "pg"

const { loadEnvConfig } = nextEnv
const { Client } = pg
const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..")

// Keep local builds consistent with Next.js and allow Vercel's build
// environment to use its configured production variables.
loadEnvConfig(projectRoot, false, true)

function value(name) {
  const value = process.env[name]
  return value && value.trim() ? value.trim() : undefined
}

function valueForBoolean(name) {
  const raw = value(name)
  return raw?.toLowerCase()
}

function booleanValue(name, fallback) {
  const parsed = valueForBoolean(name)
  if (parsed === undefined) return fallback
  if (["1", "true", "yes"].includes(parsed)) return true
  if (["0", "false", "no"].includes(parsed)) return false
  return fallback
}

function sslConfig() {
  const connectionString = value("POSTGRES_URL")
  let urlDisablesSsl = false
  if (connectionString) {
    try {
      urlDisablesSsl = new URL(connectionString).searchParams.get("sslmode")?.toLowerCase() === "disable"
    } catch {
      // Let the PostgreSQL client produce the canonical invalid-URL error.
    }
  }
  if (
    !booleanValue("POSTGRES_SSL", true) ||
    booleanValue("DISABLE_POSTGRES_SSL", false) ||
    urlDisablesSsl
  ) return false
  const rejectUnauthorized = booleanValue(
    "POSTGRES_SSL_REJECT_UNAUTHORIZED",
    false
  )
  const disableHostnameVerification = booleanValue(
    "POSTGRES_SSL_DISABLE_HOSTNAME_VERIFICATION",
    true
  )
  const servername = value("POSTGRES_SSL_SERVERNAME")
  return {
    rejectUnauthorized,
    ...(disableHostnameVerification
      ? { checkServerIdentity: () => undefined }
      : {}),
    ...(servername ? { servername } : {}),
  }
}

function connectionConfig() {
  const url = value("POSTGRES_URL")
  if (url) return { connectionString: url }

  throw new Error(
    "Postgres is not configured. Set POSTGRES_URL in the build environment."
  )
}

function databaseLabel(config) {
  if (config.connectionString) {
    try {
      const parsed = new URL(config.connectionString)
      return parsed.hostname
    } catch {
      return "configured database"
    }
  }
  return "configured database"
}

async function main() {
  const skipForVercelBuild = booleanValue("DRIVE_SKIP_SCHEMA_BUILD", booleanValue("VERCEL", false))
  if (skipForVercelBuild) {
    console.log(
      value("DRIVE_SKIP_SCHEMA_BUILD")
        ? "[db:schema] skipped because DRIVE_SKIP_SCHEMA_BUILD is enabled"
        : "[db:schema] skipped during Vercel build; runtime schema checks handle required updates"
    )
    return
  }

  const schemaPath = resolve(projectRoot, "supabase", "drive_schema.sql")
  const schema = await readFile(schemaPath, "utf8")
  const concurrentIndexes = [
    {
      name: "drive_users_status_role_idx",
      create: "create index concurrently if not exists drive_users_status_role_idx on public.drive_users (status, role)",
    },
    {
      name: "drive_users_name_trgm_idx",
      create: "create index concurrently if not exists drive_users_name_trgm_idx on public.drive_users using gin (name gin_trgm_ops)",
    },
    {
      name: "drive_users_email_trgm_idx",
      create: "create index concurrently if not exists drive_users_email_trgm_idx on public.drive_users using gin (email gin_trgm_ops)",
    },
    {
      name: "drive_users_username_trgm_idx",
      create: "create index concurrently if not exists drive_users_username_trgm_idx on public.drive_users using gin (username gin_trgm_ops)",
    },
    {
      name: "drive_projects_name_trgm_idx",
      create: "create index concurrently if not exists drive_projects_name_trgm_idx on public.drive_projects using gin (lower(name) gin_trgm_ops)",
    },
    {
      name: "drive_projects_project_id_trgm_idx",
      create: "create index concurrently if not exists drive_projects_project_id_trgm_idx on public.drive_projects using gin (lower(project_id) gin_trgm_ops)",
    },
    {
      name: "drive_projects_bucket_name_trgm_idx",
      create: "create index concurrently if not exists drive_projects_bucket_name_trgm_idx on public.drive_projects using gin (lower(coalesce(bucket_name,'')) gin_trgm_ops)",
    },
    {
      name: "drive_agent_runs_job_reference_created_idx",
      create: "create index concurrently if not exists drive_agent_runs_job_reference_created_idx on public.drive_agent_runs (job_reference, created_at desc, id desc)",
    },
    {
      name: "drive_migration_items_progress_time_idx",
      create: "create index concurrently if not exists drive_migration_items_progress_time_idx on public.drive_migration_items (coalesce(last_progress_at, updated_at, created_at))",
    },
    {
      name: "drive_project_api_events_recent_objects_idx",
      create: "create index concurrently if not exists drive_project_api_events_recent_objects_idx on public.drive_project_api_events (project_id, action, occurred_at desc, object_key) where outcome = 'success' and object_key is not null",
    },
  ]
  let transactionalSchema = schema
  for (const index of concurrentIndexes) {
    const pattern = new RegExp(`create index if not exists ${index.name}[^;]*;`, "i")
    if (!pattern.test(transactionalSchema)) throw new Error(`The canonical schema is missing ${index.name}`)
    transactionalSchema = transactionalSchema.replace(pattern, "")
  }
  const config = connectionConfig()

  console.log(`[db:schema] applying ${schemaPath} to ${databaseLabel(config)}`)

  if (booleanValue("DRIVE_SCHEMA_DRY_RUN", false)) {
    console.log("[db:schema] dry run complete; no database was changed")
    return
  }

  const maxAttempts = 4
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const client = new Client({
      ...config,
      ssl: sslConfig(),
      connectionTimeoutMillis: 15_000,
      application_name: "drive-build-schema",
    })
    let connected = false
    let locked = false
    try {
      await client.connect()
      connected = true
      // Serialize panel schema installers. Runtime workers may still perform
      // idempotent schema guards, so retry transaction-level deadlocks below.
      const lockResult = await client.query("select pg_try_advisory_lock(hashtext($1)) acquired", ["drive-schema-install-v1"])
      locked = lockResult.rows[0]?.acquired === true
      if (!locked) {
        console.warn("[db:schema] another deployment is already preparing this schema; continuing without a duplicate installer")
        return
      }
      await client.query("begin")
      await client.query("set local lock_timeout = '60s'")
      await client.query("set local statement_timeout = '10min'")
      await client.query(transactionalSchema)
      await client.query("commit")
      for (const index of concurrentIndexes) {
        const existing = await client.query(`
          select i.indisvalid as valid
          from pg_index i
          where i.indexrelid=to_regclass($1)
        `, [`public.${index.name}`])
        if (existing.rows[0] && !existing.rows[0].valid) {
          await client.query(`drop index concurrently if exists public.${index.name}`)
        }
        await client.query(index.create)
      }
      // This redundant index is already covered by the scan-object primary key.
      // Run the removal outside the schema transaction to avoid blocking reads
      // and writes while PostgreSQL releases it.
      await client.query("drop index concurrently if exists public.drive_bucket_scan_objects_key_idx")
      console.log("[db:schema] schema is ready")
      return
    } catch (error) {
      if (connected) await client.query("rollback").catch(() => undefined)
      const code = error && typeof error === "object" && "code" in error ? String(error.code ?? "") : ""
      const retryable = ["40P01", "55P03", "57014"].includes(code)
      if (retryable && attempt < maxAttempts) {
        const delayMs = attempt * 1_000
        console.warn(`[db:schema] transient database lock (${code}); retrying attempt ${attempt + 1}/${maxAttempts}`)
        await new Promise((resolveDelay) => setTimeout(resolveDelay, delayMs))
        continue
      }
      const details =
        error && typeof error === "object"
          ? {
              code: "code" in error ? error.code : undefined,
              message: "message" in error ? error.message : String(error),
            }
          : { message: String(error) }
      console.error("[db:schema] schema preparation failed", details)
      process.exitCode = 1
      return
    } finally {
      if (locked) await client.query("select pg_advisory_unlock(hashtext($1))", ["drive-schema-install-v1"]).catch(() => undefined)
      await client.end().catch(() => undefined)
    }
  }
}

await main()
