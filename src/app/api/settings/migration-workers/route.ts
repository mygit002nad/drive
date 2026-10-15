import { NextResponse } from "next/server"
import { requireAdmin } from "@/lib/server-auth"
import { recordUserActivity } from "@/lib/activity-audit"
import {
  getMigrationWorkerSettings,
  publicMigrationWorkerSettings,
  saveMigrationWorkerSettings,
} from "@/lib/migration-worker-settings-store"
import { getMigrationOrchestratorSettings } from "@/lib/migration-orchestrator-settings-store"
import { syncAllGitHubWorkerSecrets } from "@/lib/github-worker-secrets"
import { ensureMigrationQueueSchema, withMigrationQueueDatabase } from "@/lib/migration-queue-database"
import { queryDb } from "@/lib/db"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

export async function GET() {
  const auth = await requireAdmin()
  if (!auth.ok) return auth.response
  const settings = await getMigrationWorkerSettings()
  return NextResponse.json(
    { settings: publicMigrationWorkerSettings(settings) },
    { headers: { "Cache-Control": "no-store, max-age=0" } }
  )
}

export async function PUT(request: Request) {
  const auth = await requireAdmin()
  if (!auth.ok) return auth.response
  try {
    const body = await request.json().catch(() => ({})) as { sharedSecret?: unknown; queueDatabaseUrl?: unknown; queueDatabaseSsl?: unknown }
    const current = await getMigrationWorkerSettings()
    const requested = typeof body.sharedSecret === "string" ? body.sharedSecret.trim() : ""
    const sharedSecret = requested || current.sharedSecret
    const queueDatabaseSsl = typeof body.queueDatabaseSsl === "boolean" ? body.queueDatabaseSsl : current.queueDatabaseSsl
    if (sharedSecret.length < 24 || sharedSecret.length > 512) {
      throw new Error("Migration Worker shared secret must be between 24 and 512 characters")
    }

    const queueDatabaseUrl = typeof body.queueDatabaseUrl === "string" ? body.queueDatabaseUrl.trim() : ""
    const nextQueueDatabaseUrl = queueDatabaseUrl || current.queueDatabaseUrl
    const queueConfigChanging = nextQueueDatabaseUrl !== current.queueDatabaseUrl || queueDatabaseSsl !== current.queueDatabaseSsl
    if (queueConfigChanging) {
      const active = await queryDb(`select id from drive_migrations where status in('running','verifying') and options->>'executionMode'='migration_workers' limit 1`)
      if (active.rowCount) return NextResponse.json({ error: "Finish or cancel active worker-pool migrations before changing the queue database URI or SSL mode." }, { status: 409 })
    }
    if (queueDatabaseUrl) {
      await withMigrationQueueDatabase(queueDatabaseUrl, async (db) => {
        await db.query("select current_database(), current_user")
        await ensureMigrationQueueSchema(db)
      }, queueDatabaseSsl)
    }

    try {
      await saveMigrationWorkerSettings({ sharedSecret, queueDatabaseUrl: body.queueDatabaseUrl, queueDatabaseSsl })
      let syncedRepositories = 0
      if (requested) {
        const orchestration = await getMigrationOrchestratorSettings()
        const synchronization = await syncAllGitHubWorkerSecrets({
          serverUrl: orchestration.orchestratorUrl,
          sharedSecret,
        })
        syncedRepositories = synchronization.syncedRepositories
      }
      const settings = await getMigrationWorkerSettings()
      await recordUserActivity(request, auth.user.id, {
        action: queueDatabaseUrl ? "settings.migration_workers.queue_database_updated" : "settings.migration_workers.secret_updated",
        entityType: "settings", entityId: "migration-workers",
        entityLabel: queueDatabaseUrl ? "Migration queue database" : "Migration Worker credentials",
        summary: queueDatabaseUrl ? "Configured Migration queue database" : "Updated Migration Worker credentials",
        after: { configured: true, syncedRepositories },
      })
      return NextResponse.json({ settings: publicMigrationWorkerSettings(settings), syncedRepositories })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return NextResponse.json({
        error: `The Worker settings were saved, but GitHub Actions credentials could not be synchronized. Dispatch is paused until synchronization succeeds. ${message}`,
        settings: publicMigrationWorkerSettings(await getMigrationWorkerSettings()),
      }, { status: 502 })
    }
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 400 })
  }
}
