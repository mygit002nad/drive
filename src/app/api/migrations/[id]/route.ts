import { NextResponse } from "next/server"
import { deleteMigration, getMigration, getMigrationDetailBootstrap } from "@/lib/migrations-store"
import { requireAdmin } from "@/lib/server-auth"
import { getMigrationReadOnlyState } from "@/lib/migration-read-only"
import { getRequestActivityContext, recordActivity } from "@/lib/activity-store"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { readMigrationQueueLiveProjection } from "@/lib/migration-queue-database"

export async function GET(
  _request: Request,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const bootstrap = await getMigrationDetailBootstrap(id)
    if (!bootstrap) {
      return NextResponse.json({ error: "Migration not found" }, { status: 404 })
    }
    let { migration, items, workerRuns } = bootstrap
    const { accounts } = bootstrap
    let workerPoolLiveStats: Record<string, number> | null = null
    if (migration?.options?.executionMode === "migration_workers") {
      const settings = await getMigrationWorkerSettings()
      if (settings.queueDatabaseUrl) {
        const generation = Math.max(0, Number(migration.options.workerGeneration || 0))
        try {
          const live = await readMigrationQueueLiveProjection(settings.queueDatabaseUrl, settings.queueDatabaseSsl, id, generation)
          if (live) {
            items = items.map((item) => {
              const current = live.items.find((entry) => entry.id === item.id)
              return current ? { ...item, slurperStatus: current.slurperStatus, progress: { ...item.progress, live: current.progress.live }, updatedAt: current.progress.live.updatedAt || item.updatedAt } : item
            })
            workerRuns = live.workerRuns
            migration = { ...migration, status: live.migrationStatus, options: { ...migration.options, workerPoolLiveStats: live.stats } }
            workerPoolLiveStats = live.stats
          }
        } catch {
          // Keep the saved primary-database projection available during queue-database outages.
        }
      }
    }
    // Read the persisted migration projection. Synchronization is owned by the
    // orchestrator and must not delay every page/detail read.
    const readOnly = getMigrationReadOnlyState(migration)
    return NextResponse.json({ migration, items, workerRuns, accounts, workerPoolLiveStats, historyReadOnly: readOnly }, { status: 200 })
  } catch (error: unknown) {
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String((error as { message?: unknown }).message ?? "Unable to load migration")
        : "Unable to load migration"
    return NextResponse.json(
      { error: message },
      { status: 400 }
    )
  }
}

export async function DELETE(
  request: Request,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const migration = await getMigration(id)
    if (!migration) {
      return NextResponse.json({ error: "Migration not found" }, { status: 404 })
    }
    await deleteMigration(id)
    await recordActivity({
      actorUserId: auth.user.id,
      action: "migration.deleted",
      entityType: "migration",
      entityId: id,
      summary: "Deleted migration",
      detail: `Removed a migration in ${migration.status} status.`,
      before: { status: migration.status },
      ...getRequestActivityContext(request),
    })
    return NextResponse.json({ ok: true }, { status: 200 })
  } catch (error: unknown) {
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String((error as { message?: unknown }).message ?? "Unable to delete migration")
        : "Unable to delete migration"
    return NextResponse.json({ error: message }, { status: 400 })
  }
}
