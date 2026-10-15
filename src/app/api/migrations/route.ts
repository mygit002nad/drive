import { NextResponse } from "next/server"
import { getAllAccounts, getActiveAccount } from "@/lib/accounts-store"
import { r2ListBuckets } from "@/lib/cloudflare-r2-buckets"
import { createMigration, getMigrationDashboardBootstrap } from "@/lib/migrations-store"
import { getBucketStatsMap } from "@/lib/bucket-stats-store"
import { requireAdmin } from "@/lib/server-auth"
import { getRequestActivityContext } from "@/lib/activity-store"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { overlayMigrationQueueItems, readMigrationQueueLiveProjection, resolveMigrationQueueLifecycleStatus } from "@/lib/migration-queue-database"

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function jsonOk(data: unknown) {
  return NextResponse.json(data, { status: 200 })
}

function jsonBad(error: string, status = 400, extra?: Record<string, unknown>) {
  return NextResponse.json({ error, ...(extra ?? {}) }, { status })
}

export async function GET() {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    // This page receives one bounded PostgreSQL snapshot. Account credentials
    // are excluded, and all bucket counts come from worker-owned projections.
    const { migrations: savedMigrations, accounts, activeAccount, bucketStats, activeItems: savedActiveItems } =
      await getMigrationDashboardBootstrap()
    let migrations = savedMigrations
    let activeItems = savedActiveItems
    const activeMigration = migrations.find((migration) => migration.status === "running")
      ?? migrations.find((migration) => migration.status === "verifying")
      ?? migrations.find((migration) => migration.status === "draft")
      ?? migrations[0]
      ?? null
    if (activeMigration?.options.executionMode === "migration_workers") {
      const settings = await getMigrationWorkerSettings()
      if (settings.queueDatabaseUrl) {
        try {
          const live = await readMigrationQueueLiveProjection(settings.queueDatabaseUrl, settings.queueDatabaseSsl, activeMigration.id, Math.max(0, Number(activeMigration.options.workerGeneration || 0)))
          if (live) {
            activeItems = overlayMigrationQueueItems(activeItems, live)
            const failedObjects = activeItems.reduce((sum, item) => {
              const progress = item.progress && typeof item.progress === "object" ? item.progress as Record<string, unknown> : {}
              const liveProgress = progress.live && typeof progress.live === "object" ? progress.live as Record<string, unknown> : {}
              return sum + Math.max(0, Number(liveProgress.failedObjects || 0))
            }, 0)
            migrations = migrations.map((migration) => migration.id === activeMigration.id
              ? { ...migration, status: resolveMigrationQueueLifecycleStatus(migration.status, live), options: { ...migration.options, workerPoolLiveStats: { ...live.stats, failed: failedObjects }, workerPoolQueueSeeding: live.seedComplete !== true } }
              : migration)
          }
        } catch {
          // Preserve the persisted dashboard snapshot if the queue database is temporarily unavailable.
        }
      }
    }
    const bucketError = (
      !activeAccount
        ? "No active Cloudflare account"
        : !activeAccount.cloudflareAccountId
          ? "Active Cloudflare account is not synced. Sync the account first to list buckets."
          : null
    )
    return jsonOk({
      migrations,
      accounts: accounts.map(({ id, label, email, status }) => ({ id, label, email, status })),
      buckets: activeAccount?.cloudflareAccountId ? bucketStats.map((row) => ({
        id: row.bucketName,
        name: row.bucketName,
        objects: row.objects,
        bytes: row.bytes,
        statsStatus: row.status,
        statsError: row.error,
        updatedAt: row.updatedAt,
      })) : [],
      bucketError,
      activeItems,
    })
  } catch (error: unknown) {
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String((error as { message?: unknown }).message ?? "Unable to load migrations")
        : "Unable to load migrations"
    return jsonBad(message)
  }
}

export async function POST(request: Request) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const body: unknown = await request.json().catch(() => ({}))
    const data = isRecord(body) ? body : {}

    const targetAccountId =
      typeof data.targetAccountId === "string" ? data.targetAccountId : ""
    const overwrite = typeof data.overwrite === "boolean" ? data.overwrite : true
    const concurrency = typeof data.concurrency === "number" ? data.concurrency : 3
    const rawIncludeBuckets = data.includeBuckets
    const hasIncludeBuckets = Array.isArray(rawIncludeBuckets)
    const includeBuckets = hasIncludeBuckets
      ? rawIncludeBuckets.map((v) => String(v)).filter(Boolean)
      : undefined
    const rawExcludeBuckets = data.excludeBuckets
    const excludeBuckets = Array.isArray(rawExcludeBuckets)
      ? rawExcludeBuckets.map((v) => String(v)).filter(Boolean)
      : undefined
    const pathPrefix =
      typeof data.pathPrefix === "string"
        ? data.pathPrefix
        : data.pathPrefix === null
          ? null
          : undefined
    const verifyAfterCopy = typeof data.verifyAfterCopy === "boolean" ? data.verifyAfterCopy : true
    const verifyStrictDestination =
      typeof data.verifyStrictDestination === "boolean" ? data.verifyStrictDestination : false
    const verifyModeRaw = typeof data.verifyMode === "string" ? data.verifyMode : ""
    const verifyMode =
      verifyModeRaw === "sha256-small" || verifyModeRaw === "keys-and-size" ? verifyModeRaw : "keys-and-size"
    const verifyHashMaxBytes =
      typeof data.verifyHashMaxBytes === "number" && Number.isFinite(data.verifyHashMaxBytes)
        ? Math.max(0, Math.floor(data.verifyHashMaxBytes))
        : undefined
    const executionMode = data.executionMode === "migration_workers" ? "migration_workers" : "super_slurper"
    const workerShardCount =
      typeof data.workerShardCount === "number" && Number.isFinite(data.workerShardCount)
        ? Math.max(1, Math.min(128, Math.floor(data.workerShardCount)))
        : 32

    if (!targetAccountId) return jsonBad("targetAccountId is required")

    const source = await getActiveAccount()
    if (!source) return jsonBad("No active Cloudflare account")
    const accounts = await getAllAccounts()
    const target = accounts.find((a) => a.id === targetAccountId)
    if (!target) return jsonBad("Target Cloudflare account not found", 404)

    if (hasIncludeBuckets && includeBuckets?.length === 0) {
      const { migration, items } = await createMigration({
        activity: { actorUserId: auth.user.id, sourceLabel: source.label, targetLabel: target.label, ...getRequestActivityContext(request) },
        sourceAccountId: source.id,
        targetAccountId: target.id,
        options: {
          overwrite,
          concurrency,
          includeBuckets,
          excludeBuckets,
          pathPrefix,
          sourceMode: "s3",
          executionMode,
          ...(executionMode === "migration_workers" ? { workerGeneration: 1, workerShardCount } : {}),
          verifyAfterCopy,
          verifyStrictDestination,
          verifyMode,
          ...(typeof verifyHashMaxBytes !== "undefined" ? { verifyHashMaxBytes } : {}),
        },
        items: [],
      })

      return jsonOk({ migration, items })
    }

    if (!source.cloudflareAccountId) return jsonBad("Active Cloudflare account is not synced")
    if (!target.cloudflareAccountId) return jsonBad("Target Cloudflare account is not synced")

    if (!source.r2AccessKeyId || !source.r2SecretAccessKey) {
      return jsonBad("Active Cloudflare account is missing R2 access keys")
    }
    if (!target.r2AccessKeyId || !target.r2SecretAccessKey) {
      return jsonBad("Target Cloudflare account is missing R2 access keys")
    }

    const buckets = await r2ListBuckets({
      accountId: source.cloudflareAccountId,
      apiToken: source.apiToken,
    })

    const filtered = buckets.filter((bucket) => {
      if (hasIncludeBuckets) return includeBuckets?.includes(bucket.name) ?? false
      if (excludeBuckets?.length) return !excludeBuckets.includes(bucket.name)
      return true
    })

    const cachedStats = await getBucketStatsMap(source.id)

    const { migration, items } = await createMigration({
      activity: { actorUserId: auth.user.id, sourceLabel: source.label, targetLabel: target.label, ...getRequestActivityContext(request) },
      sourceAccountId: source.id,
      targetAccountId: target.id,
      // Both engines use the same account and bucket snapshot. The worker lane
      // copies through the durable queue; the Super Slurper path remains the
      // default and is unchanged.
      options: {
        overwrite,
        concurrency,
        includeBuckets,
        excludeBuckets,
        pathPrefix,
        sourceMode: "s3",
        executionMode,
        ...(executionMode === "migration_workers" ? { workerGeneration: 1, workerShardCount } : {}),
        verifyAfterCopy,
        verifyStrictDestination,
        verifyMode,
        ...(typeof verifyHashMaxBytes !== "undefined" ? { verifyHashMaxBytes } : {}),
      },
      items: filtered.map((bucket) => ({
        sourceBucket: bucket.name,
        targetBucket: bucket.name,
        sourceJurisdiction: bucket.jurisdiction,
        sourceStorageClass: bucket.storage_class,
        sourceObjects:
          cachedStats.get(bucket.name)?.status === "completed"
            ? cachedStats.get(bucket.name)!.objects
            : bucket.objects,
        sourceBytes:
          cachedStats.get(bucket.name)?.status === "completed"
            ? cachedStats.get(bucket.name)!.bytes
            : bucket.size,
      })),
    })

    return jsonOk({ migration, items })
  } catch (error: unknown) {
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String((error as { message?: unknown }).message ?? "Unable to create migration")
        : "Unable to create migration"
    return jsonBad(message)
  }
}
