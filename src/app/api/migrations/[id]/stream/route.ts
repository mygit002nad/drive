import { NextResponse } from "next/server"
import { getMigrationDetailBootstrap } from "@/lib/migrations-store"
import { requireAdmin } from "@/lib/server-auth"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { readMigrationQueueLiveProjection } from "@/lib/migration-queue-database"

export const runtime = "nodejs"

const STREAM_SNAPSHOT_CACHE_TTL_MS = 3_000
const STREAM_SNAPSHOT_CACHE_MAX = 128

type StreamSnapshot = Awaited<ReturnType<typeof getMigrationDetailBootstrap>>
type StreamSnapshotEntry = { expiresAt: number; promise: Promise<StreamSnapshot> }

declare global {
  var __driveMigrationStreamSnapshotCache: Map<string, StreamSnapshotEntry> | undefined
}

function getCachedStreamSnapshot(migrationId: string): Promise<StreamSnapshot> {
  const cache = (global.__driveMigrationStreamSnapshotCache ??= new Map())
  const now = Date.now()
  const cached = cache.get(migrationId)
  if (cached && cached.expiresAt > now) return cached.promise

  for (const [id, entry] of cache) {
    if (entry.expiresAt <= now) cache.delete(id)
  }
  if (cache.size >= STREAM_SNAPSHOT_CACHE_MAX) {
    const oldestId = cache.keys().next().value
    if (oldestId) cache.delete(oldestId)
  }

  const entry: StreamSnapshotEntry = { expiresAt: Number.POSITIVE_INFINITY, promise: Promise.resolve(null) }
  entry.promise = getMigrationDetailBootstrap(migrationId, { includeAccounts: false })
    .then((snapshot) => {
      if (snapshot) entry.expiresAt = Date.now() + STREAM_SNAPSHOT_CACHE_TTL_MS
      else if (cache.get(migrationId) === entry) cache.delete(migrationId)
      return snapshot
    })
    .catch((error) => {
      if (cache.get(migrationId) === entry) cache.delete(migrationId)
      throw error
    })
  cache.set(migrationId, entry)
  return entry.promise
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export async function GET(request: Request, context: { params: Promise<{ id: string }> }) {
  const auth = await requireAdmin()
  if (!auth.ok) return auth.response

  const { id } = await context.params
  const bootstrap = await getCachedStreamSnapshot(id)
  const queueSettings = bootstrap?.migration?.options?.executionMode === "migration_workers"
    ? await getMigrationWorkerSettings()
    : null
  const directQueue = queueSettings?.queueDatabaseUrl ? { url: queueSettings.queueDatabaseUrl, ssl: queueSettings.queueDatabaseSsl } : null

  const encoder = new TextEncoder()

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      let closed = false

      const safeEnqueue = (chunk: Uint8Array) => {
        if (closed) return
        try {
          controller.enqueue(chunk)
        } catch {
          closed = true
        }
      }

      const send = (event: string, data: unknown) => {
        safeEnqueue(encoder.encode(`event: ${event}\n`))
        safeEnqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`))
      }

      const close = () => {
        if (closed) return
        closed = true
        try {
          controller.close()
        } catch {
          // ignore
        }
      }

      if (request.signal.aborted) {
        close()
        return
      }
      request.signal.addEventListener("abort", close, { once: true })

      const loop = async () => {
        let lastSnapshot = ""
        let lastSentAt = 0
        // The stream only projects saved PostgreSQL state; worker schedules
        // advance migrations independently of any connected dashboard.
        while (!closed && !request.signal.aborted) {
          let nextDelay = 30_000
          try {
            const currentBootstrap = await getCachedStreamSnapshot(id)
            if (!currentBootstrap) {
              send("error", { error: "Migration not found" })
              break
            }
            let { migration, items, workerRuns } = currentBootstrap
            if (directQueue) {
              try {
                const live = await readMigrationQueueLiveProjection(directQueue.url, directQueue.ssl, id, Math.max(0, Number(migration.options.workerGeneration || 0)))
                if (live) {
                  items = items.map((item) => {
                    const current = live.items.find((entry) => entry.id === item.id)
                    return current ? { ...item, slurperStatus: current.slurperStatus, progress: { ...item.progress, live: current.progress.live }, updatedAt: current.progress.live.updatedAt || item.updatedAt } : item
                  })
                  workerRuns = live.workerRuns
                  migration = { ...migration, status: live.migrationStatus, options: { ...migration.options, workerPoolLiveStats: live.stats } }
                }
              } catch {
                // Fall back to the last Orchestrator projection if the queue database is briefly unreachable.
              }
            }
            const snapshot = { migration, items, workerRuns }
            const serialized = JSON.stringify(snapshot)
            const now = Date.now()
            if (serialized !== lastSnapshot || now - lastSentAt >= 15_000) {
              send("snapshot", { ...snapshot, serverTime: new Date(now).toISOString() })
              lastSnapshot = serialized
              lastSentAt = now
            } else {
              safeEnqueue(encoder.encode(": keepalive\n\n"))
            }
            nextDelay = directQueue ? 30_000 : ["running", "verifying", "queued"].includes(migration.status) ? 10_000 : 30_000
          } catch (e: unknown) {
            const message =
              typeof e === "object" && e !== null && "message" in e
                ? String((e as { message?: unknown }).message ?? "Stream error")
                : "Stream error"
            send("error", { error: message })
          }
          await sleep(nextDelay)
        }

        close()
      }

      void loop()
    },
  })

  return new NextResponse(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  })
}
