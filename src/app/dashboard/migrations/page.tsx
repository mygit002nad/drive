"use client"

import { migrationProgressPercent } from "@/lib/migration-progress"

import * as React from "react"
import {
  ExternalLink,
  Plus,
  Play,
  Square,
  Trash2,
  X,
} from "lucide-react"
import { useRouter } from "next/navigation"
import type { ColumnDef } from "@tanstack/react-table"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Progress } from "@/components/ui/progress"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Separator } from "@/components/ui/separator"
import { Switch } from "@/components/ui/switch"
import { Spinner } from "@/components/ui/spinner"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  DashboardPage,
  DashboardPageHeader,
  DashboardPageSkeleton,
} from "@/components/dashboard/page-shell"
import { DashboardDataTable } from "@/components/dashboard/data-table"
import { DashboardSearchFilterToolbar, DASHBOARD_TOOLBAR_ACTION_BUTTON_CLASS } from "@/components/dashboard/search-filter-toolbar"
import { formatLastSyncedAt } from "@/lib/dashboard-format"

type Account = {
  id: string
  label: string
  email: string
  status: "active" | "available" | "disabled"
}

type BucketSummary = {
  id: string
  name: string
  objects: number
  bytes: number
  statsStatus?: string
  statsError?: string
}

type Migration = {
  id: string
  sourceAccountId: string
  targetAccountId: string
  status: "draft" | "running" | "verifying" | "completed" | "failed" | "verification_failed" | "canceled"
  options: {
    executionMode?: "super_slurper" | "migration_workers"
    workerShardCount?: number
    overwrite?: boolean
    concurrency?: number
    pathPrefix?: string | null
  }
  createdAt: string
  startedAt?: string
  completedAt?: string
  syncStatus?: "idle" | "syncing" | "ok" | "error"
  syncMessage?: string
  summaryItemCount: number
  summaryObjects: number
  summaryBytes: number
  workerSummary: Record<string, unknown>
  detailsCompactedAt?: string
}

type MigrationItem = {
  id: string
  sourceBucket: string
  targetBucket: string
  slurperJobId?: string
  slurperStatus?: string
  progress: Record<string, unknown>
  sourceObjects?: number
  sourceBytes?: number
}

type SlurperProgressResult = {
  objects?: number
  transferredObjects?: number
  skippedObjects?: number
  failedObjects?: number
  status?: string
}


function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function readSlurperResult(progress: Record<string, unknown>): SlurperProgressResult | null {
  const cumulative = progress.slurperCumulative
  if (isRecord(cumulative)) {
    const objects = typeof cumulative.objects === "number" ? cumulative.objects : undefined
    const transferredObjects =
      typeof cumulative.transferredObjects === "number" ? cumulative.transferredObjects : undefined
    const skippedObjects =
      typeof cumulative.skippedObjects === "number" ? cumulative.skippedObjects : undefined
    const failedObjects =
      typeof cumulative.failedObjects === "number" ? cumulative.failedObjects : undefined
    const status = typeof cumulative.status === "string" ? cumulative.status : undefined
    if (objects !== undefined || transferredObjects !== undefined || skippedObjects !== undefined || failedObjects !== undefined || status)
      return { objects, transferredObjects, skippedObjects, failedObjects, status }
  }

  const normalized = progress.slurperNormalized
  if (isRecord(normalized)) {
    const objects = typeof normalized.objects === "number" ? normalized.objects : undefined
    const transferredObjects =
      typeof normalized.transferredObjects === "number" ? normalized.transferredObjects : undefined
    const skippedObjects =
      typeof normalized.skippedObjects === "number" ? normalized.skippedObjects : undefined
    const failedObjects =
      typeof normalized.failedObjects === "number" ? normalized.failedObjects : undefined
    const status = typeof normalized.status === "string" ? normalized.status : undefined
    if (objects !== undefined || transferredObjects !== undefined || skippedObjects !== undefined || failedObjects !== undefined || status)
      return { objects, transferredObjects, skippedObjects, failedObjects, status }
  }

  const slurper = progress.slurper
  if (!isRecord(slurper)) return null
  const result = slurper.result
  if (!isRecord(result)) return null

  const objects = typeof result.objects === "number" ? result.objects : undefined
  const transferredObjects =
    typeof result.transferredObjects === "number" ? result.transferredObjects : undefined
  const skippedObjects =
    typeof result.skippedObjects === "number" ? result.skippedObjects : undefined
  const failedObjects =
    typeof result.failedObjects === "number" ? result.failedObjects : undefined
  const status = typeof result.status === "string" ? result.status : undefined

  return { objects, transferredObjects, skippedObjects, failedObjects, status }
}

function normalizeStatus(value: string | undefined): string {
  return String(value ?? "").trim().toLowerCase()
}

function isCompletedStatus(value: string | undefined): boolean {
  const s = normalizeStatus(value)
  return (
    s === "completed" ||
    s === "copy_completed" ||
    s === "complete" ||
    s === "finished" ||
    s === "success" ||
    s === "succeeded"
  )
}

async function postJsonWithTimeout(input: {
  url: string
  body?: Record<string, unknown>
  timeoutMs?: number
}): Promise<Response> {
  const controller = new AbortController()
  const timeoutMs = Math.max(1_000, Math.min(60_000, input.timeoutMs ?? 12_000))
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(input.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input.body ?? {}),
      signal: controller.signal,
    })
  } finally {
    clearTimeout(timeout)
  }
}

function formatNumber(value: number | undefined): string {
  if (!value || value <= 0) return "0"
  return Intl.NumberFormat().format(value)
}

function formatBytes(value: number | undefined): string {
  if (!value || value <= 0) return "0 B"
  const units = ["B", "KB", "MB", "GB", "TB", "PB"]
  let size = value
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex++
  }
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[unitIndex]}`
}

function visibleSyncMessage(value?: string | null) {
  return value?.trim().toLowerCase() === "migration independently verified" ? "" : value ?? ""
}

function statusBadge(status: string | undefined) {
  const s = String(status ?? "unknown")
  if (s === "completed") return <Badge className="bg-green-600">Completed</Badge>
  if (s === "failed") return <Badge className="bg-red-600">Failed</Badge>
  if (s === "verification_failed") return <Badge className="bg-red-600">Verification failed</Badge>
  if (s === "canceled" || s === "cancelled" || s === "aborted") return <Badge variant="secondary">Aborted</Badge>
  if (s === "draft") return <Badge variant="outline">Draft</Badge>
  if (s === "paused") return <Badge className="bg-yellow-600">Paused</Badge>
  if (["running", "verifying", "scanning", "queued", "creating_job", "job_id_pending"].includes(s)) return <Badge className="bg-primary text-primary-foreground">Running</Badge>
  if (s.endsWith("_failed") || s.includes("error")) return <Badge className="bg-red-600">Failed</Badge>
  return <Badge variant="outline">{s}</Badge>
}

export default function MigrationsPage() {
  const router = useRouter()

  const [accounts, setAccounts] = React.useState<Account[]>([])
  const [migrations, setMigrations] = React.useState<Migration[]>([])
  const [activeMigration, setActiveMigration] = React.useState<Migration | null>(null)
  const [activeItems, setActiveItems] = React.useState<MigrationItem[]>([])
  const [buckets, setBuckets] = React.useState<BucketSummary[]>([])
  const [initialLoading, setInitialLoading] = React.useState(true)
  const [refreshing, setRefreshing] = React.useState(false)
  const [lastSyncedAt, setLastSyncedAt] = React.useState<string | null>(null)
  const [busyAction, setBusyAction] = React.useState<string | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [search, setSearch] = React.useState("")
  const [deleteId, setDeleteId] = React.useState<string | null>(null)

  React.useEffect(() => {
    if (!error) return
    const t = setTimeout(() => setError(null), 4500)
    return () => clearTimeout(t)
  }, [error])

  const activeAccount = accounts.find((a) => a.status === "active") ?? null
  const availableTargets = accounts.filter((a) => a.status === "available")
  const accountLabelById = React.useMemo(() => {
    const labels = new Map<string, string>()
    for (const account of accounts) labels.set(account.id, account.label)
    return labels
  }, [accounts])

  const filteredMigrations = React.useMemo(() => {
    const term = search.trim().toLowerCase()
    const matching = term
      ? migrations.filter((migration) => {
          const source = accountLabelById.get(migration.sourceAccountId) ?? migration.sourceAccountId
          const target = accountLabelById.get(migration.targetAccountId) ?? migration.targetAccountId
          const engine = migration.options.executionMode === "migration_workers" ? "worker pool" : "super slurper"
          return [migration.id, migration.status, source, target, migration.createdAt, engine, migration.syncMessage]
            .filter(Boolean)
            .join(" ")
            .toLowerCase()
            .includes(term)
        })
      : migrations.slice()

    return matching.sort((a, b) => (b.createdAt ?? "").localeCompare(a.createdAt ?? ""))
  }, [accountLabelById, migrations, search])

  const columns: ColumnDef<Migration>[] = [
    {
      accessorKey: "id",
      meta: { width: "min-w-[230px]" },
      header: () => <div className="text-center">Migration</div>,
      cell: ({ row }) => {
        const migration = row.original
        return (
          <div className="flex min-h-10 min-w-0 flex-col justify-center gap-0.5">
            <span className="truncate font-mono text-[11px] font-medium" title={migration.id}>
              {migration.id}
            </span>
            <span className="truncate text-[10px] leading-4 text-muted-foreground">
              {migration.options.executionMode === "migration_workers" ? "Worker pool" : "Super Slurper"}
            </span>
          </div>
        )
      },
    },
    {
      accessorKey: "status",
      meta: { width: "min-w-[125px]", align: "center" },
      header: () => <div className="text-center">Status</div>,
      cell: ({ row }) => (
        <div className="flex min-h-10 items-center justify-center">
          {statusBadge(row.original.status)}
        </div>
      ),
    },
    {
      id: "source",
      meta: { width: "min-w-[130px]", align: "center" },
      header: () => <div className="text-center">Source</div>,
      cell: ({ row }) => {
        const source = accountLabelById.get(row.original.sourceAccountId) ?? row.original.sourceAccountId
        return <span className="block truncate text-center text-[11px] text-muted-foreground" title={source}>{source}</span>
      },
    },
    {
      id: "target",
      meta: { width: "min-w-[130px]", align: "center" },
      header: () => <div className="text-center">Target</div>,
      cell: ({ row }) => {
        const target = accountLabelById.get(row.original.targetAccountId) ?? row.original.targetAccountId
        return <span className="block truncate text-center text-[11px] text-muted-foreground" title={target}>{target}</span>
      },
    },
    {
      accessorKey: "createdAt",
      meta: { width: "min-w-[130px]", align: "center" },
      header: () => <div className="text-center">Created</div>,
      cell: ({ row }) => {
        const createdAt = row.original.createdAt
        const date = createdAt ? new Date(createdAt) : null
        return (
          <div className="flex min-h-10 flex-col items-center justify-center gap-0.5 text-center text-[11px] text-muted-foreground">
            <span>{date && !Number.isNaN(date.getTime()) ? date.toLocaleDateString() : "-"}</span>
            <span>{date && !Number.isNaN(date.getTime()) ? date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : ""}</span>
          </div>
        )
      },
    },
    {
      accessorKey: "summaryObjects",
      meta: { width: "min-w-[110px]", align: "center" },
      header: () => <div className="text-center">Objects</div>,
      cell: ({ row }) => <div className="min-h-10 content-center text-center text-[12px] tabular-nums">{formatNumber(row.original.summaryObjects)}</div>,
    },
    {
      accessorKey: "summaryBytes",
      meta: { width: "min-w-[110px]", align: "center" },
      header: () => <div className="text-center">Data</div>,
      cell: ({ row }) => <div className="min-h-10 content-center text-center text-[11px] tabular-nums text-muted-foreground">{formatBytes(row.original.summaryBytes)}</div>,
    },
    {
      id: "workers",
      meta: { width: "min-w-[90px]", align: "center" },
      header: () => <div className="text-center">Workers</div>,
      cell: ({ row }) => {
        const workerRuns = row.original.workerSummary?.workerRuns
        return <div className="min-h-10 content-center text-center text-[12px] tabular-nums">{Array.isArray(workerRuns) ? workerRuns.length : 0}</div>
      },
    },
    {
      id: "actions",
      meta: { width: "min-w-[100px]", align: "center", divider: false },
      enableHiding: false,
      header: () => <div className="text-center">Actions</div>,
      cell: ({ row }) => (
        <div className="flex min-h-10 items-center justify-center gap-1.5">
          <Button
            variant="ghost"
            size="icon"
            className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md"
            onClick={() => router.push(`/dashboard/migrations/${encodeURIComponent(row.original.id)}`)}
            aria-label="View migration details"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 text-destructive shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md"
            onClick={() => setDeleteId(row.original.id)}
            aria-label="Delete migration"
            disabled={Boolean(busyAction)}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ),
    },
  ]

  const activeCount = migrations.filter((migration) => migration.status === "running" || migration.status === "verifying").length
  const completedCount = migrations.filter((migration) => migration.status === "completed").length
  const attentionCount = migrations.filter((migration) => migration.status === "failed" || migration.status === "verification_failed" || (migration.status === "verifying" && migration.syncStatus === "error")).length

  const [createOpen, setCreateOpen] = React.useState(false)
  const [targetAccountId, setTargetAccountId] = React.useState<string>("")
  const [overwrite, setOverwrite] = React.useState(true)
  const [executionMode, setExecutionMode] = React.useState<"super_slurper" | "migration_workers">("super_slurper")
  const [pathPrefix, setPathPrefix] = React.useState("")
  const [bucketQuery, setBucketQuery] = React.useState("")
  const [selectedBuckets, setSelectedBuckets] = React.useState<Record<string, boolean>>({})

  const loadAll = React.useCallback(async () => {
    setRefreshing(true)
    setError(null)
    try {
      const response = await fetch("/api/migrations", { cache: "no-store" })
      const dashboardJson: unknown = await response.json().catch(() => ({}))
      if (!response.ok) {
        const message = isRecord(dashboardJson) && typeof dashboardJson.error === "string"
          ? dashboardJson.error
          : "Unable to load migrations"
        throw new Error(message)
      }

      const nextAccounts =
        isRecord(dashboardJson) && Array.isArray(dashboardJson.accounts) ? (dashboardJson.accounts as Account[]) : []
      const nextMigrations =
        isRecord(dashboardJson) && Array.isArray(dashboardJson.migrations)
          ? (dashboardJson.migrations as Migration[])
          : []

      setAccounts(nextAccounts)
      setMigrations(nextMigrations)
      setLastSyncedAt(new Date().toISOString())

      const bucketsError =
        isRecord(dashboardJson) && typeof dashboardJson.bucketError === "string" && dashboardJson.bucketError.trim()
          ? dashboardJson.bucketError.trim()
          : null
      if (bucketsError) setError(bucketsError)

      const nextBuckets =
        isRecord(dashboardJson) && Array.isArray(dashboardJson.buckets)
          ? (dashboardJson.buckets as unknown[]).map((b) => {
              const maybe = isRecord(b) ? b : {}
              return {
                id: String(maybe.id ?? maybe.name ?? ""),
                name: String(maybe.name ?? ""),
                objects: typeof maybe.objects === "number" ? (maybe.objects as number) : 0,
                bytes: typeof maybe.bytes === "number" ? (maybe.bytes as number) : 0,
                statsStatus: typeof maybe.statsStatus === "string" ? (maybe.statsStatus as string) : undefined,
                statsError: typeof maybe.statsError === "string" ? (maybe.statsError as string) : undefined,
              } satisfies BucketSummary
            })
          : []
      setBuckets(nextBuckets.filter((b) => b.name.length > 0))

      const current =
        nextMigrations.find((m: Migration) => m.status === "running") ??
        nextMigrations.find((m: Migration) => m.status === "verifying") ??
        nextMigrations.find((m: Migration) => m.status === "draft") ??
        nextMigrations[0] ??
        null

      setActiveMigration(current)
      const activeItems = isRecord(dashboardJson) && Array.isArray(dashboardJson.activeItems)
        ? dashboardJson.activeItems as MigrationItem[]
        : []
      setActiveItems(activeItems)
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to load migrations")
          : "Unable to load migrations"
      setError(message)
    } finally {
      setInitialLoading(false)
      setRefreshing(false)
    }
  }, [])

  const confirmDelete = async () => {
    const migrationId = deleteId
    if (!migrationId) return

    setBusyAction("delete")
    setError(null)
    try {
      const res = await fetch(`/api/migrations/${encodeURIComponent(migrationId)}`, { method: "DELETE" })
      const json: unknown = await res.json().catch(() => ({}))
      const message = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to delete migration"
      if (!res.ok) throw new Error(message)

      setMigrations((current) => current.filter((migration) => migration.id !== migrationId))
      setDeleteId(null)
      await loadAll()
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to delete migration")
          : "Unable to delete migration"
      setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  React.useEffect(() => {
    void loadAll()
  }, [loadAll])

  const needsBucketStats = React.useMemo(() => buckets.some((b) => b.statsStatus && b.statsStatus !== "completed"), [buckets])

  React.useEffect(() => {
    if (!activeMigration?.id) return

    let cancelled = false
    let es: EventSource | null = null
    let retry = 0

    const connect = () => {
      if (cancelled) return
      try {
        es?.close()
      } catch {
        // ignore
      }

      const streamUrl = `/api/migrations/${encodeURIComponent(activeMigration.id)}/stream`
      es = new EventSource(streamUrl)

      const onSnapshot = (event: MessageEvent) => {
        retry = 0
        try {
          const data: unknown = JSON.parse(String(event.data ?? "{}"))
          if (isRecord(data) && isRecord(data.migration)) setActiveMigration(data.migration as Migration)
          if (isRecord(data) && Array.isArray(data.items)) setActiveItems(data.items as MigrationItem[])
        } catch {
          // ignore
        }
      }

      const onError = () => {
        if (cancelled) return
        try {
          es?.close()
        } catch {
          // ignore
        }
        es = null
        retry += 1
        const delay = Math.min(5_000, 500 + retry * 500)
        setTimeout(connect, delay)
      }

      es.addEventListener("snapshot", onSnapshot)
      es.addEventListener("error", onError)
    }

    connect()
    return () => {
      cancelled = true
      try {
        es?.close()
      } catch {
        // ignore
      }
    }
  }, [activeMigration?.id])

  const totals = React.useMemo(() => {
    if (activeItems.length === 0 && activeMigration?.detailsCompactedAt) {
      const totalObjects = activeMigration.summaryObjects ?? 0
      const objectCounts = isRecord(activeMigration.workerSummary?.objectCounts)
        ? activeMigration.workerSummary.objectCounts
        : {}
      const transferred = typeof objectCounts.transferred === "number" ? objectCounts.transferred : 0
      const skipped = typeof objectCounts.skipped === "number" ? objectCounts.skipped : 0
      const failed = typeof objectCounts.failed === "number" ? objectCounts.failed : 0
      const completed = Math.min(totalObjects, transferred + skipped + failed)
      return {
        totalObjects,
        transferred,
        skipped,
        failed,
        completed: activeMigration.status === "completed" ? totalObjects : completed,
        percent: migrationProgressPercent(transferred + skipped, totalObjects),
      }
    }
    let totalObjects = 0
    let transferred = 0
    let skipped = 0
    let failed = 0

    for (const item of activeItems) {
      const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
      const result = readSlurperResult(item.progress)
      const live = isRecord(progress.live) ? (progress.live as Record<string, unknown>) : null
      const objects = result?.objects
      const transferredObjects = result?.transferredObjects
      const skippedObjects = result?.skippedObjects
      const failedObjects = result?.failedObjects

      if (live && typeof live.totalObjects === "number") totalObjects += live.totalObjects
      else if (typeof item.sourceObjects === "number") totalObjects += item.sourceObjects
      else if (typeof objects === "number") totalObjects += objects

      if (live && typeof live.transferredObjects === "number") transferred += live.transferredObjects
      else if (typeof transferredObjects === "number") transferred += transferredObjects

      if (live && typeof live.skippedObjects === "number") skipped += live.skippedObjects
      else if (typeof skippedObjects === "number") skipped += skippedObjects

      if (live && typeof live.failedObjects === "number") failed += live.failedObjects
      else if (typeof failedObjects === "number") failed += failedObjects
    }

    const completed =
      activeMigration?.status === "completed" && totalObjects > 0
        ? totalObjects
        : totalObjects > 0
          ? Math.min(totalObjects, transferred + skipped + failed)
          : transferred + skipped + failed
    const percent = migrationProgressPercent(transferred + skipped, totalObjects)
    return { totalObjects, transferred, skipped, failed, completed, percent }
  }, [activeItems, activeMigration])

  const filteredBuckets = React.useMemo(() => {
    const query = bucketQuery.trim().toLowerCase()
    if (!query) return buckets
    return buckets.filter((b) => b.name.toLowerCase().includes(query))
  }, [bucketQuery, buckets])

  const selectedSummary = React.useMemo(() => {
    const names = Object.entries(selectedBuckets)
      .filter(([, on]) => on)
      .map(([name]) => name)
    const map = new Map(buckets.map((b) => [b.name, b] as const))
    let objects = 0
    let bytes = 0
    for (const name of names) {
      const b = map.get(name)
      if (!b) continue
      objects += b.objects || 0
      bytes += b.bytes || 0
    }
    return { count: names.length, objects, bytes }
  }, [selectedBuckets, buckets])

  const createNewMigration = async () => {
    try {
      const chosen = Object.entries(selectedBuckets)
        .filter(([, on]) => on)
        .map(([name]) => name)

      if (
        chosen.length > 0 &&
        typeof window !== "undefined" &&
        !window.confirm(
          `Start a migration for ${chosen.length} selected bucket${chosen.length === 1 ? "" : "s"}?`
        )
      ) {
        return
      }

      setBusyAction("create")
      setError(null)

      const res = await fetch("/api/migrations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            targetAccountId,
            overwrite,
            executionMode,
            pathPrefix: pathPrefix.trim() ? pathPrefix.trim() : undefined,
            includeBuckets: chosen,
        }),
      })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage =
        isRecord(json) && typeof json.error === "string" ? json.error : "Unable to create migration"
      if (!res.ok) throw new Error(errorMessage)

      setCreateOpen(false)
      setBucketQuery("")
      setSelectedBuckets({})
      await loadAll()
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to create migration")
          : "Unable to create migration"
      setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  const startMigration = async () => {
    if (!activeMigration?.id) return
    setBusyAction("start")
    setError(null)
    try {
      const res = await fetch(`/api/migrations/${encodeURIComponent(activeMigration.id)}/start?async=1`, {
        method: "POST",
      })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage =
        isRecord(json) && typeof json.error === "string" ? json.error : "Unable to start migration"
      if (!res.ok) throw new Error(errorMessage)
      await loadAll()
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to start migration")
          : "Unable to start migration"
      setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  const retryMigration = async () => {
    if (!activeMigration?.id) return
    setBusyAction("retry_migration")
    setError(null)
    try {
      const res = await postJsonWithTimeout({
        url: `/api/migrations/${encodeURIComponent(activeMigration.id)}/action`,
        body: { action: "retry_migration" },
        timeoutMs: 12_000,
      })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to retry migration"
      if (!res.ok) throw new Error(errorMessage)
      await loadAll()
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "name" in e && String((e as { name?: unknown }).name) === "AbortError"
          ? ""
          : typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to retry migration")
          : "Unable to retry migration"
      if (message) setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  const hasActiveCard = Boolean(activeMigration)

  if (initialLoading) {
    return <DashboardPageSkeleton cards={4} rows={8} />
  }

  const runMigrationAction = async (action: "cancel_migration" | "retry_migration") => {
    if (!activeMigration?.id) return
    setBusyAction(action)
    setError(null)
    try {
      const res = await postJsonWithTimeout({
        url: `/api/migrations/${encodeURIComponent(activeMigration.id)}/action`,
        body: { action },
        timeoutMs: 12_000,
      })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to update migration"
      if (!res.ok) throw new Error(errorMessage)
      await loadAll()
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : "Unable to update migration"
      setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  return (
    <DashboardPage className="dashboard-motion-stage">
      <div className="dashboard-motion-item">
        <DashboardPageHeader
          title="Migrations"
          description={formatLastSyncedAt(lastSyncedAt)}
          actions={
            <DashboardSearchFilterToolbar
              searchValue={search}
              onSearchChange={setSearch}
              searchPlaceholder="Search migrations..."
              onRefresh={() => void loadAll()}
              refreshing={refreshing}
              refreshLabel="Refresh migrations"
              actions={<Button
                size="icon"
                className={DASHBOARD_TOOLBAR_ACTION_BUTTON_CLASS}
                onClick={() => setCreateOpen(true)}
                disabled={Boolean(busyAction) || !activeAccount || availableTargets.length === 0}
              >
                <Plus className="h-4 w-4 sm:mr-2" />
                <span className="sr-only sm:not-sr-only">New Migration</span>
              </Button>}
            />
          }
        />
      </div>

      {error ? (
        <div className="rounded-md border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-200">
          {error}
        </div>
      ) : null}

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent
          showCloseButton={false}
          className="!w-[min(96vw,1180px)] sm:!w-[min(96vw,1180px)] !max-w-[1180px] sm:!max-w-[1180px] max-h-[85vh] p-0 overflow-hidden flex flex-col"
        >
          <div className="border-b px-6 py-5">
            <div className="flex items-start justify-between gap-4">
                <DialogHeader className="flex-1">
                  <DialogTitle>Create migration</DialogTitle>
                  <DialogDescription>Pick a destination account and optionally choose buckets to migrate.</DialogDescription>
                </DialogHeader>
              <div className="flex items-center gap-3">
                <Button
                  size="sm"
                  className="h-9 px-2.5"
                  onClick={createNewMigration}
                  loading={busyAction === "create"}
                  disabled={
                    busyAction === "create" ||
                    !activeAccount ||
                    availableTargets.length === 0 ||
                    !targetAccountId
                  }
                >
                  Create migration
                </Button>
                <DialogClose asChild>
                  <button
                    type="button"
                    aria-label="Close"
                    className="inline-flex h-9 w-9 items-center justify-center rounded-full bg-destructive/15 text-destructive hover:bg-destructive/25 focus:outline-none focus-visible:ring-2 focus-visible:ring-destructive/30"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </DialogClose>
              </div>
            </div>
          </div>

          <div className="grid md:grid-cols-6 overflow-hidden flex-1 min-h-0">
            <div className="md:col-span-2 p-6 space-y-5 overflow-auto min-h-0">
              <div className="space-y-2">
                <Label>Destination account</Label>
                <Select value={targetAccountId} onValueChange={setTargetAccountId}>
                  <SelectTrigger className="h-11">
                    <SelectValue placeholder="Select destination..." />
                  </SelectTrigger>
                  <SelectContent>
                    {availableTargets.map((a) => (
                      <SelectItem key={a.id} value={a.id}>
                        {a.label} ({a.email})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  Source: {activeAccount ? `${activeAccount.label} (${activeAccount.email})` : "No active account"}
                </p>
              </div>

              <Separator />

              <div className="space-y-2">
                <Label>Migration engine</Label>
                <Select value={executionMode} onValueChange={(value) => setExecutionMode(value as "super_slurper" | "migration_workers")}>
                  <SelectTrigger className="h-11">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="super_slurper">Cloudflare Super Slurper</SelectItem>
                    <SelectItem value="migration_workers">Drive migration worker pool</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  {executionMode === "migration_workers"
                    ? "The File Scanner builds one durable per-file queue. Dispatched workers claim files independently, while final verification remains owned by the File Scanner."
                    : "Uses Cloudflare-managed Super Slurper jobs and its existing three-job limit."}
                </p>
              </div>

              <Separator />

              <div className="flex items-start justify-between gap-4">
                <div className="space-y-1">
                  <Label>Overwrite on destination</Label>
                  <p className="text-xs text-muted-foreground">Replace target objects when keys already exist.</p>
                </div>
                <Switch checked={overwrite} onCheckedChange={setOverwrite} />
              </div>

              <Separator />

              <div className="space-y-2">
                <Label>Path prefix (optional)</Label>
                <Input className="h-11" value={pathPrefix} onChange={(e) => setPathPrefix(e.target.value)} placeholder="e.g. backups/" />
                <p className="text-xs text-muted-foreground">Only migrate objects under this prefix.</p>
              </div>
            </div>

            <div className="md:col-span-4 border-t md:border-t-0 md:border-l p-6 overflow-hidden flex flex-col gap-4 min-h-0">
              <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                <div className="space-y-1">
                  <Label>Buckets</Label>
                  <p className="text-xs text-muted-foreground">
                    {selectedSummary.count} selected • {formatNumber(selectedSummary.objects)} objects • {formatBytes(selectedSummary.bytes)}
                  </p>
                </div>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      const next: Record<string, boolean> = {}
                      for (const b of buckets) next[b.name] = true
                      setSelectedBuckets(next)
                    }}
                  >
                    Select all
                  </Button>
                  <Button type="button" size="sm" variant="outline" onClick={() => setSelectedBuckets({})}>
                    Clear
                  </Button>
                </div>
              </div>

              <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                <Input
                  value={bucketQuery}
                  onChange={(e) => setBucketQuery(e.target.value)}
                  placeholder="Search buckets..."
                  className="h-11 flex-1"
                />
                {needsBucketStats ? (
                  <div className="text-xs text-muted-foreground">
                    Waiting for Worker storage stats...
                  </div>
                ) : null}
              </div>

              <div className="rounded-lg border overflow-hidden flex-1 min-h-0">
                <ScrollArea className="h-full pr-2" hideScrollbar>
                  <Table className="table-fixed w-full min-w-[620px]">
                    <TableHeader>
                      <TableRow>
                        <TableHead className="sticky top-0 z-10 bg-background w-[48px]" />
                        <TableHead className="sticky top-0 z-10 bg-background">Bucket</TableHead>
                        <TableHead className="sticky top-0 z-10 bg-background w-[120px] text-center">Objects</TableHead>
                        <TableHead className="sticky top-0 z-10 bg-background w-[120px] text-center">Size</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filteredBuckets.map((b) => {
                        const checked = Boolean(selectedBuckets[b.name])
                        return (
                          <TableRow
                            key={b.name}
                            className="cursor-pointer hover:bg-muted/40"
                            onClick={() => {
                              setSelectedBuckets((prev) => {
                                const next = { ...prev }
                                if (next[b.name]) delete next[b.name]
                                else next[b.name] = true
                                return next
                              })
                            }}
                          >
                            <TableCell className="text-center" onClick={(e) => e.stopPropagation()}>
                              <Checkbox
                                checked={checked}
                                onCheckedChange={(v) => {
                                  setSelectedBuckets((prev) => {
                                    const next = { ...prev }
                                    if (v) next[b.name] = true
                                    else delete next[b.name]
                                    return next
                                  })
                                }}
                              />
                            </TableCell>
                            <TableCell className="font-medium">{b.name}</TableCell>
                            <TableCell className="text-center">
                              <div className="flex flex-col items-center">
                                <span>{formatNumber(b.objects)}</span>
                                {b.statsStatus && b.statsStatus !== "completed" ? (
                                  <span className="text-[11px] text-muted-foreground">
                                    {b.statsStatus === "error" ? "Error" : "Waiting for Worker..."}
                                  </span>
                                ) : null}
                              </div>
                            </TableCell>
                            <TableCell className="text-center">
                              <div className="flex flex-col items-center">
                                <span>{formatBytes(b.bytes)}</span>
                                {b.statsStatus && b.statsStatus !== "completed" ? (
                                  <span className="text-[11px] text-muted-foreground">
                                    {b.statsStatus === "error" ? "Error" : "Waiting for Worker..."}
                                  </span>
                                ) : null}
                              </div>
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </div>

              <div className="flex flex-col-reverse gap-3 sm:flex-row sm:items-center sm:justify-between">
                <p className="text-xs text-muted-foreground">
                  Review selection, then create the migration record. If no buckets are selected, starting it will only switch the active account.
                </p>
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <div className="dashboard-motion-item dashboard-motion-delay-1 grid grid-cols-2 gap-4 xl:grid-cols-4">
        <Card className="gap-0 py-0">
          <CardHeader className="px-4 py-3 pb-1.5 lg:px-4 lg:py-3 lg:pb-1.5">
            <CardDescription className="text-[13px] leading-4">Total Migrations</CardDescription>
            <CardTitle className="text-xl font-bold leading-none tabular-nums sm:text-2xl">{migrations.length}</CardTitle>
          </CardHeader>
          <CardContent className="px-4 pb-3 pt-0 lg:px-4 lg:pb-3"><p className="text-[11px] leading-4 text-muted-foreground">Stored migration runs</p></CardContent>
        </Card>
        <Card className="gap-0 py-0">
          <CardHeader className="px-4 py-3 pb-1.5 lg:px-4 lg:py-3 lg:pb-1.5">
            <CardDescription className="text-[13px] leading-4">In Progress</CardDescription>
            <CardTitle className="text-xl font-bold leading-none tabular-nums sm:text-2xl">{activeCount}</CardTitle>
          </CardHeader>
          <CardContent className="px-4 pb-3 pt-0 lg:px-4 lg:pb-3"><p className="text-[11px] leading-4 text-muted-foreground">Running or verifying</p></CardContent>
        </Card>
        <Card className="gap-0 py-0">
          <CardHeader className="px-4 py-3 pb-1.5 lg:px-4 lg:py-3 lg:pb-1.5">
            <CardDescription className="text-[13px] leading-4">Completed</CardDescription>
            <CardTitle className="text-xl font-bold leading-none tabular-nums sm:text-2xl">{completedCount}</CardTitle>
          </CardHeader>
          <CardContent className="px-4 pb-3 pt-0 lg:px-4 lg:pb-3"><p className="text-[11px] leading-4 text-muted-foreground">Successfully completed</p></CardContent>
        </Card>
        <Card className="gap-0 py-0">
          <CardHeader className="px-4 py-3 pb-1.5 lg:px-4 lg:py-3 lg:pb-1.5">
            <CardDescription className="text-[13px] leading-4">Needs Attention</CardDescription>
            <CardTitle className="text-xl font-bold leading-none tabular-nums sm:text-2xl">{attentionCount}</CardTitle>
          </CardHeader>
          <CardContent className="px-4 pb-3 pt-0 lg:px-4 lg:pb-3"><p className="text-[11px] leading-4 text-muted-foreground">Failed or verification issues</p></CardContent>
        </Card>
      </div>

      <div className="dashboard-motion-item dashboard-motion-delay-1">
        {hasActiveCard ? (
          <Card>
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between gap-4">
                <CardTitle className="text-lg">
                  {activeMigration?.status === "running" || activeMigration?.status === "verifying"
                    ? "Active migration"
                    : "Latest migration"}
                </CardTitle>
                {statusBadge(activeMigration?.status)}
              </div>
              <CardDescription>
                {activeMigration ? (
                  <span className="text-xs">
                    ID <span className="font-mono">{activeMigration.id}</span>{" "}
                    {visibleSyncMessage(activeMigration.syncMessage) ? `- ${visibleSyncMessage(activeMigration.syncMessage)}` : ""}
                  </span>
                ) : null}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <div className="flex items-center justify-between text-sm">
                  <span>Progress</span>
                  <span className="font-medium">{totals.percent.toFixed(1)}%</span>
                </div>
                <Progress value={totals.percent} className="h-2" />
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>
                    {formatNumber(totals.transferred)} transferred
                    {totals.skipped > 0 ? `, ${formatNumber(totals.skipped)} skipped` : ""}
                    {totals.failed > 0 ? `, ${formatNumber(totals.failed)} failed` : ""}
                  </span>
                  <span>{formatNumber(totals.totalObjects)} total objects</span>
                </div>
              </div>

              <div className="flex flex-wrap gap-2">
                {activeMigration ? (() => {
                  const workerPool = activeMigration.options.executionMode === "migration_workers"
                  const action = activeMigration.status === "draft"
                    ? "start"
                    : ["running", "verifying"].includes(activeMigration.status)
                      ? "cancel_migration"
                      : ["failed", "canceled", "verification_failed"].includes(activeMigration.status)
                        ? "retry_migration"
                        : null
                  if (!action) return null
                  const loading = busyAction === action
                  const label = action === "start" ? "Start" : action === "cancel_migration" ? "Stop" : workerPool ? "Restart" : "Rerun"
                  return (
                    <Button
                      onClick={() => action === "start" ? void startMigration() : void runMigrationAction(action)}
                      loading={loading}
                      disabled={Boolean(busyAction)}
                      variant={label === "Stop" ? "outline" : "default"}
                    >
                      {!loading ? (label === "Stop" ? <Square className="h-4 w-4 mr-0" /> : <Play className="h-4 w-4 mr-0" />) : null}
                      {label}
                    </Button>
                  )
                })() : null}
                {activeMigration ? (
                  <Button
                    onClick={() => router.push(`/dashboard/migrations/${encodeURIComponent(activeMigration.id)}`)}
                    variant="outline"
                  >
                    <ExternalLink className="h-4 w-4 mr-0" />
                    Details
                  </Button>
                ) : null}
              </div>
            </CardContent>
          </Card>
        ) : null}
      </div>

      <DashboardDataTable
        data={filteredMigrations}
        columns={columns}
        minWidth="1180px"
        emptyState={search.trim() ? "No migrations match your search." : "No migrations yet."}
        resetKey={search}
        className="dashboard-motion-delay-2"
      />

      <AlertDialog open={Boolean(deleteId)} onOpenChange={(open) => (!open && busyAction !== "delete" ? setDeleteId(null) : null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete migration?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the migration and its stored items from the database. It does not cancel Cloudflare jobs that may already be running.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busyAction === "delete"}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={(event) => {
                event.preventDefault()
                void confirmDelete()
              }}
              disabled={busyAction === "delete"}
            >
              {busyAction === "delete" ? <Spinner className="mr-2" /> : null}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </DashboardPage>
  )
}




