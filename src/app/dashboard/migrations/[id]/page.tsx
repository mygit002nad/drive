"use client"

import * as React from "react"
import type { ColumnDef } from "@tanstack/react-table"
import { useParams, useRouter } from "next/navigation"
import {
  AlertCircle,
  CalendarDays,
  CheckCircle2,
  CircleX,
  Clock,
  GripVertical,
  Pause,
  Play,
  RefreshCw,
  ScrollText,
  Square,
  ShieldCheck,
  Trash2,
  ListTodo,
  Users,
} from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
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
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Skeleton } from "@/components/ui/skeleton"
import { Spinner } from "@/components/ui/spinner"
import { DashboardLogsCard } from "@/components/dashboard/logs-card"
import { DashboardDataTable } from "@/components/dashboard/data-table"
import {
  getBucketDisplayStatusRank,
  getMergedBucketSnapshot,
  getItemDisplayStatus,
  getItemStatus,
  isAbortedStatus,
  isCompletedStatus,
  isFailedLikeStatus,
  isTerminalBucketDisplayStatus,
  isRecord,
  normalizeStatus,
  readLiveBucketState,
  readRepairWorkerState,
  readVerifyState,
} from "@/lib/migration-bucket-state"
import { cn } from "@/lib/utils"
import { migrationProgressPercent } from "@/lib/migration-progress"
import { getMigrationReadOnlyState } from "@/lib/migration-read-only"

type Account = {
  id: string
  label: string
  email: string
  status: "active" | "available" | "disabled"
}

type Migration = {
  id: string
  sourceAccountId: string
  targetAccountId: string
  status: "draft" | "running" | "verifying" | "completed" | "failed" | "verification_failed" | "canceled"
  options: {
    executionMode?: "super_slurper" | "migration_workers"
    workerGeneration?: number
    workerShardCount?: number
    overwrite?: boolean
    concurrency?: number
    pathPrefix?: string | null
    manualCompleted?: boolean
    targetActivatedAt?: string
    historyReadOnlyAt?: string
    historyReadOnlyReason?: string
  }
  createdAt: string
  updatedAt?: string
  startedAt?: string
  completedAt?: string
  lastSyncedAt?: string
  syncStatus?: "idle" | "syncing" | "ok" | "error"
  syncMessage?: string
  summaryItemCount: number
  summaryObjects: number
  summaryBytes: number
  workerSummary: { workerRuns?: unknown[] }
  detailsCompactedAt?: string
}

type MigrationItem = {
  id: string
  sourceBucket: string
  targetBucket: string
  slurperJobId?: string
  slurperStatus?: string
  verificationState?: {
    generation: number
    status: string
    missingObjects: number
    mismatchedObjects: number
    extraObjects: number
    attemptId?: string
    strictDestination: boolean
    updatedAt?: string
  }
  progress: Record<string, unknown>
  sourceObjects?: number
  sourceBytes?: number
  updatedAt?: string
}

type FailedObjectDiagnostic = {
  key: string
  message: string
  at?: string | null
  source: {
    exists: boolean | null
    size?: number
    etag?: string
    lastModified?: string
    contentType?: string
    readable?: boolean | null
    error?: string
  }
  destination: {
    exists: boolean | null
    size?: number
    etag?: string
    lastModified?: string
    contentType?: string
    readable?: boolean | null
    error?: string
  }
  diagnosis: {
    category: string
    reason: string
    recommendation: string
  }
  download?: {
    source?: string | null
    destination?: string | null
  }
}

type FailedDiagnosticsBucket = {
  missing?: boolean
  item: {
    id: string
    sourceBucket: string
    targetBucket: string
    jobId?: string | null
  }
  summary: {
    totalFailedEntries: number
    detailedFailedEntries?: number
    cloudflareDetailedEntries?: number
    fallbackDetailedEntries?: number
    inferredDetailedEntries?: number
    missingDetailedEntries?: number
    sourceMissing: number
    sourceAccessIssues: number
    destinationExists: number
    transientOrProviderIssues: number
    unknown: number
  }
  failures: FailedObjectDiagnostic[]
}

type MigrationWorkerRun = {
  id: string
  jobId?: string
  agentId: string
  workerGeneration?: number
  abortRequested?: boolean
  status: string
  online: boolean
  externalRunId?: string
  instanceId?: string
  currentFile?: Record<string, unknown>
  currentStatus?: string
  lastHeartbeatAt?: string
  completedFiles: number
  failedFiles: number
  completedBytes: number
  createdAt: string
  updatedAt: string
}

function getEffectiveSourceBytes(
  item: MigrationItem,
  repairState?: ReturnType<typeof readRepairWorkerState> | null
): number {
  if (repairState?.details && typeof repairState.details.sourceBytes === "number") return Number(repairState.details.sourceBytes)
  return typeof item.sourceBytes === "number" ? item.sourceBytes : 0
}

function formatNumber(value: number | undefined): string {
  if (!value || value <= 0) return "0"
  return Intl.NumberFormat().format(value)
}

function formatDate(value?: string): string {
  if (!value) return "-"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
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

function statusBadge(
  status: string | undefined,
  opts?: { hadProgress?: boolean; syncStatus?: string; syncMessage?: string }
) {
  const s = String(status ?? "unknown")
  if (s === "verifying" && opts?.syncStatus === "error") {
    return <Badge className="bg-red-600">{opts.syncMessage?.toLowerCase().includes("settings sync") ? "Settings sync failed" : "Verification failed"}</Badge>
  }
  if (s === "verifying" && opts?.syncMessage?.toLowerCase().includes("syncing settings")) {
    return <Badge className="bg-purple-600">Settings sync</Badge>
  }
  if (s === "settings_syncing") return <Badge className="bg-purple-600">Settings sync</Badge>
  if (s === "settings_failed") return <Badge className="bg-red-600">Settings sync failed</Badge>
  if (s === "completed") return <Badge className="bg-green-600">Completed</Badge>
  if (s === "verifying") return <Badge className="bg-purple-600">Verifying</Badge>
  if (s === "scanning") return <Badge className="bg-sky-600">Scanning</Badge>
  if (s === "queued") return <Badge variant="secondary">Queued</Badge>
  if (s === "running") return <Badge className="bg-primary text-primary-foreground">Running</Badge>
  if (s === "progress_fetch_failed") return <Badge className="bg-primary text-primary-foreground">Running</Badge>
  if (s === "paused") return <Badge className="bg-yellow-600">Paused</Badge>
  if (s === "failed") return <Badge className="bg-red-600">Failed</Badge>
  if (s === "verification_failed") return <Badge className="bg-red-600">Verification failed</Badge>
  if (s === "no_files") return <Badge className="bg-green-600">Completed</Badge>
  if (s === "canceled" || s === "aborted" || s === "copy_aborted") {
    const hadProgress = Boolean(opts?.hadProgress)
    return <Badge variant="secondary">{hadProgress ? "Aborted" : "Canceled"}</Badge>
  }
  if (s === "draft") return <Badge variant="outline">Draft</Badge>
  if (s === "creating_job") return <Badge className="bg-primary text-primary-foreground">Creating job</Badge>
  if (s === "job_id_pending") return <Badge className="bg-yellow-600">Job pending</Badge>
  if (s === "precheck_failed") return <Badge className="bg-red-600">Precheck failed</Badge>
  if (s.endsWith("_failed") || s.includes("error")) return <Badge className="bg-red-600">Error</Badge>
  return <Badge variant="outline">{s}</Badge>
}

function migrationWorkerBadge(status: string | undefined) {
  const value = String(status || "").toLowerCase()
  if (value === "completed") return <Badge className="bg-green-600">Worker completed</Badge>
  if (value === "deploying") return <Badge className="bg-yellow-600">Worker deploying</Badge>
  if (value === "aborting") return <Badge className="bg-yellow-600">Worker stopping</Badge>
  if (value === "running") return <Badge className="bg-primary text-primary-foreground">Worker running</Badge>
  if (value === "verifying") return <Badge className="bg-purple-600">Worker verifying</Badge>
  if (value === "claimed") return <Badge className="bg-sky-600">Worker claimed</Badge>
  if (value === "pending" || value === "queued") return <Badge variant="secondary">Worker queued</Badge>
  if (value === "failed") return <Badge className="bg-red-600">Worker failed</Badge>
  if (value === "canceled" || value === "aborted") return <Badge variant="outline">Worker stopped</Badge>
  return <Badge variant="outline">{value || "Worker idle"}</Badge>
}

function migrationStatusBadge(status: string | undefined) {
  const s = String(status ?? "unknown").trim().toLowerCase()
  if (s === "completed") return <Badge className="bg-green-600">Completed</Badge>
  if (s === "verification_failed") return <Badge className="bg-red-600">Verification failed</Badge>
  if (s === "failed") return <Badge className="bg-red-600">Failed</Badge>
  if (["canceled", "cancelled", "aborted"].includes(s)) return <Badge variant="secondary">Aborted</Badge>
  if (s === "draft") return <Badge variant="outline">Draft</Badge>
  if (s === "paused") return <Badge className="bg-yellow-600">Paused</Badge>
  if (["running", "verifying", "scanning", "queued", "creating_job", "job_id_pending"].includes(s)) return <Badge className="bg-primary text-primary-foreground">Running</Badge>
  return <Badge variant="outline">{s}</Badge>
}

function mergeIncomingItem(prev: MigrationItem | undefined, next: MigrationItem): MigrationItem {
  if (!prev) return next

  const previousItemTime = Date.parse(String(prev.updatedAt || ""))
  const nextItemTime = Date.parse(String(next.updatedAt || ""))
  if (Number.isFinite(previousItemTime) && Number.isFinite(nextItemTime) && nextItemTime < previousItemTime) return prev

  const prevProgress = isRecord(prev.progress) ? (prev.progress as Record<string, unknown>) : {}
  const nextProgress = isRecord(next.progress) ? (next.progress as Record<string, unknown>) : {}
  const prevFileVerification = isRecord(prevProgress.fileVerification) ? prevProgress.fileVerification : null
  const nextFileVerification = isRecord(nextProgress.fileVerification) ? nextProgress.fileVerification : null
  const verificationWasExplicitlyRequeued =
    nextFileVerification?.status === "pending" &&
    typeof nextFileVerification.requestedAt === "string" &&
    nextFileVerification.requestedAt !== prevFileVerification?.requestedAt
  if (verificationWasExplicitlyRequeued) return next

  const prevLive = readLiveBucketState(prevProgress)
  const nextLive = readLiveBucketState(nextProgress)

  if (!prevLive || !nextLive) return next

  const sameSlurperJob = (prevLive.slurperJobId ?? prev.slurperJobId ?? null) === (nextLive.slurperJobId ?? next.slurperJobId ?? null)
  const sameRepairJob = (prevLive.repairJobId ?? null) === (nextLive.repairJobId ?? null)
  const prevQueue = isRecord(prevProgress.migrationQueue) ? prevProgress.migrationQueue : null
  const nextQueue = isRecord(nextProgress.migrationQueue) ? nextProgress.migrationQueue : null
  const prevGeneration = typeof prevQueue?.generation === "number" ? prevQueue.generation : null
  const nextGeneration = typeof nextQueue?.generation === "number" ? nextQueue.generation : null
  const isWorkerStage = (stage: string | null) => stage === "migration" || stage === "verification"
  const sameWorkerGeneration = isWorkerStage(prevLive.workerStage) && isWorkerStage(nextLive.workerStage) &&
    (prevGeneration === null || nextGeneration === null || prevGeneration === nextGeneration)
  const sameJobCycle = Boolean(prevLive.slurperJobId || prevLive.repairJobId || nextLive.slurperJobId || nextLive.repairJobId) && sameSlurperJob && sameRepairJob
  const sameCycle = sameWorkerGeneration || sameJobCycle
  if (!sameCycle) return next

  const prevUpdatedAt = prevLive.updatedAt ? Date.parse(prevLive.updatedAt) : NaN
  const nextUpdatedAt = nextLive.updatedAt ? Date.parse(nextLive.updatedAt) : NaN
  if (Number.isFinite(prevUpdatedAt) && Number.isFinite(nextUpdatedAt) && nextUpdatedAt < prevUpdatedAt) {
    return prev
  }

  const mergedLive = {
    ...nextLive,
    totalObjects: Math.max(prevLive.totalObjects, nextLive.totalObjects),
    transferredObjects: Math.max(prevLive.transferredObjects, nextLive.transferredObjects),
    transferredBytes: Math.max(prevLive.transferredBytes, nextLive.transferredBytes),
    skippedObjects: isTerminalBucketDisplayStatus(nextLive.status)
      ? nextLive.skippedObjects
      : Math.max(prevLive.skippedObjects, nextLive.skippedObjects),
    failedObjects: isTerminalBucketDisplayStatus(nextLive.status)
      ? nextLive.failedObjects
      : Math.max(prevLive.failedObjects, nextLive.failedObjects),
    verifyIssues: isTerminalBucketDisplayStatus(nextLive.status)
      ? nextLive.verifyIssues
      : Math.max(prevLive.verifyIssues, nextLive.verifyIssues),
  }

  if (isTerminalBucketDisplayStatus(prevLive.status) && !isTerminalBucketDisplayStatus(nextLive.status)) {
    mergedLive.status = prevLive.status
  } else if (
    !isTerminalBucketDisplayStatus(prevLive.status) &&
    !isTerminalBucketDisplayStatus(nextLive.status) &&
    normalizeStatus(nextLive.status) !== "queued" &&
    getBucketDisplayStatusRank(prevLive.status) > getBucketDisplayStatusRank(nextLive.status)
  ) {
    mergedLive.status = prevLive.status
  }

  return {
    ...next,
    progress: {
      ...nextProgress,
      live: {
        ...(nextProgress.live && isRecord(nextProgress.live) ? (nextProgress.live as Record<string, unknown>) : {}),
        ...mergedLive,
      },
    },
  }
}

function mergeIncomingItems(prevItems: MigrationItem[], nextItems: MigrationItem[]): MigrationItem[] {
  const prevById = new Map(prevItems.map((item) => [item.id, item]))
  return nextItems.map((item) => mergeIncomingItem(prevById.get(item.id), item))
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

function mergeItemLogs(item: MigrationItem | null): string {
  if (!item) return "Select a bucket to view logs."
  const logs = isRecord(item.progress) ? (item.progress as Record<string, unknown>).logs : undefined
  if (!logs) {
    const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
    const verify = isRecord(progress.verify) ? (progress.verify as Record<string, unknown>) : null
    const verifySamples = isRecord(progress.verifySamples) ? (progress.verifySamples as Record<string, unknown>) : null
    const hasVerifyInfo =
      (typeof progress.error === "string" && progress.error.trim().length > 0) ||
      (verify && typeof verify.status === "string") ||
      Boolean(verifySamples)

    if (hasVerifyInfo) {
      const dump: Record<string, unknown> = {
        status: getItemDisplayStatus(item) ?? getItemStatus(item) ?? null,
        error: progress.error ?? progress.lastError ?? null,
        verify: verify ?? null,
        verifySamples: verifySamples ?? null,
      }
      try {
        return JSON.stringify(dump, null, 2)
      } catch {
        return String(dump)
      }
    }

    return 'No logs fetched yet. Use "Logs" on a bucket row.'
  }
  try {
    return JSON.stringify(logs, null, 2)
  } catch {
    return String(logs)
  }
}

function buildMigrationLogDump(items: MigrationItem[]): string {
  const chunks: string[] = []
  for (const item of items) {
    const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
    const events = Array.isArray(progress.events) ? progress.events : null
    const entry: Record<string, unknown> = {
      bucket: item.sourceBucket,
      jobId: item.slurperJobId ?? null,
      status: getItemDisplayStatus(item) ?? getItemStatus(item) ?? null,
      lastProgressAt: progress.lastProgressAt ?? null,
      stage: progress.stage ?? null,
      error: progress.error ?? progress.lastError ?? null,
      events,
      logs: progress.logs ?? null,
    }

    // Only include entries that have something interesting.
    const hasInteresting =
      Boolean(entry.jobId) ||
      Boolean(entry.error) ||
      Boolean(entry.logs) ||
      String(entry.status ?? "").toLowerCase() === "precheck_failed" ||
      String(entry.status ?? "").toLowerCase().includes("failed") ||
      String(entry.status ?? "").toLowerCase().includes("error")

    if (!hasInteresting) continue

    chunks.push(JSON.stringify(entry, null, 2))
  }

  if (chunks.length === 0) return "No logs yet."
  return chunks.join("\n\n---\n\n")
}

function mergeItemLogsFull(item: MigrationItem | null): string {
  if (!item) return "Select a bucket to view logs."
  const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
  const events = Array.isArray(progress.events) ? progress.events : []
  const dump: Record<string, unknown> = {
    bucket: item.sourceBucket,
    jobId: item.slurperJobId ?? null,
    status: getItemDisplayStatus(item) ?? getItemStatus(item) ?? null,
    events,
    cloudflareLogs: progress.logs ?? null,
    slurper: progress.slurper ?? null,
  }
  try {
    return JSON.stringify(dump, null, 2)
  } catch {
    return String(dump)
  }
}

type LogLine = {
  at: number
  atIso: string
  bucket: string
  stage: string
  status: string
  message: string
  verificationGeneration?: number
  verificationAttemptUnknown?: boolean
  verificationHistorical?: boolean
}

function mergeIncomingMigration(prev: Migration | null, next: Migration): Migration {
  if (!prev) return next
  const previousTime = Date.parse(String(prev.updatedAt || ""))
  const nextTime = Date.parse(String(next.updatedAt || ""))
  if (Number.isFinite(previousTime) && Number.isFinite(nextTime) && nextTime < previousTime) return prev
  return next
}

function collectLogLines(items: MigrationItem[], workerRuns: MigrationWorkerRun[] = []): LogLine[] {
  const lines: LogLine[] = []

  for (const item of items) {
    const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
    const bucket = item.sourceBucket
    const events = Array.isArray(progress.events) ? (progress.events as unknown[]) : []
    let previousEventSignature = ""

    for (const event of events) {
      if (!isRecord(event)) continue
      const atIso = typeof event.at === "string" ? event.at : ""
      const at = atIso ? Date.parse(atIso) : NaN
      if (!Number.isFinite(at)) continue
      const stage = typeof event.stage === "string" ? event.stage : ""
      const status = typeof event.status === "string" ? event.status : String(event.status ?? "")
      const rawMessage = typeof event.message === "string" ? event.message : ""
      const eventGeneration = typeof event.generation === "number" && Number.isInteger(event.generation) && event.generation > 0
        ? event.generation
        : undefined
      const eventAttemptId = typeof event.attemptId === "string" && event.attemptId.trim() ? event.attemptId : undefined
      const isVerificationEvent = stage.startsWith("file_verification")
      const currentVerificationGeneration = item.verificationState?.generation
      const currentVerificationAttemptId = item.verificationState?.attemptId
      const message = stage === "super_slurper_completed" && rawMessage === "Bucket migration completed"
        ? "Super Slurper transfer completed; File Scanner verification pending"
        : rawMessage
      const signature = JSON.stringify([stage, status, message, eventGeneration, eventAttemptId])
      if (signature === previousEventSignature) continue
      previousEventSignature = signature
      lines.push({ at, atIso, bucket, stage, status, message,
        ...(isVerificationEvent ? {
          verificationGeneration: eventGeneration,
          verificationAttemptUnknown: eventAttemptId === undefined,
          verificationHistorical:
            (eventGeneration !== undefined && currentVerificationGeneration !== undefined && eventGeneration !== currentVerificationGeneration) ||
            (eventAttemptId !== undefined && currentVerificationAttemptId !== undefined && eventAttemptId !== currentVerificationAttemptId),
        } : {}) })
    }

    if (events.length === 0) {
      const stage = typeof progress.stage === "string" ? progress.stage : ""
      const status = String(getItemDisplayStatus(item) ?? getItemStatus(item) ?? "")
      const message =
        typeof progress.error === "string"
          ? (progress.error as string)
          : typeof progress.lastError === "string"
            ? (progress.lastError as string)
            : ""
      if (stage || status || message) {
        const atIso =
          typeof progress.lastProgressAt === "string"
            ? (progress.lastProgressAt as string)
            : new Date().toISOString()
        const at = Date.parse(atIso)
        lines.push({ at, atIso, bucket, stage, status, message })
      }
    }
  }

  void workerRuns
  lines.sort((a, b) => a.at - b.at)
  return lines
}

function formatLogTime(iso: string): string {
  const t = Date.parse(iso)
  if (!Number.isFinite(t)) return iso
  return new Date(t).toLocaleString()
}

function ProgressStacked({
  transferredPct,
  skippedPct,
  failedPct,
  unaccountedPct,
}: {
  transferredPct: number
  skippedPct: number
  failedPct: number
  unaccountedPct: number
}) {
  const safeTransferred = Number.isFinite(transferredPct) ? Math.max(0, Math.min(100, transferredPct)) : 0
  const safeSkipped = Number.isFinite(skippedPct) ? Math.max(0, Math.min(100 - safeTransferred, skippedPct)) : 0
  const safeFailed = Number.isFinite(failedPct)
    ? Math.max(0, Math.min(100 - safeTransferred - safeSkipped, failedPct))
    : 0
  const safeUnaccounted = Number.isFinite(unaccountedPct)
    ? Math.max(0, Math.min(100 - safeTransferred - safeSkipped - safeFailed, unaccountedPct))
    : 0
  return (
    <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
      <div className="flex h-full w-full">
        <div className="h-full bg-primary" style={{ width: `${safeTransferred}%` }} />
        <div className="h-full bg-yellow-500" style={{ width: `${safeSkipped}%` }} />
        <div className="h-full bg-red-500" style={{ width: `${safeFailed}%` }} />
        <div className="h-full bg-muted-foreground/45" style={{ width: `${safeUnaccounted}%` }} />
      </div>
    </div>
  )
}

function formatDiagnosticCategory(category: string | undefined): string {
  const value = String(category || "").trim()
  if (!value) return "Unclassified"
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function diagnosticCategoryTone(category: string | undefined): string {
  return "text-foreground border-border bg-muted/50"
}

function StickyScrollPanel({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}) {
  return (
    <div
      className={cn(
        "min-h-0 overflow-auto [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden",
        className
      )}
    >
      {children}
    </div>
  )
}

function PinnedScrollableTable({
  children,
  className,
  maxHeightClassName = "max-h-[34rem]",
}: {
  children: React.ReactNode
  className?: string
  maxHeightClassName?: string
}) {
  const viewportRef = React.useRef<HTMLDivElement | null>(null)
  const contentRef = React.useRef<HTMLDivElement | null>(null)
  const horizontalRef = React.useRef<HTMLDivElement | null>(null)
  const [sizes, setSizes] = React.useState({ scrollWidth: 0, clientWidth: 0 })

  React.useEffect(() => {
    const viewport = viewportRef.current
    const content = contentRef.current
    if (!viewport || !content) return

    const update = () => {
      const next = {
        scrollWidth: Math.max(viewport.scrollWidth, content.scrollWidth),
        clientWidth: viewport.clientWidth,
      }
      setSizes((prev) => (prev.scrollWidth === next.scrollWidth && prev.clientWidth === next.clientWidth ? prev : next))
    }

    update()
    const resizeObserver = typeof ResizeObserver !== "undefined" ? new ResizeObserver(() => update()) : null
    resizeObserver?.observe(viewport)
    resizeObserver?.observe(content)
    window.addEventListener("resize", update)

    return () => {
      resizeObserver?.disconnect()
      window.removeEventListener("resize", update)
    }
  }, [children])

  const syncFromViewport = React.useCallback(() => {
    if (!viewportRef.current || !horizontalRef.current) return
    if (horizontalRef.current.scrollLeft !== viewportRef.current.scrollLeft) {
      horizontalRef.current.scrollLeft = viewportRef.current.scrollLeft
    }
  }, [])

  const syncFromBar = React.useCallback(() => {
    if (!viewportRef.current || !horizontalRef.current) return
    if (viewportRef.current.scrollLeft !== horizontalRef.current.scrollLeft) {
      viewportRef.current.scrollLeft = horizontalRef.current.scrollLeft
    }
  }, [])

  const showHorizontal = sizes.scrollWidth > sizes.clientWidth + 1

  return (
    <div className={cn("min-h-0", className)}>
      <div
        ref={viewportRef}
        className={cn(
          "overflow-x-scroll overflow-y-auto overscroll-contain [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden",
          maxHeightClassName
        )}
        onScroll={syncFromViewport}
      >
        <div ref={contentRef} className="min-w-fit">
          {children}
        </div>
      </div>
      <div className="sticky bottom-0 z-20 border-t bg-background/95 supports-[backdrop-filter]:bg-background/80">
        {showHorizontal ? (
          <div
            ref={horizontalRef}
            className="overflow-x-scroll overflow-y-hidden px-0 py-0 [scrollbar-width:thin] [scrollbar-color:#0b0b0c_#d4d4d8] dark:[scrollbar-color:#f4f4f5_#27272a] [&::-webkit-scrollbar]:h-3 [&::-webkit-scrollbar-track]:bg-neutral-200 dark:[&::-webkit-scrollbar-track]:bg-neutral-900 [&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-neutral-950 dark:[&::-webkit-scrollbar-thumb]:bg-neutral-100"
            onScroll={syncFromBar}
          >
            <div style={{ width: sizes.scrollWidth, height: 1 }} />
          </div>
        ) : (
          <div className="h-3" />
        )}
      </div>
    </div>
  )
}

const DIAGNOSTICS_TABLE_GRID_TEMPLATE = "340px 280px 180px 180px 220px 180px minmax(320px, 1fr)"

export default function MigrationDetailsPage() {
  const router = useRouter()
  const params = useParams<{ id: string }>()
  const id = typeof params?.id === "string" ? params.id : ""

  const [accounts, setAccounts] = React.useState<Account[]>([])
  const [migration, setMigration] = React.useState<Migration | null>(null)
  const [items, setItems] = React.useState<MigrationItem[]>([])
  const [error, setError] = React.useState<string | null>(null)
  const [initialLoading, setInitialLoading] = React.useState(true)
  const [busyAction, setBusyAction] = React.useState<string | null>(null)
  const [busyItemAction, setBusyItemAction] = React.useState<Record<string, string>>({})
  const [deleteOpen, setDeleteOpen] = React.useState(false)
  const [manualCompleteOpen, setManualCompleteOpen] = React.useState(false)
  const [logsOpen, setLogsOpen] = React.useState(false)
  const [logsItemId, setLogsItemId] = React.useState<string | null>(null)
  const [failedOpen, setFailedOpen] = React.useState(false)
  const [failedItemId, setFailedItemId] = React.useState<string | null>(null)
  const [failedScope, setFailedScope] = React.useState<"single" | "all">("single")
  const [failedLoading, setFailedLoading] = React.useState(false)
  const [failedData, setFailedData] = React.useState<FailedDiagnosticsBucket[]>([])
  const [workerRuns, setWorkerRuns] = React.useState<MigrationWorkerRun[]>([])

  const bucketLogsRef = React.useRef<HTMLDivElement | null>(null)

  const [bucketLogCols, setBucketLogCols] = React.useState<{ time: number; stage: number }>({
    time: 170,
    stage: 180,
  })

  const resizeRef = React.useRef<
    | {
        kind: "bucket"
        key: "time" | "stage"
        pointerId: number
        startX: number
        startWidth: number
      }
    | null
  >(null)

  React.useEffect(() => {
    try {
      const raw = localStorage.getItem("drive:bucketLogsCols:v1")
      if (raw) {
        const parsed = JSON.parse(raw) as Partial<typeof bucketLogCols>
        setBucketLogCols((prev) => ({
          time: typeof parsed.time === "number" ? parsed.time : prev.time,
          stage: typeof parsed.stage === "number" ? parsed.stage : prev.stage,
        }))
      }
    } catch {
      // ignore
    }
  }, [])

  React.useEffect(() => {
    const onMove = (e: PointerEvent) => {
      const active = resizeRef.current
      if (!active) return
      if (e.pointerId !== active.pointerId) return
      const delta = e.clientX - active.startX
      const nextWidth = Math.max(110, active.startWidth + delta)

      setBucketLogCols((prev) => {
        const next = { ...prev, [active.key]: nextWidth } as typeof prev
        try {
          localStorage.setItem("drive:bucketLogsCols:v1", JSON.stringify(next))
        } catch {
          // ignore
        }
        return next
      })
    }

    const onUp = () => {
      resizeRef.current = null
    }

    window.addEventListener("pointermove", onMove)
    window.addEventListener("pointerup", onUp)
    window.addEventListener("pointercancel", onUp)
    return () => {
      window.removeEventListener("pointermove", onMove)
      window.removeEventListener("pointerup", onUp)
      window.removeEventListener("pointercancel", onUp)
      resizeRef.current = null
    }
  }, [])

  React.useEffect(() => {
    if (!error) return
    const t = setTimeout(() => setError(null), 4500)
    return () => clearTimeout(t)
  }, [error])

  const accountLabelById = React.useMemo(() => {
    const map = new Map<string, string>()
    for (const a of accounts) map.set(a.id, a.label)
    return map
  }, [accounts])

  const dialogLogItem = React.useMemo(
    () => items.find((i) => i.id === logsItemId) ?? null,
    [items, logsItemId]
  )
  const dialogFailedItem = React.useMemo(
    () => items.find((i) => i.id === failedItemId) ?? null,
    [items, failedItemId]
  )
  const missingFailedSelections = React.useMemo(
    () => failedData.filter((group) => group.missing),
    [failedData]
  )

  const logLines = React.useMemo(() => collectLogLines(items, workerRuns), [items, workerRuns])
  const bucketLogLines = React.useMemo(
    () => collectLogLines(dialogLogItem ? [dialogLogItem] : [], workerRuns),
    [dialogLogItem, workerRuns]
  )
  const diagnosticsSummary = React.useMemo(() => {
    return {
      buckets: failedData.length,
      totalFailedEntries: failedData.reduce((sum, group) => sum + (group.summary.totalFailedEntries || 0), 0),
      detailedEntries: failedData.reduce((sum, group) => sum + (group.summary.detailedFailedEntries || 0), 0),
      cloudflareEvidence: failedData.reduce((sum, group) => sum + (group.summary.cloudflareDetailedEntries || 0), 0),
      fallbackEvidence: failedData.reduce((sum, group) => sum + (group.summary.fallbackDetailedEntries || 0), 0),
      inferredEvidence: failedData.reduce((sum, group) => sum + (group.summary.inferredDetailedEntries || 0), 0),
      unresolved: failedData.reduce((sum, group) => sum + (group.summary.missingDetailedEntries || 0), 0),
      sourceMissing: failedData.reduce((sum, group) => sum + (group.summary.sourceMissing || 0), 0),
      sourceAccessIssues: failedData.reduce((sum, group) => sum + (group.summary.sourceAccessIssues || 0), 0),
      destinationExists: failedData.reduce((sum, group) => sum + (group.summary.destinationExists || 0), 0),
      transientIssues: failedData.reduce((sum, group) => sum + (group.summary.transientOrProviderIssues || 0), 0),
    }
  }, [failedData])

  React.useEffect(() => {
    const el = bucketLogsRef.current
    if (!el) return
    if (!logsOpen) return
    el.scrollTop = el.scrollHeight
  }, [bucketLogLines.length, logsOpen])

  const getBucketSnapshot = React.useCallback((item: MigrationItem) => {
    const snapshot = getMergedBucketSnapshot(item)
    if (["canceled", "cancelled", "aborted"].includes(normalizeStatus(migration?.status))) {
      return { ...snapshot, displayStatus: "aborted", queued: 0 }
    }
    return snapshot
  }, [migration?.status])

  const readBucketSettingsStatus = (item: MigrationItem): "syncing" | "synced" | "failed" | null => {
    const progress = isRecord(item.progress) ? item.progress : {}
    const orchestratorSettings = isRecord(progress.orchestratorSettings) ? progress.orchestratorSettings : null
    const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
    const status = orchestratorSettings?.status ?? settingsSync?.status
    if (status === "syncing") return "syncing"
    if (status === "synced" || status === "completed") return "synced"
    if (status === "failed") return "failed"
    if (status === "pending") return "syncing"
    return null
  }
  const showSettingsColumn = items.some((item) => readBucketSettingsStatus(item) !== null)

  const bucketCounts = React.useMemo(() => {
    let completed = 0
    let failed = 0
    let verificationFailed = 0
    let aborted = 0
    let running = 0
    let scanning = 0
    let verifying = 0

    for (const item of items) {
      const s = getBucketSnapshot(item).displayStatus
      if (normalizeStatus(s) === "scanning") scanning += 1
      else if (normalizeStatus(s) === "verifying") verifying += 1
      else if (normalizeStatus(s) === "verification_failed") verificationFailed += 1
      else if (isCompletedStatus(s)) completed += 1
      else if (isAbortedStatus(s)) aborted += 1
      else if (isFailedLikeStatus(s)) failed += 1
      else if (normalizeStatus(s)) running += 1
    }

    return { completed, failed, verificationFailed, aborted, running, scanning, verifying, total: items.length }
  }, [getBucketSnapshot, items])

  const failedBuckets = React.useMemo(
    () =>
      items.filter((item) => {
        const snapshot = getBucketSnapshot(item)
        const s = String(snapshot.displayStatus ?? getItemStatus(item) ?? "").toLowerCase()
        return snapshot.failed > 0 || snapshot.verifyIssues > 0 || s.includes("failed") || s.includes("error")
      }),
    [getBucketSnapshot, items]
  )

  const totals = React.useMemo(() => {
    let totalObjects = 0
    let transferred = 0
    let skipped = 0
    let copyFailed = 0
    let rawUnaccounted = 0
    let verifyIssues = 0
    let totalBytes = 0

    for (const item of items) {
      const snapshot = getBucketSnapshot(item)
      const progress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
      const repairState = readRepairWorkerState(progress)
      totalObjects += snapshot.total
      transferred += snapshot.transferred
      skipped += snapshot.skipped
      copyFailed += snapshot.failed
      rawUnaccounted += snapshot.unaccounted
      verifyIssues += snapshot.verifyIssues
      totalBytes += getEffectiveSourceBytes(item, repairState)
    }

    const resolvedTransferred = totalObjects > 0 ? Math.min(totalObjects, transferred) : transferred
    const resolvedSkipped = totalObjects > 0 ? Math.min(Math.max(0, totalObjects - resolvedTransferred), skipped) : skipped
    const remainingAfterTransferSkip =
      totalObjects > 0 ? Math.max(0, totalObjects - Math.min(totalObjects, resolvedTransferred + resolvedSkipped)) : 0
    const resolvedCopyFailed = totalObjects > 0 ? Math.min(copyFailed, remainingAfterTransferSkip) : copyFailed
    const done = resolvedTransferred + resolvedSkipped + resolvedCopyFailed
    const residualUnaccounted = totalObjects > 0 ? Math.max(0, totalObjects - Math.min(totalObjects, done)) : rawUnaccounted
    const unaccounted = totalObjects > 0 ? Math.min(rawUnaccounted, residualUnaccounted) : rawUnaccounted
    const percent = migrationProgressPercent(resolvedTransferred + resolvedSkipped, totalObjects)
    const transferredPct = totalObjects > 0 ? Math.max(0, Math.min(100, (resolvedTransferred / totalObjects) * 100)) : 0
    const skippedPct = totalObjects > 0 ? Math.max(0, Math.min(100, (resolvedSkipped / totalObjects) * 100)) : 0
    const copyFailedPct = totalObjects > 0 ? Math.max(0, Math.min(100, (resolvedCopyFailed / totalObjects) * 100)) : 0
    const unaccountedPct = totalObjects > 0 ? Math.max(0, Math.min(100, (unaccounted / totalObjects) * 100)) : 0
    return {
      totalObjects,
      transferred: resolvedTransferred,
      skipped: resolvedSkipped,
      copyFailed: resolvedCopyFailed,
      unaccounted,
      verifyIssues,
      percent,
      transferredPct,
      skippedPct,
      copyFailedPct,
      unaccountedPct,
      totalBytes,
    }
  }, [getBucketSnapshot, items])

  const overviewProgress = totals
  const hasActiveSuperSlurper = React.useMemo(
    () => {
      if (migration?.options.executionMode === "migration_workers") return false
      return (
      items.some((item) => {
        const status = normalizeStatus(getBucketSnapshot(item).displayStatus)
        const rawStatus = normalizeStatus(item.slurperStatus)
        if (item.slurperJobId) {
          return ![
            "completed",
            "complete",
            "finished",
            "success",
            "succeeded",
            "failed",
            "error",
            "aborted",
            "canceled",
            "cancelled",
            "copy_completed",
            "copy_failed",
            "copy_aborted",
            "verification_failed",
            "no_files",
          ].includes(status || rawStatus)
        }
        return ["queued", "creating_job", "job_id_pending", "running", "scanning", "verifying"].includes(status || rawStatus)
      })
      )
    },
    [getBucketSnapshot, items, migration?.options.executionMode]
  )

  const overviewBadgeStatus = React.useMemo(() => {
    if (migration && ["completed", "failed", "verification_failed", "canceled"].includes(migration.status)) return migration.status
    if (migration?.status === "verifying" && migration.syncMessage?.toLowerCase().includes("settings")) return "verifying"
    if (bucketCounts.scanning > 0) return "scanning"
    if (bucketCounts.running > 0) return "running"
    if (bucketCounts.verifying > 0) return "verifying"
    if (bucketCounts.failed > 0) return "failed"
    if (bucketCounts.verificationFailed > 0) return "verification_failed"
    if (bucketCounts.aborted > 0 && bucketCounts.completed + bucketCounts.failed + bucketCounts.aborted === bucketCounts.total && bucketCounts.failed === 0)
      return "aborted"
    if (bucketCounts.completed === bucketCounts.total && bucketCounts.total > 0) return "completed"
    return migration?.status ?? "draft"
  }, [bucketCounts, migration])

  const effectiveMigrationStatus = React.useMemo(() => {
    if (overviewBadgeStatus === "aborted") return "canceled"
    return overviewBadgeStatus
  }, [overviewBadgeStatus])

  const loadInitial = React.useCallback(async () => {
    if (!id) return
    setError(null)
    try {
      const detailsRes = await fetch(`/api/migrations/${encodeURIComponent(id)}`, { cache: "no-store" })
      const detailsJson: unknown = await detailsRes.json().catch(() => null)

      if (!detailsRes.ok) {
        const message =
          isRecord(detailsJson) && typeof detailsJson.error === "string"
            ? detailsJson.error
            : "Unable to load migration"
        throw new Error(message)
      }

      const nextAccounts =
        isRecord(detailsJson) && Array.isArray(detailsJson.accounts) ? (detailsJson.accounts as Account[]) : []
      setAccounts(nextAccounts)

      const nextMigration =
        isRecord(detailsJson) && isRecord(detailsJson.migration) ? (detailsJson.migration as Migration) : null
      const nextItems =
        isRecord(detailsJson) && Array.isArray(detailsJson.items) ? (detailsJson.items as MigrationItem[]) : []
      const nextWorkerRuns =
        isRecord(detailsJson) && Array.isArray(detailsJson.workerRuns) ? (detailsJson.workerRuns as MigrationWorkerRun[]) : []
      setMigration((prev) => nextMigration ? mergeIncomingMigration(prev, nextMigration) : null)
      setItems((prev) => mergeIncomingItems(prev, nextItems))
      setWorkerRuns(nextWorkerRuns)
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to load migration")
          : "Unable to load migration"
      setError(message)
    } finally {
      setInitialLoading(false)
    }
  }, [id])

  React.useEffect(() => {
    void loadInitial()
  }, [loadInitial])

  const runMigrationAction = React.useCallback(
    async (action: "pause_all" | "resume_all" | "cancel_migration" | "mark_completed" | "retry_migration" | "repair_migration" | "verify_all" | "settings_sync") => {
      if (!id) return
      if (busyAction) return
      setBusyAction(action)
      setError(null)
      try {
        const res = await postJsonWithTimeout({
          url: `/api/migrations/${encodeURIComponent(id)}/action`,
          body: { action },
          timeoutMs: action === "settings_sync" || action === "mark_completed" ? 120_000 : 12_000,
        })
        const json: unknown = await res.json().catch(() => ({}))
        const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to run action"
        if (!res.ok) throw new Error(errorMessage)
        // The orchestrator owns worker execution; the panel only reloads the
        // persisted projection after recording the requested action.
        void loadInitial()
      } catch (e: unknown) {
        const message =
          typeof e === "object" && e !== null && "name" in e && String((e as { name?: unknown }).name) === "AbortError"
            ? ""
            : typeof e === "object" && e !== null && "message" in e
            ? String((e as { message?: unknown }).message ?? "Unable to run action")
            : "Unable to run action"
        if (message) setError(message)
      } finally {
        setBusyAction(null)
      }
    },
    [busyAction, id, loadInitial]
  )

  React.useEffect(() => {
    if (!id) return

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

      const streamUrl = `/api/migrations/${encodeURIComponent(id)}/stream`
      es = new EventSource(streamUrl)

      const onSnapshot = (event: MessageEvent) => {
        retry = 0
        try {
          const data: unknown = JSON.parse(String(event.data ?? "{}"))
          if (isRecord(data) && isRecord(data.migration)) setMigration((prev) => mergeIncomingMigration(prev, data.migration as Migration))
          if (isRecord(data) && Array.isArray(data.items)) {
            const nextItems = data.items as MigrationItem[]
            setItems((prev) => mergeIncomingItems(prev, nextItems))
          }
          if (isRecord(data) && Array.isArray(data.workerRuns)) setWorkerRuns(data.workerRuns as MigrationWorkerRun[])
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

      es.addEventListener("snapshot", onSnapshot as EventListener)
      es.addEventListener("error", onError as EventListener)
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
  }, [id])

  const syncNow = async () => {
    if (!id) return
    setBusyAction("sync")
    setError(null)
    try {
      await loadInitial()
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "name" in e && String((e as { name?: unknown }).name) === "AbortError"
          ? ""
          : typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to sync migration")
          : "Unable to sync migration"
      if (message) setError(message)
    } finally {
      setBusyAction(null)
    }
  }

  const startMigration = async () => {
    if (!id) return
    setBusyAction("start")
    setError(null)
    try {
      const res = await fetch(`/api/migrations/${encodeURIComponent(id)}/start?async=1`, { method: "POST" })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to start migration"
      if (!res.ok) throw new Error(errorMessage)
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

  const deleteMigration = async () => {
    if (!id) return
    setBusyAction("delete")
    setError(null)
    try {
      const res = await fetch(`/api/migrations/${encodeURIComponent(id)}`, { method: "DELETE" })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to delete migration"
      if (!res.ok) throw new Error(errorMessage)
      router.push("/dashboard/migrations")
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to delete migration")
          : "Unable to delete migration"
      setError(message)
    } finally {
      setBusyAction(null)
      setDeleteOpen(false)
    }
  }

  const runItemAction = async (itemId: string, action: "pause" | "resume" | "abort" | "logs" | "retry" | "verify") => {
    if (!id) return
    setError(null)
    setBusyItemAction((prev) => ({ ...prev, [itemId]: action }))
    try {
      const res = await postJsonWithTimeout({
        url: `/api/migrations/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}/action`,
        body: { action },
        timeoutMs: 12_000,
      })
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage = isRecord(json) && typeof json.error === "string" ? json.error : "Unable to run action"
      if (!res.ok) throw new Error(errorMessage)

      // State-changing routes own their worker wake-up; SSE refreshes this DB snapshot.
    } catch (e: unknown) {
      const message =
        typeof e === "object" && e !== null && "name" in e && String((e as { name?: unknown }).name) === "AbortError"
          ? ""
          : typeof e === "object" && e !== null && "message" in e
          ? String((e as { message?: unknown }).message ?? "Unable to run action")
        : "Unable to run action"
      if (message) setError(message)
    } finally {
      setBusyItemAction((prev) => {
        const next = { ...prev }
        delete next[itemId]
        return next
      })
    }
  }

  const fetchFailedDiagnosticsForItem = React.useCallback(
    async (itemId: string): Promise<FailedDiagnosticsBucket | null> => {
      if (!id) return null
      const item = items.find((x) => x.id === itemId)
      const res = await fetch(
        `/api/migrations/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}/failures?limit=250`
      )
      const json: unknown = await res.json().catch(() => ({}))
      const errorMessage =
        isRecord(json) && typeof json.error === "string"
          ? json.error
          : "Unable to load failed object diagnostics"
      if (!res.ok) throw new Error(errorMessage)

      const summary =
        isRecord(json) && isRecord(json.summary)
          ? {
              totalFailedEntries:
                typeof json.summary.totalFailedEntries === "number" ? json.summary.totalFailedEntries : 0,
              detailedFailedEntries:
                typeof json.summary.detailedFailedEntries === "number" ? json.summary.detailedFailedEntries : 0,
              cloudflareDetailedEntries:
                typeof json.summary.cloudflareDetailedEntries === "number" ? json.summary.cloudflareDetailedEntries : 0,
              fallbackDetailedEntries:
                typeof json.summary.fallbackDetailedEntries === "number" ? json.summary.fallbackDetailedEntries : 0,
              inferredDetailedEntries:
                typeof json.summary.inferredDetailedEntries === "number" ? json.summary.inferredDetailedEntries : 0,
              missingDetailedEntries:
                typeof json.summary.missingDetailedEntries === "number" ? json.summary.missingDetailedEntries : 0,
              sourceMissing: typeof json.summary.sourceMissing === "number" ? json.summary.sourceMissing : 0,
              sourceAccessIssues:
                typeof json.summary.sourceAccessIssues === "number" ? json.summary.sourceAccessIssues : 0,
              destinationExists:
                typeof json.summary.destinationExists === "number" ? json.summary.destinationExists : 0,
              transientOrProviderIssues:
                typeof json.summary.transientOrProviderIssues === "number"
                  ? json.summary.transientOrProviderIssues
                  : 0,
              unknown: typeof json.summary.unknown === "number" ? json.summary.unknown : 0,
            }
          : {
              totalFailedEntries: 0,
              detailedFailedEntries: 0,
              cloudflareDetailedEntries: 0,
              fallbackDetailedEntries: 0,
              inferredDetailedEntries: 0,
              missingDetailedEntries: 0,
              sourceMissing: 0,
              sourceAccessIssues: 0,
              destinationExists: 0,
              transientOrProviderIssues: 0,
              unknown: 0,
            }

      const failures =
        isRecord(json) && Array.isArray(json.failures) ? (json.failures as FailedObjectDiagnostic[]) : []

      return {
        missing: isRecord(json) && json.missing === true,
        item: {
          id: itemId,
          sourceBucket:
            isRecord(json) && isRecord(json.item) && typeof json.item.sourceBucket === "string"
              ? json.item.sourceBucket
              : item?.sourceBucket ?? itemId,
          targetBucket: item?.targetBucket ?? "",
          jobId: item?.slurperJobId ?? null,
        },
        summary,
        failures,
      }
    },
    [id, items]
  )

  const openFailedDiagnosticsForSingle = React.useCallback(
    async (itemId: string) => {
      setFailedItemId(itemId)
      setFailedScope("single")
      setFailedOpen(true)
      setFailedData([])
      setFailedLoading(true)
      setError(null)
      try {
        const data = await fetchFailedDiagnosticsForItem(itemId)
        setFailedData(data ? [data] : [])
      } catch (e: unknown) {
        const message =
          typeof e === "object" && e !== null && "message" in e
            ? String((e as { message?: unknown }).message ?? "Unable to load failed object diagnostics")
            : "Unable to load failed object diagnostics"
        setError(message)
        setFailedData([])
      } finally {
        setFailedLoading(false)
      }
    },
    [fetchFailedDiagnosticsForItem]
  )

  const openFailedDiagnosticsForAll = React.useCallback(
    async (itemIds: string[]) => {
      setFailedItemId(null)
      setFailedScope("all")
      setFailedOpen(true)
      setFailedData([])
      setFailedLoading(true)
      setError(null)
      try {
        const results = await Promise.all(
          itemIds.map(async (itemId) => {
            try {
              return await fetchFailedDiagnosticsForItem(itemId)
            } catch {
              return null
            }
          })
        )
        setFailedData(results.filter((x): x is FailedDiagnosticsBucket => Boolean(x)))
      } catch (e: unknown) {
        const message =
          typeof e === "object" && e !== null && "message" in e
            ? String((e as { message?: unknown }).message ?? "Unable to load failed object diagnostics")
            : "Unable to load failed object diagnostics"
        setError(message)
        setFailedData([])
      } finally {
        setFailedLoading(false)
      }
    },
    [fetchFailedDiagnosticsForItem]
  )

  if (initialLoading) {
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between gap-3">
          <div className="space-y-2">
            <Skeleton className="h-8 w-56" />
            <Skeleton className="h-4 w-72" />
          </div>
          <Skeleton className="h-10 w-40" />
        </div>
        <Skeleton className="h-[220px] w-full" />
        <Skeleton className="h-[360px] w-full" />
      </div>
    )
  }

  if (!migration) {
    return (
      <div className="space-y-4">
        <div className="text-sm text-muted-foreground">Migration not found.</div>
        <Button variant="outline" onClick={() => router.push("/dashboard/migrations")}>
          Back to migrations
        </Button>
      </div>
    )
  }

  const historyReadOnly = getMigrationReadOnlyState(migration)
  const missingHistoricalDetails = historyReadOnly.readOnly && items.length === 0

  const sourceLabel = accountLabelById.get(migration.sourceAccountId) ?? migration.sourceAccountId
  const targetLabel = accountLabelById.get(migration.targetAccountId) ?? migration.targetAccountId
  const showSkippedColumn = items.some((item) => getBucketSnapshot(item).skipped > 0)
  const showFailedColumn = items.some((item) => getBucketSnapshot(item).failed > 0)

  const bucketColumns: ColumnDef<MigrationItem, unknown>[] = [
    {
      id: "bucket",
      header: "Bucket",
      meta: { width: "min-w-[260px]", align: "left" },
      cell: ({ row }) => (
        <div className="leading-tight">
          <div className="truncate font-medium">{row.original.sourceBucket}</div>
          <div className="truncate text-xs text-muted-foreground"><span className="font-mono">{row.original.id}</span></div>
        </div>
      ),
    },
    {
      id: "status",
      header: "Status",
      meta: { width: "min-w-[130px]", align: "center" },
      cell: ({ row }) => {
        const item = row.original
        const progress = isRecord(item.progress) ? item.progress : {}
        const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
        const snapshot = getBucketSnapshot(item)
        const displayStatus = settingsSync?.status === "syncing"
          ? "settings_syncing"
          : settingsSync?.status === "failed"
            ? "settings_failed"
            : snapshot.displayStatus
        return (
          <div className="space-y-1">
            <div>{statusBadge(displayStatus, { hadProgress: snapshot.transferred > 0 || snapshot.skipped > 0 || snapshot.failed > 0 })}</div>
            {settingsSync?.status === "completed" ? <div className="text-[11px] text-muted-foreground">Settings synced</div> : null}
          </div>
        )
      },
    },
    {
      id: "transferred",
      header: "Transferred",
      meta: { width: "min-w-[120px]", align: "center" },
      cell: ({ row }) => <span className="font-mono text-xs">{formatNumber(getBucketSnapshot(row.original).transferred)}</span>,
    },
    ...(showSkippedColumn ? [{
      id: "skipped",
      header: "Skipped",
      meta: { width: "min-w-[100px]", align: "center" },
      cell: ({ row }) => <span className="font-mono text-xs">{formatNumber(getBucketSnapshot(row.original).skipped)}</span>,
    } satisfies ColumnDef<MigrationItem, unknown>] : []),
    ...(migration?.options.executionMode === "migration_workers" ? [{
      id: "queued",
      header: "Queue",
      meta: { width: "min-w-[100px]", align: "center" },
      cell: ({ row }) => <span className="font-mono text-xs">{formatNumber(getBucketSnapshot(row.original).queued)}</span>,
    } satisfies ColumnDef<MigrationItem, unknown>] : []),
    ...(showFailedColumn ? [{
      id: "failed",
      header: "Failed",
      meta: { width: "min-w-[100px]", align: "center" },
      cell: ({ row }) => <span className="font-mono text-xs">{formatNumber(getBucketSnapshot(row.original).failed)}</span>,
    } satisfies ColumnDef<MigrationItem, unknown>] : []),
    {
      id: "total",
      header: "Total",
      meta: { width: "min-w-[100px]", align: "center" },
      cell: ({ row }) => {
        const item = row.original
        const progress = isRecord(item.progress) ? item.progress : {}
        const scanComplete = progress.sourceScanStatus === "completed"
        const scanInProgress = progress.sourceScanStatus === "running" || progress.sourceScanStatus === "pending"
        const snapshot = getBucketSnapshot(item)
        const repairState = readRepairWorkerState(progress)
        const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
        const displayStatus = settingsSync?.status === "syncing"
          ? "settings_syncing"
          : settingsSync?.status === "failed"
            ? "settings_failed"
            : snapshot.displayStatus
        const hasKnownEmpty = typeof item.sourceObjects === "number" && item.sourceObjects === 0 &&
          getEffectiveSourceBytes(item, repairState) === 0
        const scannedObjects = typeof progress.sourceScanObjects === "number" ? progress.sourceScanObjects : undefined
        const total = displayStatus === "no_files" || hasKnownEmpty
          ? 0
          : scanInProgress && scannedObjects !== undefined
            ? scannedObjects
            : snapshot.total > 0 || scanComplete
            ? snapshot.total
            : undefined
        return (
          <span
            className="font-mono text-xs"
            title={scanComplete ? "Total from File Scanner (authoritative)" : scanInProgress ? "Live partial count from File Scanner; updates as pages are scanned" : "Total"}
          >
            {typeof total === "number" ? formatNumber(total) : "—"}
          </span>
        )
      },
    },
    {
      id: "size",
      header: "Size",
      meta: { width: "min-w-[120px]", align: "center" },
      cell: ({ row }) => {
        const item = row.original
        const progress = isRecord(item.progress) ? item.progress : {}
        const scanComplete = progress.sourceScanStatus === "completed"
        const scanInProgress = progress.sourceScanStatus === "running" || progress.sourceScanStatus === "pending"
        const repairState = readRepairWorkerState(progress)
        const scannedBytes = typeof progress.sourceScanBytes === "number" ? progress.sourceScanBytes : undefined
        const sourceBytes = scanInProgress && scannedBytes !== undefined
          ? scannedBytes
          : getEffectiveSourceBytes(item, repairState)
        const snapshot = getBucketSnapshot(item)
        const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
        const displayStatus = settingsSync?.status === "syncing"
          ? "settings_syncing"
          : settingsSync?.status === "failed"
            ? "settings_failed"
            : snapshot.displayStatus
        return <span className="font-mono text-xs" title={scanInProgress ? "Live partial size from File Scanner; updates as pages are scanned" : undefined}>{displayStatus === "no_files" ? "0 B" : scanComplete || scanInProgress || sourceBytes > 0 ? formatBytes(sourceBytes) : "—"}</span>
      },
    },
    ...(showSettingsColumn ? [{
      id: "settings",
      header: "Settings",
      meta: { width: "min-w-[130px]", align: "center" },
      cell: ({ row }: { row: { original: MigrationItem } }) => {
        const item = row.original
        const settingsStatus = readBucketSettingsStatus(item) ?? (isCompletedStatus(getItemStatus(item)) ? "syncing" : null)
        if (!settingsStatus) return <span className="text-xs text-muted-foreground">—</span>
        if (settingsStatus === "synced") return <Badge className="bg-green-600">Synced</Badge>
        if (settingsStatus === "failed") return <Badge className="bg-red-600">Failed</Badge>
        return <Badge className="bg-purple-600">Syncing</Badge>
      },
    } satisfies ColumnDef<MigrationItem, unknown>] : []),
    {
      id: "actions",
      header: "Actions",
      meta: { width: "min-w-[170px]", align: "center", divider: false },
      cell: ({ row }) => {
        const item = row.original
        const progress = isRecord(item.progress) ? item.progress : {}
        const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
        const snapshot = getBucketSnapshot(item)
        const status = String(getItemStatus(item) ?? "").toLowerCase()
        const displayStatus = settingsSync?.status === "syncing"
          ? "settings_syncing"
          : settingsSync?.status === "failed"
            ? "settings_failed"
            : snapshot.displayStatus
        const normalizedDisplayStatus = normalizeStatus(displayStatus)
        const itemBusy = busyItemAction[item.id]
        const workerPoolMigration = migration.options.executionMode === "migration_workers"
        const canPause = !workerPoolMigration && Boolean(item.slurperJobId) && status === "running"
        const canResume = !workerPoolMigration && Boolean(item.slurperJobId) && status === "paused"
        const verifyState = readVerifyState(item.progress)
        const verifyStatus = verifyState?.status ?? null
        const canRetry = !workerPoolMigration && normalizedDisplayStatus !== "verification_failed" && (verifyStatus === "error" || normalizedDisplayStatus === "queued" || normalizedDisplayStatus === "job_id_pending" || normalizedDisplayStatus.endsWith("_failed") || normalizedDisplayStatus.includes("failed") || normalizedDisplayStatus.includes("error"))
        const canAbort = !["canceled", "completed"].includes(migration?.status ?? "") && !workerPoolMigration && (Boolean(item.slurperJobId) || canRetry) && !["completed", "aborted", "failed", "verification_failed", "no_files"].includes(normalizedDisplayStatus)
        const canVerify = migration.status !== "completed" && !workerPoolMigration && (normalizedDisplayStatus === "verification_failed" || (isCompletedStatus(displayStatus) && verifyStatus !== "pending" && verifyStatus !== "running"))
        const canInspectFailures = snapshot.failed > 0 || snapshot.verifyIssues > 0 || normalizedDisplayStatus.includes("failed") || normalizedDisplayStatus.includes("error")
        const lifecycleAction: "pause" | "resume" | "retry" | null = canPause ? "pause" : canRetry ? "retry" : canResume ? "resume" : null
        const lifecycleBusy = itemBusy === "pause" || itemBusy === "resume" || itemBusy === "retry"
        return (
          <div className="flex min-h-[40px] w-full items-center justify-center gap-1.5 text-center">
            <Button variant="ghost" size="icon" loading={itemBusy === "verify"} className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md" title={verifyStatus === null ? "Run verification" : "Re-run verification"} aria-label="Verify" disabled={historyReadOnly.readOnly || Boolean(itemBusy) || !canVerify} onClick={() => { void runItemAction(item.id, "verify") }}>
              {itemBusy !== "verify" ? <ShieldCheck className="h-3.5 w-3.5" /> : null}
            </Button>
            <Button variant="ghost" size="icon" loading={itemBusy === "logs"} className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md" title="View logs" disabled={Boolean(itemBusy)} onClick={() => { setLogsItemId(item.id); setLogsOpen(true); if (bucketCounts.scanning > 0 || bucketCounts.running > 0 || bucketCounts.verifying > 0) void runItemAction(item.id, "logs") }}>
              {itemBusy !== "logs" ? <ScrollText className="h-3.5 w-3.5" /> : null}
            </Button>
            <Button variant="ghost" size="icon" loading={lifecycleBusy} className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md" title="Failed Diagnostics" aria-label="Failed Diagnostics" disabled={historyReadOnly.readOnly || Boolean(itemBusy) || !canInspectFailures} onClick={() => { void openFailedDiagnosticsForSingle(item.id) }}>
              <AlertCircle className="h-3.5 w-3.5" />
            </Button>
            <Button variant="ghost" size="icon" className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:shadow-md" title={lifecycleAction === "pause" ? "Stop" : lifecycleAction === "retry" ? "Start (retry)" : lifecycleAction === "resume" ? "Start" : "Start/Stop"} aria-label={lifecycleAction === "pause" ? "Stop" : lifecycleAction === "retry" ? "Start (retry)" : lifecycleAction === "resume" ? "Start" : "Start/Stop"} disabled={historyReadOnly.readOnly || Boolean(itemBusy) || lifecycleAction === null} onClick={() => { if (lifecycleAction) void runItemAction(item.id, lifecycleAction) }}>
              {!lifecycleBusy && lifecycleAction === "pause" ? <Square className="h-3.5 w-3.5" /> : !lifecycleBusy ? <Play className="h-3.5 w-3.5" /> : null}
            </Button>
            <Button variant="ghost" size="icon" loading={itemBusy === "abort"} className="!h-7 !w-7 !min-h-7 !min-w-7 flex-none !rounded-full !border !border-white/15 !bg-background/85 !p-0 text-destructive shadow-sm backdrop-blur-sm transition-[border-color,background-color,box-shadow] hover:!border-white/25 hover:!bg-muted/55 hover:!shadow-md" title="Abort" aria-label="Abort" disabled={historyReadOnly.readOnly || Boolean(itemBusy) || !canAbort} onClick={() => { void runItemAction(item.id, "abort") }}>
              {itemBusy !== "abort" ? <CircleX className="h-3.5 w-3.5" /> : null}
            </Button>
          </div>
        )
      },
    },
  ]

  return (
    <div className="dashboard-motion-stage space-y-6 max-w-full">
      {error ? (
        <div className="rounded-md border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-200">{error}</div>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between gap-3">
            <span>Overview</span>
            {migrationStatusBadge(overviewBadgeStatus)}
          </CardTitle>
          <CardDescription>
            {migration.options.executionMode === "migration_workers" ? "Worker pool" : "Cloudflare Super Slurper"}
            {" · Source: "}{sourceLabel}{" · Destination: "}{targetLabel}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {migration.syncStatus === "error" && migration.syncMessage ? (
            <div role="alert" className="flex items-start gap-3 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-3 text-sm">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />
              <span>{migration.syncMessage}</span>
            </div>
          ) : null}
          <dl className="grid gap-y-4 border-y py-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r lg:first:pl-0">
              <CalendarDays className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
              <div className="min-w-0">
                <dt className="text-xs font-medium text-muted-foreground">Created</dt>
                <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatDate(migration.createdAt)}</dd>
              </div>
            </div>
            <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r">
              <Clock className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
              <div className="min-w-0">
                <dt className="text-xs font-medium text-muted-foreground">Started</dt>
                <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatDate(migration.startedAt)}</dd>
              </div>
            </div>
            <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r">
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
              <div className="min-w-0">
                <dt className="text-xs font-medium text-muted-foreground">Completed</dt>
                <dd className="mt-1 truncate text-sm font-medium tabular-nums">{migration.status === "completed" ? formatDate(migration.completedAt) : "-"}</dd>
              </div>
            </div>
            <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:pr-0">
              <RefreshCw className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
              <div className="min-w-0">
                <dt className="text-xs font-medium text-muted-foreground">Last saved</dt>
                <dd className="mt-1 truncate text-sm font-medium tabular-nums">
                  {formatDate(migration.lastSyncedAt || migration.completedAt || migration.createdAt)}
                </dd>
              </div>
            </div>
          </dl>

          {missingHistoricalDetails ? (
            <div className="flex gap-3 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-3 text-sm">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0 text-amber-500" />
              <div className="space-y-1">
                <div className="font-medium text-foreground">Detailed snapshot unavailable</div>
                <div className="text-muted-foreground">
                  This migration predates detailed bucket snapshots. Only its migration summary was stored, so bucket names, transfer counters, progress, and logs cannot be recovered from the database.
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Progress</span>
                <span className="font-mono">{overviewProgress.percent.toFixed(1)}%</span>
              </div>
                <ProgressStacked
                  transferredPct={overviewProgress.transferredPct}
                  skippedPct={overviewProgress.skippedPct}
                  failedPct={overviewProgress.copyFailedPct}
                  unaccountedPct={migration.options.executionMode === "migration_workers" ? 0 : overviewProgress.unaccountedPct}
                />
              <div className="flex items-center justify-between text-xs text-muted-foreground">
                <div className="flex items-center gap-3">
                  {overviewProgress.transferred > 0 ? (
                    <span>
                      {formatNumber(overviewProgress.transferred)} transferred ({(Math.floor(overviewProgress.transferredPct * 10) / 10).toFixed(1)}%)
                    </span>
                  ) : null}
                  {overviewProgress.skipped > 0 ? (
                    <span className="text-yellow-500">
                      {formatNumber(overviewProgress.skipped)} skipped objects ({overviewProgress.skippedPct.toFixed(1)}%)
                    </span>
                  ) : null}
                  {overviewProgress.copyFailed > 0 ? (
                    <span className="text-red-500">
                      {formatNumber(overviewProgress.copyFailed)} copy failed objects ({overviewProgress.copyFailedPct.toFixed(1)}%)
                    </span>
                  ) : null}
                  {migration.options.executionMode !== "migration_workers" && overviewProgress.unaccounted > 0 ? (
                    <span className="text-muted-foreground">
                      {formatNumber(overviewProgress.unaccounted)} not reported by Cloudflare counters ({overviewProgress.unaccountedPct.toFixed(1)}%)
                    </span>
                  ) : null}
                  {overviewProgress.verifyIssues > 0 ? (
                    <span className="text-red-500">{formatNumber(overviewProgress.verifyIssues)} verification issues</span>
                  ) : null}
                  {overviewProgress.transferred === 0 && overviewProgress.skipped === 0 && overviewProgress.copyFailed === 0 ? (
                    <span>0 transferred</span>
                  ) : null}
                </div>
                <span>
                  {formatNumber(overviewProgress.totalObjects)} objects - {formatBytes(overviewProgress.totalBytes)}
                </span>
              </div>
            </div>
          )}

          <div className="flex flex-wrap gap-2">
            {(() => {
              if (historyReadOnly.readOnly) return null
              const allBucketsTerminal =
                items.length > 0 &&
                items.every((i) => {
                  const s = getBucketSnapshot(i).displayStatus
                  return isCompletedStatus(s) || isAbortedStatus(s) || isFailedLikeStatus(s)
                })
              const anyRunning = items.some((i) => normalizeStatus(getBucketSnapshot(i).displayStatus) === "running")
              const anyPaused = items.some((i) => Boolean(i.slurperJobId) && String(getItemStatus(i) ?? "").toLowerCase() === "paused")
              const workerPoolMigration = migration.options.executionMode === "migration_workers"
              const hasVerificationFailure = failedBuckets.some((item) =>
                normalizeStatus(getBucketSnapshot(item).displayStatus) === "verification_failed"
              ) || migration.syncMessage?.toLowerCase().includes("verification failed") === true
              const showCancel = !workerPoolMigration && !allBucketsTerminal && !["completed", "failed", "canceled"].includes(String(effectiveMigrationStatus))
              const settingsSyncFailed =
                (migration.syncStatus === "error" && migration.syncMessage?.toLowerCase().includes("settings sync failed")) ||
                items.some((item) => {
                  const progress = isRecord(item.progress) ? item.progress : {}
                  const settingsSync = isRecord(progress.settingsSync) ? progress.settingsSync : null
                  return settingsSync?.status === "failed"
                })
              const settingsSyncInProgress =
                migration.syncStatus === "syncing" || items.some((item) => readBucketSettingsStatus(item) === "syncing")
              const showMarkCompleted =
                allBucketsTerminal && !["completed", "failed"].includes(String(effectiveMigrationStatus)) && !settingsSyncFailed

              return (
                <>
                  {(() => {
                    const paused = !workerPoolMigration && anyPaused && !anyRunning
                    const active = ["running", "verifying"].includes(String(effectiveMigrationStatus))
                    const primaryAction = effectiveMigrationStatus === "draft"
                      ? "start"
                      : paused
                        ? "resume_all"
                        : active
                          ? workerPoolMigration || !anyRunning ? "cancel_migration" : "pause_all"
                          : ["failed", "canceled", "aborted", "verification_failed"].includes(String(effectiveMigrationStatus)) ? "retry_migration" : null
                    if (!primaryAction) return null
                    const loading = busyAction === primaryAction
                    const label = primaryAction === "start"
                      ? "Start"
                      : primaryAction === "pause_all" || primaryAction === "cancel_migration"
                        ? "Stop"
                        : primaryAction === "resume_all" ? "Resume" : workerPoolMigration ? "Restart" : "Rerun"
                    return (
                      <Button
                        onClick={() => {
                          if (primaryAction === "start") void startMigration()
                          else void runMigrationAction(primaryAction as "pause_all" | "resume_all" | "cancel_migration" | "retry_migration")
                        }}
                        loading={loading}
                        disabled={Boolean(busyAction)}
                        variant={label === "Stop" ? "outline" : "default"}
                      >
                        {!loading ? (label === "Stop" ? <Square className="h-4 w-4 mr-0" /> : <Play className="h-4 w-4 mr-0" />) : null}
                        {label}
                      </Button>
                    )
                  })()}

                  <Button onClick={syncNow} loading={busyAction === "sync"} disabled={Boolean(busyAction)} variant="outline">
                    {busyAction !== "sync" ? <RefreshCw className="h-4 w-4 mr-0" /> : null}
                    Sync now
                  </Button>

                  {settingsSyncFailed ? (
                    <Button onClick={() => void runMigrationAction("settings_sync")} loading={busyAction === "settings_sync"} disabled={Boolean(busyAction)} variant="outline">
                      {busyAction !== "settings_sync" ? <ShieldCheck className="h-4 w-4 mr-0" /> : null}
                      Settings sync
                    </Button>
                  ) : null}

                  {failedBuckets.length > 0 ? (
                    <Button
                      onClick={() => {
                        void openFailedDiagnosticsForAll(failedBuckets.map((b) => b.id))
                      }}
                      disabled={Boolean(busyAction)}
                      variant="outline"
                    >
                      <AlertCircle className="h-4 w-4 mr-0" />
                      Failed Diagnostics
                    </Button>
                  ) : null}

                  {!settingsSyncFailed && effectiveMigrationStatus !== "completed" && (hasVerificationFailure || overviewProgress.verifyIssues > 0) ? (
                    <Button
                      onClick={() => void runMigrationAction("verify_all")}
                      loading={busyAction === "verify_all"}
                      disabled={Boolean(busyAction)}
                      variant="outline"
                    >
                      {busyAction !== "verify_all" ? <ShieldCheck className="h-4 w-4 mr-0" /> : null}
                      Verify
                    </Button>
                  ) : null}

                  {migration.status !== "completed" && !workerPoolMigration &&
                    !settingsSyncInProgress && effectiveMigrationStatus !== "completed" &&
                    items.length > 0 && migration.status !== "draft" && !hasActiveSuperSlurper ? (
                    <Button
                      onClick={() => void runMigrationAction("repair_migration")}
                      loading={busyAction === "repair_migration"}
                      disabled={Boolean(busyAction)}
                      variant="outline"
                    >
                      {busyAction !== "repair_migration" ? <RefreshCw className="h-4 w-4 mr-0" /> : null}
                      Use worker pool
                    </Button>
                  ) : null}

                  {showCancel ? (
                    <Button
                      onClick={() => void runMigrationAction("cancel_migration")}
                      loading={busyAction === "cancel_migration"}
                      disabled={Boolean(busyAction)}
                      variant="destructive"
                    >
                      {busyAction !== "cancel_migration" ? <CircleX className="h-4 w-4 mr-0" /> : null}
                      Cancel
                    </Button>
                  ) : null}

                  {showMarkCompleted ? (
                    <Button
                      onClick={() => setManualCompleteOpen(true)}
                      loading={busyAction === "mark_completed"}
                      disabled={Boolean(busyAction)}
                      variant="secondary"
                    >
                      {busyAction !== "mark_completed" ? <CheckCircle2 className="h-4 w-4 mr-0" /> : null}
                      Mark completed
                    </Button>
                  ) : null}
                </>
              )
            })()}
          </div>
        </CardContent>
      </Card>
      {migration.options.executionMode === "migration_workers" ? (
        (() => {
              const currentGeneration = Number(migration.options.workerGeneration) || 1
              const currentRuns = workerRuns.filter((run) => (Number(run.workerGeneration) || 1) === currentGeneration)
              const activeRuns = currentRuns.filter((run) => run.online)
              // A GitHub workflow is already deployed/running before its first
              // application heartbeat reaches the agent row. Heartbeat health
              // controls the online counter, but must not regress the pool
              // lifecycle badge back to "deploying".
              const runningRuns = currentRuns.filter((run) => String(run.status).toLowerCase() === "running")
              const latestRun = [...currentRuns].sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)))[0]
              const latestRunStatus = String(latestRun?.status || "").toLowerCase()
              // Cancellation is still in progress only while a worker is
              // actually heartbeating, or while an undispatched pending run
              // has not received the durable abort intent. An offline run
              // with abortRequested must not leave the card on "stopping"
              // forever after GitHub has already terminated it.
              const stoppingRuns = currentRuns.filter((run) =>
                run.online || (String(run.status).toLowerCase() === "pending" && !run.abortRequested)
              )
              const migrationStatus = String(migration.status || "").toLowerCase()
              const canAbortPool = ["running", "verifying"].includes(migrationStatus)
              const status = ["completed"].includes(migrationStatus)
                ? "completed"
                : ["failed", "verification_failed"].includes(migrationStatus)
                  ? stoppingRuns.length > 0 ? "aborting" : "failed"
                  : ["canceled", "cancelled", "aborted"].includes(migrationStatus)
                    ? stoppingRuns.length > 0 ? "aborting" : "aborted"
                    : migrationStatus === "verifying"
                      ? "verifying"
                      : runningRuns.length > 0
                        ? "running"
                        : ["pending", "queued", "created", "dispatching"].includes(latestRunStatus)
                          ? "deploying"
                          : "queued"
              const queueRemaining = items.reduce((sum, item) => sum + getBucketSnapshot(item).queued, 0)
              const lastActivityAt = latestRun?.lastHeartbeatAt || latestRun?.updatedAt || migration.updatedAt

              return (
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between gap-3">
                      <span>Worker Pool</span>
                      {migrationWorkerBadge(status)}
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <dl className="grid gap-y-4 border-y py-4 sm:grid-cols-2 lg:grid-cols-4">
                      <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r lg:first:pl-0">
                        <Users className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                        <div className="min-w-0">
                          <dt className="text-xs font-medium text-muted-foreground">Online workers</dt>
                          <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatNumber(activeRuns.length)}</dd>
                        </div>
                      </div>
                      <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r">
                        <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                        <div className="min-w-0">
                          <dt className="text-xs font-medium text-muted-foreground">Transferred</dt>
                          <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatNumber(overviewProgress.transferred)}</dd>
                        </div>
                      </div>
                      <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:border-r">
                        <ListTodo className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                        <div className="min-w-0">
                          <dt className="text-xs font-medium text-muted-foreground">Queue remaining</dt>
                          <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatNumber(queueRemaining)}</dd>
                        </div>
                      </div>
                      <div className="flex min-w-0 items-start gap-3 sm:px-3 lg:pr-0">
                        <Clock className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground" />
                        <div className="min-w-0">
                          <dt className="text-xs font-medium text-muted-foreground">Last activity</dt>
                          <dd className="mt-1 truncate text-sm font-medium tabular-nums">{formatDate(lastActivityAt)}</dd>
                        </div>
                      </div>
                    </dl>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        variant="outline"
                        onClick={() => router.push(`/dashboard/migrations/${encodeURIComponent(id)}/worker-pool`)}
                      >
                        Details
                      </Button>
                      {canAbortPool ? (
                        <Button
                          variant="destructive"
                          onClick={() => void runMigrationAction("cancel_migration")}
                          loading={busyAction === "cancel_migration"}
                          disabled={Boolean(busyAction)}
                        >
                          {busyAction !== "cancel_migration" ? <CircleX data-icon="inline-start" className="h-4 w-4" /> : null}
                          Abort
                        </Button>
                      ) : null}
                    </div>
                  </CardContent>
                </Card>
              )
        })()
      ) : null}

      <div className="space-y-6">
      <DashboardDataTable
        data={items}
        columns={bucketColumns}
        pageSize={10}
        minWidth="980px"
        emptyState={
          migration.detailsCompactedAt
            ? `Detailed records were compacted. Summary retained: ${migration.summaryObjects.toLocaleString()} objects, ${formatBytes(migration.summaryBytes)}, and ${migration.workerSummary?.workerRuns?.length ?? 0} worker runs.`
            : "No bucket data stored for this migration."
        }
        resetKey={migration.id}
      />


      <DashboardLogsCard
        title="Migration Logs"
        entries={logLines.map(line => ({
          at: line.atIso, context: line.bucket, status: line.status, message: line.message,
          stage: line.stage ? `${line.stage}${line.verificationGeneration ? ` · generation ${line.verificationGeneration}${line.verificationHistorical ? " (not current)" : ""}` : line.verificationAttemptUnknown ? " · attempt unknown (legacy event)" : ""}` : "",
        }))}
        storageKey="drive:migrationLogsCols:v1"
        emptyState="Waiting for scanner and orchestrator lifecycle events."
      />
      </div>

      <Dialog open={failedOpen} onOpenChange={setFailedOpen}>
        <DialogContent className="w-[96vw] max-w-[96vw] sm:max-w-[min(96vw,72rem)] h-[88vh] sm:h-[min(88vh,56rem)] overflow-hidden p-0 flex flex-col gap-0">
          <DialogHeader className="shrink-0 border-b bg-background px-6 pt-6 pb-4">
            <DialogTitle>Diagnostics</DialogTitle>
            <DialogDescription className="pt-1 text-sm leading-6 text-muted-foreground">
              {failedScope === "all"
                ? `Review object-level migration failures across ${formatNumber(diagnosticsSummary.buckets)} bucket${diagnosticsSummary.buckets === 1 ? "" : "s"}, including source and destination inspection, evidence quality, and recovery guidance.`
                : dialogFailedItem
                  ? `Review object-level migration failures for ${dialogFailedItem.sourceBucket} -> ${dialogFailedItem.targetBucket}.`
                  : missingFailedSelections.length === 1
                    ? "The selected bucket is no longer available in this migration, so only retained diagnostics can be shown."
                  : "Review object-level migration failures and supporting evidence."}
            </DialogDescription>
          </DialogHeader>
          {failedLoading ? (
            <div className="mx-6 my-6 rounded-2xl border bg-muted/30 p-4 text-sm text-muted-foreground">
              <Spinner className="mr-2 inline-flex" />
              Loading diagnostics...
            </div>
          ) : (
            <div className="min-h-0 flex flex-1 flex-col overflow-auto bg-background px-6 pb-4 [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
              {missingFailedSelections.length > 0 ? (
                <div className="pt-5">
                  <div className="rounded-2xl border bg-muted/30 px-4 py-4 text-sm text-muted-foreground">
                    {missingFailedSelections.length === 1
                      ? "The selected bucket item is no longer present in the current migration snapshot. Diagnostics below only reflect any retained records."
                      : `${formatNumber(missingFailedSelections.length)} selected bucket items are no longer present in the current migration snapshot. Diagnostics below only reflect any retained records.`}
                  </div>
                </div>
              ) : null}
              {failedData.length > 0 ? (
                <div className="shrink-0 space-y-4 pt-5">
                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">Failure Scope</div>
                      <div className="mt-3 flex items-end justify-between gap-3">
                        <div>
                          <div className="text-3xl font-semibold tracking-tight">{formatNumber(diagnosticsSummary.totalFailedEntries)}</div>
                          <div className="mt-1 text-sm text-muted-foreground">Reported object-level failures</div>
                        </div>
                        <AlertCircle className="h-8 w-8 text-muted-foreground" />
                      </div>
                    </div>
                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">Evidence Coverage</div>
                      <div className="mt-3 flex items-end justify-between gap-3">
                        <div>
                          <div className="text-3xl font-semibold tracking-tight">{formatNumber(diagnosticsSummary.detailedEntries)}</div>
                          <div className="mt-1 text-sm text-muted-foreground">Failures with object-level detail</div>
                        </div>
                        <CheckCircle2 className="h-8 w-8 text-muted-foreground" />
                      </div>
                    </div>
                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">Outstanding Gaps</div>
                      <div className="mt-3 flex items-end justify-between gap-3">
                        <div>
                          <div className="text-3xl font-semibold tracking-tight">{formatNumber(diagnosticsSummary.unresolved)}</div>
                          <div className="mt-1 text-sm text-muted-foreground">Failures still lacking object evidence</div>
                        </div>
                        <CircleX className="h-8 w-8 text-muted-foreground" />
                      </div>
                    </div>
                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">Affected Buckets</div>
                      <div className="mt-3 flex items-end justify-between gap-3">
                        <div>
                          <div className="text-3xl font-semibold tracking-tight">{formatNumber(diagnosticsSummary.buckets)}</div>
                          <div className="mt-1 text-sm text-muted-foreground">Buckets represented in this view</div>
                        </div>
                        <Clock className="h-8 w-8 text-muted-foreground" />
                      </div>
                    </div>
                  </div>

                  <div className="grid gap-3 lg:grid-cols-[1.3fr_1fr]">
                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-sm font-semibold">Evidence Sources</div>
                      <div className="mt-3 flex flex-wrap gap-2 text-sm">
                        <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                          {formatNumber(diagnosticsSummary.cloudflareEvidence)} Cloudflare log records
                        </div>
                        <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                          {formatNumber(diagnosticsSummary.fallbackEvidence)} verification or worker records
                        </div>
                        <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                          {formatNumber(diagnosticsSummary.inferredEvidence)} inferred from source and destination state
                        </div>
                        <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                          {formatNumber(diagnosticsSummary.unresolved)} unresolved after reconstruction
                        </div>
                      </div>
                    </div>

                    <div className="rounded-2xl border bg-background px-4 py-4">
                      <div className="text-sm font-semibold">Observed Patterns</div>
                      <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                        <div>
                          <div className="text-lg font-semibold">{formatNumber(diagnosticsSummary.sourceMissing)}</div>
                          <div className="text-muted-foreground">Source missing</div>
                        </div>
                        <div>
                          <div className="text-lg font-semibold">{formatNumber(diagnosticsSummary.sourceAccessIssues)}</div>
                          <div className="text-muted-foreground">Source access issues</div>
                        </div>
                        <div>
                          <div className="text-lg font-semibold">{formatNumber(diagnosticsSummary.destinationExists)}</div>
                          <div className="text-muted-foreground">Already present in destination</div>
                        </div>
                        <div>
                          <div className="text-lg font-semibold">{formatNumber(diagnosticsSummary.transientIssues)}</div>
                          <div className="text-muted-foreground">Transient or provider-side issues</div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="pt-5">
                  <div className="rounded-2xl border bg-background px-4 py-4 text-sm text-muted-foreground">
                    No diagnostics are currently available for this selection.
                  </div>
                </div>
              )}

              <div className="mt-4 min-h-[30rem] overscroll-contain pb-1">
                  {failedData.length === 0 ? (
                    <div className="flex h-full items-center justify-center rounded-2xl border bg-background p-6 text-sm text-muted-foreground">
                      No object-level failure records were identified from logs, verification, worker evidence, or reconstructed source and destination comparisons.
                    </div>
                  ) : (
                    <div className="space-y-5">
                      {failedData.map((group) => (
                        <div key={group.item.id} className="overflow-hidden rounded-2xl border bg-background">
                          <div className="border-b bg-muted/30 px-4 py-4">
                            <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                              <div className="space-y-1">
                                <div className="text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">Bucket Pair</div>
                                <div className="font-mono text-sm text-foreground">
                                  {group.item.sourceBucket} <span className="text-muted-foreground">{"->"}</span> {group.item.targetBucket}
                                </div>
                                <div className="text-sm text-muted-foreground">
                                  {group.item.jobId ? `Job reference: ${group.item.jobId}` : "Job reference unavailable"}
                                </div>
                              </div>
                              <div className="flex flex-wrap gap-2 text-xs">
                                <div className="rounded-full border px-3 py-1.5">
                                  {formatNumber(group.summary.totalFailedEntries)} reported failures
                                </div>
                                <div className="rounded-full border px-3 py-1.5">
                                  {formatNumber(group.summary.detailedFailedEntries || 0)} with detail
                                </div>
                                {group.summary.cloudflareDetailedEntries ? (
                                  <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                                    {formatNumber(group.summary.cloudflareDetailedEntries)} Cloudflare
                                  </div>
                                ) : null}
                                {group.summary.fallbackDetailedEntries ? (
                                  <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                                    {formatNumber(group.summary.fallbackDetailedEntries)} verification or worker
                                  </div>
                                ) : null}
                                {group.summary.inferredDetailedEntries ? (
                                  <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                                    {formatNumber(group.summary.inferredDetailedEntries)} inferred
                                  </div>
                                ) : null}
                                {group.summary.missingDetailedEntries ? (
                                  <div className="rounded-full border bg-muted/40 px-3 py-1.5 text-foreground">
                                    {formatNumber(group.summary.missingDetailedEntries)} unresolved
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          </div>

                          {group.failures.length === 0 && (group.summary.totalFailedEntries || 0) > 0 ? (
                            <div className="border-b bg-muted/30 px-4 py-3 text-sm text-muted-foreground">
                              {formatNumber(group.summary.totalFailedEntries)} failures were reported for this bucket pair, but the system could not reconstruct object identifiers from Cloudflare logs, verification data, worker output, or current source and destination state.
                            </div>
                          ) : null}

                          <PinnedScrollableTable maxHeightClassName="max-h-[22.5rem]">
                            <div className="min-w-[1720px] text-sm">
                              <div
                                className="sticky top-0 z-10 grid border-b bg-background"
                                style={{ gridTemplateColumns: DIAGNOSTICS_TABLE_GRID_TEMPLATE }}
                              >
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Object</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Evidence</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Source State</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Destination State</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Diagnosis</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Downloads</div>
                                <div className="text-foreground h-11 px-3 py-3 font-medium whitespace-nowrap">Recommended Action</div>
                              </div>
                              <div>
                                {group.failures.map((failure, idx) => (
                                  <div
                                    key={`${group.item.id}-${failure.key}-${failure.at ?? ""}-${idx}`}
                                    className="grid border-b transition-colors hover:bg-muted/50"
                                    style={{ gridTemplateColumns: DIAGNOSTICS_TABLE_GRID_TEMPLATE }}
                                  >
                                    <div className="p-3 font-mono text-xs">
                                      <div className="max-w-[320px] break-all text-foreground">{failure.key}</div>
                                    </div>
                                    <div className="p-3 text-xs">
                                      <div className="max-w-[260px] leading-5 text-foreground">{failure.message}</div>
                                      {failure.at ? <div className="mt-2 text-muted-foreground">{failure.at}</div> : null}
                                    </div>
                                    <div className="p-3 text-xs">
                                      {failure.source.exists === true ? (
                                        <div className="space-y-1">
                                          <div className="text-green-600">Exists</div>
                                          <div className="text-muted-foreground">{formatBytes(failure.source.size)}</div>
                                          {failure.source.contentType ? (
                                            <div className="text-muted-foreground">{failure.source.contentType}</div>
                                          ) : null}
                                          {failure.source.readable === true ? (
                                            <div className="text-green-600">Readable</div>
                                          ) : failure.source.readable === false ? (
                                            <div className="text-red-600">Read failed</div>
                                          ) : null}
                                        </div>
                                      ) : failure.source.exists === false ? (
                                        <div className="text-red-600">Missing</div>
                                      ) : (
                                        <div className="space-y-1">
                                          <div className="text-yellow-600">Unknown</div>
                                          {failure.source.error ? <div className="text-muted-foreground break-words">{failure.source.error}</div> : null}
                                        </div>
                                      )}
                                    </div>
                                    <div className="p-3 text-xs">
                                      {failure.destination.exists === true ? (
                                        <div className="space-y-1">
                                          <div className="text-green-600">Exists</div>
                                          <div className="text-muted-foreground">{formatBytes(failure.destination.size)}</div>
                                          {failure.destination.contentType ? (
                                            <div className="text-muted-foreground">{failure.destination.contentType}</div>
                                          ) : null}
                                        </div>
                                      ) : failure.destination.exists === false ? (
                                        <div className="text-muted-foreground">Not found</div>
                                      ) : (
                                        <div className="space-y-1">
                                          <div className="text-yellow-600">Unknown</div>
                                          {failure.destination.error ? <div className="text-muted-foreground break-words">{failure.destination.error}</div> : null}
                                        </div>
                                      )}
                                    </div>
                                    <div className="p-3 text-xs">
                                      <div className={cn("inline-flex rounded-full border px-2.5 py-1 text-[11px] font-medium", diagnosticCategoryTone(failure.diagnosis.category))}>
                                        {formatDiagnosticCategory(failure.diagnosis.category)}
                                      </div>
                                      <div className="mt-2 max-w-[200px] break-words text-muted-foreground">{failure.diagnosis.reason}</div>
                                    </div>
                                    <div className="p-3 text-xs">
                                      <div className="flex flex-col gap-1">
                                        {failure.download?.source ? (
                                          <a
                                            className="text-primary hover:underline"
                                            href={failure.download.source}
                                            target="_blank"
                                            rel="noreferrer"
                                          >
                                            Download source
                                          </a>
                                        ) : (
                                          <span className="text-muted-foreground">Source n/a</span>
                                        )}
                                        {failure.download?.destination ? (
                                          <a
                                            className="text-primary hover:underline"
                                            href={failure.download.destination}
                                            target="_blank"
                                            rel="noreferrer"
                                          >
                                            Download destination
                                          </a>
                                        ) : (
                                          <span className="text-muted-foreground">Destination n/a</span>
                                        )}
                                      </div>
                                    </div>
                                    <div className="p-3 text-xs">
                                      <div className="max-w-[300px] break-words leading-5">{failure.diagnosis.recommendation}</div>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </PinnedScrollableTable>
                        </div>
                      ))}
                    </div>
                  )}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={logsOpen} onOpenChange={setLogsOpen}>
        <DialogContent className="w-[92vw] sm:max-w-[min(92vw,64rem)]">
          <DialogHeader>
            <DialogTitle>Bucket Logs</DialogTitle>
            <DialogDescription className="truncate">
              {dialogLogItem ? (
                <span className="font-mono">
                  {dialogLogItem.sourceBucket} - {dialogLogItem.slurperJobId ?? "no job"}
                </span>
              ) : (
                "No bucket selected"
              )}
            </DialogDescription>
          </DialogHeader>
          <div className="rounded-md border bg-muted/20">
            <ScrollArea ref={bucketLogsRef} className="max-h-[min(620px,75vh)]" hideScrollbar>
              {bucketLogLines.length === 0 ? (
                <div className="p-3 text-sm text-muted-foreground">No logs yet.</div>
              ) : (
                <div className="min-w-[660px] p-2 text-xs font-mono">
                  <div className="sticky top-0 z-10 border-b bg-background/80 px-2 py-2 backdrop-blur supports-[backdrop-filter]:bg-background/60">
                    <div
                      className="grid gap-3 text-[11px] text-muted-foreground select-none"
                      style={{ gridTemplateColumns: `${bucketLogCols.time}px ${bucketLogCols.stage}px 1fr` }}
                    >
                      <div className="relative pr-8">
                        Time
                        <div
                          className="absolute right-0 top-1/2 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded text-muted-foreground/70 hover:bg-muted/40 hover:text-muted-foreground cursor-col-resize"
                          role="separator"
                          aria-label="Resize Time column"
                          onPointerDown={(e) => {
                            e.preventDefault()
                            e.stopPropagation()
                            resizeRef.current = {
                              kind: "bucket",
                              key: "time",
                              pointerId: e.pointerId,
                              startX: e.clientX,
                              startWidth: bucketLogCols.time,
                            }
                            try {
                              ;(e.currentTarget as HTMLElement).setPointerCapture(e.pointerId)
                            } catch {
                              // ignore
                            }
                          }}
                        >
                          <GripVertical className="h-4 w-4" />
                        </div>
                      </div>
                      <div className="relative pr-8">
                        Stage / Status
                        <div
                          className="absolute right-0 top-1/2 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded text-muted-foreground/70 hover:bg-muted/40 hover:text-muted-foreground cursor-col-resize"
                          role="separator"
                          aria-label="Resize Stage column"
                          onPointerDown={(e) => {
                            e.preventDefault()
                            e.stopPropagation()
                            resizeRef.current = {
                              kind: "bucket",
                              key: "stage",
                              pointerId: e.pointerId,
                              startX: e.clientX,
                              startWidth: bucketLogCols.stage,
                            }
                            try {
                              ;(e.currentTarget as HTMLElement).setPointerCapture(e.pointerId)
                            } catch {
                              // ignore
                            }
                          }}
                        >
                          <GripVertical className="h-4 w-4" />
                        </div>
                      </div>
                      <div>Message</div>
                    </div>
                  </div>
                  <div className="space-y-1 px-2 py-2">
                    {bucketLogLines.map((line, idx) => (
                      <div
                        key={`${line.atIso}-${idx}`}
                        className="grid gap-3"
                        style={{ gridTemplateColumns: `${bucketLogCols.time}px ${bucketLogCols.stage}px 1fr` }}
                      >
                        <div className="truncate text-muted-foreground">{formatLogTime(line.atIso)}</div>
                        <div className="truncate text-muted-foreground">
                          {line.stage ? `${line.stage}${line.verificationGeneration ? ` · generation ${line.verificationGeneration}${line.verificationHistorical ? " (not current)" : ""}` : line.verificationAttemptUnknown ? " · attempt unknown (legacy event)" : ""}` : "-"}
                          {line.status ? ` - ${line.status}` : ""}
                        </div>
                        <div className="whitespace-pre-wrap break-words">{line.message || "-"}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </ScrollArea>
          </div>
        </DialogContent>
      </Dialog>

      <div className="flex justify-end border-t pt-4">
        <Button variant="destructive" onClick={() => setDeleteOpen(true)} disabled={Boolean(busyAction)}>
          <Trash2 className="h-4 w-4 mr-0" />
          Delete migration
        </Button>
      </div>

      <AlertDialog open={manualCompleteOpen} onOpenChange={setManualCompleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Mark migration completed?</AlertDialogTitle>
            <AlertDialogDescription>
              This will mark the migration as completed and switch the active Cloudflare account to the migrated target account.
              The previous active account will be disabled. There is no automatic rollback after this switch.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={Boolean(busyAction)}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                setManualCompleteOpen(false)
                void runMigrationAction("mark_completed")
              }}
              disabled={Boolean(busyAction)}
            >
              {busyAction === "mark_completed" ? <Spinner className="mr-0" /> : null}
              Confirm
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete migration?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the migration and its stored items from the database. It does not cancel Cloudflare jobs that may already be running.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={Boolean(busyAction)}>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={deleteMigration} disabled={Boolean(busyAction)}>
              {busyAction === "delete" ? <Spinner className="mr-0" /> : null}
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}


