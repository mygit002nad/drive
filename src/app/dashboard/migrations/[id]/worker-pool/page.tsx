"use client"

import * as React from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import type { ColumnDef } from "@tanstack/react-table"
import { ArrowLeft, Clock3, Files, RefreshCw, Trash2, Workflow, XCircle } from "lucide-react"
import { toast } from "sonner"

import { DashboardDataTable } from "@/components/dashboard/data-table"
import { DashboardPage } from "@/components/dashboard/page-shell"
import { formatLastSyncedAt } from "@/lib/dashboard-format"
import { currentMigrationFiles } from "@/lib/migration-live-files"
import { formatLogStage } from "@/lib/dashboard-log-format"
import { poolBucketTransferStatus } from "@/lib/migration-pool-transfer-status"
import { migrationProgressPercent } from "@/lib/migration-progress"
import { DashboardLogsCard } from "@/components/dashboard/logs-card"
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Skeleton } from "@/components/ui/skeleton"

type WorkerJob = { id: string; status: string; mode?: string; claimedByAgentId?: string; claimed_by_agent_id?: string; progress?: Record<string, unknown>; result?: Record<string, unknown> }
type JobRow = { id: string; status: string; claimedByAgentId?: string; summary?: string; error?: string; createdAt?: string; updatedAt?: string; lastHeartbeatAt?: string; completedAt?: string; objectKey?: string; objectSize?: number; sourceBucket?: string; targetBucket?: string; transferred?: number; skipped?: number; failed?: number }
type RepairJob = { id: string; migrationId: string; status: string; mode: string; claimedByAgentId?: string; payload: Record<string, unknown>; progress: Record<string, unknown>; result: Record<string, unknown>; summary?: string; error?: string; createdAt?: string; updatedAt?: string }
type BucketStat = { id: string; sourceBucket: string; targetBucket: string; status: string; totalObjects: number; queuedObjects?: number; runningObjects?: number; transferredObjects: number; skippedObjects: number; failedObjects: number; sourceBytes: number }
type PoolSnapshot = { workerGeneration?: number; onlineWorkers?: number; activeTransfers?: number; totalJobs?: number; queuedJobs?: number; runningJobs?: number; remainingJobs?: number; completedJobs?: number; failedJobs?: number; canceledJobs?: number; totalObjects?: number; transferred?: number; inProgressFiles?: number; skipped?: number; failed?: number; processedFiles?: number; completedBytes?: number; inProgressBytes?: number; workerRuntimeSeconds?: number; buckets?: BucketStat[]; updatedAt?: string; sampledAt?: string }
type Pagination = { pageIndex: number; pageSize: number; pageCount: number; total: number }
type PoolAttempt = { generation: number; status: string; workerCount: number; runningWorkers: number; onlineWorkers: number; totalJobs: number; queuedJobs: number; runningJobs: number; remainingJobs?: number; completedJobs: number; failedJobs: number; canceledJobs: number; createdAt?: string; updatedAt?: string }
type PoolWorkerRun = { id: string; agentId: string; status: string; online: boolean; abortRequested?: boolean; externalRunId?: string; instanceId?: string; jobId?: string; currentStatus?: string; lastHeartbeatAt?: string; completedJobs?: number; failedJobs?: number; finishedJobs?: number; completedFiles?: number; failedFiles?: number; completedBytes?: number; createdAt?: string; updatedAt?: string }

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null }
function num(value: unknown) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : 0 }
function formatNumber(value: number) { return new Intl.NumberFormat().format(Math.max(0, value)) }
function formatDate(value?: string) { if (!value) return "—"; const date = new Date(value); return Number.isNaN(date.getTime()) ? value : date.toLocaleString() }
function formatBytes(value: number) { if (value <= 0) return "0 B"; const units = ["B", "KB", "MB", "GB", "TB"]; let size = value; let unit = 0; while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1 } return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[unit]}` }
function formatWorkerHours(value: number) { const seconds = Math.max(0, Math.floor(value)); return `${(seconds / 3600).toFixed(2)} h` }
function percentage(done: number, total: number) { return total > 0 ? Math.max(0, Math.min(100, (done / total) * 100)) : 0 }

function WorkerTableDate({ value }: { value?: string }) {
  const date = value ? new Date(value) : null
  if (!date || Number.isNaN(date.getTime())) return <span className="text-muted-foreground">—</span>
  return (
    <time dateTime={date.toISOString()} title={date.toLocaleString()} className="flex min-h-10 flex-col items-center justify-center gap-0.5 text-center text-[11px] text-muted-foreground">
      <span>{date.toLocaleDateString()}</span>
      <span>{date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</span>
    </time>
  )
}

function statusBadge(status?: string) {
  const value = String(status || "").toLowerCase()
  if (["completed", "complete", "copied", "verified"].includes(value)) return <Badge>Completed</Badge>
  if (value === "processing") return <Badge variant="secondary">Processing</Badge>
  if (value === "dispatching") return <Badge variant="secondary">Dispatching</Badge>
  if (["running", "copying", "transferring"].includes(value)) return <Badge>Running</Badge>
  if (["scanning", "verifying"].includes(value)) return <Badge variant="secondary">{value === "scanning" ? "Scanning" : "Verifying"}</Badge>
  if (["pending", "queued", "claimed"].includes(value)) return <Badge variant="outline">Queued</Badge>
  if (value === "failed") return <Badge variant="destructive">Failed</Badge>
  if (["canceled", "aborted"].includes(value)) return <Badge variant="outline">Canceled</Badge>
  return <Badge variant="outline">{value || "Pending"}</Badge>
}

function MetricCard({ label, value, detail, icon: Icon }: { label: string; value: string; detail: string; icon: React.ComponentType<{ className?: string }> }) {
  return <Card className="gap-0 py-0"><CardHeader className="px-4 py-3 pb-1.5"><div className="flex items-center justify-between gap-3"><CardDescription className="text-[13px] leading-4">{label}</CardDescription><Icon className="size-4 text-muted-foreground" /></div><CardTitle className="text-xl font-bold leading-none tabular-nums sm:text-2xl">{value}</CardTitle></CardHeader><CardContent className="px-4 pb-3 pt-0"><p className="text-[11px] leading-4 text-muted-foreground">{detail}</p></CardContent></Card>
}

function PageSkeleton() {
  return <DashboardPage><div className="flex flex-col gap-5"><Skeleton className="h-16 w-full" /><div className="grid grid-cols-2 gap-4 xl:grid-cols-5">{Array.from({ length: 5 }, (_, i) => <Skeleton key={i} className="h-24" />)}</div><Skeleton className="h-96" /></div></DashboardPage>
}

export default function MigrationWorkerPoolDetailsPage() {
  const params = useParams<{ id: string }>()
  const migrationId = typeof params?.id === "string" ? params.id : ""
  const [jobs, setJobs] = React.useState<WorkerJob[]>([])
  const [liveFiles, setLiveFiles] = React.useState<Array<Record<string, unknown>>>([])
  const [liveNow, setLiveNow] = React.useState(() => Date.now())
  const [jobPage, setJobPage] = React.useState<JobRow[]>([])
  const [pagination, setPagination] = React.useState<Pagination>({ pageIndex: 0, pageSize: 25, pageCount: 1, total: 0 })
  const [snapshot, setSnapshot] = React.useState<PoolSnapshot>({})
  const [attempts, setAttempts] = React.useState<PoolAttempt[]>([])
  const [workerRuns, setWorkerRuns] = React.useState<PoolWorkerRun[]>([])
  const [selectedGeneration, setSelectedGeneration] = React.useState(0)
  const selectedGenerationRef = React.useRef(0)
  selectedGenerationRef.current = selectedGeneration
  const [selectedJob, setSelectedJob] = React.useState<RepairJob | null>(null)
  const [selectedJobId, setSelectedJobId] = React.useState<string | null>(null)
  const [loading, setLoading] = React.useState(true)
  const [refreshing, setRefreshing] = React.useState(false)
  const [mutating, setMutating] = React.useState(false)
  const [deleteOpen, setDeleteOpen] = React.useState(false)
  const inFlight = React.useRef(false)

  const load = React.useCallback(async (options?: { manual?: boolean; background?: boolean; page?: number }) => {
    if (!migrationId || inFlight.current) return
    inFlight.current = true
    const nextPage = options?.page ?? pagination.pageIndex
    try {
      if (options?.manual) setRefreshing(true)
      else if (!options?.background) setLoading(true)
      const poolRequest = fetch(`/api/migrations/${encodeURIComponent(migrationId)}/worker-pool?page=${nextPage}&pageSize=${pagination.pageSize}&generation=${selectedGeneration}`, { cache: "no-store" })
      const detailRequest = selectedJobId ? fetch(`/api/repair-jobs/${encodeURIComponent(selectedJobId)}`, { cache: "no-store" }) : null
      const [poolResponse, detailResponse] = await Promise.all([poolRequest, detailRequest])
      const data = await poolResponse.json().catch(() => ({}))
      if (!poolResponse.ok) throw new Error(data.error || "Unable to load migration worker pool")
      if (selectedGenerationRef.current > 0 && Number(data.selectedGeneration) !== selectedGenerationRef.current) return
      setJobs(Array.isArray(data.jobs) ? data.jobs : [])
      setLiveFiles(Array.isArray(data.liveFiles) ? data.liveFiles.filter(isRecord) : [])
      setLiveNow(Date.now())
      setJobPage(Array.isArray(data.jobPage) ? data.jobPage : [])
      const incomingSnapshot = isRecord(data.snapshot) ? data.snapshot as PoolSnapshot : {}
      setSnapshot((previous) => {
        const incoming = incomingSnapshot
        const sameGeneration = Number(previous.workerGeneration || 0) === Number(incoming.workerGeneration || 0)
        const previousTime = Date.parse(String(previous.sampledAt || previous.updatedAt || ""))
        const incomingTime = Date.parse(String(incoming.sampledAt || incoming.updatedAt || ""))
        return sameGeneration && Number.isFinite(previousTime) && Number.isFinite(incomingTime) && incomingTime < previousTime ? previous : incoming
      })
      setAttempts(Array.isArray(data.attempts) ? data.attempts : [])
      setWorkerRuns(Array.isArray(data.workerRuns) ? data.workerRuns : [])
      if (selectedGeneration === 0 && Number(data.selectedGeneration) > 0) setSelectedGeneration(Number(data.selectedGeneration))
      if (isRecord(data.jobPagination)) setPagination(data.jobPagination as Pagination)
      if (detailResponse) {
        const detail = await detailResponse.json().catch(() => ({}))
        if (detailResponse.ok) setSelectedJob(detail.job ?? null)
        else if (detailResponse.status === 404) { setSelectedJobId(null); setSelectedJob(null) }
      }
    } catch (error) {
      setLiveFiles([])
      if (!options?.background) toast.error(error instanceof Error ? error.message : "Unable to load migration worker pool")
    } finally {
      inFlight.current = false
      setLoading(false)
      setRefreshing(false)
    }
  }, [migrationId, pagination.pageIndex, pagination.pageSize, selectedGeneration, selectedJobId])

  React.useEffect(() => { void load() }, [load])
  React.useEffect(() => {
    const tick = () => { if (document.visibilityState === "visible") setLiveNow(Date.now()) }
    const timer = window.setInterval(tick, 10_000)
    document.addEventListener("visibilitychange", tick)
    return () => { window.clearInterval(timer); document.removeEventListener("visibilitychange", tick) }
  }, [])
  React.useEffect(() => {
    const refresh = () => { if (document.visibilityState === "visible") void load({ background: true }) }
    const timer = window.setInterval(refresh, 30_000)
    document.addEventListener("visibilitychange", refresh)
    return () => { window.clearInterval(timer); document.removeEventListener("visibilitychange", refresh) }
  }, [load])
  React.useEffect(() => { setLiveFiles([]) }, [migrationId, selectedGeneration])

  const openJob = React.useCallback(async (id: string) => {
    setSelectedJobId(id); setSelectedJob(null)
    try {
      const response = await fetch(`/api/migrations/${encodeURIComponent(migrationId)}/worker-pool?jobId=${encodeURIComponent(id)}&generation=${selectedGeneration}`, { cache: "no-store" })
      const data = await response.json().catch(() => ({}))
      if (!response.ok) throw new Error(data.error || "Unable to load worker job")
      setSelectedJob(data.job ?? null)
    } catch (error) { toast.error(error instanceof Error ? error.message : "Unable to load worker job") }
  }, [migrationId, selectedGeneration])

  const mutateSelected = React.useCallback(async (method: "POST" | "DELETE") => {
    if (!selectedJobId) return
    try {
      setMutating(true)
      const response = await fetch(`/api/repair-jobs/${encodeURIComponent(selectedJobId)}`, { method, ...(method === "POST" ? { headers: { "Content-Type": "application/json" }, body: JSON.stringify({ action: "abort" }) } : {}) })
      const data = await response.json().catch(() => ({}))
      if (!response.ok) throw new Error(data.error || `Unable to ${method === "POST" ? "abort" : "delete"} worker job`)
      toast.success(method === "POST" ? "Worker job abort requested" : "Worker job deleted")
      if (method === "DELETE") { setSelectedJobId(null); setSelectedJob(null); setDeleteOpen(false) }
      await load({ manual: true })
    } catch (error) { toast.error(error instanceof Error ? error.message : "Unable to update worker job") }
    finally { setMutating(false) }
  }, [load, selectedJobId])

  const telemetry = React.useMemo(() => {
    const logs: Array<Record<string, unknown>> = []
    const seen = new Set<string>()
    for (const job of jobs) {
      const entries = [
        ...(Array.isArray(job.progress?.logs) ? job.progress.logs : []),
        ...(Array.isArray(job.result?.logs) ? job.result.logs : []),
      ]
      for (const entry of entries) {
        if (!isRecord(entry)) continue
        const key = `${String(entry.at || "")}:${String(entry.message || "")}:${String(entry.itemId || "")}`
        if (seen.has(key)) continue
        seen.add(key)
        logs.push({ ...entry, operation: job.mode, bucket: entry.bucket || snapshot.buckets?.find(bucket => bucket.id === entry.itemId)?.sourceBucket })
      }
    }
    logs.sort((a, b) => String(a.at || "").localeCompare(String(b.at || "")))
    return { logs: logs.slice(-100).map(entry => ({
      at: String(entry.at || ""), context: String(entry.bucket || ""), stage: String(entry.stage || ""),
      operation: String(entry.operation || ""), status: String(entry.status || ""), message: String(entry.message || ""),
    })) }
  }, [jobs, snapshot.buckets])
  const activeFiles = React.useMemo(() => currentMigrationFiles(liveFiles, liveNow), [liveFiles, liveNow])

  const bucketColumns = React.useMemo<ColumnDef<BucketStat, unknown>[]>(() => [
    {
      accessorKey: "sourceBucket", header: "Source", meta: { width: "min-w-[240px]" },
      cell: ({ row }) => <div className="flex min-h-10 items-center gap-1.5">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-lg border bg-background"><Files className="size-3 text-muted-foreground" /></span>
        <div className="min-w-0 max-w-[260px]">
          <div className="truncate text-[13px] font-medium leading-4" title={row.original.sourceBucket}>{row.original.sourceBucket}</div>
          <div className="text-[10px] leading-3.5 text-muted-foreground">{formatBytes(num(row.original.sourceBytes))}</div>
        </div>
      </div>,
    },
    { accessorKey: "targetBucket", header: "Target", meta: { width: "min-w-[200px]", align: "center" }, cell: ({ row }) => <span className="break-all text-[13px] font-medium">{row.original.targetBucket}</span> },
    { accessorKey: "status", header: "Status", meta: { width: "min-w-[120px]", align: "center" }, cell: ({ row }) => <div className="flex min-h-10 items-center justify-center">{statusBadge(poolBucketTransferStatus(row.original))}</div> },
    { id: "total", header: "Total", meta: { width: "min-w-[110px]", align: "center" }, cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.totalObjects))}</span> },
    { id: "queue", header: "Queue", meta: { width: "min-w-[110px]", align: "center" }, cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.queuedObjects))}</span> },
    { id: "inProgress", header: "In progress", meta: { width: "min-w-[110px]", align: "center" }, cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.runningObjects))}</span> },
    { id: "transferred", header: "Transferred", meta: { width: "min-w-[130px]", align: "center" }, cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.transferredObjects))}</span> },
    ...(snapshot.buckets?.some((bucket) => num(bucket.skippedObjects) > 0) ? [{ id: "skipped", header: "Skipped", meta: { width: "min-w-[120px]", align: "center" }, cell: ({ row }: { row: { original: BucketStat } }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.skippedObjects))}</span> }] : []),
    ...(snapshot.buckets?.some((bucket) => num(bucket.failedObjects) > 0) ? [{ id: "failed", header: "Failed", meta: { width: "min-w-[120px]", align: "center" }, cell: ({ row }: { row: { original: BucketStat } }) => <span className="text-[13px] font-medium tabular-nums text-destructive">{formatNumber(num(row.original.failedObjects))}</span> }] : []),
    { id: "progress", header: "Progress", meta: { width: "min-w-[180px]", align: "center", divider: false }, cell: ({ row }) => { const done = num(row.original.transferredObjects) + num(row.original.skippedObjects); const value = migrationProgressPercent(done, num(row.original.totalObjects)); return <div className="flex min-h-10 flex-col justify-center gap-1.5"><Progress value={value} className="h-1.5" /><span className="text-[11px] tabular-nums text-muted-foreground">{value.toFixed(1)}% · {formatNumber(done)} / {formatNumber(num(row.original.totalObjects))}</span></div> } },
  ], [snapshot.buckets])

  const fileColumns = React.useMemo<ColumnDef<Record<string, unknown>, unknown>[]>(() => [
    {
      id: "object", header: "Object", meta: { width: "min-w-[280px]" },
      cell: ({ row }) => (
        <div className="flex min-h-10 items-center gap-1.5">
          <span className="flex size-7 shrink-0 items-center justify-center rounded-lg border bg-background"><Files className="size-3 text-muted-foreground" /></span>
          <div className="min-w-0 max-w-[320px]">
            <div className="truncate text-[13px] font-medium leading-4" title={String(row.original.key)}>{String(row.original.key)}</div>
            <div className="truncate font-mono text-[10px] leading-3.5 text-muted-foreground" title={String(row.original.bucket || "")}>{String(row.original.bucket || "—")}</div>
          </div>
        </div>
      ),
    },
    {
      id: "stage", header: "Stage", meta: { width: "min-w-[150px]", align: "center" },
      cell: ({ row }) => <span className="text-[11px] text-muted-foreground">{formatLogStage(String(row.original.stage || ""), String(row.original.operation || ""))}</span>,
    },
    {
      id: "status", header: "Status", meta: { width: "min-w-[120px]", align: "center" },
      cell: ({ row }) => <div className="flex min-h-10 items-center justify-center">{statusBadge(String(row.original.status))}</div>,
    },
    {
      id: "progress", header: "Transferred", meta: { width: "min-w-[180px]", align: "center" },
      cell: ({ row }) => (
        <div className="flex min-h-10 flex-col justify-center gap-1.5">
          <Progress value={percentage(num(row.original.bytesTransferred), num(row.original.bytesTotal ?? row.original.size))} className="h-1.5" />
          <span className="text-[11px] tabular-nums text-muted-foreground">{formatBytes(num(row.original.bytesTransferred))} / {formatBytes(num(row.original.bytesTotal ?? row.original.size))}</span>
        </div>
      ),
    },
    {
      id: "size", header: "Size", meta: { width: "min-w-[120px]", align: "center" },
      cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatBytes(num(row.original.size ?? row.original.bytesTotal))}</span>,
    },
    {
      id: "worker", header: "Worker", meta: { width: "min-w-[240px]", align: "center" },
      cell: ({ row }) => <span className="break-all font-mono text-[10px] text-muted-foreground">{String(row.original.workerId || "—")}</span>,
    },
    {
      id: "updated", header: "Updated", meta: { width: "min-w-[160px]", align: "center", divider: false },
      cell: ({ row }) => <WorkerTableDate value={String(row.original.updatedAt || row.original.startedAt || row.original.lastHeartbeatAt || "")} />,
    },
  ], [])

  const jobColumns = React.useMemo<ColumnDef<JobRow, unknown>[]>(() => [
    {
      id: "object", header: "Object", meta: { width: "min-w-[280px]" },
      cell: ({ row }) => <div className="flex min-h-10 items-center gap-1.5">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-lg border bg-background"><Files className="size-3 text-muted-foreground" /></span>
        <div className="min-w-0 max-w-[320px]">
          <div className="truncate text-[13px] font-medium leading-4" title={row.original.objectKey || row.original.id}>{row.original.objectKey || row.original.id}</div>
          <div className="truncate font-mono text-[10px] leading-3.5 text-muted-foreground" title={row.original.sourceBucket}>{row.original.sourceBucket || "Unknown bucket"}</div>
        </div>
      </div>,
    },
    { accessorKey: "status", header: "Status", meta: { width: "min-w-[120px]", align: "center" }, cell: ({ row }) => <div className="flex min-h-10 items-center justify-center">{statusBadge(row.original.status)}</div> },
    { id: "size", header: "Size", meta: { width: "min-w-[120px]", align: "center" }, cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatBytes(num(row.original.objectSize))}</span> },
    { id: "worker", header: "Worker", meta: { width: "min-w-[240px]", align: "center" }, cell: ({ row }) => <span className="break-all font-mono text-[10px] text-muted-foreground">{row.original.claimedByAgentId || "\u2014"}</span> },
    { id: "updated", header: "Updated", meta: { width: "min-w-[160px]", align: "center" }, cell: ({ row }) => <WorkerTableDate value={row.original.updatedAt} /> },
    { id: "actions", header: "Actions", meta: { width: "min-w-[100px]", align: "center", divider: false }, cell: ({ row }) => <Button variant="outline" size="sm" onClick={() => void openJob(row.original.id)}>Details</Button> },
  ], [openJob])

  const workerRunColumns = React.useMemo<ColumnDef<PoolWorkerRun, unknown>[]>(() => [
    {
      id: "worker",
      header: "Worker",
      meta: { width: "min-w-[280px]" },
      cell: ({ row }) => (
        <div className="flex min-h-10 items-center gap-1.5">
          <span className="flex size-7 shrink-0 items-center justify-center rounded-lg border bg-background">
            <Workflow className="size-3 text-muted-foreground" />
          </span>
          <div className="min-w-0 max-w-[280px]">
            <div className="truncate text-[13px] font-medium leading-4" title={row.original.instanceId || row.original.agentId}>Worker {row.original.instanceId ? row.original.instanceId.slice(0, 8) : "instance"}</div>
            <div className="break-all font-mono text-[10px] leading-3.5 text-muted-foreground" title={row.original.agentId}>Workflow {row.original.agentId || "unknown"}</div>
          </div>
        </div>
      ),
    },
    {
      accessorKey: "status",
      header: "Status",
      meta: { width: "min-w-[120px]", align: "center" },
      cell: ({ row }) => <div className="flex min-h-10 items-center justify-center">{statusBadge(row.original.online ? "running" : row.original.status)}</div>,
    },
    {
      id: "heartbeat",
      header: "Last heartbeat",
      meta: { width: "min-w-[160px]", align: "center" },
      cell: ({ row }) => row.original.lastHeartbeatAt
        ? <WorkerTableDate value={row.original.lastHeartbeatAt} />
        : <span className="text-[11px] text-muted-foreground">Not started</span>,
    },
    {
      id: "completed",
      header: "Files transferred",
      meta: { width: "min-w-[130px]", align: "center" },
      cell: ({ row }) => <span className="text-[13px] font-medium tabular-nums">{formatNumber(num(row.original.completedFiles))}</span>,
    },
    ...(workerRuns.some((worker) => num(worker.failedFiles) > 0) ? [{
      id: "failedFiles",
      header: "Files failed",
      meta: { width: "min-w-[130px]", align: "center" },
      cell: ({ row }: { row: { original: PoolWorkerRun } }) => <span className="text-[13px] font-medium tabular-nums text-destructive">{formatNumber(num(row.original.failedFiles))}</span>,
    }] : []),
    {
      id: "updated",
      header: "Updated",
      meta: { width: "min-w-[160px]", align: "center", divider: false },
      cell: ({ row }) => <WorkerTableDate value={row.original.updatedAt} />,
    },
  ], [workerRuns])

  if (loading) return <PageSkeleton />
  const buckets = Array.isArray(snapshot.buckets) ? snapshot.buckets : []
  const totalJobs = num(snapshot.totalJobs); const queuedJobs = num(snapshot.queuedJobs); const runningJobs = num(snapshot.runningJobs)
  const selectedActive = selectedJob && ["pending", "claimed", "running"].includes(selectedJob.status)
  const selectedTotals = isRecord(selectedJob?.result?.totals) ? selectedJob?.result.totals : isRecord(selectedJob?.progress?.totals) ? selectedJob?.progress.totals : {}
  const selectedFailureSamples = Array.isArray(selectedJob?.result?.items)
    ? selectedJob.result.items.flatMap((item) => isRecord(item) && Array.isArray(item.failureSamples) ? item.failureSamples.filter(isRecord) : []).slice(0, 25)
    : []
  const selectedAttempt = attempts.find((attempt) => Number(attempt.generation) === selectedGeneration) ?? attempts[0]
  const attemptTotalJobs = selectedAttempt ? num(selectedAttempt.totalJobs) : totalJobs
  const attemptQueuedJobs = selectedAttempt ? num(selectedAttempt.queuedJobs) : queuedJobs
  const attemptRunningJobs = selectedAttempt ? num(selectedAttempt.runningJobs) : runningJobs
  const attemptRemainingJobs = selectedAttempt ? num(selectedAttempt.remainingJobs ?? (attemptQueuedJobs + attemptRunningJobs)) : num(snapshot.remainingJobs ?? (queuedJobs + runningJobs))
  const poolCompletedJobs = num(selectedAttempt?.completedJobs)
  const poolProgress = percentage(poolCompletedJobs, attemptTotalJobs)
  const workerRuntimeSeconds = num(snapshot.workerRuntimeSeconds)
  const inProgressFiles = num(snapshot.inProgressFiles ?? snapshot.activeTransfers)

  return <DashboardPage className="dashboard-motion-stage">
    <div className="flex flex-col gap-4">
      <div className="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <p className="text-sm text-muted-foreground">{formatLastSyncedAt(snapshot.updatedAt)}</p>
        <div className="flex w-full gap-2 sm:w-auto"><Button asChild variant="outline" size="sm" className="flex-1 rounded-xl sm:flex-none"><Link href={`/dashboard/migrations/${encodeURIComponent(migrationId)}`}><ArrowLeft data-icon="inline-start" />Back</Link></Button><Button variant="outline" size="sm" className="flex-1 rounded-xl sm:flex-none" onClick={() => void load({ manual: true })} disabled={refreshing}><RefreshCw data-icon="inline-start" className={refreshing ? "animate-spin" : undefined} />Refresh</Button></div>
      </div>
      <div className="flex max-w-full items-center gap-1 overflow-x-auto rounded-xl border bg-muted/30 p-1">
          {[...attempts].sort((a, b) => b.generation - a.generation).map((attempt) => {
            const selected = attempt.generation === selectedGeneration
            return <Button key={attempt.generation} type="button" variant="ghost" aria-pressed={selected} className={`h-8 shrink-0 gap-2 rounded-lg border px-3 text-xs ${selected ? "border-border bg-background text-foreground shadow-sm hover:bg-background" : "border-transparent text-muted-foreground hover:bg-background/70 hover:text-foreground"}`} onClick={() => {
              if (selected) return
              selectedGenerationRef.current = attempt.generation
              setSelectedGeneration(attempt.generation)
              setPagination((current) => ({ ...current, pageIndex: 0 }))
              setSelectedJobId(null)
              setSelectedJob(null)
            }}>
              <span className="font-medium">{attempt.generation === 1 ? "Initial pool" : `Retry ${attempt.generation - 1}`}</span>
              <span className="text-muted-foreground">{formatNumber(num(attempt.completedJobs))}/{formatNumber(num(attempt.totalJobs))}</span>
              <span className="sr-only">{attempt.status}</span>
            </Button>
          })}
          {attempts.length === 0 ? <span className="px-3 py-1.5 text-xs text-muted-foreground">No worker pools have been dispatched yet.</span> : null}
      </div>
      <Card className="gap-0 py-0">
        <CardHeader className="flex flex-row items-center justify-between gap-4 px-5 py-3 pb-2">
          <div><CardTitle className="text-sm">Selected pool progress</CardTitle><CardDescription className="mt-1">{formatNumber(poolCompletedJobs)} of {formatNumber(attemptTotalJobs)} jobs completed</CardDescription></div>
          <span className="font-mono text-sm font-semibold tabular-nums">{poolProgress.toFixed(1)}%</span>
        </CardHeader>
        <CardContent className="px-5 pb-4 pt-0"><Progress value={poolProgress} className="h-2" /></CardContent>
      </Card>
        <div className="grid grid-cols-2 gap-4 xl:grid-cols-4"><MetricCard label="Migration queue" value={formatNumber(attemptRemainingJobs)} detail={`${formatNumber(attemptTotalJobs)} total jobs`} icon={Clock3} /><MetricCard label="Total transfer" value={formatNumber(num(snapshot.transferred) + inProgressFiles)} detail={`${formatNumber(num(snapshot.transferred))} completed · ${formatNumber(inProgressFiles)} in progress`} icon={RefreshCw} /><MetricCard label="Skipped" value={formatNumber(num(snapshot.skipped))} detail="Existing objects preserved" icon={Files} /><MetricCard label="Worker hours" value={formatWorkerHours(workerRuntimeSeconds)} detail={`${formatNumber(num(selectedAttempt?.onlineWorkers))} online - elapsed time across all worker instances`} icon={Workflow} /></div>
        <DashboardDataTable
          data={buckets}
          columns={bucketColumns}
          pageSize={10}
          minWidth="1120px"
          resetKey={`${migrationId}:${selectedGeneration}`}
          emptyState="Bucket statistics are not available yet."
        />
        {activeFiles.length > 0 ? (
        <DashboardDataTable
          data={activeFiles}
          columns={fileColumns}
          pageSize={25}
          minWidth="1250px"
          resetKey={`${migrationId}:${selectedGeneration}`}
          emptyState="No files are currently transferring."
        />
        ) : null}
        <DashboardDataTable
          data={workerRuns}
          columns={workerRunColumns}
          pageSize={10}
          minWidth="1020px"
          resetKey={selectedGeneration}
          emptyState="No workers were dispatched for this attempt."
        />
        <DashboardLogsCard
          key={selectedGeneration}
          title="Worker Logs"
          entries={telemetry.logs}
          storageKey="drive:workerPoolLogsCols:v1"
          emptyState="No worker logs captured yet."
        />
        <DashboardDataTable data={jobPage} columns={jobColumns} pageSize={pagination.pageSize} minWidth="1020px" withCard={false} serverPagination={{ pageIndex: pagination.pageIndex, pageCount: pagination.pageCount, onPageChange: (page) => { setPagination((current) => ({ ...current, pageIndex: page })); void load({ page }) } }} emptyState="No file jobs were materialized for this attempt." />
        {selectedJobId ? <Card className="gap-0 overflow-hidden py-0"><CardHeader className="border-b px-5 py-4"><div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between"><div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><CardTitle className="text-base">Job details</CardTitle>{selectedJob ? statusBadge(selectedJob.status) : <Badge variant="outline">Loading</Badge>}</div><CardDescription className="mt-1 truncate font-mono">{selectedJobId}</CardDescription></div>{selectedJob ? <div className="flex flex-wrap gap-2">{selectedActive ? <Button variant="destructive" size="sm" onClick={() => void mutateSelected("POST")} disabled={mutating}><XCircle data-icon="inline-start" />Abort</Button> : null}<Button variant="outline" size="sm" onClick={() => setDeleteOpen(true)} disabled={mutating || Boolean(selectedActive)}><Trash2 data-icon="inline-start" />Delete</Button></div> : null}</div></CardHeader><CardContent className="p-5">{selectedJob ? <div className="flex flex-col gap-5"><div className="grid gap-px overflow-hidden rounded-xl border bg-border sm:grid-cols-2 xl:grid-cols-4">{[["Transferred", num(selectedTotals?.transferred)], ["Skipped", num(selectedTotals?.skipped)], ["Failed", num(selectedTotals?.failed)], ["Worker", selectedJob.claimedByAgentId || "—"]].map(([label, value]) => <div key={String(label)} className="bg-background p-4"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 truncate font-medium tabular-nums">{typeof value === "number" ? formatNumber(value) : value}</p></div>)}</div>{selectedJob.summary || selectedJob.error ? <div className="rounded-xl border bg-muted/25 p-4"><p className="text-sm font-medium">{selectedJob.summary || "Worker job update"}</p>{selectedJob.error ? <p className="mt-2 text-sm text-destructive">{selectedJob.error}</p> : null}</div> : null}<div className="grid gap-3 text-sm sm:grid-cols-2 xl:grid-cols-4"><div><p className="text-xs text-muted-foreground">Mode</p><p className="mt-1 font-medium">{selectedJob.mode}</p></div><div><p className="text-xs text-muted-foreground">Created</p><p className="mt-1 font-medium">{formatDate(selectedJob.createdAt)}</p></div><div><p className="text-xs text-muted-foreground">Updated</p><p className="mt-1 font-medium">{formatDate(selectedJob.updatedAt)}</p></div><div><p className="text-xs text-muted-foreground">Migration</p><p className="mt-1 truncate font-mono text-xs">{selectedJob.migrationId}</p></div></div></div> : <Skeleton className="h-40 w-full" />}</CardContent></Card> : null}
        {selectedJob && selectedFailureSamples.length > 0 ? <Card className="gap-0 py-0"><CardHeader className="border-b px-5 py-4"><CardTitle className="text-base">File failure details</CardTitle><CardDescription>Worker recorded these copy errors for this job.</CardDescription></CardHeader><CardContent className="max-h-96 space-y-2 overflow-auto p-5">{selectedFailureSamples.map((sample, index) => <div key={`${String(sample.key || "file")}:${index}`} className="rounded-lg border border-destructive/20 bg-muted/30 p-3"><p className="break-all font-mono text-xs">{String(sample.key || "Unknown file")}</p><p className="mt-1 break-words text-xs text-destructive">{String(sample.error || "No error message was recorded")}</p></div>)}</CardContent></Card> : null}
    </div>
    <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}><AlertDialogContent><AlertDialogHeader><AlertDialogTitle>Delete this worker job?</AlertDialogTitle><AlertDialogDescription>This removes the terminal job record. Active jobs must be aborted and fully stopped first.</AlertDialogDescription></AlertDialogHeader><AlertDialogFooter><AlertDialogCancel disabled={mutating}>Keep job</AlertDialogCancel><AlertDialogAction className="bg-destructive text-destructive-foreground hover:bg-destructive/90" onClick={(event) => { event.preventDefault(); void mutateSelected("DELETE") }} disabled={mutating}>Delete job</AlertDialogAction></AlertDialogFooter></AlertDialogContent></AlertDialog>
  </DashboardPage>
}
