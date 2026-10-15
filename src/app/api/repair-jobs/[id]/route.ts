import { NextResponse } from "next/server"
import { cookies } from "next/headers"
import { getAgentById, getAgentGithubToken, getLatestAgentRunByJobReference, listAgentRunsByAgentId, updateAgent, updateAgentRun } from "@/lib/agents-store"
import {
  cancelGitHubWorkflowRun,
  forceCancelGitHubWorkflowRun,
  getGitHubWorkflowRun,
  GITHUB_TOKEN_COOKIE,
} from "@/lib/github-oauth"
import { abortRepairJob, deleteRepairJob, getRepairJob, getRepairJobDetail } from "@/lib/repair-jobs-store"
import { requireAdmin } from "@/lib/server-auth"
import { recordUserActivity } from "@/lib/activity-audit"
import { queryDb } from "@/lib/db"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { withMigrationQueueDatabase } from "@/lib/migration-queue-database"

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback
}

function getGitHubTokenFallback(): string {
  return (
    process.env.GITHUB_TOKEN ||
    process.env.GITHUB_PERSONAL_ACCESS_TOKEN ||
    process.env.GH_TOKEN ||
    ""
  ).trim()
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function normalizeGitHubRunTerminalStatus(status: string, conclusion: string): "completed" | "failed" | "canceled" | "running" {
  if (status !== "completed") return "running"
  if (conclusion === "success") return "completed"
  if (conclusion === "cancelled" || conclusion === "cancelled_by_user") return "canceled"
  return "failed"
}

async function ensureGitHubRunCanceled(input: {
  token: string
  owner: string
  repo: string
  runId: string
}): Promise<{
  terminal: boolean
  status: "completed" | "failed" | "canceled" | "running"
  githubStatus?: string
  githubConclusion?: string
  htmlUrl?: string
  updatedAt?: string
}> {
  await cancelGitHubWorkflowRun(input)
  await forceCancelGitHubWorkflowRun(input).catch(() => undefined)

  for (let attempt = 0; attempt < 12; attempt += 1) {
    await sleep(attempt === 0 ? 1200 : 2500)
    const run = await getGitHubWorkflowRun(input)
    const currentStatus = String(run.status ?? "").toLowerCase()
    const conclusion = String(run.conclusion ?? "").toLowerCase()
    const status = normalizeGitHubRunTerminalStatus(currentStatus, conclusion)
    if (status === "canceled") {
      return {
        terminal: true,
        status,
        githubStatus: currentStatus,
        githubConclusion: conclusion,
        htmlUrl: run.htmlUrl,
        updatedAt: run.updatedAt,
      }
    }
    if (status === "completed" || status === "failed") {
      return {
        terminal: true,
        status,
        githubStatus: currentStatus,
        githubConclusion: conclusion,
        htmlUrl: run.htmlUrl,
        updatedAt: run.updatedAt,
      }
    }
    if (attempt < 8 && (attempt === 1 || attempt === 3 || attempt === 5 || attempt === 7)) {
      await cancelGitHubWorkflowRun(input).catch(() => undefined)
      await forceCancelGitHubWorkflowRun(input).catch(() => undefined)
    }
  }

  const finalRun = await getGitHubWorkflowRun(input)
  const currentStatus = String(finalRun.status ?? "").toLowerCase()
  const conclusion = String(finalRun.conclusion ?? "").toLowerCase()
  return {
    terminal: currentStatus === "completed",
    status: normalizeGitHubRunTerminalStatus(currentStatus, conclusion),
    githubStatus: currentStatus,
    githubConclusion: conclusion,
    htmlUrl: finalRun.htmlUrl,
    updatedAt: finalRun.updatedAt,
  }
}

async function resolveGitHubRunIdsForAbort(input: {
  agentId: string
  jobId: string
  owner: string
  repo: string
  token: string
  workflow?: string
  branch?: string
  linkedRun?: Awaited<ReturnType<typeof getLatestAgentRunByJobReference>> | null
}): Promise<string[]> {
  const agentRuns = await listAgentRunsByAgentId(input.agentId, 50).catch(() => [])

  const relevantRuns = agentRuns.filter((run) => {
    return run.runType === "github_dispatch" && run.jobReference === input.jobId
  })

  const runIds = new Set<string>()
  if (input.linkedRun?.externalRunId) runIds.add(input.linkedRun.externalRunId)
  for (const run of relevantRuns) {
    if (run.externalRunId) runIds.add(run.externalRunId)
  }

  return Array.from(runIds)
}

export async function GET(_request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    if (/^[0-9a-f-]{36}$/i.test(id)) {
      const settings = await getMigrationWorkerSettings()
      if (settings.queueDatabaseUrl) {
        const queued = await withMigrationQueueDatabase(settings.queueDatabaseUrl, async (db) => db.query(`
          select id,migration_id,status,payload,progress,result,summary,error,agent_id,claimed_by,created_at,updated_at,completed_at,last_heartbeat_at
          from drive_migration_queue_jobs where id=$1 limit 1
        `, [id]), settings.queueDatabaseSsl)
        const row = queued.rows[0]
        if (row) {
          const sourcePayload = row.payload && typeof row.payload === "object" ? row.payload : {}
          const safePayload = {
            job: sourcePayload.job || {}, migration: sourcePayload.migration || {},
            items: Array.isArray(sourcePayload.items) ? sourcePayload.items.map((item: Record<string, unknown>) => ({
              id: item.id, sourceBucket: item.sourceBucket, targetBucket: item.targetBucket,
              sourceObjects: item.sourceObjects, sourceBytes: item.sourceBytes, progress: item.progress,
            })) : [],
            ...(Array.isArray(sourcePayload.inventoryObjects) ? { inventoryObjects: sourcePayload.inventoryObjects } : {}),
          }
          return NextResponse.json({ job: { id: row.id, migrationId: row.migration_id, mode: safePayload.job?.mode || "migration", status: row.status, claimedByAgentId: row.agent_id || row.claimed_by, payload: safePayload, progress: row.progress || {}, result: row.result || {}, summary: row.summary, error: row.error, createdAt: row.created_at, updatedAt: row.updated_at, completedAt: row.completed_at, lastHeartbeatAt: row.last_heartbeat_at, linkedRun: null } }, { headers: { "Cache-Control": "no-store, max-age=0" } })
        }
      }
    }
    const detail = await getRepairJobDetail(id)
    if (!detail) return NextResponse.json({ error: "Repair job not found" }, { status: 404 })
    return NextResponse.json({ job: { ...detail.job, linkedRun: detail.linkedRun } })
  } catch (error: unknown) {
    return NextResponse.json({ error: errorMessage(error, "Unable to load repair job") }, { status: 500 })
  }
}

export async function POST(request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const body = (await request.json().catch(() => ({}))) as Record<string, unknown>
    const action = typeof body.action === "string" ? body.action : ""
    if (action !== "abort" && action !== "stop_github_run") {
      return NextResponse.json({ error: "Unsupported repair job action" }, { status: 400 })
    }

    const job = await getRepairJob(id)
    if (!job) return NextResponse.json({ error: "Repair job not found" }, { status: 404 })

    const linkedRun = await getLatestAgentRunByJobReference(id).catch(() => null)
    const agentId = job.claimedByAgentId || job.requestedByAgentId
    const agent = agentId ? await getAgentById(agentId).catch(() => null) : null
    const locallyAbortedJob = await abortRepairJob(id)
    if (locallyAbortedJob) {
      await recordUserActivity(request, auth.user.id, {
        action: "repair_job.abort_requested",
        entityType: "repair_job",
        entityId: id,
        entityLabel: job.mode === "migration" ? "Migration worker job" : "Repair job",
        summary: `Requested stop for ${job.mode === "migration" ? "migration worker" : "repair"} job`,
        before: { status: job.status },
        after: { status: locallyAbortedJob.status, remoteCancellationPending: job.mode === "migration" },
      })
    }

    // Migration worker pools are controlled by Migration Orchestrator. The
    // dashboard records the canceled durable job; the orchestrator reconciles
    // and confirms cancellation of every associated GitHub run.
    if (job.mode === "migration") {
      const refreshedRun = await getLatestAgentRunByJobReference(id).catch(() => null)
      return NextResponse.json({
        ok: true,
        abortRequested: true,
        remoteCancellationPending: true,
        warning: "Migration Orchestrator is stopping the active worker pool and will confirm each GitHub run is terminated.",
        job: { ...(locallyAbortedJob || job), linkedRun: refreshedRun },
      })
    }

    if (
      agent &&
      agent.provider === "github_actions" &&
      agent.githubRepoOwner &&
      agent.githubRepoName
    ) {
      const githubToken =
        (await getAgentGithubToken(agent.id).catch(() => null)) ||
        (await cookies()).get(GITHUB_TOKEN_COOKIE)?.value ||
        getGitHubTokenFallback()

      if (!githubToken) {
        if (locallyAbortedJob) {
          return NextResponse.json({
            ok: true,
            abortRequested: true,
            remoteCancellationPending: true,
            warning: "Worker job was aborted, but no GitHub token was available to stop the workflow run.",
            job: locallyAbortedJob,
          })
        }
        return NextResponse.json({ error: "No GitHub token available to cancel the workflow run" }, { status: 400 })
      }

      const runIds = await resolveGitHubRunIdsForAbort({
        agentId: agent.id,
        jobId: id,
        token: githubToken,
        owner: agent.githubRepoOwner,
        repo: agent.githubRepoName,
        workflow: agent.githubWorkflowFile || (typeof linkedRun?.payload?.workflowFile === "string" ? linkedRun.payload.workflowFile : undefined),
        branch: agent.githubRef || (typeof linkedRun?.payload?.ref === "string" ? linkedRun.payload.ref : undefined),
        linkedRun,
      })

      if (runIds.length === 0) {
        if (locallyAbortedJob) {
          return NextResponse.json({
            ok: true,
            abortRequested: true,
            remoteCancellationPending: true,
            warning: "Worker job was aborted, but the GitHub workflow run could not be located.",
            job: locallyAbortedJob,
          })
        }
        return NextResponse.json({ error: "Could not find the GitHub workflow run to cancel" }, { status: 404 })
      }

      const cancelResults = []
      for (const runId of runIds) {
        cancelResults.push(
          await ensureGitHubRunCanceled({
            token: githubToken,
            owner: agent.githubRepoOwner,
            repo: agent.githubRepoName,
            runId,
          })
        )
      }

      const uncanceled = cancelResults.find((result) => result.status !== "canceled")
      if (uncanceled) {
        if (linkedRun) {
          await updateAgentRun(linkedRun.id, {
            summary: "GitHub workflow abort requested, but GitHub has not confirmed cancellation yet",
            payload: {
              ...(linkedRun.payload ?? {}),
              githubAbortRequestedAt: new Date().toISOString(),
              githubStatus: uncanceled.githubStatus ?? null,
              githubConclusion: uncanceled.githubConclusion ?? null,
              githubUpdatedAt: uncanceled.updatedAt ?? null,
              ...(uncanceled.htmlUrl ? { htmlUrl: uncanceled.htmlUrl } : {}),
            },
          }).catch(() => undefined)
        }

        if (locallyAbortedJob) {
          return NextResponse.json({
            ok: true,
            abortRequested: true,
            remoteCancellationPending: uncanceled.status === "running",
            warning:
              uncanceled.status === "running"
                ? "Worker job was aborted; GitHub cancellation is still pending."
                : `Worker job was aborted; GitHub workflow ended as ${uncanceled.status}.`,
            job: locallyAbortedJob,
          })
        }
        return NextResponse.json({ error: "GitHub workflow cancellation was not confirmed." }, { status: 409 })
      }

      const now = new Date().toISOString()
      if (linkedRun) {
        const lastCancelResult = cancelResults[cancelResults.length - 1]
        await updateAgentRun(linkedRun.id, {
          status: "canceled",
          completedAt: now,
          summary: "GitHub workflow abort requested by user",
          payload: {
            ...(linkedRun.payload ?? {}),
            githubRunId: runIds[0] ?? null,
            githubAbortRequestedAt: now,
            githubStatus: lastCancelResult?.githubStatus ?? null,
            githubConclusion: lastCancelResult?.githubConclusion ?? null,
            githubUpdatedAt: lastCancelResult?.updatedAt ?? null,
            ...(lastCancelResult?.htmlUrl ? { htmlUrl: lastCancelResult.htmlUrl } : {}),
          },
        }).catch(() => undefined)
      }

      const agentRuns = await listAgentRunsByAgentId(agent.id, 50).catch(() => [])
      for (const run of agentRuns) {
        if (run.runType !== "github_dispatch") continue
        if (run.jobReference !== id && run.status !== "pending" && run.status !== "running") continue
        await updateAgentRun(run.id, {
          status: "canceled",
          completedAt: now,
          summary: "GitHub worker aborted by user",
          payload: {
            ...(run.payload ?? {}),
            githubAbortRequestedAt: now,
          },
        }).catch(() => undefined)
      }

      await updateAgent(agent.id, {
        status: "offline",
        lastError: null,
        metadata: {
          ...(agent.metadata ?? {}),
          activeRepairJobId: null,
          lastRepairJobAbortAt: now,
          githubAbortRequestedAt: now,
        },
      }).catch(() => undefined)

      if (action === "stop_github_run") {
        const currentJob = await getRepairJob(id).catch(() => null)
        const refreshedRun = await getLatestAgentRunByJobReference(id).catch(() => null)
        return NextResponse.json({
          ok: true,
          githubRunStopped: true,
          job: currentJob ? { ...currentJob, linkedRun: refreshedRun } : null,
        })
      }

      const updatedJob = locallyAbortedJob ?? (await abortRepairJob(id))
      const refreshedRun = await getLatestAgentRunByJobReference(id).catch(() => null)
      return NextResponse.json({
        ok: true,
        abortRequested: true,
        job: { ...updatedJob, linkedRun: refreshedRun },
      })
    }

    if (agent) {
      await updateAgent(agent.id, {
        status: agent.provider === "self_hosted" || agent.provider === "local" ? "online" : "offline",
        lastError: null,
        metadata: {
          ...(agent.metadata ?? {}),
          activeRepairJobId: null,
          lastRepairJobAbortAt: new Date().toISOString(),
        },
      }).catch(() => undefined)
    }

    const updatedJob = locallyAbortedJob ?? (await abortRepairJob(id))
    const refreshedRun = await getLatestAgentRunByJobReference(id).catch(() => null)
    return NextResponse.json({ ok: true, job: { ...updatedJob, linkedRun: refreshedRun } })
  } catch (error: unknown) {
    return NextResponse.json({ error: errorMessage(error, "Unable to update repair job") }, { status: 400 })
  }
}

export async function DELETE(_request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const job = await getRepairJob(id)
    if (!job) return NextResponse.json({ error: "Repair job not found" }, { status: 404 })

    if (job.mode === "migration") {
      if (["pending", "claimed", "running"].includes(job.status)) {
        return NextResponse.json({ error: "Abort this worker job and wait for the Migration Orchestrator to confirm its workers have stopped before deleting it." }, { status: 409 })
      }
      const generation = job.workKey?.match(/:generation:(\d+):/)?.[1] ?? null
      const { rows } = await queryDb<{ count: string }>(`
        select count(*)::text count
        from drive_agent_runs r
        where r.run_type='github_dispatch' and r.status in('pending','running')
          and (
            r.job_reference=$1
            or ($2::int is not null and r.payload->>'migrationId'=$3 and r.payload->>'pool'='true'
              and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=$2::int)
            or ($2::int is null and r.payload->>'migrationId'=$3 and r.payload->>'pool'='true')
          )
      `, [id, generation, job.migrationId])
      if (Number(rows[0]?.count || 0) > 0) {
        return NextResponse.json({ error: "The Migration Orchestrator is still stopping workers for this job. Refresh after all associated runs are terminal, then delete it." }, { status: 409 })
      }
      if (job.status === "canceled") {
        const { rows: migrationRows } = await queryDb<{ status: string; has_newer_job: boolean }>(`
          select m.status,exists(
            select 1 from drive_repair_jobs newer
            where newer.migration_id=m.id and newer.mode='migration'
              and (newer.created_at,newer.id)>(current_job.created_at,current_job.id)
          ) has_newer_job
          from drive_migrations m join drive_repair_jobs current_job on current_job.id=$1
          where m.id=$2 limit 1
        `, [id, job.migrationId])
        const migrationStatus = String(migrationRows[0]?.status || "").toLowerCase()
        if (["running", "verifying"].includes(migrationStatus) && !migrationRows[0]?.has_newer_job) {
          return NextResponse.json({ error: "This canceled job is the active migration's stop marker. Cancel the migration or start a new worker-pool attempt before deleting it." }, { status: 409 })
        }
      }
    }

    if (job.status === "pending" || job.status === "claimed" || job.status === "running") {
      await abortRepairJob(id)
    }
    await deleteRepairJob(id)
    await recordUserActivity(_request, auth.user.id, {
      action: "repair_job.deleted",
      entityType: "repair_job",
      entityId: id,
      entityLabel: job.mode === "migration" ? "Migration worker job" : "Repair job",
      summary: `Deleted ${job.mode === "migration" ? "migration worker" : "repair"} job`,
      before: { status: job.status, mode: job.mode, migrationId: job.migrationId },
    })
    return NextResponse.json({ ok: true })
  } catch (error: unknown) {
    return NextResponse.json({ error: errorMessage(error, "Unable to delete repair job") }, { status: 400 })
  }
}
