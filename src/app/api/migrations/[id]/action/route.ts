import { NextResponse } from "next/server"
import { getAgentById, getAgentGithubToken, getLatestAgentRunByJobReference, updateAgent, updateAgentRun } from "@/lib/agents-store"
import { activateAccountForCompletedMigration, getAllAccounts } from "@/lib/accounts-store"
import { getRequestActivityContext, recordActivity } from "@/lib/activity-store"
import { recordUserActivity } from "@/lib/activity-audit"
import { slurperAbortJob, slurperPauseJob, slurperResumeJob } from "@/lib/cloudflare-r2-super-slurper"
import { cancelGitHubWorkflowRun, forceCancelGitHubWorkflowRun, getGitHubWorkflowRun } from "@/lib/github-oauth"
import { getMigration, listMigrationItems, updateMigration, updateMigrationItem } from "@/lib/migrations-store"
import { abortRepairJob, listRepairJobsByMigration } from "@/lib/repair-jobs-store"
import { createInitialBucketVerifyState } from "@/lib/bucket-verifier"
import { requireAdmin } from "@/lib/server-auth"
import { syncMigrationBucketSettings } from "@/lib/migration-settings-sync"
import { getMigrationReadOnlyState, isPermanentAccountCommunicationFailure } from "@/lib/migration-read-only"
import { queryDb } from "@/lib/db"
import { getMigrationOrchestratorSettings } from "@/lib/migration-orchestrator-settings-store"

export const runtime = "nodejs"

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function normalizeStatus(value: string | undefined): string {
  return String(value ?? "").trim().toLowerCase()
}

function isTerminalSlurperStatus(value: string | undefined): boolean {
  return ["completed", "complete", "finished", "success", "succeeded", "failed", "aborted", "verification_failed", "copy_completed", "copy_failed", "copy_aborted", "no_files", "bucket_create_failed", "precheck_failed"].includes(normalizeStatus(value))
}

function isActiveSlurperStatus(value: string | undefined): boolean {
  return ["queued", "pending", "creating_job", "job_id_pending", "running", "scanning", "verifying"].includes(normalizeStatus(value))
}

function abortedProgress(item: { progress: Record<string, unknown> }, stage: string, status: "requested" | "confirmed" | "unconfirmed", at: string, error?: string) {
  const live = isRecord(item.progress.live) ? item.progress.live : {}
  return {
    ...item.progress,
    stage,
    live: { ...live, status: "aborted", updatedAt: at },
    abortRequest: { status, at, ...(error ? { error } : {}) },
  }
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

function readVerifyStatus(progress: Record<string, unknown>): "pending" | "running" | "ok" | "error" | null {
  const verify = isRecord(progress.verify) ? (progress.verify as Record<string, unknown>) : null
  if (!verify) return null
  const status = typeof verify.status === "string" ? verify.status : ""
  if (status === "pending" || status === "running" || status === "ok" || status === "error") return status
  return null
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

async function wakeMigrationOrchestrator(options?: { requireFileScanner?: boolean }): Promise<void> {
  const settings = await getMigrationOrchestratorSettings()
  if (!settings.migrationEnabled || !settings.orchestratorUrl || settings.sharedSecret.length < 24) {
    throw new Error("Migration Orchestrator is not configured and enabled")
  }
  if (options?.requireFileScanner && (!settings.fileScannerEnabled || !settings.fileScannerUrl || settings.fileScannerSecret.length < 24)) {
    throw new Error("File Scanner is not configured and enabled")
  }
  const response = await fetch(`${settings.orchestratorUrl.replace(/\/+$/, "")}/wake`, {
    method: "POST",
    headers: { Authorization: `Bearer ${settings.sharedSecret}` },
    cache: "no-store",
    signal: AbortSignal.timeout(8_000),
  })
  if (!response.ok) throw new Error(`Migration Orchestrator wake-up returned HTTP ${response.status}`)
}

async function reserveMigrationWorkerGeneration(migrationId: string, expectedStatus: string, itemIds: string[], action: "repair_migration" | "retry_migration") {
  const result = await queryDb<{ generation: string }>(`
    with claimed as (
      update drive_migrations m
      set status='running',completed_at=null,sync_status='syncing',
          sync_message='Worker-pool attempt reserved; File Scanner inventory pending',
          options=jsonb_set(coalesce(m.options,'{}'::jsonb),'{workerGeneration}',to_jsonb(greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1))+1),true),
          last_synced_at=now(),updated_at=now()
      where m.id=$1 and m.status=$2
        and $2=any(array['completed','failed','verification_failed','canceled','aborted']::text[])
        and not exists (
          select 1 from drive_agent_runs r
          where r.run_type='github_dispatch' and r.payload->>'migrationId'=m.id::text
            and greatest(1,coalesce(nullif(r.payload->>'workerGeneration','')::int,1))=greatest(1,coalesce(nullif(m.options->>'workerGeneration','')::int,1))
            and r.status in('pending','running')
        )
        and not exists (
          select 1 from drive_migration_orchestrator_state s
          where s.id=true and s.status='running' and s.lease_expires_at>now()
        )
      returning m.id, m.options->>'workerGeneration' generation
    ), reset_items as (
      update drive_migration_items i
      set slurper_job_id=null,slurper_status='queued',
          progress=(coalesce(i.progress,'{}'::jsonb)||jsonb_build_object(
            'stage','awaiting_source_scan',
            'migrationInventory',jsonb_build_object('generation',claimed.generation::int,'status','pending'),
            'migrationQueue',jsonb_build_object('generation',claimed.generation::int,'status','pending'),
            'repairWorker',null,'live',null,
            'lastAction',jsonb_build_object('action',$4::text,'at',now())
          )),
          last_progress_at=now(),updated_at=now()
      from claimed
      where i.migration_id=claimed.id and i.id=any($3::uuid[])
      returning i.id
    )
    select generation from claimed
  `, [migrationId, expectedStatus, itemIds, action])
  return result.rows[0] ? Number(result.rows[0].generation) : null
}

async function waitForOrchestratorCycleToRelease(migrationId: string): Promise<boolean> {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    const result = await queryDb<{ lease_owner: string | null; lease_expires_at: string | null; last_migration_id: string | null }>(
      `select lease_owner,lease_expires_at,last_migration_id from drive_migration_orchestrator_state where id=true limit 1`
    )
    const state = result.rows[0]
    if (!state?.lease_owner || state.last_migration_id !== migrationId || !state.lease_expires_at || Date.parse(state.lease_expires_at) <= Date.now()) return true
    await sleep(400)
  }
  return false
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
  // GitHub can reject a second cancel request for an already-terminal run.
  // Always inspect the actual run afterward before treating cancellation as failed.
  await cancelGitHubWorkflowRun(input).catch(() => undefined)
  await forceCancelGitHubWorkflowRun(input).catch(() => undefined)

  for (let attempt = 0; attempt < 12; attempt += 1) {
    await sleep(attempt === 0 ? 1200 : 2500)
    const run = await getGitHubWorkflowRun(input)
    const currentStatus = String(run.status ?? "").toLowerCase()
    const conclusion = String(run.conclusion ?? "").toLowerCase()
    const status = normalizeGitHubRunTerminalStatus(currentStatus, conclusion)
    if (status === "canceled" || status === "completed" || status === "failed") {
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

async function resolveGitHubRunIdForAbort(input: {
  token: string
  owner: string
  repo: string
  workflow?: string
  branch?: string
  externalRunId?: string
}): Promise<string | null> {
  return input.externalRunId ?? null
}

async function abortRepairJobsForMigration(migrationId: string, options: { orchestratorOwnsGitHubCancellation?: boolean } = {}): Promise<{
  abortedJobs: number
  blockedJobs: Array<{ jobId: string; reason: string }>
}> {
  const repairJobs = await listRepairJobsByMigration(migrationId, 500).catch(() => [])
  const activeJobs = repairJobs.filter((job) => ["pending", "claimed", "running"].includes(String(job.status)))
  const blockedJobs: Array<{ jobId: string; reason: string }> = []
  let abortedJobs = 0

  for (const job of activeJobs) {
    await abortRepairJob(job.id).catch(() => undefined)
    abortedJobs += 1
    if (options.orchestratorOwnsGitHubCancellation) continue
    const linkedRun = await getLatestAgentRunByJobReference(job.id).catch(() => null)
    const agentId = job.claimedByAgentId || job.requestedByAgentId
    const agent = agentId ? await getAgentById(agentId).catch(() => null) : null

    if (
      linkedRun &&
      agent &&
      agent.provider === "github_actions" &&
      agent.githubRepoOwner &&
      agent.githubRepoName
    ) {
      const githubToken =
        (await getAgentGithubToken(agent.id).catch(() => null)) ||
        getGitHubTokenFallback()

      if (!githubToken) {
        blockedJobs.push({ jobId: job.id, reason: "No GitHub token available to cancel the workflow run" })
        continue
      }

      const runId = await resolveGitHubRunIdForAbort({
        token: githubToken,
        owner: agent.githubRepoOwner,
        repo: agent.githubRepoName,
        workflow: agent.githubWorkflowFile || (typeof linkedRun.payload?.workflowFile === "string" ? linkedRun.payload.workflowFile : undefined),
        branch: agent.githubRef || (typeof linkedRun.payload?.ref === "string" ? linkedRun.payload.ref : undefined),
        externalRunId: linkedRun.externalRunId,
      })

      if (!runId) {
        blockedJobs.push({ jobId: job.id, reason: "Could not find the GitHub workflow run to cancel" })
        continue
      }

      const cancelResult = await ensureGitHubRunCanceled({
        token: githubToken,
        owner: agent.githubRepoOwner,
        repo: agent.githubRepoName,
        runId,
      }).catch(() => null)

      if (!cancelResult) {
        blockedJobs.push({ jobId: job.id, reason: "Unable to confirm GitHub workflow cancellation" })
        continue
      }

      if (cancelResult.status !== "canceled") {
        await updateAgentRun(linkedRun.id, {
          summary: "GitHub workflow abort requested by migration cancel, but cancellation is not confirmed yet",
          payload: {
            ...(linkedRun.payload ?? {}),
            githubRunId: runId,
            githubAbortRequestedAt: new Date().toISOString(),
            githubStatus: cancelResult.githubStatus ?? null,
            githubConclusion: cancelResult.githubConclusion ?? null,
            githubUpdatedAt: cancelResult.updatedAt ?? null,
            ...(cancelResult.htmlUrl ? { htmlUrl: cancelResult.htmlUrl } : {}),
          },
        }).catch(() => undefined)

        blockedJobs.push({
          jobId: job.id,
          reason:
            cancelResult.status === "running"
              ? "GitHub accepted the abort request, but the workflow run is still running"
              : cancelResult.status === "failed"
                ? "GitHub workflow ended as failed instead of canceled"
                : "GitHub workflow completed before it could be canceled",
        })
        continue
      }

      await updateAgentRun(linkedRun.id, {
        status: "canceled",
        completedAt: new Date().toISOString(),
        summary: "GitHub workflow abort requested by migration cancel",
        payload: {
          ...(linkedRun.payload ?? {}),
          githubRunId: runId,
          githubAbortRequestedAt: new Date().toISOString(),
          githubStatus: cancelResult.githubStatus ?? null,
          githubConclusion: cancelResult.githubConclusion ?? null,
          githubUpdatedAt: cancelResult.updatedAt ?? null,
          ...(cancelResult.htmlUrl ? { htmlUrl: cancelResult.htmlUrl } : {}),
        },
      }).catch(() => undefined)

      await updateAgent(agent.id, {
        status: "offline",
        lastError: null,
        metadata: {
          ...(agent.metadata ?? {}),
          activeRepairJobId: null,
          lastRepairJobAbortAt: new Date().toISOString(),
        },
      }).catch(() => undefined)
    }

  }

  return { abortedJobs, blockedJobs }
}

async function cancelActiveMigrationFileJobs(migrationId: string, summary: string): Promise<number> {
  const result = await queryDb<{ count: string }>(`
    with canceled as (
      update drive_repair_jobs set status='canceled',summary=$2,error=null,
        completed_at=now(),last_heartbeat_at=now(),updated_at=now()
      where migration_id=$1 and status in('pending','claimed','running')
      returning 1
    ) select count(*)::text count from canceled
  `, [migrationId, summary])
  return Number(result.rows[0]?.count || 0)
}

export async function POST(request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const actorUserId = auth.user.id
    const body: unknown = await request.json().catch(() => ({}))
    const data = isRecord(body) ? body : {}
    const action = typeof data.action === "string" ? data.action : ""

    const migration = await getMigration(id)
    if (!migration) return NextResponse.json({ error: "Migration not found" }, { status: 404 })
    if (action === "cancel_migration" && migration.status === "completed") {
      return NextResponse.json({ error: "A completed migration cannot be canceled" }, { status: 409 })
    }
    const readOnly = getMigrationReadOnlyState(migration)
    const workerMaintenanceAction =
      (migration.options.executionMode === "migration_workers" && action === "verify_all") || action === "repair_migration"
    if (readOnly.readOnly && !workerMaintenanceAction && action !== "cancel_migration") {
      return NextResponse.json({ error: `Migration history is read-only: ${readOnly.reason}` }, { status: 409 })
    }

    if (
      migration.options.executionMode === "migration_workers" &&
      (action === "pause_all" || action === "resume_all")
    ) {
      return NextResponse.json(
        { error: "Shared worker migrations do not pause individual buckets. Stop the worker workflows or cancel the migration, then retry it when ready." },
        { status: 409 }
      )
    }

    const items = await listMigrationItems(id)
    if (workerMaintenanceAction && items.length === 0) {
      return NextResponse.json({ error: "Migration item history has been compacted; there are no bucket records available to verify or repair" }, { status: 409 })
    }
    const accounts = await getAllAccounts()
    const target = accounts.find((a) => a.id === migration.targetAccountId)
    if (!target?.cloudflareAccountId && action !== "cancel_migration") {
      return NextResponse.json({ error: "Target Cloudflare account is not synced" }, { status: 400 })
    }

    const now = new Date().toISOString()
    const migrationLabel = `Migration ${id.slice(0, 8)}`
    const jobArgsBase = { accountId: target?.cloudflareAccountId || "", apiToken: target?.apiToken || "" }

    if (action === "pause_all") {
      const candidates = items.filter((i) => Boolean(i.slurperJobId) && normalizeStatus(i.slurperStatus) === "running")
      for (const item of candidates) {
        await slurperPauseJob({ ...jobArgsBase, jobId: item.slurperJobId! })
        await updateMigrationItem(item.id, {
          slurperStatus: "paused",
          progress: { ...item.progress, stage: "paused_all" },
          lastProgressAt: now,
        })
      }
      await updateMigration(id, { syncStatus: "ok", syncMessage: `Paused ${candidates.length} job(s)`, lastSyncedAt: now })
      await recordUserActivity(request, auth.user.id, { action: "migration.paused", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Paused ${migrationLabel}`, after: { pausedBuckets: candidates.length } })
      return NextResponse.json({ ok: true, paused: candidates.length }, { status: 200 })
    }

    if (action === "resume_all") {
      const candidates = items.filter((i) => Boolean(i.slurperJobId) && normalizeStatus(i.slurperStatus) === "paused")
      for (const item of candidates) {
        await slurperResumeJob({ ...jobArgsBase, jobId: item.slurperJobId! })
        await updateMigrationItem(item.id, {
          slurperStatus: "running",
          progress: { ...item.progress, stage: "resumed_all" },
          lastProgressAt: now,
        })
      }
      await updateMigration(id, { syncStatus: "ok", syncMessage: `Resumed ${candidates.length} job(s)`, lastSyncedAt: now })
      await recordUserActivity(request, auth.user.id, { action: "migration.resumed", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Resumed ${migrationLabel}`, after: { resumedBuckets: candidates.length } })
      return NextResponse.json({ ok: true, resumed: candidates.length }, { status: 200 })
    }

    if (action === "cancel_migration") {
      const abortTargets = items.filter((item) => !isTerminalSlurperStatus(item.slurperStatus))
      // Fence the scheduled orchestrator before making remote abort calls. An
      // already-running cycle must see a terminal parent row and stop writing.
      await updateMigration(id, {
        status: "canceled",
        completedAt: null,
        syncStatus: "syncing",
        syncMessage: "Cancellation requested",
        lastSyncedAt: now,
        options: { ...migration.options, manualCompleted: false, targetActivatedAt: undefined },
      })

      await Promise.all(abortTargets.map((item) => updateMigrationItem(item.id, {
        slurperStatus: "aborted",
        progress: abortedProgress(item, "aborted_all", "requested", now),
        lastProgressAt: now,
      })))

      const workerMode = migration.options.executionMode === "migration_workers"
      const cancelRepairResult = workerMode
        ? { abortedJobs: await cancelActiveMigrationFileJobs(id, "Migration canceled by user"), blockedJobs: [] as Array<{ jobId: string; reason: string }> }
        : await abortRepairJobsForMigration(id)
      const candidates = abortTargets.filter((item) => Boolean(item.slurperJobId))
      const remoteCancellationWarnings: Array<{ itemId: string; reason: string }> = []
      for (const item of candidates) {
        try {
          await slurperAbortJob({ ...jobArgsBase, jobId: item.slurperJobId! })
          await updateMigrationItem(item.id, { progress: abortedProgress(item, "aborted_all", "confirmed", new Date().toISOString()), lastProgressAt: new Date().toISOString() })
        } catch (error) {
          const reason = error instanceof Error ? error.message : String(error)
          remoteCancellationWarnings.push({ itemId: item.id, reason })
          const failedAt = new Date().toISOString()
          await updateMigrationItem(item.id, { progress: abortedProgress(item, "aborted_all", "unconfirmed", failedAt, reason), lastProgressAt: failedAt })
        }
      }

      let orchestratorWakeError: string | null = null
      if (workerMode) {
        try {
          // GitHub dispatch and cancellation belong to the orchestrator. The
          // dashboard only persists the terminal intent and wakes that owner.
          await wakeMigrationOrchestrator()
        } catch (error) {
          orchestratorWakeError = error instanceof Error ? error.message : String(error)
        }
      }
      const activeDispatches = workerMode
        ? Number((await queryDb<{ count: string }>(`select count(*)::text count from drive_agent_runs where run_type='github_dispatch' and payload->>'migrationId'=$1 and status in('pending','running')`, [id])).rows[0]?.count || 0)
        : 0
      const workerCancellationPending = activeDispatches > 0

      const warningCount = remoteCancellationWarnings.length + cancelRepairResult.blockedJobs.length + (orchestratorWakeError ? 1 : 0)
      await updateMigration(id, {
        status: "canceled",
        completedAt: null,
        syncStatus: warningCount ? "error" : workerCancellationPending ? "syncing" : "ok",
        syncMessage: warningCount
          ? `Migration canceled; remote stop was not confirmed for ${remoteCancellationWarnings.length + cancelRepairResult.blockedJobs.length} worker job(s)${orchestratorWakeError ? `; Migration Orchestrator wake failed: ${orchestratorWakeError}` : ""}`
          : workerCancellationPending
            ? `Migration canceled; waiting for ${activeDispatches} GitHub worker run(s) to stop`
          : `Migration canceled${cancelRepairResult.abortedJobs > 0 ? `; aborted ${cancelRepairResult.abortedJobs} worker job(s)` : ""}`,
        lastSyncedAt: new Date().toISOString(),
      })
      await recordUserActivity(request, auth.user.id, { action: "migration.canceled", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Canceled ${migrationLabel}`, before: { status: migration.status }, after: { status: "canceled", stoppedBuckets: candidates.length } })
      return NextResponse.json({
        ok: true,
        abortedRepairJobs: cancelRepairResult.abortedJobs,
        canceledGitHubRuns: 0,
        abortedSlurperJobs: candidates.length,
        remoteCancellationWarnings: [...cancelRepairResult.blockedJobs, ...remoteCancellationWarnings],
        orchestratorWakeError,
        workerCancellationPending,
      }, { status: 200 })
    }

    if (action === "settings_sync") {
      await updateMigration(id, {
        syncStatus: "syncing",
        syncMessage: "Syncing settings",
        lastSyncedAt: now,
      })
      try {
        await syncMigrationBucketSettings(id)
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Settings sync failed"
        const failedAt = new Date().toISOString()
        await updateMigration(id, {
          syncStatus: "error",
          syncMessage: message,
          lastSyncedAt: failedAt,
          options: {
            ...migration.options,
            ...(isPermanentAccountCommunicationFailure(message)
              ? {
                  historyReadOnlyAt: failedAt,
                  historyReadOnlyReason: "Cloudflare account communication failed during settings sync",
                }
              : {}),
          },
        })
        return NextResponse.json({ error: message }, { status: 400 })
      }
      const completedAt = new Date().toISOString()
      try {
        await activateAccountForCompletedMigration({ targetAccountId: migration.targetAccountId, completedAt })
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : "Failed to activate migrated account"
        const permanentFailure = isPermanentAccountCommunicationFailure(message)
        await updateMigration(id, {
          status: "verifying",
          completedAt: null,
          syncStatus: "error",
          syncMessage: `Settings synced, but target activation failed: ${message}`,
          lastSyncedAt: new Date().toISOString(),
          options: {
            ...migration.options,
            targetActivatedAt: undefined,
            ...(permanentFailure
              ? {
                  historyReadOnlyAt: completedAt,
                  historyReadOnlyReason: "Target account activation is unavailable",
                }
              : {}),
          },
        })
        return NextResponse.json({ error: `Settings synced, but the migration remains verifying because target activation failed: ${message}` }, { status: 502 })
      }
      await updateMigration(id, {
        status: "completed",
        completedAt,
        syncStatus: "ok",
        syncMessage: "",
        lastSyncedAt: completedAt,
        options: { ...migration.options, targetActivatedAt: completedAt },
      })
      await recordActivity({
        actorUserId,
        action: "migration.settings_synced",
        entityType: "migration",
        entityId: id,
        entityLabel: `Migration ${id}`,
        summary: "Synchronized settings and completed migration",
        detail: `Applied settings for ${items.length} bucket(s), activated the target account, and completed the migration.`,
        before: { migration },
        after: { bucketCount: items.length, settings: ["publicDevelopmentUrl", "cors"] },
        undoable: false,
        undoReason: "Bucket settings are applied directly to Cloudflare.",
        ...getRequestActivityContext(request),
      })
      return NextResponse.json({ ok: true, syncedBuckets: items.length }, { status: 200 })
    }

    if (action === "mark_completed") {
      await updateMigration(id, {
        status: "verifying",
        completedAt: null,
        syncStatus: "syncing",
        syncMessage: "Syncing settings",
        lastSyncedAt: now,
        options: { ...migration.options, manualCompleted: true, targetActivatedAt: undefined },
      })
      let settingsSyncWarning = ""
      try {
        await syncMigrationBucketSettings(id)
      } catch (error: unknown) {
        settingsSyncWarning = error instanceof Error ? error.message : "Settings sync failed"
      }
      if (settingsSyncWarning) {
        const failedAt = new Date().toISOString()
        await updateMigration(id, {
          status: "verifying",
          completedAt: null,
          syncStatus: "error",
          syncMessage: settingsSyncWarning,
          lastSyncedAt: failedAt,
          options: {
            ...migration.options,
            manualCompleted: true,
            targetActivatedAt: undefined,
            ...(isPermanentAccountCommunicationFailure(settingsSyncWarning)
              ? { historyReadOnlyAt: failedAt, historyReadOnlyReason: "Cloudflare account communication failed during settings sync" }
              : {}),
          },
        })
        return NextResponse.json({ error: `Settings synchronization failed; the migration remains verifying: ${settingsSyncWarning}` }, { status: 502 })
      }
      const completedAt = new Date().toISOString()
      let activationError = ""
      try {
        await activateAccountForCompletedMigration({ targetAccountId: migration.targetAccountId, completedAt })
      } catch (error: unknown) {
        activationError = error instanceof Error ? error.message : "Failed to activate migrated account"
      }
      if (activationError) {
        const failedAt = new Date().toISOString()
        await updateMigration(id, {
          status: "verifying",
          completedAt: null,
          syncStatus: "error",
          syncMessage: `Settings synced, but target activation failed: ${activationError}`,
          lastSyncedAt: failedAt,
          options: {
            ...migration.options,
            manualCompleted: true,
            targetActivatedAt: undefined,
            ...(isPermanentAccountCommunicationFailure(activationError)
              ? { historyReadOnlyAt: failedAt, historyReadOnlyReason: "Target account activation is unavailable" }
              : {}),
          },
        })
        return NextResponse.json({ error: `Target account activation failed; the migration remains verifying: ${activationError}` }, { status: 502 })
      }
      await updateMigration(id, {
        status: "completed",
        completedAt,
        syncStatus: "ok",
        syncMessage: "",
        lastSyncedAt: completedAt,
        options: {
          ...migration.options,
          manualCompleted: true,
          targetActivatedAt: completedAt,
        },
      })
      const afterAccounts = await getAllAccounts()
      await recordActivity({
        actorUserId,
        action: "migration.completed_activate_target",
        entityType: "migration",
        entityId: id,
        entityLabel: `Migration ${id}`,
        summary: "Marked migration completed and activated target account",
        detail: "Completed migrations permanently activate the target account and disable the previous active account.",
        before: {
          migration,
          accounts: accounts.map((account) => ({
            id: account.id,
            label: account.label,
            status: account.status,
            lastMigrated: account.lastMigrated,
          })),
        },
        after: {
          accounts: afterAccounts.map((account) => ({
            id: account.id,
            label: account.label,
            status: account.status,
            lastMigrated: account.lastMigrated,
          })),
        },
        undoable: false,
        undoReason: "Completed migrations permanently change the active account. Disabled accounts cannot be restored.",
        ...getRequestActivityContext(request),
      })
      return NextResponse.json({ ok: true }, { status: 200 })
    }

    if (action === "verify_all") {
      if (migration.options.executionMode === "migration_workers") {
        const generation =
          typeof migration.options.workerGeneration === "number" && Number.isFinite(migration.options.workerGeneration)
            ? Math.max(1, Math.floor(migration.options.workerGeneration))
            : 1
        const verificationResult = await queryDb(`
          insert into drive_migration_verification_state(migration_item_id,migration_id,generation,status,phase,updated_at)
          select i.id,i.migration_id,$2,'pending','source',now()
          from drive_migration_items i
          where i.migration_id=$1
            and (
              i.slurper_status in('completed','verification_failed')
              or exists (
                select 1 from drive_migration_verification_state current
                where current.migration_item_id=i.id and current.generation=$2 and current.status in('failed','completed')
              )
            )
          on conflict(migration_item_id) do update set
            generation=$2,status='pending',phase='source',source_scan_id=null,destination_scan_id=null,
            source_cursor=null,destination_cursor=null,source_objects=0,source_bytes=0,destination_objects=0,destination_bytes=0,
            missing_objects=0,mismatched_objects=0,extra_objects=0,attempt_count=0,attempt_generation=null,last_error=null,
            lease_owner=null,lease_expires_at=null,completed_at=null,updated_at=now()
        `, [id, generation])
        const verifying = verificationResult.rowCount ?? 0
        if (verifying === 0) {
          return NextResponse.json({ error: "No completed or failed buckets are ready for verification" }, { status: 409 })
        }
        await updateMigration(id, {
          status: migration.status === "running" ? "running" : "verifying",
          completedAt: null,
          syncStatus: "syncing",
          syncMessage: `File Scanner verification requested for ${verifying} bucket(s)`,
          lastSyncedAt: now,
        })
        await wakeMigrationOrchestrator({ requireFileScanner: true })
        await recordUserActivity(request, auth.user.id, { action: "migration.verification_queued", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Queued File Scanner verification for ${migrationLabel}`, after: { verifyingBuckets: verifying, scanner: true } })
        return NextResponse.json({ ok: true, verifying, scanner: true }, { status: 200 })
      }
      const prefix =
        typeof migration.options?.pathPrefix === "string" && migration.options.pathPrefix.trim().length > 0
          ? migration.options.pathPrefix
          : undefined

      const candidates = items.filter((i) =>
        isCompletedStatus(i.slurperStatus) || normalizeStatus(i.slurperStatus) === "verification_failed"
      )
      for (const item of candidates) {
        await updateMigrationItem(item.id, {
          progress: {
            ...item.progress,
            stage: "verify_requested",
            verify: createInitialBucketVerifyState({ prefix }),
            destScanId: null,
          },
          lastProgressAt: now,
        })
      }

      await updateMigration(id, {
        status: candidates.length > 0 ? "verifying" : migration.status,
        completedAt: null,
        syncStatus: "ok",
        syncMessage: candidates.length > 0 ? `Verification started for ${candidates.length} bucket(s)` : "No completed buckets to verify",
        lastSyncedAt: now,
        options: { ...migration.options, manualCompleted: false, targetActivatedAt: undefined },
      })

      await wakeMigrationOrchestrator()

      await recordUserActivity(request, auth.user.id, { action: "migration.verification_queued", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Queued bucket verification for ${migrationLabel}`, after: { verifyingBuckets: candidates.length } })

      return NextResponse.json({ ok: true, verifying: candidates.length }, { status: 200 })
    }

    if (action === "repair_migration") {
      if (migration.status === "draft") return NextResponse.json({ error: "Start the migration before repairing it with the worker pool" }, { status: 409 })
      if (migration.options.executionMode !== "migration_workers" && items.some((item) => isActiveSlurperStatus(item.slurperStatus))) {
        return NextResponse.json({ error: "Wait for the active Cloudflare Super Slurper jobs to finish or abort before starting worker-pool repair" }, { status: 409 })
      }
      const orchestratorSettings = await getMigrationOrchestratorSettings()
      if (!orchestratorSettings.migrationEnabled || !orchestratorSettings.orchestratorUrl || orchestratorSettings.sharedSecret.length < 24) {
        return NextResponse.json({ error: "Migration Orchestrator must be configured and enabled before worker-pool repair" }, { status: 409 })
      }
      if (!orchestratorSettings.fileScannerEnabled || !orchestratorSettings.fileScannerUrl || orchestratorSettings.fileScannerSecret.length < 24) {
        return NextResponse.json({ error: "File Scanner must be configured and enabled before worker-pool repair" }, { status: 409 })
      }
      await cancelActiveMigrationFileJobs(id, "Superseded by worker-pool repair")
      let reservableStatus = migration.status
      if (migration.options.executionMode !== "migration_workers" && ["running", "verifying"].includes(migration.status)) {
        await updateMigration(id, {
          status: "failed",
          completedAt: null,
          syncStatus: "syncing",
          syncMessage: "Fencing the Super Slurper cycle before worker-pool repair",
          lastSyncedAt: now,
        })
        if (!(await waitForOrchestratorCycleToRelease(id))) {
          return NextResponse.json({ error: "Migration Orchestrator is still finishing the previous Super Slurper cycle. Retry worker-pool repair shortly." }, { status: 409 })
        }
        reservableStatus = "failed"
      }
      // Every repair reserves a fresh generation and re-enters the exact same
      // File Scanner inventory/materialization path used by a normal worker
      // migration. This also preserves the durable Slurper counters already
      // stored on each item while clearing only the current worker attempt.
      const nextGeneration = await reserveMigrationWorkerGeneration(id, reservableStatus, items.map((item) => item.id), "repair_migration")
      if (nextGeneration === null) {
        return NextResponse.json({ error: "This worker-pool migration is active, or its previous workers are still stopping. Wait for the current attempt to finish before starting another pool." }, { status: 409 })
      }
      await updateMigration(id, {
        status: "running",
        completedAt: null,
        syncStatus: "syncing",
        syncMessage: "Worker-pool repair requested; File Scanner inventory pending",
        lastSyncedAt: now,
        options: {
          ...migration.options,
          executionMode: "migration_workers",
          workerGeneration: nextGeneration,
          // Repair uses the same scanner -> durable queue -> worker pool
          // lifecycle as a normal worker migration, but it must preserve
          // every destination object that already exists. The workers count
          // those objects as skipped and copy only missing keys.
          overwrite: false,
          workerRepairMismatchedObjects: false,
          manualCompleted: false,
          targetActivatedAt: undefined,
        },
      })
      await wakeMigrationOrchestrator()
      await recordUserActivity(request, auth.user.id, { action: "migration.repair_started", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Started worker-pool repair for ${migrationLabel}`, after: { repairedBuckets: items.length, generation: nextGeneration } })
      return NextResponse.json({ ok: true, repairing: items.length, generation: nextGeneration, executionMode: "migration_workers" }, { status: 200 })
    }

    if (action === "retry_migration") {
      const workerMode = migration.options.executionMode === "migration_workers"
      const candidates = items.filter((item) => {
        const s = normalizeStatus(item.slurperStatus)
        const verifyStatus = readVerifyStatus(
          isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
        )
        if (isCompletedStatus(s)) return verifyStatus === "error"
        return true
      })

      let workerGeneration = 0
      if (workerMode) {
        if (!candidates.length) return NextResponse.json({ error: "No failed or incomplete buckets need a worker-pool retry" }, { status: 409 })
        const reserved = await reserveMigrationWorkerGeneration(id, migration.status, candidates.map((item) => item.id), "retry_migration")
        if (reserved === null) {
          return NextResponse.json({ error: "This worker-pool migration is active, or its previous workers are still stopping. Wait for the current attempt to finish before retrying." }, { status: 409 })
        }
        workerGeneration = reserved
        await cancelActiveMigrationFileJobs(id, "Superseded by worker-pool retry")
      }

      for (const item of candidates) {
        if (workerMode) continue
        const prevProgress = isRecord(item.progress) ? (item.progress as Record<string, unknown>) : {}
        const prevCumulative = isRecord(prevProgress.slurperCumulative)
          ? (prevProgress.slurperCumulative as Record<string, unknown>)
          : null
        const baselineTransferred =
          prevCumulative && typeof prevCumulative.transferredObjects === "number"
            ? prevCumulative.transferredObjects
            : typeof prevProgress.slurperNormalized === "object" &&
                prevProgress.slurperNormalized !== null &&
                typeof (prevProgress.slurperNormalized as Record<string, unknown>).transferredObjects === "number"
              ? ((prevProgress.slurperNormalized as Record<string, unknown>).transferredObjects as number)
              : 0
        const nextRerunCount =
          typeof prevProgress.rerunCount === "number" && Number.isFinite(prevProgress.rerunCount)
            ? Math.max(1, Math.floor(prevProgress.rerunCount) + 1)
            : 1
        await updateMigrationItem(item.id, {
          slurperJobId: null,
          slurperStatus: "queued",
          progress: {
            ...prevProgress,
            stage: "retry_requested",
            rerunNoOverwrite: true,
            rerunCount: nextRerunCount,
            rerunBaselineTransferred: Math.max(0, baselineTransferred),
            error: null,
            lastError: null,
            verify: null,
            verifySamples: null,
            destScanId: null,
            ...(workerMode ? { repairWorker: null, live: null } : {}),
            lastAction: { action, at: now },
          },
          lastProgressAt: now,
        })
      }

      await updateMigration(id, {
        status: "running",
        completedAt: null,
        syncStatus: "ok",
        syncMessage:
          candidates.length > 0
            ? `Retry queued for ${candidates.length} bucket(s) with overwrite disabled`
            : "No buckets require retry",
        lastSyncedAt: now,
        options: {
          ...migration.options,
          manualCompleted: false,
          targetActivatedAt: undefined,
          ...(workerMode
            ? { workerGeneration }
            : {}),
        },
      })

      await wakeMigrationOrchestrator()

      await recordUserActivity(request, auth.user.id, { action: "migration.retried", entityType: "migration", entityId: id, entityLabel: migrationLabel, summary: `Retried ${migrationLabel}`, after: { retriedBuckets: candidates.length, ...(workerMode ? { generation: workerGeneration } : {}) } })

      return NextResponse.json({ ok: true, retried: candidates.length, ...(workerMode ? { generation: workerGeneration, executionMode: "migration_workers" } : {}) }, { status: 200 })
    }

    return NextResponse.json({ error: "Unsupported action" }, { status: 400 })
  } catch (error: unknown) {
    const message =
      typeof error === "object" && error !== null && "message" in error
        ? String((error as { message?: unknown }).message ?? "Unable to perform action")
        : "Unable to perform action"
    return NextResponse.json({ error: message }, { status: 400 })
  }
}
