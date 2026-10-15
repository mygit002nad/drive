import { cookies } from "next/headers"
import crypto from "node:crypto"
import { NextResponse } from "next/server"
import { createAgentRun, getAgentById, getAgentGithubToken, listAgentRunsByAgentId, updateAgent, updateAgentRun } from "@/lib/agents-store"
import { abortRepairJob, createRepairJob, ensureMigrationWorkerJobs, findActiveRepairJobForDispatch, listRepairJobs, type RepairJobMode } from "@/lib/repair-jobs-store"
import { GITHUB_TOKEN_COOKIE, listGitHubWorkflowRuns } from "@/lib/github-oauth"
import { syncGitHubWorkerSecrets } from "@/lib/github-worker-secrets"
import { assertWorkerWorkflow } from "@/lib/github-worker-setup"
import { syncWorkerRepository } from "@/lib/github-worker-sync"
import { enrollMigrationWorkerAgents, getMigration, listMigrationItems } from "@/lib/migrations-store"
import { getMigrationWorkerSettings } from "@/lib/migration-worker-settings-store"
import { getMigrationOrchestratorSettings } from "@/lib/migration-orchestrator-settings-store"
import { requireAdmin } from "@/lib/server-auth"

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback
}

function normalizeStatus(value: unknown): string {
  return String(value ?? "").trim().toLowerCase()
}

function hasActiveSuperSlurper(items: Array<{ slurperJobId?: string; slurperStatus?: string | null }>): boolean {
  const activeStatuses = new Set(["queued", "pending", "creating_job", "job_id_pending", "running", "scanning", "verifying"])
  return items.some((item) => {
    const status = normalizeStatus(item.slurperStatus)
    return activeStatuses.has(status)
  })
}

function getGitHubTokenFallback(): string {
  return (
    process.env.GITHUB_TOKEN ||
    process.env.GITHUB_PERSONAL_ACCESS_TOKEN ||
    process.env.GH_TOKEN ||
    ""
  ).trim()
}

function isRecentIso(value: string | undefined, maxAgeMs: number): boolean {
  if (!value) return false
  const time = Date.parse(value)
  return Number.isFinite(time) && Date.now() - time <= maxAgeMs
}

export async function POST(request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const auth = await requireAdmin()
    if (!auth.ok) return auth.response

    const { id } = await context.params
    const agent = await getAgentById(id)
    if (!agent) return NextResponse.json({ error: "Agent not found" }, { status: 404 })

    const body = (await request.json().catch(() => ({}))) as Record<string, unknown>
    const migrationId = typeof body.migrationId === "string" ? body.migrationId.trim() : ""
    const requestedMode = (
      typeof body.mode === "string" && ["verify_only", "repair_only", "repair_and_verify"].includes(body.mode)
        ? body.mode
        : "repair_and_verify"
    ) as RepairJobMode
    const poolAgentIds = Array.isArray(body.poolAgentIds)
      ? Array.from(new Set(body.poolAgentIds.filter((value): value is string => typeof value === "string" && /^[0-9a-f-]{36}$/i.test(value))) )
      : []

    if (!migrationId) return NextResponse.json({ error: "migrationId is required" }, { status: 400 })
    const migration = await getMigration(migrationId)
    if (!migration) return NextResponse.json({ error: "Migration not found" }, { status: 404 })
    const items = await listMigrationItems(migrationId)
    const pool = body.pool === true || migration.options.executionMode === "migration_workers"
    // A worker-pool dispatch is a first-class migration operation. Repair and
    // verify modes remain available only to the existing non-pool worker path.
    const mode: RepairJobMode = pool ? "migration" : requestedMode
    if (body.pool === true && migration.options.executionMode !== "migration_workers") {
      return NextResponse.json({ error: "Worker pool dispatch requires a migration created with the worker engine" }, { status: 409 })
    }
    if (pool && !["running", "verifying"].includes(migration.status)) {
      return NextResponse.json({ error: "Start the migration before dispatching its worker pool" }, { status: 409 })
    }
    if (pool && !agent.capabilities.includes("bulk_migrate")) {
      return NextResponse.json(
        { error: "This worker is not registered for full migrations. Update the worker runtime and wait for its heartbeat before dispatching a worker-pool migration." },
        { status: 409 }
      )
    }
    if (migration.options.executionMode !== "migration_workers" && hasActiveSuperSlurper(items)) {
      return NextResponse.json({ error: "Cannot run with worker while Super Slurper is still active for this migration." }, { status: 409 })
    }
    if (!pool) {
      const existingJob = await findActiveRepairJobForDispatch({
        migrationId,
        requestedByAgentId: id,
      })
      if (existingJob) {
        return NextResponse.json(
          { error: `A worker job is already active for this migration on this worker (${existingJob.id}).`, job: existingJob },
          { status: 409 }
        )
      }
    }

    if (agent.provider !== "github_actions") {
      if (
        (agent.provider === "self_hosted" || agent.provider === "local") &&
        (agent.status !== "online" || !isRecentIso(agent.lastHeartbeatAt, 60_000))
      ) {
        return NextResponse.json(
          { error: "Selected self-hosted worker is offline. Start the worker before dispatching this job." },
          { status: 409 }
        )
      }
      if (pool) {
        const activePoolJob = (await listRepairJobs(500)).find(
          (job) => job.claimedByAgentId === id && ["pending", "claimed", "running"].includes(job.status)
        )
        if (activePoolJob) {
          return NextResponse.json(
            { error: `This worker already owns active pool shard ${activePoolJob.id}.`, job: activePoolJob },
            { status: 409 }
          )
        }
      }
      const queued = pool
        ? await ensureMigrationWorkerJobs({ migrationId, mode })
        : null
      const job = pool
        ? null
        : await createRepairJob({
            migrationId,
            mode,
            requestedByAgentId: id,
            payload: { source: agent.provider, agentId: id },
          })

      await updateAgent(id, {
        status: agent.provider === "self_hosted" || agent.provider === "local" ? "online" : agent.status,
        lastError: null,
        metadata: {
          ...(agent.metadata ?? {}),
          activeRepairJobId: job?.id ?? null,
          ...(pool ? { activeMigrationId: migrationId } : { activeMigrationId: null }),
        },
      }).catch(() => undefined)

      return NextResponse.json({ ok: true, job, jobs: queued?.jobs ?? [] }, { status: 200 })
    }

    if (!agent.githubRepoOwner || !agent.githubRepoName || !agent.githubWorkflowFile) {
      return NextResponse.json({ error: "GitHub repo owner, repo name, and workflow file are required" }, { status: 400 })
    }
    const githubRepoOwner = agent.githubRepoOwner
    const githubRepoName = agent.githubRepoName
    const githubWorkflowFile = agent.githubWorkflowFile

    if (pool) {
      const selectedIds = poolAgentIds.length > 0 ? poolAgentIds : [id]
      const orchestrator = await getMigrationOrchestratorSettings()
      if (!orchestrator.orchestratorUrl || !orchestrator.sharedSecret) {
        return NextResponse.json({ error: "Migration Orchestrator URL and secret are required for worker-pool dispatch" }, { status: 409 })
      }
      const workerSettings = await getMigrationWorkerSettings()
      const workerSecret = workerSettings.sharedSecret
      if (!workerSettings.queueDatabaseUrl) return NextResponse.json({ error: "Configure the Migration Queue PostgreSQL URL in Settings before starting a worker-pool migration" }, { status: 409 })
      if (workerSecret.length < 24 || workerSecret.length > 512) {
        return NextResponse.json({ error: "Configure the Migration Worker secret in Settings before dispatching a workflow" }, { status: 409 })
      }
      // Validate and synchronize every selected workflow before enrolling any
      // of them. A partial enrollment must never leave an undispatchable fleet.
      for (const selectedId of selectedIds) {
        const selected = await getAgentById(selectedId)
        if (!selected || selected.provider !== "github_actions" || !selected.githubRepoOwner || !selected.githubRepoName || !selected.githubWorkflowFile) {
          return NextResponse.json({ error: `Selected workflow ${selectedId} is missing its GitHub repository configuration` }, { status: 409 })
        }
        const token = await getAgentGithubToken(selectedId)
        if (!token) return NextResponse.json({ error: `Selected workflow ${selected.name} is missing its GitHub token` }, { status: 409 })
        await syncGitHubWorkerSecrets({
          token,
          owner: selected.githubRepoOwner,
          repo: selected.githubRepoName,
          serverUrl: orchestrator.orchestratorUrl,
          sharedSecret: workerSecret,
          agentId: selected.id,
          includeLegacyAgentId: false,
        })
      }
      await enrollMigrationWorkerAgents(migrationId, selectedIds)
      await ensureMigrationWorkerJobs({ migrationId, mode })
      const wake = await fetch(`${orchestrator.orchestratorUrl.replace(/\/+$/, "")}/run`, {
        method: "POST",
        headers: { Authorization: `Bearer ${orchestrator.sharedSecret}` },
        signal: AbortSignal.timeout(15_000),
      }).catch(() => null)
      if (wake && !wake.ok) return NextResponse.json({ error: `Migration Orchestrator wake returned HTTP ${wake.status}` }, { status: 502 })
      return NextResponse.json({ ok: true, queued: true, workflowIds: selectedIds }, { status: 202 })
    }

    if (agent.provider === "github_actions") {
      return NextResponse.json({ error: "GitHub Actions workers are dispatched only by the Migration Orchestrator. Start or retry this migration with the worker pool." }, { status: 409 })
    }

    const workerJobs = await listRepairJobs(500)
    const activeWorkerJobs = workerJobs.filter(
      (job) =>
        !["completed", "failed", "canceled"].includes(job.status) &&
        (job.claimedByAgentId === id || (!pool && job.requestedByAgentId === id))
    )
    if (!pool && activeWorkerJobs.length > 0) {
      return NextResponse.json(
        {
          error: `This worker already has ${activeWorkerJobs.length} active repair job(s). Stop or abort them before dispatching another workflow.`,
          jobs: activeWorkerJobs,
        },
        { status: 409 }
      )
    }

    const activeWorkerRuns = await listAgentRunsByAgentId(id, 20)
    const workerJobStatusById = new Map(workerJobs.map((job) => [job.id, job.status]))
    const activeDispatchRuns = activeWorkerRuns.filter(
      (run) =>
        run.runType === "github_dispatch" &&
        (run.status === "pending" || run.status === "running") &&
        (pool || !run.jobReference || !["completed", "failed", "canceled"].includes(workerJobStatusById.get(run.jobReference) ?? ""))
    )
    if (!pool && activeDispatchRuns.length > 0) {
      return NextResponse.json(
        {
          error: `This worker already has ${activeDispatchRuns.length} active GitHub workflow run(s). Stop the worker before dispatching again.`,
          runs: activeDispatchRuns,
        },
        { status: 409 }
      )
    }
    const dispatchCount = pool ? agent.workerCount - activeDispatchRuns.length : 1
    if (dispatchCount <= 0) {
      return NextResponse.json({ error: `All ${agent.workerCount} configured workflow workers are already active.`, runs: activeDispatchRuns }, { status: 409 })
    }

    const githubToken = (await getAgentGithubToken(id)) || (await cookies()).get(GITHUB_TOKEN_COOKIE)?.value || getGitHubTokenFallback()
    if (!githubToken) {
      return NextResponse.json(
        { error: "No GitHub token available. Save one on the agent or set GITHUB_TOKEN on the server." },
        { status: 400 }
      )
    }

    const sharedSecret = (await getMigrationWorkerSettings()).sharedSecret
    if (sharedSecret.length < 24 || sharedSecret.length > 512) {
      return NextResponse.json({ error: "Configure the Migration Worker secret in Settings before dispatching a GitHub worker." }, { status: 409 })
    }
    const serverUrl = (await getMigrationOrchestratorSettings()).orchestratorUrl
    if (!serverUrl) {
      return NextResponse.json(
        { error: "Configure the Migration Orchestrator URL in Settings before dispatching a GitHub worker." },
        { status: 409 }
      )
    }
    const codeSync = await syncWorkerRepository({ token: githubToken, owner: githubRepoOwner, repo: githubRepoName, workflow: githubWorkflowFile, sourceRepo: process.env.GITHUB_WORKER_SOURCE_REPO, activateActions: agent.status === "dispatch_ready" })
    await assertWorkerWorkflow({ token: githubToken, owner: githubRepoOwner, repo: githubRepoName, ref: codeSync.targetSha, workflow: githubWorkflowFile })
    await updateAgent(id, { githubRef: codeSync.defaultBranch })
    const dispatchRequestedAt = new Date().toISOString()
    const workerInstanceId = crypto.randomUUID()
    // Every dispatch carries a cryptographically unique instance id, so it can
    // be reconciled without an expensive before/after run-list request.
    const runsBeforeDispatch: Awaited<ReturnType<typeof listGitHubWorkflowRuns>> = []
    const runIdsBeforeDispatch = new Set(runsBeforeDispatch.map((candidate) => candidate.id))
    let secretSyncError: string | null = null
    try {
      await syncGitHubWorkerSecrets({ token: githubToken, owner: githubRepoOwner, repo: githubRepoName, serverUrl, sharedSecret, agentId: id, includeLegacyAgentId: false })
    } catch (error: unknown) {
      secretSyncError = errorMessage(error, "Unable to sync GitHub worker secrets")
    }
    if (secretSyncError) {
      return NextResponse.json(
        { error: `GitHub worker secret synchronization failed: ${secretSyncError}` },
        { status: 502 }
      )
    }
    if (pool) await enrollMigrationWorkerAgents(migrationId, poolAgentIds.length > 0 ? poolAgentIds : [id])
    const queued = pool
      ? await ensureMigrationWorkerJobs({ migrationId, mode })
      : null
    const job = pool
      ? null
      : await createRepairJob({
          migrationId,
          mode,
          requestedByAgentId: id,
          payload: { source: "github_actions", agentId: id },
        })

    let run: Awaited<ReturnType<typeof createAgentRun>>
    try {
      run = await createAgentRun({
        agentId: id,
        runType: "github_dispatch",
        status: "pending",
        jobReference: job?.id,
        summary: pool ? `Queued GitHub dispatch for ${agent.workerCount} migration worker${agent.workerCount === 1 ? "" : "s"}` : `Queued GitHub dispatch for repair job ${job?.id}`,
        payload: {
          migrationId,
          mode,
          pool,
          workerCount: pool ? agent.workerCount : 1,
          workerInstanceId,
          repoOwner: githubRepoOwner,
          repoName: githubRepoName,
          workflowFile: githubWorkflowFile,
          ref: codeSync.defaultBranch,
          sourceCommit: codeSync.sourceSha,
          workerCommit: codeSync.targetSha,
          dispatchRequestedAt,
          githubRunIdsBeforeDispatch: Array.from(runIdsBeforeDispatch),
        },
      })
    } catch (error) {
      if (job?.id) await abortRepairJob(job.id).catch(() => undefined)
      throw error
    }

    await updateAgent(id, {
      status: "offline",
      lastError: null,
      metadata: {
        ...(agent.metadata ?? {}),
        activeRepairJobId: job?.id ?? null,
        githubDispatchRequestedAt: dispatchRequestedAt,
      },
    }).catch(() => undefined)

    const dispatchWorkflow = async (instanceId: string) =>
      fetch(
        `https://api.github.com/repos/${encodeURIComponent(githubRepoOwner)}/${encodeURIComponent(githubRepoName)}/dispatches`,
        {
          method: "POST",
          headers: {
            Accept: "application/vnd.github+json",
            Authorization: `Bearer ${githubToken}`,
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            event_type: "drive-migration-worker",
            client_payload: {
              migration_id: migrationId,
              ...(job?.id ? { repair_job_id: job.id } : {}),
              agent_id: id,
              worker_instance_id: instanceId,
              workflow_file: githubWorkflowFile,
              code_ref: codeSync.targetSha,
            },
          }),
        }
      )

    let response: Response
    try {
      response = await dispatchWorkflow(workerInstanceId)
    } catch (error: unknown) {
      if (job?.id) await abortRepairJob(job.id).catch(() => undefined)
      return NextResponse.json({ error: errorMessage(error, "Unable to send GitHub workflow dispatch request") }, { status: 400 })
    }

    if (!response.ok) {
      const text = await response.text().catch(() => "")
      if (job?.id) await abortRepairJob(job.id).catch(() => undefined)
      await updateAgentRun(run.id, {
        status: "failed",
        summary: `GitHub dispatch failed: ${response.status}`,
        payload: { errorBody: text },
        completedAt: new Date().toISOString(),
      }).catch(() => undefined)
      await updateAgent(id, {
        status: "offline",
        lastError: `GitHub dispatch failed: ${response.status}`,
        metadata: {
          ...(agent.metadata ?? {}),
          activeRepairJobId: null,
        },
      }).catch(() => undefined)
      return NextResponse.json(
        {
          error: `GitHub dispatch failed (${response.status}). ${
            text || "Check token/repo/workflow access."
          }${secretSyncError ? ` Secret sync warning: ${secretSyncError}` : ""}`,
        },
        { status: 400 }
      )
    }

    let matchedRun: Awaited<ReturnType<typeof listGitHubWorkflowRuns>>[number] | undefined

    const updatedRun = await updateAgentRun(run.id, {
      status: matchedRun ? (matchedRun.status === "completed" ? "completed" : "running") : "pending",
      externalRunId: matchedRun?.id ?? null,
      summary: matchedRun
        ? pool
          ? `Workflow dispatched for the migration worker pool (run #${matchedRun.runNumber ?? matchedRun.id})`
          : `Workflow dispatched for repair job ${job?.id} (run #${matchedRun.runNumber ?? matchedRun.id})`
        : pool
          ? "Workflow dispatch queued for the migration worker pool; waiting for GitHub to start the run"
          : `Workflow dispatch queued for repair job ${job?.id}; waiting for GitHub to start the run`,
      payload: {
        migrationId,
        mode,
        pool,
        workerCount: pool ? agent.workerCount : 1,
        workerInstanceId,
        repoOwner: githubRepoOwner,
        repoName: githubRepoName,
        workflowFile: githubWorkflowFile,
        ref: agent.githubRef || "main",
        dispatchRequestedAt,
        githubRunIdsBeforeDispatch: Array.from(runIdsBeforeDispatch),
        dispatchTransport: "repository_dispatch",
        ...(secretSyncError ? { secretSyncWarning: secretSyncError } : {}),
        ...(matchedRun?.htmlUrl ? { htmlUrl: matchedRun.htmlUrl } : {}),
      },
      ...(matchedRun?.status === "completed" ? { completedAt: new Date().toISOString() } : {}),
    })

    const dispatchedRuns = [updatedRun]
    for (let slot = 1; slot < dispatchCount; slot += 1) {
      const additionalInstanceId = crypto.randomUUID()
      const additionalRequestedAt = new Date().toISOString()
      const before: Awaited<ReturnType<typeof listGitHubWorkflowRuns>> = []
      const beforeIds = new Set(before.map((candidate) => candidate.id))
      const pendingRun = await createAgentRun({
        agentId: id,
        runType: "github_dispatch",
        status: "pending",
        summary: `Queued independent workflow worker ${slot + 1} of ${dispatchCount}`,
        payload: {
          migrationId,
          mode,
          pool: true,
          workerCount: agent.workerCount,
          workerInstanceId: additionalInstanceId,
          repoOwner: githubRepoOwner,
          repoName: githubRepoName,
          workflowFile: githubWorkflowFile,
          ref: agent.githubRef || "main",
          dispatchRequestedAt: additionalRequestedAt,
          githubRunIdsBeforeDispatch: Array.from(beforeIds),
        },
      })
      let additionalResponse: Response
      try {
        additionalResponse = await dispatchWorkflow(additionalInstanceId)
      } catch (error: unknown) {
        await updateAgentRun(pendingRun.id, {
          status: "failed",
          summary: errorMessage(error, "Unable to dispatch independent GitHub worker"),
          completedAt: new Date().toISOString(),
        }).catch(() => undefined)
        throw error
      }
      if (!additionalResponse.ok) {
        const errorBody = await additionalResponse.text().catch(() => "")
        await updateAgentRun(pendingRun.id, {
          status: "failed",
          summary: `GitHub dispatch failed: ${additionalResponse.status}`,
          payload: { ...(pendingRun.payload ?? {}), errorBody },
          completedAt: new Date().toISOString(),
        }).catch(() => undefined)
        throw new Error(`GitHub dispatch failed (${additionalResponse.status}). ${errorBody || "Check token/repo/workflow access."}`)
      }

      let additionalMatch: Awaited<ReturnType<typeof listGitHubWorkflowRuns>>[number] | undefined
      dispatchedRuns.push(await updateAgentRun(pendingRun.id, {
        status: additionalMatch ? (additionalMatch.status === "completed" ? "completed" : "running") : "pending",
        externalRunId: additionalMatch?.id ?? null,
        summary: additionalMatch
          ? `Independent workflow worker started (run #${additionalMatch.runNumber ?? additionalMatch.id})`
          : "Independent workflow worker dispatched; waiting for GitHub to index the run",
        payload: {
          ...(pendingRun.payload ?? {}),
          dispatchTransport: "repository_dispatch",
          ...(additionalMatch?.htmlUrl ? { htmlUrl: additionalMatch.htmlUrl } : {}),
        },
        ...(additionalMatch?.status === "completed" ? { completedAt: new Date().toISOString() } : {}),
      }))
    }

    await updateAgent(id, {
      status: matchedRun && matchedRun.status !== "completed" ? "online" : "offline",
      lastError: null,
      metadata: {
        ...(agent.metadata ?? {}),
        activeRepairJobId: matchedRun && matchedRun.status !== "completed" ? job?.id ?? null : null,
        githubAbortRequestedAt: null,
        githubDispatchRequestedAt: dispatchRequestedAt,
        ...(matchedRun?.id ? { githubRunId: matchedRun.id } : {}),
        ...(matchedRun?.status ? { githubRunStatus: matchedRun.status } : {}),
        ...(matchedRun?.conclusion ? { githubRunConclusion: matchedRun.conclusion } : {}),
        ...(matchedRun?.htmlUrl ? { githubRunUrl: matchedRun.htmlUrl } : {}),
      },
    }).catch(() => undefined)

    return NextResponse.json({ ok: true, job, jobs: queued?.jobs ?? [], run: updatedRun, runs: dispatchedRuns }, { status: 200 })
  } catch (error: unknown) {
    return NextResponse.json({ error: errorMessage(error, "Unable to dispatch GitHub workflow") }, { status: 400 })
  }
}
