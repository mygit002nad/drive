type TransferBucket = {
  status: string
  totalObjects: number
  transferredObjects: number
  skippedObjects: number
  failedObjects: number
  queuedObjects?: number
}

export function poolBucketTransferStatus(bucket: TransferBucket): string {
  const status = String(bucket.status || "pending").toLowerCase()
  if (status === "retrying") return "retrying"
  if (["running", "dispatching", "queued"].includes(status)) return status
  if (Number(bucket.failedObjects) > 0) return "failed"
  const total = Number(bucket.totalObjects) || 0
  const done = (Number(bucket.transferredObjects) || 0) + (Number(bucket.skippedObjects) || 0)
  const queued = Number(bucket.queuedObjects) || 0
  if (Number.isFinite(total) && Number.isFinite(done) && total > 0 && done >= total && queued === 0) return "completed"
  if (["aborted", "canceled", "cancelled"].includes(status)) return "aborted"
  if (["verifying", "verification_failed", "settings_syncing", "settings_failed", "completed"].includes(status)) {
    if (bucket.failedObjects > 0) return "failed"
    return queued > 0 ? "queued" : "pending"
  }
  if (status === "no_files") return "completed"
  return status
}
