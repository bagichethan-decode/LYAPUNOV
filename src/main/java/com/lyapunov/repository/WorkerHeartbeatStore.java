package com.lyapunov.repository;

/**
 * Persistence boundary used by a running relay worker to publish lifecycle
 * and progress information without depending on a concrete JDBC repository.
 */
public interface WorkerHeartbeatStore {
    void heartbeat(String workerId, String instanceId, long publishedCount,
                   long failedCount, long staleClaimCount, String lastError) throws Exception;

    boolean markStopped(String workerId, String instanceId) throws Exception;
}
