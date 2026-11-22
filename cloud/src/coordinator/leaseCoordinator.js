/**
 * Pipvane v3 - Cloud Control Plane
 * Active/Standby State Lease Coordinator (Anti-Split-Brain Controller)
 *
 * Implements:
 * - 30-Second Lease TTL (Time-To-Live)
 * - 10-Second Heartbeat Protocol for Active Node Renewal
 * - Automatic Failover Promotion for Standby Node upon Lease Expiry
 * - Strict Per-License Concurrency Control (KeyedMutex)
 * - Zero Split-Brain Invariant: Strictly <= 1 Active node per license at any timestamp t
 */

const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');
const { KeyedMutex } = require('./mutex');

// Timing constants
const DEFAULT_LEASE_TTL_MS = 30000;         // 30 seconds active execution lock
const DEFAULT_HEARTBEAT_INTERVAL_MS = 10000; // 10 seconds expected ping

class LeaseCoordinator extends EventEmitter {
  /**
   * @param {object} [options]
   * @param {number} [options.leaseTtlMs=30000] - TTL in milliseconds
   * @param {number} [options.heartbeatIntervalMs=10000] - Expected ping interval in ms
   * @param {object} [options.stateStore] - Optional external store (e.g. Redis interface)
   */
  constructor(options = {}) {
    super();
    this.leaseTtlMs = options.leaseTtlMs || DEFAULT_LEASE_TTL_MS;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs || DEFAULT_HEARTBEAT_INTERVAL_MS;
    this.mutex = new KeyedMutex();

    // In-memory state map: license_id -> LeaseState
    this.leases = new Map();

    // Standby registry: license_id -> Map<node_id, StandbyNodeRecord>
    this.standbys = new Map();

    // Background expiration checker (optional, runs periodically)
    this.sweepIntervalMs = options.sweepIntervalMs || 1000;
    this.sweepTimer = null;
    if (options.autoSweep !== false) {
      this.sweepTimer = setInterval(() => this.sweepExpiredLeases(), this.sweepIntervalMs);
      if (this.sweepTimer.unref) {
        this.sweepTimer.unref();
      }
    }
  }

  /**
   * Request an "Active" execution lease for a specific license_id.
   *
   * Logic:
   * 1. If no active lease exists, or existing lease has expired (now >= expires_at):
   *    Grant lease to calling node with 30s TTL.
   * 2. If active unexpired lease exists:
   *    - If caller is already the active holder, renew TTL.
   *    - If caller is a different node (e.g. Standby node attempting promotion prematurely),
   *      deny request and place caller into Standby mode.
   *
   * @param {object} params
   * @param {string} params.licenseId - Unique license identifier
   * @param {string} params.nodeId - Unique node hardware / agent identifier
   * @param {'PRIMARY'|'STANDBY'} [params.nodeType='PRIMARY'] - Role of calling node
   * @param {number} [params.currentEpochMs] - Optional explicit timestamp for deterministic testing
   * @returns {Promise<object>} Lease decision
   */
  async acquireLease({
    licenseId,
    nodeId,
    nodeType = 'PRIMARY',
    currentEpochMs,
  }) {
    if (!licenseId || !nodeId) {
      const err = new Error('licenseId and nodeId are required');
      err.code = 'INVALID_ACQUIRE_PARAMS';
      throw err;
    }

    return await this.mutex.runExclusive(licenseId, async () => {
      const now = currentEpochMs !== undefined ? currentEpochMs : Date.now();
      const existing = this.leases.get(licenseId);

      // Check if existing lease is present and valid
      if (existing && existing.activeNodeId) {
        if (now < existing.expiresAt) {
          // Lease is currently active!
          if (existing.activeNodeId === nodeId) {
            // Same node renewing its lease
            existing.expiresAt = now + this.leaseTtlMs;
            existing.lastHeartbeatAt = now;

            const res = {
              status: 'RENEWED',
              active: true,
              role: existing.activeNodeType,
              licenseId,
              nodeId,
              leaseToken: existing.leaseToken,
              epochVersion: existing.epochVersion,
              expiresAt: existing.expiresAt,
              ttlMs: this.leaseTtlMs,
              heartbeatIntervalMs: this.heartbeatIntervalMs,
            };

            this.emit('lease_renewed', res);
            return res;
          }

          // A DIFFERENT node holds the active lease: REJECT acquisition to prevent split-brain!
          this._registerStandbyInternal(licenseId, nodeId, nodeType, now);

          const res = {
            status: 'DENIED',
            active: false,
            role: 'STANDBY',
            reason: 'ACTIVE_LEASE_HELD',
            licenseId,
            nodeId,
            currentHolder: existing.activeNodeId,
            currentHolderType: existing.activeNodeType,
            epochVersion: existing.epochVersion,
            expiresAt: existing.expiresAt,
            msRemaining: existing.expiresAt - now,
          };

          this.emit('lease_denied', res);
          return res;
        }

        // Lease expired! Log failover
        this.emit('lease_expired', {
          licenseId,
          expiredNodeId: existing.activeNodeId,
          expiredAt: existing.expiresAt,
          promotedNodeId: nodeId,
        });
      }

      // No active lease, or previous lease expired -> Grant new active lease
      const nextEpochVersion = existing ? existing.epochVersion + 1 : 1;
      const leaseToken = crypto.randomUUID();
      const expiresAt = now + this.leaseTtlMs;

      const newLease = {
        licenseId,
        activeNodeId: nodeId,
        activeNodeType: nodeType,
        leaseToken,
        epochVersion: nextEpochVersion,
        grantedAt: now,
        expiresAt,
        lastHeartbeatAt: now,
      };

      this.leases.set(licenseId, newLease);
      // Remove this node from standby list if it was previously waiting
      this._unregisterStandbyInternal(licenseId, nodeId);

      const res = {
        status: 'GRANTED',
        active: true,
        role: nodeType,
        failoverPromoted: Boolean(existing && existing.activeNodeId && existing.activeNodeId !== nodeId),
        previousNodeId: existing ? existing.activeNodeId : null,
        licenseId,
        nodeId,
        leaseToken,
        epochVersion: nextEpochVersion,
        grantedAt: now,
        expiresAt,
        ttlMs: this.leaseTtlMs,
        heartbeatIntervalMs: this.heartbeatIntervalMs,
      };

      this.emit('lease_granted', res);
      return res;
    });
  }

  /**
   * Renew active lease via heartbeat ping (expected every 10 seconds).
   *
   * @param {object} params
   * @param {string} params.licenseId
   * @param {string} params.nodeId
   * @param {string} params.leaseToken - Must match current active token
   * @param {number} [params.currentEpochMs]
   * @returns {Promise<object>} Heartbeat response
   */
  async renewHeartbeat({
    licenseId,
    nodeId,
    leaseToken,
    currentEpochMs,
  }) {
    if (!licenseId || !nodeId || !leaseToken) {
      const err = new Error('licenseId, nodeId, and leaseToken are required');
      err.code = 'INVALID_HEARTBEAT_PARAMS';
      throw err;
    }

    return await this.mutex.runExclusive(licenseId, async () => {
      const now = currentEpochMs !== undefined ? currentEpochMs : Date.now();
      const lease = this.leases.get(licenseId);

      if (!lease || !lease.activeNodeId) {
        return {
          status: 'FENCED',
          active: false,
          licenseId,
          nodeId,
          reason: 'NO_ACTIVE_LEASE',
          shouldHalt: true,
        };
      }

      if (lease.activeNodeId !== nodeId) {
        return {
          status: 'FENCED',
          active: false,
          licenseId,
          nodeId,
          reason: 'NODE_NOT_ACTIVE',
          currentHolder: lease.activeNodeId,
          shouldHalt: true,
        };
      }

      if (lease.leaseToken !== leaseToken) {
        return {
          status: 'FENCED',
          active: false,
          licenseId,
          nodeId,
          reason: 'TOKEN_MISMATCH',
          shouldHalt: true,
        };
      }

      if (now >= lease.expiresAt) {
        // Lease has expired already
        return {
          status: 'EXPIRED',
          active: false,
          licenseId,
          nodeId,
          reason: 'LEASE_EXPIRED',
          shouldHalt: true,
        };
      }

      // Renew 30-second TTL
      lease.expiresAt = now + this.leaseTtlMs;
      lease.lastHeartbeatAt = now;

      const res = {
        status: 'RENEWED',
        active: true,
        licenseId,
        nodeId,
        leaseToken: lease.leaseToken,
        epochVersion: lease.epochVersion,
        expiresAt: lease.expiresAt,
        ttlMs: this.leaseTtlMs,
        nextHeartbeatDueMs: this.heartbeatIntervalMs,
      };

      this.emit('lease_renewed', res);
      return res;
    });
  }

  /**
   * Gracefully release an active lease (e.g. on clean shutdown or manual failover).
   *
   * @param {object} params
   * @param {string} params.licenseId
   * @param {string} params.nodeId
   * @param {string} params.leaseToken
   * @returns {Promise<object>}
   */
  async releaseLease({
    licenseId,
    nodeId,
    leaseToken,
  }) {
    return await this.mutex.runExclusive(licenseId, async () => {
      const lease = this.leases.get(licenseId);

      if (lease && lease.activeNodeId === nodeId && lease.leaseToken === leaseToken) {
        lease.activeNodeId = null;
        lease.leaseToken = null;
        lease.expiresAt = 0;

        const res = {
          status: 'RELEASED',
          licenseId,
          nodeId,
          success: true,
        };

        this.emit('lease_released', res);
        return res;
      }

      return {
        status: 'IGNORED',
        licenseId,
        nodeId,
        reason: 'NOT_ACTIVE_HOLDER',
      };
    });
  }

  /**
   * Get current lease snapshot for a license_id
   * @param {string} licenseId
   * @param {number} [currentEpochMs]
   * @returns {object}
   */
  getLeaseStatus(licenseId, currentEpochMs) {
    const now = currentEpochMs !== undefined ? currentEpochMs : Date.now();
    const lease = this.leases.get(licenseId);
    if (!lease || !lease.activeNodeId) {
      return {
        hasActiveLease: false,
        licenseId,
        activeNodeId: null,
      };
    }

    const isExpired = now >= lease.expiresAt;
    return {
      hasActiveLease: !isExpired,
      isExpired,
      licenseId,
      activeNodeId: isExpired ? null : lease.activeNodeId,
      activeNodeType: isExpired ? null : lease.activeNodeType,
      epochVersion: lease.epochVersion,
      expiresAt: lease.expiresAt,
      msRemaining: Math.max(0, lease.expiresAt - now),
      standbyCount: this.getStandbyCount(licenseId),
    };
  }

  /**
   * Register standby node
   */
  _registerStandbyInternal(licenseId, nodeId, nodeType, now) {
    let standbyMap = this.standbys.get(licenseId);
    if (!standbyMap) {
      standbyMap = new Map();
      this.standbys.set(licenseId, standbyMap);
    }
    standbyMap.set(nodeId, {
      nodeId,
      nodeType,
      registeredAt: now,
      lastSeenAt: now,
    });
  }

  _unregisterStandbyInternal(licenseId, nodeId) {
    const standbyMap = this.standbys.get(licenseId);
    if (standbyMap) {
      standbyMap.delete(nodeId);
      if (standbyMap.size === 0) {
        this.standbys.delete(licenseId);
      }
    }
  }

  getStandbyCount(licenseId) {
    const standbyMap = this.standbys.get(licenseId);
    return standbyMap ? standbyMap.size : 0;
  }

  /**
   * Sweep and detect expired leases
   */
  sweepExpiredLeases(currentEpochMs) {
    const now = currentEpochMs !== undefined ? currentEpochMs : Date.now();
    for (const [licenseId, lease] of this.leases.entries()) {
      if (lease.activeNodeId && now >= lease.expiresAt) {
        const expiredNodeId = lease.activeNodeId;
        // Invalidate active holder
        lease.activeNodeId = null;
        lease.leaseToken = null;

        this.emit('lease_vacant', {
          licenseId,
          expiredNodeId,
          standbys: Array.from(this.standbys.get(licenseId)?.keys() || []),
        });
      }
    }
  }

  /**
   * Clean shutdown
   */
  close() {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }
}

module.exports = {
  LeaseCoordinator,
  DEFAULT_LEASE_TTL_MS,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
};
