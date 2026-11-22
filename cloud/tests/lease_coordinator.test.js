/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * LeaseCoordinator Unit & Concurrency Tests
 *
 * Verifies:
 * 1. Active lease grant with 30-second TTL (30,000ms)
 * 2. 10-second heartbeat renewal extending TTL
 * 3. Strict Concurrency / Anti-Split-Brain Invariant:
 *    Simultaneous acquire requests for the same license_id NEVER produce two active leases
 * 4. Failover promotion: Standby successfully promoted once Primary's lease expires
 * 5. Fencing of stale / former active holders
 * 6. Voluntary lease release
 * 7. Multi-tenant isolation across distinct licenses
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert');
const { LeaseCoordinator } = require('../src/coordinator/leaseCoordinator');

describe('LeaseCoordinator Service & Concurrency Invariant', () => {
  let coordinator;

  beforeEach(() => {
    coordinator = new LeaseCoordinator({
      leaseTtlMs: 30000,
      heartbeatIntervalMs: 10000,
      autoSweep: false,
    });
  });

  afterEach(() => {
    coordinator.close();
  });

  // =========================================================================
  // 1. Basic Lease Grant & 30-Second TTL
  // =========================================================================
  describe('Lease Grant & 30-Second TTL', () => {
    it('grants an active lease with 30-second TTL (30,000ms)', async () => {
      const fixedEpochMs = 1770000000000;
      const res = await coordinator.acquireLease({
        licenseId: 'lic_test_alpha',
        nodeId: 'node_primary_alpha',
        nodeType: 'PRIMARY',
        currentEpochMs: fixedEpochMs,
      });

      assert.strictEqual(res.status, 'GRANTED');
      assert.strictEqual(res.active, true);
      assert.strictEqual(res.role, 'PRIMARY');
      assert.strictEqual(res.licenseId, 'lic_test_alpha');
      assert.strictEqual(res.nodeId, 'node_primary_alpha');
      assert.strictEqual(res.ttlMs, 30000);
      assert.strictEqual(res.expiresAt, fixedEpochMs + 30000);
      assert.strictEqual(res.epochVersion, 1);
      assert.ok(res.leaseToken);

      // Verify status query
      const status = coordinator.getLeaseStatus('lic_test_alpha', fixedEpochMs);
      assert.strictEqual(status.hasActiveLease, true);
      assert.strictEqual(status.activeNodeId, 'node_primary_alpha');
      assert.strictEqual(status.msRemaining, 30000);
    });

    it('renews lease when the current active holder requests acquire again', async () => {
      const startMs = 1770000000000;
      const first = await coordinator.acquireLease({
        licenseId: 'lic_renew_test',
        nodeId: 'node_primary_01',
        currentEpochMs: startMs,
      });

      // 10 seconds later, same node requests acquire again
      const renewMs = startMs + 10000;
      const renewed = await coordinator.acquireLease({
        licenseId: 'lic_renew_test',
        nodeId: 'node_primary_01',
        currentEpochMs: renewMs,
      });

      assert.strictEqual(renewed.status, 'RENEWED');
      assert.strictEqual(renewed.active, true);
      assert.strictEqual(renewed.leaseToken, first.leaseToken);
      assert.strictEqual(renewed.expiresAt, renewMs + 30000);
    });
  });

  // =========================================================================
  // 2. 10-Second Heartbeat Protocol
  // =========================================================================
  describe('Heartbeat Protocol', () => {
    it('renews lease on periodic heartbeat every 10 seconds', async () => {
      const startMs = 1770000000000;
      const initial = await coordinator.acquireLease({
        licenseId: 'lic_hb_test',
        nodeId: 'node_primary_hb',
        currentEpochMs: startMs,
      });

      // Ping 1 (at 10s)
      const ping1Ms = startMs + 10000;
      const hb1 = await coordinator.renewHeartbeat({
        licenseId: 'lic_hb_test',
        nodeId: 'node_primary_hb',
        leaseToken: initial.leaseToken,
        currentEpochMs: ping1Ms,
      });

      assert.strictEqual(hb1.status, 'RENEWED');
      assert.strictEqual(hb1.active, true);
      assert.strictEqual(hb1.expiresAt, ping1Ms + 30000);

      // Ping 2 (at 20s)
      const ping2Ms = ping1Ms + 10000;
      const hb2 = await coordinator.renewHeartbeat({
        licenseId: 'lic_hb_test',
        nodeId: 'node_primary_hb',
        leaseToken: initial.leaseToken,
        currentEpochMs: ping2Ms,
      });

      assert.strictEqual(hb2.status, 'RENEWED');
      assert.strictEqual(hb2.expiresAt, ping2Ms + 30000);
    });

    it('rejects heartbeat if leaseToken does not match current active token', async () => {
      const startMs = 1770000000000;
      await coordinator.acquireLease({
        licenseId: 'lic_hb_mismatch',
        nodeId: 'node_primary_hb',
        currentEpochMs: startMs,
      });

      const res = await coordinator.renewHeartbeat({
        licenseId: 'lic_hb_mismatch',
        nodeId: 'node_primary_hb',
        leaseToken: 'invalid-stale-token-uuid',
        currentEpochMs: startMs + 5000,
      });

      assert.strictEqual(res.status, 'FENCED');
      assert.strictEqual(res.shouldHalt, true);
      assert.strictEqual(res.reason, 'TOKEN_MISMATCH');
    });

    it('returns EXPIRED if heartbeat arrives after 30s TTL has passed', async () => {
      const startMs = 1770000000000;
      const initial = await coordinator.acquireLease({
        licenseId: 'lic_hb_late',
        nodeId: 'node_primary_hb',
        currentEpochMs: startMs,
      });

      // Advance clock past 30s TTL (e.g. 31s later)
      const lateMs = startMs + 31000;
      const res = await coordinator.renewHeartbeat({
        licenseId: 'lic_hb_late',
        nodeId: 'node_primary_hb',
        leaseToken: initial.leaseToken,
        currentEpochMs: lateMs,
      });

      assert.strictEqual(res.status, 'EXPIRED');
      assert.strictEqual(res.shouldHalt, true);
      assert.strictEqual(res.reason, 'LEASE_EXPIRED');
    });
  });

  // =========================================================================
  // 3. Strict Concurrency & Anti-Split-Brain Invariant
  // =========================================================================
  describe('Strict Concurrency & Anti-Split-Brain Invariant', () => {
    it('guarantees that two nodes sharing the same license_id NEVER both hold an active lease simultaneously', async () => {
      const startMs = 1770000000000;

      // 1. Primary acquires active lease
      const primaryRes = await coordinator.acquireLease({
        licenseId: 'lic_shared_account',
        nodeId: 'primary_node_01',
        nodeType: 'PRIMARY',
        currentEpochMs: startMs,
      });
      assert.strictEqual(primaryRes.status, 'GRANTED');
      assert.strictEqual(primaryRes.active, true);

      // 2. Standby requests active lease while Primary is active (at t + 5s)
      const standbyRes = await coordinator.acquireLease({
        licenseId: 'lic_shared_account',
        nodeId: 'standby_node_02',
        nodeType: 'STANDBY',
        currentEpochMs: startMs + 5000,
      });

      // MUST BE DENIED!
      assert.strictEqual(standbyRes.status, 'DENIED');
      assert.strictEqual(standbyRes.active, false);
      assert.strictEqual(standbyRes.role, 'STANDBY');
      assert.strictEqual(standbyRes.reason, 'ACTIVE_LEASE_HELD');
      assert.strictEqual(standbyRes.currentHolder, 'primary_node_01');
      assert.strictEqual(standbyRes.msRemaining, 25000);

      // Invariant check: only 1 node holds active lease
      const status = coordinator.getLeaseStatus('lic_shared_account', startMs + 5000);
      assert.strictEqual(status.activeNodeId, 'primary_node_01');
    });

    it('resolves massive concurrent race conditions atomically (100 simultaneous requests)', async () => {
      const licenseId = 'lic_stress_race';
      const nodeCount = 100;
      const timestamp = 1770000000000;

      // Dispatch 100 simultaneous acquire requests for the same license
      const promises = [];
      for (let i = 0; i < nodeCount; i++) {
        promises.push(
          coordinator.acquireLease({
            licenseId,
            nodeId: `node_candidate_${i}`,
            nodeType: i === 0 ? 'PRIMARY' : 'STANDBY',
            currentEpochMs: timestamp,
          })
        );
      }

      const results = await Promise.all(promises);

      // Filter who was granted active status
      const granted = results.filter((r) => r.status === 'GRANTED');
      const denied = results.filter((r) => r.status === 'DENIED');

      // MATHEMATICAL INVARIANT: EXACTLY 1 node granted active lock!
      assert.strictEqual(granted.length, 1, 'Exactly one node must be granted the active lease');
      assert.strictEqual(denied.length, nodeCount - 1, 'All other nodes must be denied');

      const winner = granted[0];
      assert.strictEqual(winner.active, true);

      // Verify all denied responses reference the winner
      for (const d of denied) {
        assert.strictEqual(d.active, false);
        assert.strictEqual(d.currentHolder, winner.nodeId);
      }
    });
  });

  // =========================================================================
  // 4. Failover & Standby Promotion
  // =========================================================================
  describe('Failover & Standby Promotion', () => {
    it('promotes Standby node to Active when Primary lease expires', async () => {
      const t0 = 1770000000000;

      // 1. Primary acquires active lease
      const primary = await coordinator.acquireLease({
        licenseId: 'lic_failover_demo',
        nodeId: 'primary_vps_1',
        nodeType: 'PRIMARY',
        currentEpochMs: t0,
      });
      assert.strictEqual(primary.status, 'GRANTED');

      // 2. Primary heartbeats once at t + 10s
      const hb1 = await coordinator.renewHeartbeat({
        licenseId: 'lic_failover_demo',
        nodeId: 'primary_vps_1',
        leaseToken: primary.leaseToken,
        currentEpochMs: t0 + 10000,
      });
      assert.strictEqual(hb1.status, 'RENEWED');
      // Lease now expires at t0 + 10000 + 30000 = t0 + 40000ms

      // 3. Primary crashes / loses internet! No more heartbeats sent.
      // Standby checks at t0 + 30s: still active, Standby denied
      const standbyCheck1 = await coordinator.acquireLease({
        licenseId: 'lic_failover_demo',
        nodeId: 'standby_vps_2',
        nodeType: 'STANDBY',
        currentEpochMs: t0 + 30000,
      });
      assert.strictEqual(standbyCheck1.status, 'DENIED');

      // 4. Time advances past expiration: t0 + 40,001ms
      const tPromote = t0 + 40001;
      const standbyPromote = await coordinator.acquireLease({
        licenseId: 'lic_failover_demo',
        nodeId: 'standby_vps_2',
        nodeType: 'STANDBY',
        currentEpochMs: tPromote,
      });

      // PROMOTION GRANTED!
      assert.strictEqual(standbyPromote.status, 'GRANTED');
      assert.strictEqual(standbyPromote.active, true);
      assert.strictEqual(standbyPromote.failoverPromoted, true);
      assert.strictEqual(standbyPromote.previousNodeId, 'primary_vps_1');
      assert.strictEqual(standbyPromote.nodeId, 'standby_vps_2');
      assert.strictEqual(standbyPromote.epochVersion, 2);
      assert.strictEqual(standbyPromote.expiresAt, tPromote + 30000);

      // 5. If old Primary reconnects and tries to heartbeat with old token: FENCED!
      const zombiePrimaryHb = await coordinator.renewHeartbeat({
        licenseId: 'lic_failover_demo',
        nodeId: 'primary_vps_1',
        leaseToken: primary.leaseToken,
        currentEpochMs: tPromote + 1000,
      });

      assert.strictEqual(zombiePrimaryHb.status, 'FENCED');
      assert.strictEqual(zombiePrimaryHb.reason, 'NODE_NOT_ACTIVE');
      assert.strictEqual(zombiePrimaryHb.shouldHalt, true);
      assert.strictEqual(zombiePrimaryHb.currentHolder, 'standby_vps_2');
    });

    it('allows immediate promotion on voluntary lease release', async () => {
      const t0 = 1770000000000;
      const primary = await coordinator.acquireLease({
        licenseId: 'lic_graceful_switch',
        nodeId: 'node_graceful_pri',
        currentEpochMs: t0,
      });

      // Voluntary release at t0 + 2s
      const releaseRes = await coordinator.releaseLease({
        licenseId: 'lic_graceful_switch',
        nodeId: 'node_graceful_pri',
        leaseToken: primary.leaseToken,
      });
      assert.strictEqual(releaseRes.status, 'RELEASED');

      // Standby acquires immediately at t0 + 2.1s without waiting for 30s TTL
      const standbyRes = await coordinator.acquireLease({
        licenseId: 'lic_graceful_switch',
        nodeId: 'node_graceful_std',
        nodeType: 'STANDBY',
        currentEpochMs: t0 + 2100,
      });

      assert.strictEqual(standbyRes.status, 'GRANTED');
      assert.strictEqual(standbyRes.nodeId, 'node_graceful_std');
    });
  });

  // =========================================================================
  // 5. Multi-Tenant License Isolation
  // =========================================================================
  describe('Multi-Tenant License Isolation', () => {
    it('manages distinct licenses independently without interference', async () => {
      const t0 = 1770000000000;

      const [resA, resB] = await Promise.all([
        coordinator.acquireLease({
          licenseId: 'lic_customer_A',
          nodeId: 'node_A_primary',
          currentEpochMs: t0,
        }),
        coordinator.acquireLease({
          licenseId: 'lic_customer_B',
          nodeId: 'node_B_primary',
          currentEpochMs: t0,
        }),
      ]);

      assert.strictEqual(resA.status, 'GRANTED');
      assert.strictEqual(resB.status, 'GRANTED');
      assert.strictEqual(resA.nodeId, 'node_A_primary');
      assert.strictEqual(resB.nodeId, 'node_B_primary');
      assert.notStrictEqual(resA.leaseToken, resB.leaseToken);
    });
  });
});
