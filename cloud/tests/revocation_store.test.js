/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * Revocation Store Unit Tests
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert');
const {
  MemoryRevocationStore,
  DynamoDBRevocationStore,
  RedisRevocationStore,
} = require('../src/services/revocationStore');

describe('Revocation Store Tests', () => {
  describe('MemoryRevocationStore', () => {
    let store;

    beforeEach(() => {
      store = new MemoryRevocationStore({ autoPrune: false });
    });

    it('records and checks revoked JTIs correctly', async () => {
      const jti = 'jti-test-uuid-1';
      assert.strictEqual(await store.isRevoked(jti), false);

      await store.revoke(jti, Math.floor(Date.now() / 1000) + 3600);
      assert.strictEqual(await store.isRevoked(jti), true);
      assert.strictEqual(store.size(), 1);
    });

    it('prunes expired JTIs correctly based on natural expiry', async () => {
      const now = Math.floor(Date.now() / 1000);
      const expiredJti = 'jti-expired-1';
      const activeJti = 'jti-active-2';

      // Expired 10 seconds ago
      await store.revoke(expiredJti, now - 10);
      // Active for 3600 seconds
      await store.revoke(activeJti, now + 3600);

      assert.strictEqual(store.size(), 2);

      store.prune();

      assert.strictEqual(store.size(), 1);
      assert.strictEqual(await store.isRevoked(expiredJti), false);
      assert.strictEqual(await store.isRevoked(activeJti), true);
    });

    it('clears all entries', async () => {
      await store.revoke('jti-1', 1800000000);
      await store.revoke('jti-2', 1800000000);
      assert.strictEqual(store.size(), 2);

      await store.clear();
      assert.strictEqual(store.size(), 0);
      assert.strictEqual(await store.isRevoked('jti-1'), false);
    });
  });

  describe('DynamoDBRevocationStore Interface', () => {
    it('interacts with mock DynamoDB client', async () => {
      const mockItems = new Map();
      const mockDynamoClient = {
        get: (params) => {
          return {
            promise: async () => ({
              Item: mockItems.get(params.Key.jti),
            }),
          };
        },
        put: (params) => {
          return {
            promise: async () => {
              mockItems.set(params.Item.jti, params.Item);
              return {};
            },
          };
        },
      };

      const dynamoStore = new DynamoDBRevocationStore({
        dynamoClient: mockDynamoClient,
        tableName: 'test_revocations',
      });

      const testJti = 'dynamo-jti-abc';
      assert.strictEqual(await dynamoStore.isRevoked(testJti), false);

      await dynamoStore.revoke(testJti, Math.floor(Date.now() / 1000) + 3600);
      assert.strictEqual(await dynamoStore.isRevoked(testJti), true);
    });
  });

  describe('RedisRevocationStore Interface', () => {
    it('interacts with mock Redis client', async () => {
      const redisStoreMap = new Map();
      const mockRedisClient = {
        exists: async (key) => (redisStoreMap.has(key) ? 1 : 0),
        set: async (key, val, ex, ttl) => {
          redisStoreMap.set(key, val);
          return 'OK';
        },
        keys: async (pattern) => Array.from(redisStoreMap.keys()),
        del: async (keys) => {
          for (const k of keys) redisStoreMap.delete(k);
          return keys.length;
        },
      };

      const redisStore = new RedisRevocationStore({
        redisClient: mockRedisClient,
        prefix: 'test:revoked:',
      });

      const testJti = 'redis-jti-xyz';
      assert.strictEqual(await redisStore.isRevoked(testJti), false);

      await redisStore.revoke(testJti, Math.floor(Date.now() / 1000) + 3600);
      assert.strictEqual(await redisStore.isRevoked(testJti), true);

      await redisStore.clear();
      assert.strictEqual(await redisStore.isRevoked(testJti), false);
    });
  });
});
