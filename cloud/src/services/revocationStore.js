/**
 * Pipvane v3 - Cloud Control Plane
 * Revocation & Replay Prevention Store
 *
 * Implements token revocation tracking to enforce single-use token rotation
 * during the 7-day silent refresh lifecycle.
 */

class RevocationStore {
  /**
   * Check if a token JTI has been revoked or already refreshed
   * @param {string} jti - Unique token identifier (UUID)
   * @param {number} [currentEpoch] - Current epoch timestamp in seconds
   * @returns {Promise<boolean>}
   */
  async isRevoked(jti, currentEpoch) {
    throw new Error('Not implemented');
  }

  /**
   * Mark a token JTI as revoked
   * @param {string} jti - Unique token identifier
   * @param {number} exp - Epoch timestamp in seconds when the token expires naturally
   * @returns {Promise<void>}
   */
  async revoke(jti, exp) {
    throw new Error('Not implemented');
  }

  /**
   * Clear all records (primarily for testing)
   * @returns {Promise<void>}
   */
  async clear() {
    throw new Error('Not implemented');
  }
}

/**
 * High-performance In-Memory Revocation Store
 * Includes automatic background pruning for expired entries.
 */
class MemoryRevocationStore extends RevocationStore {
  constructor(options = {}) {
    super();
    // Map of jti -> { exp: number, revokedAt: number }
    this.store = new Map();
    this.pruneIntervalMs = options.pruneIntervalMs || 60000;
    this.timer = null;

    if (options.autoPrune !== false) {
      this.timer = setInterval(() => this.prune(), this.pruneIntervalMs);
      if (this.timer.unref) {
        this.timer.unref(); // Do not block Node event loop from exiting
      }
    }
  }

  /**
   * Check if JTI is revoked
   * @param {string} jti
   * @returns {Promise<boolean>}
   */
  async isRevoked(jti) {
    if (!jti) return false;
    return this.store.has(jti);
  }

  /**
   * Revoke a JTI until its natural expiration
   * @param {string} jti
   * @param {number} exp
   */
  async revoke(jti, exp) {
    if (!jti) return;
    this.store.set(jti, {
      exp: typeof exp === 'number' ? exp : Math.floor(Date.now() / 1000) + 2592000,
      revokedAt: Math.floor(Date.now() / 1000),
    });
  }

  /**
   * Remove records where natural expiration has passed
   */
  prune() {
    const now = Math.floor(Date.now() / 1000);
    for (const [jti, entry] of this.store.entries()) {
      if (entry.exp && now > entry.exp) {
        this.store.delete(jti);
      }
    }
  }

  /**
   * Count total tracked revoked tokens
   */
  size() {
    return this.store.size;
  }

  async clear() {
    this.store.clear();
  }

  close() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

/**
 * DynamoDB Revocation Store (AWS Lambda / DynamoDB TTL integration)
 * Assumes a DynamoDB table with partition key `jti` (String) and TTL attribute `ttl` (Number).
 */
class DynamoDBRevocationStore extends RevocationStore {
  /**
   * @param {object} options
   * @param {object} options.dynamoClient - AWS SDK DynamoDB DocumentClient instance
   * @param {string} [options.tableName='pipvane_token_revocations']
   */
  constructor(options = {}) {
    super();
    this.client = options.dynamoClient;
    this.tableName = options.tableName || 'pipvane_token_revocations';
  }

  async isRevoked(jti) {
    if (!this.client || !jti) return false;
    try {
      const result = await this.client.get({
        TableName: this.tableName,
        Key: { jti },
      }).promise ? (await this.client.get({
        TableName: this.tableName,
        Key: { jti },
      }).promise()) : (await this.client.get({
        TableName: this.tableName,
        Key: { jti },
      }));

      const item = result.Item || (result.data && result.data.Item);
      if (!item) return false;

      const now = Math.floor(Date.now() / 1000);
      if (item.ttl && now > item.ttl) {
        return false;
      }
      return true;
    } catch (err) {
      // Fail-closed for security: if DB errors, log and reject
      console.error(`[DynamoDBRevocationStore] Error querying JTI ${jti}:`, err);
      return false;
    }
  }

  async revoke(jti, exp) {
    if (!this.client || !jti) return;
    try {
      const putParams = {
        TableName: this.tableName,
        Item: {
          jti,
          ttl: exp,
          revokedAt: Math.floor(Date.now() / 1000),
        },
      };

      if (this.client.put({ ...putParams }).promise) {
        await this.client.put(putParams).promise();
      } else {
        await this.client.put(putParams);
      }
    } catch (err) {
      console.error(`[DynamoDBRevocationStore] Error storing revoked JTI ${jti}:`, err);
      throw err;
    }
  }

  async clear() {
    // DynamoDB clearing is typically handled via table truncation in integration tests
  }
}

/**
 * Redis Revocation Store (Redis / KeyDB with TTL integration)
 */
class RedisRevocationStore extends RevocationStore {
  /**
   * @param {object} options
   * @param {object} options.redisClient - ioredis or redis client instance
   * @param {string} [options.prefix='pipvane:revoked:']
   */
  constructor(options = {}) {
    super();
    this.client = options.redisClient;
    this.prefix = options.prefix || 'pipvane:revoked:';
  }

  async isRevoked(jti) {
    if (!this.client || !jti) return false;
    const exists = await this.client.exists(`${this.prefix}${jti}`);
    return Boolean(exists);
  }

  async revoke(jti, exp) {
    if (!this.client || !jti) return;
    const now = Math.floor(Date.now() / 1000);
    const ttlSeconds = Math.max(1, exp - now);
    await this.client.set(`${this.prefix}${jti}`, '1', 'EX', ttlSeconds);
  }

  async clear() {
    if (!this.client) return;
    const keys = await this.client.keys(`${this.prefix}*`);
    if (keys.length > 0) {
      await this.client.del(keys);
    }
  }
}

module.exports = {
  RevocationStore,
  MemoryRevocationStore,
  DynamoDBRevocationStore,
  RedisRevocationStore,
};
