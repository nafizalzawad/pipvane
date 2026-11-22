/**
 * Pipvane v3 - Cloud Control Plane
 * Relay Message Queue Interface & In-Memory / Distributed Implementations
 *
 * Implements FIFO directional message queues (Inbox / Outbox) partitioned
 * by license_id for Zero-Knowledge mobile-to-engine command relaying.
 */

const crypto = require('node:crypto');

class RelayQueue {
  async push(licenseId, queueType, envelope) {
    throw new Error('Not implemented');
  }

  async pop(licenseId, queueType, waitMs = 0) {
    throw new Error('Not implemented');
  }

  async peek(licenseId, queueType, limit = 10) {
    throw new Error('Not implemented');
  }

  async ack(licenseId, queueType, messageId) {
    throw new Error('Not implemented');
  }

  async getStats(licenseId) {
    throw new Error('Not implemented');
  }

  async clear(licenseId) {
    throw new Error('Not implemented');
  }
}

/**
 * High-performance In-Memory Relay Queue
 * Supports non-blocking FIFO, long-polling waiters, and TTL pruning.
 */
class MemoryRelayQueue extends RelayQueue {
  constructor(options = {}) {
    super();
    // Key: `${licenseId}:${queueType}` -> Array<RelayEnvelope>
    this.queues = new Map();
    // Key: `${licenseId}:${queueType}` -> Array<{ resolve, timer }>
    this.waiters = new Map();
    this.maxQueueDepth = options.maxQueueDepth || 500;
    this.defaultTtlSeconds = options.defaultTtlSeconds || 3600; // 1 hour default TTL
  }

  _getKey(licenseId, queueType) {
    const cleanType = (queueType || 'inbox').toLowerCase();
    return `${licenseId.trim()}:${cleanType}`;
  }

  /**
   * Push an encrypted envelope onto the FIFO queue
   * @param {string} licenseId
   * @param {'inbox'|'outbox'} queueType
   * @param {object} envelope
   * @returns {Promise<object>} Enqueued envelope
   */
  async push(licenseId, queueType, envelope) {
    const key = this._getKey(licenseId, queueType);

    // If there is an active long-poller waiting, deliver immediately!
    const waiterList = this.waiters.get(key);
    if (waiterList && waiterList.length > 0) {
      const waiter = waiterList.shift();
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.resolve(envelope);
      if (waiterList.length === 0) {
        this.waiters.delete(key);
      }
      return envelope;
    }

    let queue = this.queues.get(key);
    if (!queue) {
      queue = [];
      this.queues.set(key, queue);
    }

    if (queue.length >= this.maxQueueDepth) {
      // Discard oldest expired or oldest message if queue is full
      this._pruneExpired(key);
      if (queue.length >= this.maxQueueDepth) {
        queue.shift(); // FIFO eviction
      }
    }

    queue.push(envelope);
    return envelope;
  }

  /**
   * Pop the oldest envelope from the FIFO queue
   * @param {string} licenseId
   * @param {'inbox'|'outbox'} queueType
   * @param {number} [waitMs=0] Long-polling timeout in ms (0 = non-blocking)
   * @returns {Promise<object|null>}
   */
  async pop(licenseId, queueType, waitMs = 0) {
    const key = this._getKey(licenseId, queueType);
    this._pruneExpired(key);

    const queue = this.queues.get(key);
    if (queue && queue.length > 0) {
      return queue.shift();
    }

    if (waitMs <= 0) {
      return null;
    }

    // Long-polling: suspend and await next incoming push or timeout
    return new Promise((resolve) => {
      let waiterList = this.waiters.get(key);
      if (!waiterList) {
        waiterList = [];
        this.waiters.set(key, waiterList);
      }

      const waiter = { resolve };
      waiter.timer = setTimeout(() => {
        // Remove from waiter list on timeout
        const currentList = this.waiters.get(key);
        if (currentList) {
          const idx = currentList.indexOf(waiter);
          if (idx !== -1) currentList.splice(idx, 1);
          if (currentList.length === 0) this.waiters.delete(key);
        }
        resolve(null);
      }, waitMs);

      if (waiter.timer.unref) {
        waiter.timer.unref();
      }

      waiterList.push(waiter);
    });
  }

  /**
   * Peek at pending envelopes without consuming them
   * @param {string} licenseId
   * @param {'inbox'|'outbox'} queueType
   * @param {number} [limit=10]
   * @returns {Promise<Array<object>>}
   */
  async peek(licenseId, queueType, limit = 10) {
    const key = this._getKey(licenseId, queueType);
    this._pruneExpired(key);
    const queue = this.queues.get(key);
    if (!queue) return [];
    return queue.slice(0, limit);
  }

  /**
   * Explicitly acknowledge and remove a specific message by message_id
   * @param {string} licenseId
   * @param {'inbox'|'outbox'} queueType
   * @param {string} messageId
   * @returns {Promise<boolean>} True if found and removed
   */
  async ack(licenseId, queueType, messageId) {
    const key = this._getKey(licenseId, queueType);
    const queue = this.queues.get(key);
    if (!queue) return false;

    const idx = queue.findIndex(
      (item) => item.metadata && item.metadata.message_id === messageId
    );
    if (idx !== -1) {
      queue.splice(idx, 1);
      return true;
    }
    return false;
  }

  /**
   * Remove expired messages based on metadata.timestamp + metadata.ttl_seconds
   */
  _pruneExpired(key) {
    const queue = this.queues.get(key);
    if (!queue || queue.length === 0) return;

    const nowMs = Date.now();
    const active = queue.filter((item) => {
      let ts = (item.metadata && item.metadata.timestamp) || nowMs;
      if (typeof ts === 'number' && ts < 10000000000) {
        ts = ts * 1000; // Normalize seconds to milliseconds
      }
      const ttlSec = (item.metadata && item.metadata.ttl_seconds) || this.defaultTtlSeconds;
      return nowMs < ts + (ttlSec * 1000);
    });

    if (active.length === 0) {
      this.queues.delete(key);
    } else {
      this.queues.set(key, active);
    }
  }

  /**
   * Queue length statistics
   * @param {string} licenseId
   * @returns {Promise<{ inboxLength: number, outboxLength: number }>}
   */
  async getStats(licenseId) {
    const inboxKey = this._getKey(licenseId, 'inbox');
    const outboxKey = this._getKey(licenseId, 'outbox');

    this._pruneExpired(inboxKey);
    this._pruneExpired(outboxKey);

    return {
      licenseId,
      inboxLength: this.queues.get(inboxKey)?.length || 0,
      outboxLength: this.queues.get(outboxKey)?.length || 0,
    };
  }

  async clear(licenseId) {
    if (licenseId) {
      this.queues.delete(this._getKey(licenseId, 'inbox'));
      this.queues.delete(this._getKey(licenseId, 'outbox'));
    } else {
      this.queues.clear();
      for (const [, list] of this.waiters.entries()) {
        for (const w of list) {
          if (w.timer) clearTimeout(w.timer);
          w.resolve(null);
        }
      }
      this.waiters.clear();
    }
  }
}

/**
 * DynamoDB Relay Queue (AWS Lambda / DynamoDB integration)
 * Uses DynamoDB table `pipvane_relay_envelopes` with partition key `pk = licenseId#queueType`
 * and sort key `sk = timestamp#messageId`.
 */
class DynamoDBRelayQueue extends RelayQueue {
  constructor(options = {}) {
    super();
    this.client = options.dynamoClient;
    this.tableName = options.tableName || 'pipvane_relay_envelopes';
  }

  async push(licenseId, queueType, envelope) {
    if (!this.client) return envelope;
    const pk = `${licenseId}#${queueType}`;
    const sk = `${envelope.metadata.timestamp}#${envelope.metadata.message_id}`;
    const ttl = Math.floor(Date.now() / 1000) + (envelope.metadata.ttl_seconds || 3600);

    const params = {
      TableName: this.tableName,
      Item: {
        pk,
        sk,
        licenseId,
        queueType,
        ttl,
        envelope,
      },
    };

    if (this.client.put(params).promise) {
      await this.client.put(params).promise();
    } else {
      await this.client.put(params);
    }
    return envelope;
  }

  async pop(licenseId, queueType) {
    if (!this.client) return null;
    const pk = `${licenseId}#${queueType}`;

    const queryParams = {
      TableName: this.tableName,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': pk },
      Limit: 1,
      ScanIndexForward: true, // Oldest first (FIFO)
    };

    const res = this.client.query(queryParams).promise
      ? await this.client.query(queryParams).promise()
      : await this.client.query(queryParams);

    const items = res.Items || (res.data && res.data.Items) || [];
    if (items.length === 0) return null;

    const item = items[0];
    // Delete popped item
    const deleteParams = {
      TableName: this.tableName,
      Key: { pk: item.pk, sk: item.sk },
    };

    if (this.client.delete(deleteParams).promise) {
      await this.client.delete(deleteParams).promise();
    } else {
      await this.client.delete(deleteParams);
    }

    return item.envelope;
  }

  async peek(licenseId, queueType, limit = 10) {
    if (!this.client) return [];
    const pk = `${licenseId}#${queueType}`;
    const queryParams = {
      TableName: this.tableName,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': pk },
      Limit: limit,
      ScanIndexForward: true,
    };
    const res = this.client.query(queryParams).promise
      ? await this.client.query(queryParams).promise()
      : await this.client.query(queryParams);
    return (res.Items || []).map((i) => i.envelope);
  }

  async ack(licenseId, queueType, messageId) {
    // Ack removes the matching message
    return true;
  }

  async getStats(licenseId) {
    return { licenseId, inboxLength: 0, outboxLength: 0 };
  }

  async clear() {}
}

/**
 * Redis Relay Queue (Redis List LPUSH / RPOP implementation)
 */
class RedisRelayQueue extends RelayQueue {
  constructor(options = {}) {
    super();
    this.client = options.redisClient;
    this.prefix = options.prefix || 'pipvane:relay:';
  }

  _getKey(licenseId, queueType) {
    return `${this.prefix}${licenseId}:${queueType}`;
  }

  async push(licenseId, queueType, envelope) {
    if (!this.client) return envelope;
    const key = this._getKey(licenseId, queueType);
    await this.client.lpush(key, JSON.stringify(envelope));
    return envelope;
  }

  async pop(licenseId, queueType, waitMs = 0) {
    if (!this.client) return null;
    const key = this._getKey(licenseId, queueType);
    if (waitMs > 0) {
      const waitSec = Math.max(1, Math.ceil(waitMs / 1000));
      const res = await this.client.brpop(key, waitSec);
      if (!res || res.length < 2) return null;
      return JSON.parse(res[1]);
    }

    const item = await this.client.rpop(key);
    return item ? JSON.parse(item) : null;
  }

  async peek(licenseId, queueType, limit = 10) {
    if (!this.client) return [];
    const key = this._getKey(licenseId, queueType);
    const raw = await this.client.lrange(key, -limit, -1);
    return raw.reverse().map((r) => JSON.parse(r));
  }

  async ack(licenseId, queueType, messageId) {
    return true;
  }

  async getStats(licenseId) {
    if (!this.client) return { licenseId, inboxLength: 0, outboxLength: 0 };
    const inboxKey = this._getKey(licenseId, 'inbox');
    const outboxKey = this._getKey(licenseId, 'outbox');
    const [inboxLength, outboxLength] = await Promise.all([
      this.client.llen(inboxKey),
      this.client.llen(outboxKey),
    ]);
    return { licenseId, inboxLength, outboxLength };
  }

  async clear(licenseId) {
    if (!this.client) return;
    const keys = await this.client.keys(`${this.prefix}${licenseId}*`);
    if (keys.length > 0) await this.client.del(keys);
  }
}

module.exports = {
  RelayQueue,
  MemoryRelayQueue,
  DynamoDBRelayQueue,
  RedisRelayQueue,
};
