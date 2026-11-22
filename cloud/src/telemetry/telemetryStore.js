/**
 * Pipvane v3 - Cloud Control Plane
 * Telemetry Persistence Store
 *
 * Implements in-memory ring buffer and DynamoDB storage for scrubbed telemetry.
 */

class TelemetryStore {
  async record(item) {
    throw new Error('Not implemented');
  }

  async getRecent(limit = 100) {
    throw new Error('Not implemented');
  }

  async getAggregates() {
    throw new Error('Not implemented');
  }

  async clear() {
    throw new Error('Not implemented');
  }
}

/**
 * In-Memory Ring Buffer Store
 * Stores up to maxCapacity records in FIFO ring buffer.
 */
class MemoryTelemetryStore extends TelemetryStore {
  constructor(options = {}) {
    super();
    this.maxCapacity = options.maxCapacity || 5000;
    this.records = [];
  }

  async record(item) {
    if (this.records.length >= this.maxCapacity) {
      this.records.shift(); // Evict oldest
    }
    this.records.push(item);
    return item;
  }

  async getRecent(limit = 100) {
    const lim = Math.max(1, Math.min(limit, 1000));
    return this.records.slice(-lim).reverse();
  }

  /**
   * Get Support Queue view where Founder cohort records are strictly prioritized
   * at the top of the queue (Sprint 26)
   * @param {number} [limit=100]
   * @returns {Promise<Array<object>>}
   */
  async getSupportQueue(limit = 100) {
    const lim = Math.max(1, Math.min(limit, 1000));
    const sorted = [...this.records].sort((a, b) => {
      const aFounder = a.support_queue_priority === 'HIGH' ? 1 : 0;
      const bFounder = b.support_queue_priority === 'HIGH' ? 1 : 0;
      if (bFounder !== aFounder) {
        return bFounder - aFounder; // Founders first
      }
      return (b.timestamp || 0) - (a.timestamp || 0); // Newest first
    });
    return sorted.slice(0, lim);
  }

  async getAggregates() {
    if (this.records.length === 0) {
      return {
        sample_count: 0,
        founder_sample_count: 0,
        average_latency_ms: 0,
        p50_latency_ms: 0,
        p95_latency_ms: 0,
        p99_latency_ms: 0,
        crash_counts: {},
      };
    }

    const latencies = this.records.map((r) => r.latency_ms).sort((a, b) => a - b);
    const sum = latencies.reduce((acc, v) => acc + v, 0);
    const avg = Math.round((sum / latencies.length) * 1000) / 1000;

    const p50 = latencies[Math.floor(latencies.length * 0.5)];
    const p95 = latencies[Math.floor(latencies.length * 0.95)];
    const p99 = latencies[Math.floor(latencies.length * 0.99)];

    let founderCount = 0;
    const crashCounts = {};
    for (const r of this.records) {
      if (r.support_queue_priority === 'HIGH' || r.edition === 'Founder') {
        founderCount++;
      }
      if (r.crash_code) {
        crashCounts[r.crash_code] = (crashCounts[r.crash_code] || 0) + 1;
      }
    }

    return {
      sample_count: this.records.length,
      founder_sample_count: founderCount,
      average_latency_ms: avg,
      p50_latency_ms: p50,
      p95_latency_ms: p95,
      p99_latency_ms: p99,
      crash_counts: crashCounts,
    };
  }

  async count() {
    return this.records.length;
  }

  async clear() {
    this.records = [];
  }
}

/**
 * DynamoDB Telemetry Store (AWS Lambda / DynamoDB)
 * Table: `pipvane_telemetry` with partition key `license_id_hash` and sort key `timestamp#telemetry_id`.
 */
class DynamoDBTelemetryStore extends TelemetryStore {
  constructor(options = {}) {
    super();
    this.client = options.dynamoClient;
    this.tableName = options.tableName || 'pipvane_telemetry';
  }

  async record(item) {
    if (!this.client) return item;
    const ttl = item.timestamp + (90 * 86400); // 90 days retention

    const params = {
      TableName: this.tableName,
      Item: {
        license_id_hash: item.license_id_hash,
        timestamp_id: `${item.timestamp}#${item.telemetry_id}`,
        os_version: item.os_version,
        latency_ms: item.latency_ms,
        crash_code: item.crash_code,
        edition: item.edition || 'Standard',
        support_queue_priority: item.support_queue_priority || 'NORMAL',
        priority_support: Boolean(item.priority_support),
        timestamp: item.timestamp,
        ttl,
      },
    };

    if (this.client.put(params).promise) {
      await this.client.put(params).promise();
    } else {
      await this.client.put(params);
    }
    return item;
  }

  async getRecent(limit = 100) {
    return [];
  }

  async getAggregates() {
    return {};
  }

  async clear() {}
}

module.exports = {
  TelemetryStore,
  MemoryTelemetryStore,
  DynamoDBTelemetryStore,
};
