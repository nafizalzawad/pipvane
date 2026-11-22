/**
 * Pipvane v3 - Cloud Control Plane
 * Keyed Asynchronous Mutex
 *
 * Ensures strictly serialized, race-condition-free execution per key (license_id).
 * Guarantees that concurrent requests for the same license_id execute in FIFO order,
 * while distinct license_ids execute concurrently without blocking.
 */

class KeyedMutex {
  constructor() {
    // Map of key -> Promise representing the tail of the execution queue
    this.chains = new Map();
  }

  /**
   * Run an asynchronous task with exclusive lock on the given key.
   *
   * @template T
   * @param {string} key - Unique key to lock on (e.g. license_id)
   * @param {() => Promise<T>|T} taskFn - The task to execute exclusively
   * @returns {Promise<T>} Result of taskFn
   */
  async runExclusive(key, taskFn) {
    if (!key) {
      throw new Error('KeyedMutex.runExclusive requires a non-empty key');
    }

    // Capture the existing lock chain (or a resolved promise if none)
    const previousPromise = this.chains.get(key) || Promise.resolve();

    let releaseLock;
    const currentPromise = new Promise((resolve) => {
      releaseLock = resolve;
    });

    // Update the queue tail for this key
    this.chains.set(key, currentPromise);

    try {
      // Wait for all prior tasks on this key to finish
      await previousPromise;
      // Execute the critical section
      return await taskFn();
    } finally {
      // Release lock for the next task in queue
      releaseLock();
      // Clean up the map if this was the last task in the chain
      if (this.chains.get(key) === currentPromise) {
        this.chains.delete(key);
      }
    }
  }

  /**
   * Check if a key currently has active/pending operations
   * @param {string} key
   * @returns {boolean}
   */
  isLocked(key) {
    return this.chains.has(key);
  }

  /**
   * Current number of distinct keys with active locks
   * @returns {number}
   */
  activeLockCount() {
    return this.chains.size;
  }
}

module.exports = {
  KeyedMutex,
};
