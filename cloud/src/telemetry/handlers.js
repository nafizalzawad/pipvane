/**
 * Pipvane v3 - Cloud Control Plane
 * Telemetry Ingestion HTTP Handlers (POST /telemetry)
 */

const { TelemetryService } = require('./telemetryService');
const { parseJsonBody, sendJSON } = require('../api/handlers');

/**
 * Create Telemetry API router / dispatcher
 * @param {TelemetryService} [service]
 * @returns {object} Route handlers
 */
function createTelemetryApi(service) {
  const telemetryService = service || new TelemetryService();

  /**
   * POST /telemetry
   * Ingests anonymous telemetry payload, explicitly stripping IP, User-Agent, and raw Telegram text.
   */
  async function handleIngest(req, res) {
    try {
      const body = await parseJsonBody(req);

      // Ingest and scrub
      const result = await telemetryService.ingest(body);

      return sendJSON(res, 201, result);
    } catch (err) {
      const statusCode = err.code && (
        err.code.startsWith('INVALID_') ||
        err.code.startsWith('MISSING_')
      ) ? 400 : 500;

      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'TELEMETRY_INGEST_ERROR',
      });
    }
  }

  /**
   * GET /telemetry/metrics
   * Retrieves aggregated latency percentiles and crash statistics
   */
  async function handleMetrics(req, res) {
    try {
      const metrics = await telemetryService.getMetrics();
      return sendJSON(res, 200, {
        success: true,
        metrics,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  /**
   * GET /telemetry/support-queue
   * Retrieves prioritized support queue items with Founder cohort prioritized at the top (Sprint 26)
   */
  async function handleSupportQueue(req, res, searchParams) {
    try {
      const limit = searchParams && searchParams.get('limit') ? parseInt(searchParams.get('limit'), 10) : 100;
      const queue = await telemetryService.getSupportQueue(limit);
      return sendJSON(res, 200, {
        success: true,
        count: queue.length,
        support_queue: queue,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  return {
    telemetryService,
    handleIngest,
    handleMetrics,
    handleSupportQueue,
  };
}

module.exports = {
  createTelemetryApi,
};
