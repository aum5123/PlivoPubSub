/**
 * REST API routes for observability (health and stats)
 */

const express = require('express');
const router = express.Router();

/**
 * Creates observability routes
 * @param {TopicRegistry} topicRegistry - The topic registry instance
 * @param {WebSocketHandler} wsHandler - The WebSocket handler instance
 * @param {object} options - Configuration options
 * @returns {express.Router} - Express router
 */
function createObservabilityRoutes(topicRegistry, wsHandler, options = {}) {
  const { apiKey, startTime } = options;

  // Middleware for API key authentication (if enabled)
  const authenticate = (req, res, next) => {
    if (!apiKey) {
      return next(); // No auth required
    }

    const providedKey = req.headers['x-api-key'];
    if (!providedKey || providedKey !== apiKey) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Valid API key required'
      });
    }

    next();
  };

  // Apply authentication to all routes
  router.use(authenticate);

  /**
   * GET /health - Health check endpoint
   */
  router.get('/health', (req, res) => {
    try {
      const uptimeSec = Math.floor((Date.now() - startTime) / 1000);
      const topics = topicRegistry.getAllTopics().length;
      const subscribers = topicRegistry.getTotalSubscriberCount();

      res.json({
        uptime_sec: uptimeSec,
        topics,
        subscribers
      });
    } catch (error) {
      console.error('Error getting health status:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to get health status'
      });
    }
  });

  /**
   * GET /stats - Statistics endpoint
   */
  router.get('/stats', (req, res) => {
    try {
      const stats = topicRegistry.getStats();
      res.json(stats);
    } catch (error) {
      console.error('Error getting statistics:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to get statistics'
      });
    }
  });

  /**
   * GET /debug - Debug information (optional, for development)
   */
  router.get('/debug', (req, res) => {
    try {
      const debugInfo = {
        topics: topicRegistry.getAllTopics().map(name => {
          const topic = topicRegistry.getTopic(name);
          return {
            name: topic.name,
            stats: topic.getStats(),
            subscribers: Array.from(topic.subscribers.keys())
          };
        }),
        connections: wsHandler.getStats(),
        uptime: Date.now() - startTime,
        memory: process.memoryUsage()
      };

      res.json(debugInfo);
    } catch (error) {
      console.error('Error getting debug information:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to get debug information'
      });
    }
  });

  return router;
}

module.exports = createObservabilityRoutes;
