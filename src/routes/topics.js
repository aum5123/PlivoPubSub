/**
 * REST API routes for topic management
 */

const express = require('express');
const router = express.Router();

/**
 * Creates topic management routes
 * @param {TopicRegistry} topicRegistry - The topic registry instance
 * @param {object} options - Configuration options
 * @returns {express.Router} - Express router
 */
function createTopicRoutes(topicRegistry, options = {}) {
  const { apiKey } = options;

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
   * POST /topics - Create a new topic
   */
  router.post('/', (req, res) => {
    try {
      const { name } = req.body;

      // Validate request body
      if (!name || typeof name !== 'string' || name.trim() === '') {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Topic name is required and must be a non-empty string'
        });
      }

      const topicName = name.trim();

      // Check if topic already exists
      if (topicRegistry.hasTopic(topicName)) {
        return res.status(409).json({
          error: 'Conflict',
          message: 'Topic already exists'
        });
      }

      // Create topic
      topicRegistry.createTopic(topicName);
      console.log(`Created topic: ${topicName}`);

      res.status(201).json({
        status: 'created',
        topic: topicName
      });
    } catch (error) {
      console.error('Error creating topic:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to create topic'
      });
    }
  });

  /**
   * DELETE /topics/:name - Delete a topic
   */
  router.delete('/:name', (req, res) => {
    try {
      const { name } = req.params;

      if (!name || name.trim() === '') {
        return res.status(400).json({
          error: 'Bad Request',
          message: 'Topic name is required'
        });
      }

      const topicName = name.trim();

      // Check if topic exists
      if (!topicRegistry.hasTopic(topicName)) {
        return res.status(404).json({
          error: 'Not Found',
          message: 'Topic not found'
        });
      }

      // Delete topic (this will notify all subscribers)
      const deleted = topicRegistry.deleteTopic(topicName);
      
      if (deleted) {
        console.log(`Deleted topic: ${topicName}`);
        res.status(200).json({
          status: 'deleted',
          topic: topicName
        });
      } else {
        res.status(500).json({
          error: 'Internal Server Error',
          message: 'Failed to delete topic'
        });
      }
    } catch (error) {
      console.error('Error deleting topic:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to delete topic'
      });
    }
  });

  /**
   * GET /topics - List all topics
   */
  router.get('/', (req, res) => {
    try {
      const topics = topicRegistry.getTopicsInfo();
      res.json({ topics });
    } catch (error) {
      console.error('Error listing topics:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to list topics'
      });
    }
  });

  return router;
}

module.exports = createTopicRoutes;
