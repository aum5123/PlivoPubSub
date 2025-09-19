/**
 * Main application setup with Express and WebSocket server
 */

const express = require('express');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const pino = require('pino');

const { TopicRegistry } = require('./topics');
const WebSocketHandler = require('./ws-handler');
const createTopicRoutes = require('./routes/topics');
const createObservabilityRoutes = require('./routes/observability');

class PubSubApp {
  /**
   * Creates a new Pub/Sub application
   * @param {object} options - Configuration options
   */
  constructor(options = {}) {
    this.options = {
      port: process.env.PORT || 8080,
      wsPath: process.env.WS_PATH || '/ws',
      queueMax: parseInt(process.env.QUEUE_MAX) || 100,
      ringBufferSize: parseInt(process.env.RING_BUFFER_SIZE) || 100,
      slowConsumerThreshold: parseInt(process.env.SLOW_CONSUMER_THRESHOLD) || 50,
      gracefulTimeoutMs: parseInt(process.env.GRACEFUL_TIMEOUT_MS) || 5000,
      apiKey: process.env.API_KEY || null,
      heartbeatInterval: parseInt(process.env.HEARTBEAT_INTERVAL) || 30000,
      ...options
    };

    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      transport: process.env.NODE_ENV === 'development' ? {
        target: 'pino-pretty'
      } : undefined
    });

    this.startTime = Date.now();
    this.shuttingDown = false;
    this.heartbeatInterval = null;

    // Initialize components
    this.topicRegistry = new TopicRegistry({
      ringBufferSize: this.options.ringBufferSize,
      queueMax: this.options.queueMax,
      slowConsumerThreshold: this.options.slowConsumerThreshold
    });

    this.wsHandler = new WebSocketHandler(this.topicRegistry, {
      queueMax: this.options.queueMax,
      slowConsumerThreshold: this.options.slowConsumerThreshold
    });

    this.setupExpress();
    this.setupWebSocket();
    this.setupGracefulShutdown();
  }

  /**
   * Sets up Express application and routes
   */
  setupExpress() {
    this.app = express();
    
    // Middleware
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));

    // CORS middleware
    this.app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, X-API-Key');
      
      if (req.method === 'OPTIONS') {
        res.sendStatus(200);
      } else {
        next();
      }
    });

    // Routes
    this.app.use('/topics', createTopicRoutes(this.topicRegistry, {
      apiKey: this.options.apiKey
    }));

    this.app.use('/', createObservabilityRoutes(this.topicRegistry, this.wsHandler, {
      apiKey: this.options.apiKey,
      startTime: this.startTime
    }));

    // Root endpoint
    this.app.get('/', (req, res) => {
      res.json({
        service: 'Plivo Pub/Sub Service',
        version: '1.0.0',
        endpoints: {
          websocket: `ws://localhost:${this.options.port}${this.options.wsPath}`,
          rest: {
            topics: '/topics',
            health: '/health',
            stats: '/stats'
          }
        }
      });
    });

    // 404 handler
    this.app.use('*', (req, res) => {
      res.status(404).json({
        error: 'Not Found',
        message: 'Endpoint not found'
      });
    });

    // Error handler
    this.app.use((error, req, res, next) => {
      this.logger.error('Express error:', error);
      res.status(500).json({
        error: 'Internal Server Error',
        message: 'An unexpected error occurred'
      });
    });
  }

  /**
   * Sets up WebSocket server
   */
  setupWebSocket() {
    this.server = require('http').createServer(this.app);
    
    this.wss = new WebSocket.Server({
      server: this.server,
      path: this.options.wsPath,
      verifyClient: (info) => {
        // Optional API key verification for WebSocket connections
        if (this.options.apiKey) {
          const url = new URL(info.req.url, `http://${info.req.headers.host}`);
          const apiKey = url.searchParams.get('api_key') || info.req.headers['x-api-key'];
          return apiKey === this.options.apiKey;
        }
        return true;
      }
    });

    this.wss.on('connection', (ws, req) => {
      const connectionId = uuidv4();
      this.logger.info(`New WebSocket connection: ${connectionId}`);
      
      this.wsHandler.handleConnection(ws, connectionId);
    });

    this.wss.on('error', (error) => {
      this.logger.error('WebSocket server error:', error);
    });
  }

  /**
   * Sets up graceful shutdown handling
   */
  setupGracefulShutdown() {
    const gracefulShutdown = (signal) => {
      this.logger.info(`Received ${signal}, starting graceful shutdown...`);
      this.shuttingDown = true;

      // Stop accepting new connections
      this.wss.close(() => {
        this.logger.info('WebSocket server closed');
      });

      // Attempt to flush existing connections
      const flushTimeout = setTimeout(() => {
        this.logger.warn('Graceful shutdown timeout reached, forcing exit');
        process.exit(1);
      }, this.options.gracefulTimeoutMs);

      // Close all WebSocket connections
      this.wsHandler.closeAll('Server shutting down');

      // Clean up resources
      this.topicRegistry.cleanupDisconnectedSubscribers();

      // Stop heartbeat
      if (this.heartbeatInterval) {
        clearInterval(this.heartbeatInterval);
      }

      // Close HTTP server
      this.server.close(() => {
        clearTimeout(flushTimeout);
        this.logger.info('HTTP server closed, graceful shutdown complete');
        process.exit(0);
      });
    };

    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  }

  /**
   * Starts the heartbeat service
   */
  startHeartbeat() {
    if (this.heartbeatInterval) {
      return;
    }

    this.heartbeatInterval = setInterval(() => {
      if (this.shuttingDown) {
        return;
      }

      // Log heartbeat info
      const stats = this.topicRegistry.getStats();
      const connectionStats = this.wsHandler.getStats();
      
      this.logger.info('Heartbeat', {
        topics: Object.keys(stats.topics).length,
        subscribers: Object.values(stats.topics).reduce((sum, topic) => sum + topic.subscribers, 0),
        connections: connectionStats.activeConnections,
        uptime: Math.floor((Date.now() - this.startTime) / 1000)
      });
    }, this.options.heartbeatInterval);
  }

  /**
   * Starts the server
   */
  async start() {
    return new Promise((resolve, reject) => {
      this.server.listen(this.options.port, (error) => {
        if (error) {
          this.logger.error('Failed to start server:', error);
          reject(error);
          return;
        }

        this.logger.info(`Pub/Sub service started on port ${this.options.port}`);
        this.logger.info(`WebSocket endpoint: ws://localhost:${this.options.port}${this.options.wsPath}`);
        this.logger.info(`REST API: http://localhost:${this.options.port}`);
        
        if (this.options.apiKey) {
          this.logger.info('API key authentication enabled');
        }

        // Start heartbeat
        this.startHeartbeat();

        resolve();
      });
    });
  }

  /**
   * Stops the server
   */
  async stop() {
    this.logger.info('Stopping server...');
    
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }

    return new Promise((resolve) => {
      this.server.close(() => {
        this.logger.info('Server stopped');
        resolve();
      });
    });
  }
}

module.exports = PubSubApp;
