/**
 * WebSocket message handler for the Pub/Sub service
 */

const { 
  validateWebSocketMessage, 
  createErrorResponse, 
  createAckResponse, 
  createPongResponse 
} = require('./utils');

class WebSocketHandler {
  /**
   * Creates a new WebSocket handler
   * @param {TopicRegistry} topicRegistry - The topic registry instance
   * @param {object} options - Configuration options
   */
  constructor(topicRegistry, options = {}) {
    this.topicRegistry = topicRegistry;
    this.options = options;
    this.connections = new Map(); // connectionId -> { ws, clientId, topics }
  }

  /**
   * Handles a new WebSocket connection
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} connectionId - Unique connection identifier
   */
  handleConnection(ws, connectionId) {
    console.log(`New WebSocket connection: ${connectionId}`);
    
    this.connections.set(connectionId, {
      ws,
      clientId: null,
      topics: new Set()
    });

    ws.on('message', (data) => {
      this.handleMessage(ws, connectionId, data);
    });

    ws.on('close', () => {
      this.handleDisconnection(connectionId);
    });

    ws.on('error', (error) => {
      console.error(`WebSocket error for connection ${connectionId}:`, error);
      this.handleDisconnection(connectionId);
    });
  }

  /**
   * Handles incoming WebSocket messages
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} connectionId - Connection identifier
   * @param {Buffer} data - Raw message data
   */
  handleMessage(ws, connectionId, data) {
    try {
      const message = JSON.parse(data.toString('utf8'));
      
      // Validate message structure
      const validation = validateWebSocketMessage(message);
      if (!validation.isValid) {
        this.sendError(ws, 'BAD_REQUEST', validation.error, message.request_id);
        return;
      }

      // Route message to appropriate handler
      switch (message.type) {
        case 'subscribe':
          this.handleSubscribe(ws, connectionId, message);
          break;
        case 'unsubscribe':
          this.handleUnsubscribe(ws, connectionId, message);
          break;
        case 'publish':
          this.handlePublish(ws, connectionId, message);
          break;
        case 'ping':
          this.handlePing(ws, message);
          break;
        default:
          this.sendError(ws, 'BAD_REQUEST', 'Unknown message type', message.request_id);
      }
    } catch (error) {
      console.error(`Error parsing message from connection ${connectionId}:`, error);
      this.sendError(ws, 'BAD_REQUEST', 'Invalid JSON message');
    }
  }

  /**
   * Handles subscribe messages
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} connectionId - Connection identifier
   * @param {object} message - Subscribe message
   */
  handleSubscribe(ws, connectionId, message) {
    const { topic, client_id, last_n, request_id } = message;
    
    // Check if topic exists
    if (!this.topicRegistry.hasTopic(topic)) {
      this.sendError(ws, 'TOPIC_NOT_FOUND', `Topic '${topic}' does not exist`, request_id);
      return;
    }

    const connection = this.connections.get(connectionId);
    if (!connection) {
      this.sendError(ws, 'INTERNAL_ERROR', 'Connection not found', request_id);
      return;
    }

    // Update connection info
    connection.clientId = client_id;
    connection.topics.add(topic);

    // Get or create subscriber
    const topicObj = this.topicRegistry.getTopic(topic);
    let subscriber = topicObj.getSubscriber(client_id);
    
    if (!subscriber) {
      subscriber = topicObj.addSubscriber(client_id, ws, {
        queueMax: this.options.queueMax,
        slowConsumerThreshold: this.options.slowConsumerThreshold
      });
    } else {
      // Update WebSocket reference in case of reconnection
      subscriber.ws = ws;
    }

    // Handle replay if last_n is specified
    if (last_n && last_n > 0) {
      topicObj.replayMessages(subscriber, last_n);
    }

    // Send ack
    this.sendAck(ws, topic, request_id);
  }

  /**
   * Handles unsubscribe messages
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} connectionId - Connection identifier
   * @param {object} message - Unsubscribe message
   */
  handleUnsubscribe(ws, connectionId, message) {
    const { topic, client_id, request_id } = message;
    
    // Check if topic exists
    if (!this.topicRegistry.hasTopic(topic)) {
      this.sendError(ws, 'TOPIC_NOT_FOUND', `Topic '${topic}' does not exist`, request_id);
      return;
    }

    const connection = this.connections.get(connectionId);
    if (connection) {
      connection.topics.delete(topic);
    }

    // Remove subscriber from topic
    const topicObj = this.topicRegistry.getTopic(topic);
    if (topicObj) {
      topicObj.removeSubscriber(client_id);
    }

    // Send ack
    this.sendAck(ws, topic, request_id);
  }

  /**
   * Handles publish messages
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} connectionId - Connection identifier
   * @param {object} message - Publish message
   */
  handlePublish(ws, connectionId, message) {
    const { topic, message: msg, request_id } = message;
    
    // Check if topic exists
    if (!this.topicRegistry.hasTopic(topic)) {
      this.sendError(ws, 'TOPIC_NOT_FOUND', `Topic '${topic}' does not exist`, request_id);
      return;
    }

    // Publish message to topic
    const topicObj = this.topicRegistry.getTopic(topic);
    const deliveredCount = topicObj.publishMessage(msg);
    
    console.log(`Published message to topic '${topic}', delivered to ${deliveredCount} subscribers`);

    // Send ack
    this.sendAck(ws, topic, request_id);
  }

  /**
   * Handles ping messages
   * @param {WebSocket} ws - WebSocket connection
   * @param {object} message - Ping message
   */
  handlePing(ws, message) {
    this.sendPong(ws, message.request_id);
  }

  /**
   * Handles WebSocket disconnection
   * @param {string} connectionId - Connection identifier
   */
  handleDisconnection(connectionId) {
    console.log(`WebSocket disconnected: ${connectionId}`);
    
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    // Remove subscriber from all topics
    for (const topicName of connection.topics) {
      const topic = this.topicRegistry.getTopic(topicName);
      if (topic && connection.clientId) {
        topic.removeSubscriber(connection.clientId);
      }
    }

    this.connections.delete(connectionId);
  }

  /**
   * Sends an error response
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} code - Error code
   * @param {string} message - Error message
   * @param {string} requestId - Optional request ID
   */
  sendError(ws, code, message, requestId = null) {
    if (ws.readyState === ws.OPEN) {
      const errorResponse = createErrorResponse(code, message, requestId);
      ws.send(JSON.stringify(errorResponse));
    }
  }

  /**
   * Sends an ack response
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} topic - Topic name
   * @param {string} requestId - Optional request ID
   */
  sendAck(ws, topic, requestId = null) {
    if (ws.readyState === ws.OPEN) {
      const ackResponse = createAckResponse(topic, requestId);
      ws.send(JSON.stringify(ackResponse));
    }
  }

  /**
   * Sends a pong response
   * @param {WebSocket} ws - WebSocket connection
   * @param {string} requestId - Optional request ID
   */
  sendPong(ws, requestId = null) {
    if (ws.readyState === ws.OPEN) {
      const pongResponse = createPongResponse(requestId);
      ws.send(JSON.stringify(pongResponse));
    }
  }

  /**
   * Gets connection statistics
   * @returns {object} - Connection stats
   */
  getStats() {
    return {
      totalConnections: this.connections.size,
      activeConnections: Array.from(this.connections.values()).filter(
        conn => conn.ws.readyState === conn.ws.OPEN
      ).length
    };
  }

  /**
   * Closes all connections
   * @param {string} reason - Reason for closing
   */
  closeAll(reason = 'Server shutdown') {
    for (const [connectionId, connection] of this.connections) {
      try {
        connection.ws.close(1000, reason);
      } catch (error) {
        console.error(`Error closing connection ${connectionId}:`, error);
      }
    }
    this.connections.clear();
  }
}

module.exports = WebSocketHandler;
