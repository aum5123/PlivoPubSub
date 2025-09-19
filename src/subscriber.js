/**
 * Subscriber class managing individual client connections and message queues
 */

const { createErrorResponse } = require('./utils');

class Subscriber {
  /**
   * Creates a new subscriber
   * @param {string} clientId - Client identifier
   * @param {WebSocket} ws - WebSocket connection
   * @param {number} queueMax - Maximum queue size
   * @param {number} slowConsumerThreshold - Threshold for slow consumer detection
   */
  constructor(clientId, ws, queueMax = 100, slowConsumerThreshold = 50) {
    this.clientId = clientId;
    this.ws = ws;
    this.queue = [];
    this.queueMax = queueMax;
    this.slowConsumerThreshold = slowConsumerThreshold;
    this.sending = false;
    this.droppedCount = 0;
    this.lastSeen = Date.now();
  }

  /**
   * Adds a message to the subscriber's queue with backpressure handling
   * @param {object} message - Message to enqueue
   * @returns {boolean} - True if message was enqueued, false if subscriber should be disconnected
   */
  enqueueMessage(message) {
    // Check if queue is full
    if (this.queue.length >= this.queueMax) {
      // Backpressure: drop oldest message (FIFO drop)
      this.queue.shift();
      this.droppedCount++;
      
      // Check if subscriber is too slow
      if (this.droppedCount >= this.slowConsumerThreshold) {
        this.sendSlowConsumerError();
        return false; // Signal to disconnect
      }
    }

    // Add new message to queue
    this.queue.push(message);
    this.lastSeen = Date.now();
    
    // Trigger flush if not currently sending
    if (!this.sending) {
      this.flush();
    }
    
    return true;
  }

  /**
   * Sends a slow consumer error and prepares for disconnection
   */
  sendSlowConsumerError() {
    const errorResponse = createErrorResponse(
      'SLOW_CONSUMER',
      'subscriber queue overflow - disconnecting'
    );
    
    try {
      if (this.ws.readyState === this.ws.OPEN) {
        this.ws.send(JSON.stringify(errorResponse));
      }
    } catch (error) {
      console.error('Error sending slow consumer error:', error);
    }
  }

  /**
   * Flushes queued messages to the WebSocket connection
   */
  async flush() {
    if (this.sending || this.queue.length === 0) {
      return;
    }

    this.sending = true;

    try {
      while (this.queue.length > 0 && this.ws.readyState === this.ws.OPEN) {
        const message = this.queue.shift();
        this.ws.send(JSON.stringify(message));
        
        // Small delay to prevent overwhelming the client
        await new Promise(resolve => setImmediate(resolve));
      }
    } catch (error) {
      console.error(`Error flushing messages for client ${this.clientId}:`, error);
    } finally {
      this.sending = false;
    }
  }

  /**
   * Checks if the subscriber is still connected
   * @returns {boolean} - True if connected
   */
  isConnected() {
    return this.ws.readyState === this.ws.OPEN;
  }

  /**
   * Gets subscriber statistics
   * @returns {object} - Subscriber stats
   */
  getStats() {
    return {
      clientId: this.clientId,
      queueLength: this.queue.length,
      droppedCount: this.droppedCount,
      lastSeen: this.lastSeen,
      isConnected: this.isConnected()
    };
  }

  /**
   * Closes the WebSocket connection
   * @param {string} reason - Reason for closing
   */
  close(reason = 'Subscriber disconnected') {
    try {
      if (this.ws.readyState === this.ws.OPEN) {
        this.ws.close(1000, reason);
      }
    } catch (error) {
      console.error(`Error closing connection for client ${this.clientId}:`, error);
    }
  }
}

module.exports = Subscriber;
