/**
 * Topic management and in-memory storage
 */

const CircularBuffer = require('./circular-buffer');
const Subscriber = require('./subscriber');
const { createEventResponse, createInfoResponse } = require('./utils');

class Topic {
  /**
   * Creates a new topic
   * @param {string} name - Topic name
   * @param {number} ringBufferSize - Size of the ring buffer
   */
  constructor(name, ringBufferSize = 100) {
    this.name = name;
    this.subscribers = new Map(); // clientId -> Subscriber
    this.ringBuffer = new CircularBuffer(ringBufferSize);
    this.stats = {
      messages: 0
    };
    this.createdAt = Date.now();
  }

  /**
   * Adds a subscriber to this topic
   * @param {string} clientId - Client identifier
   * @param {WebSocket} ws - WebSocket connection
   * @param {object} options - Subscriber options
   * @returns {Subscriber} - The created subscriber
   */
  addSubscriber(clientId, ws, options = {}) {
    const subscriber = new Subscriber(
      clientId,
      ws,
      options.queueMax,
      options.slowConsumerThreshold
    );
    
    this.subscribers.set(clientId, subscriber);
    return subscriber;
  }

  /**
   * Removes a subscriber from this topic
   * @param {string} clientId - Client identifier
   * @returns {boolean} - True if subscriber was removed
   */
  removeSubscriber(clientId) {
    return this.subscribers.delete(clientId);
  }

  /**
   * Gets a subscriber by client ID
   * @param {string} clientId - Client identifier
   * @returns {Subscriber|null} - The subscriber or null if not found
   */
  getSubscriber(clientId) {
    return this.subscribers.get(clientId) || null;
  }

  /**
   * Publishes a message to all subscribers of this topic
   * @param {object} message - Message to publish
   * @returns {number} - Number of subscribers that received the message
   */
  publishMessage(message) {
    // Store message in ring buffer
    this.ringBuffer.push(message);
    this.stats.messages++;

    // Create event response
    const eventResponse = createEventResponse(this.name, message);

    // Send to all subscribers
    let deliveredCount = 0;
    const disconnectedClients = [];

    for (const [clientId, subscriber] of this.subscribers) {
      if (!subscriber.isConnected()) {
        disconnectedClients.push(clientId);
        continue;
      }

      const success = subscriber.enqueueMessage(eventResponse);
      if (success) {
        deliveredCount++;
      } else {
        // Subscriber should be disconnected due to slow consumer
        disconnectedClients.push(clientId);
      }
    }

    // Clean up disconnected subscribers
    for (const clientId of disconnectedClients) {
      this.removeSubscriber(clientId);
    }

    return deliveredCount;
  }

  /**
   * Replays the last n messages to a subscriber
   * @param {Subscriber} subscriber - The subscriber to replay to
   * @param {number} lastN - Number of messages to replay
   */
  replayMessages(subscriber, lastN) {
    const messages = this.ringBuffer.getLastN(lastN);
    
    for (const message of messages) {
      const eventResponse = createEventResponse(this.name, message);
      subscriber.enqueueMessage(eventResponse);
    }
  }

  /**
   * Notifies all subscribers about topic deletion
   */
  notifyTopicDeleted() {
    const infoResponse = createInfoResponse(this.name, 'topic_deleted');
    
    for (const [clientId, subscriber] of this.subscribers) {
      if (subscriber.isConnected()) {
        try {
          subscriber.ws.send(JSON.stringify(infoResponse));
          subscriber.close('Topic deleted');
        } catch (error) {
          console.error(`Error notifying subscriber ${clientId} of topic deletion:`, error);
        }
      }
    }
  }

  /**
   * Gets topic statistics
   * @returns {object} - Topic stats
   */
  getStats() {
    return {
      name: this.name,
      subscribers: this.subscribers.size,
      messages: this.stats.messages,
      ringBufferSize: this.ringBuffer.length(),
      createdAt: this.createdAt
    };
  }

  /**
   * Gets subscriber count
   * @returns {number} - Number of active subscribers
   */
  getSubscriberCount() {
    return this.subscribers.size;
  }
}

class TopicRegistry {
  /**
   * Creates a new topic registry
   * @param {object} options - Configuration options
   */
  constructor(options = {}) {
    this.topics = new Map(); // topicName -> Topic
    this.ringBufferSize = options.ringBufferSize || 100;
    this.queueMax = options.queueMax || 100;
    this.slowConsumerThreshold = options.slowConsumerThreshold || 50;
  }

  /**
   * Creates a new topic
   * @param {string} name - Topic name
   * @returns {Topic} - The created topic
   */
  createTopic(name) {
    if (this.topics.has(name)) {
      throw new Error('Topic already exists');
    }

    const topic = new Topic(name, this.ringBufferSize);
    this.topics.set(name, topic);
    return topic;
  }

  /**
   * Deletes a topic
   * @param {string} name - Topic name
   * @returns {boolean} - True if topic was deleted
   */
  deleteTopic(name) {
    const topic = this.topics.get(name);
    if (!topic) {
      return false;
    }

    // Notify all subscribers before deletion
    topic.notifyTopicDeleted();
    
    this.topics.delete(name);
    return true;
  }

  /**
   * Gets a topic by name
   * @param {string} name - Topic name
   * @returns {Topic|null} - The topic or null if not found
   */
  getTopic(name) {
    return this.topics.get(name) || null;
  }

  /**
   * Checks if a topic exists
   * @param {string} name - Topic name
   * @returns {boolean} - True if topic exists
   */
  hasTopic(name) {
    return this.topics.has(name);
  }

  /**
   * Gets all topics
   * @returns {Array} - Array of topic names
   */
  getAllTopics() {
    return Array.from(this.topics.keys());
  }

  /**
   * Gets all topics with their subscriber counts
   * @returns {Array} - Array of topic info objects
   */
  getTopicsInfo() {
    return Array.from(this.topics.values()).map(topic => ({
      name: topic.name,
      subscribers: topic.getSubscriberCount()
    }));
  }

  /**
   * Gets detailed statistics for all topics
   * @returns {object} - Statistics object
   */
  getStats() {
    const stats = {};
    for (const [name, topic] of this.topics) {
      stats[name] = {
        messages: topic.stats.messages,
        subscribers: topic.getSubscriberCount()
      };
    }
    return { topics: stats };
  }

  /**
   * Gets total subscriber count across all topics
   * @returns {number} - Total subscriber count
   */
  getTotalSubscriberCount() {
    let total = 0;
    for (const topic of this.topics.values()) {
      total += topic.getSubscriberCount();
    }
    return total;
  }

  /**
   * Cleans up disconnected subscribers from all topics
   */
  cleanupDisconnectedSubscribers() {
    for (const topic of this.topics.values()) {
      const disconnectedClients = [];
      
      for (const [clientId, subscriber] of topic.subscribers) {
        if (!subscriber.isConnected()) {
          disconnectedClients.push(clientId);
        }
      }
      
      for (const clientId of disconnectedClients) {
        topic.removeSubscriber(clientId);
      }
    }
  }
}

module.exports = { Topic, TopicRegistry };
