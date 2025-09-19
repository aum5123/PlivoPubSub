/**
 * Utility functions for the Pub/Sub service
 */

const { v4: uuidv4 } = require('uuid');

/**
 * Validates if a string is a valid UUID
 * @param {string} id - The string to validate
 * @returns {boolean} - True if valid UUID
 */
function isValidUUID(id) {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(id);
}

/**
 * Generates a timestamp in ISO8601 format
 * @returns {string} - ISO8601 timestamp
 */
function getTimestamp() {
  return new Date().toISOString();
}

/**
 * Creates a standardized error response
 * @param {string} code - Error code
 * @param {string} message - Error message
 * @param {string} requestId - Optional request ID
 * @returns {object} - Error response object
 */
function createErrorResponse(code, message, requestId = null) {
  const response = {
    type: 'error',
    error: { code, message },
    ts: getTimestamp()
  };
  if (requestId) {
    response.request_id = requestId;
  }
  return response;
}

/**
 * Creates a standardized ack response
 * @param {string} topic - Topic name
 * @param {string} requestId - Optional request ID
 * @returns {object} - Ack response object
 */
function createAckResponse(topic, requestId = null) {
  const response = {
    type: 'ack',
    topic,
    status: 'ok',
    ts: getTimestamp()
  };
  if (requestId) {
    response.request_id = requestId;
  }
  return response;
}

/**
 * Creates a standardized event response
 * @param {string} topic - Topic name
 * @param {object} message - Message object
 * @returns {object} - Event response object
 */
function createEventResponse(topic, message) {
  return {
    type: 'event',
    topic,
    message,
    ts: getTimestamp()
  };
}

/**
 * Creates a standardized pong response
 * @param {string} requestId - Optional request ID
 * @returns {object} - Pong response object
 */
function createPongResponse(requestId = null) {
  const response = {
    type: 'pong',
    ts: getTimestamp()
  };
  if (requestId) {
    response.request_id = requestId;
  }
  return response;
}

/**
 * Creates a standardized info response
 * @param {string} topic - Topic name
 * @param {string} msg - Info message
 * @returns {object} - Info response object
 */
function createInfoResponse(topic, msg) {
  return {
    type: 'info',
    topic,
    msg,
    ts: getTimestamp()
  };
}

/**
 * Validates a WebSocket message structure
 * @param {object} message - The message to validate
 * @returns {object} - Validation result with isValid and error
 */
function validateWebSocketMessage(message) {
  if (!message || typeof message !== 'object') {
    return { isValid: false, error: 'Message must be a valid JSON object' };
  }

  if (!message.type || typeof message.type !== 'string') {
    return { isValid: false, error: 'Message must have a valid type field' };
  }

  const validTypes = ['subscribe', 'unsubscribe', 'publish', 'ping'];
  if (!validTypes.includes(message.type)) {
    return { isValid: false, error: `Invalid message type. Must be one of: ${validTypes.join(', ')}` };
  }

  // Validate specific message types
  switch (message.type) {
    case 'subscribe':
    case 'unsubscribe':
      if (!message.topic || typeof message.topic !== 'string') {
        return { isValid: false, error: 'Topic is required for subscribe/unsubscribe' };
      }
      if (!message.client_id || typeof message.client_id !== 'string') {
        return { isValid: false, error: 'client_id is required for subscribe/unsubscribe' };
      }
      if (message.last_n !== undefined && (typeof message.last_n !== 'number' || message.last_n < 0)) {
        return { isValid: false, error: 'last_n must be a non-negative number' };
      }
      break;

    case 'publish':
      if (!message.topic || typeof message.topic !== 'string') {
        return { isValid: false, error: 'Topic is required for publish' };
      }
      if (!message.message || typeof message.message !== 'object') {
        return { isValid: false, error: 'Message object is required for publish' };
      }
      if (!message.message.id || typeof message.message.id !== 'string') {
        return { isValid: false, error: 'Message id is required for publish' };
      }
      if (!isValidUUID(message.message.id)) {
        return { isValid: false, error: 'Message id must be a valid UUID' };
      }
      break;

    case 'ping':
      // No additional validation needed for ping
      break;
  }

  return { isValid: true };
}

module.exports = {
  isValidUUID,
  getTimestamp,
  createErrorResponse,
  createAckResponse,
  createEventResponse,
  createPongResponse,
  createInfoResponse,
  validateWebSocketMessage
};
