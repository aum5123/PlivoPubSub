/**
 * Circular buffer implementation for storing messages in topics
 */

class CircularBuffer {
  /**
   * Creates a new circular buffer
   * @param {number} size - Maximum size of the buffer
   */
  constructor(size = 100) {
    this.size = size;
    this.buffer = new Array(size);
    this.head = 0;
    this.count = 0;
  }

  /**
   * Adds a message to the buffer
   * @param {object} message - Message to add
   */
  push(message) {
    this.buffer[this.head] = message;
    this.head = (this.head + 1) % this.size;
    if (this.count < this.size) {
      this.count++;
    }
  }

  /**
   * Gets the last n messages from the buffer
   * @param {number} n - Number of messages to retrieve
   * @returns {Array} - Array of messages in chronological order (oldest first)
   */
  getLastN(n) {
    if (n <= 0 || this.count === 0) {
      return [];
    }

    const messages = [];
    const actualCount = Math.min(n, this.count);
    
    // Calculate starting index
    let startIndex;
    if (this.count < this.size) {
      // Buffer not full, start from beginning
      startIndex = 0;
    } else {
      // Buffer is full, start from head position
      startIndex = this.head;
    }

    // Collect messages in chronological order
    for (let i = 0; i < actualCount; i++) {
      const index = (startIndex + i) % this.size;
      if (this.buffer[index]) {
        messages.push(this.buffer[index]);
      }
    }

    return messages;
  }

  /**
   * Gets all messages in the buffer
   * @returns {Array} - Array of all messages in chronological order
   */
  getAll() {
    return this.getLastN(this.count);
  }

  /**
   * Gets the current number of messages in the buffer
   * @returns {number} - Number of messages
   */
  length() {
    return this.count;
  }

  /**
   * Clears the buffer
   */
  clear() {
    this.buffer.fill(undefined);
    this.head = 0;
    this.count = 0;
  }
}

module.exports = CircularBuffer;
