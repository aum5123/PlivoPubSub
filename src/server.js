#!/usr/bin/env node

/**
 * Entry point for the Plivo Pub/Sub service
 */

const PubSubApp = require('./app');

async function main() {
  try {
    const app = new PubSubApp();
    await app.start();
  } catch (error) {
    console.error('Failed to start Pub/Sub service:', error);
    process.exit(1);
  }
}

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start the service
if (require.main === module) {
  main();
}

module.exports = main;
