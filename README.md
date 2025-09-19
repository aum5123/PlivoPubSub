# Plivo Pub/Sub Service

A high-performance, in-memory Pub/Sub service built with Node.js, featuring WebSocket real-time messaging and REST API for topic management and observability.

## Features

- **Real-time Messaging**: WebSocket-based publish/subscribe with low latency
- **REST API**: Complete topic management and observability endpoints
- **In-Memory Storage**: No external dependencies (Redis, Kafka, etc.)
- **Backpressure Handling**: Drop-oldest policy with slow consumer detection
- **Message Replay**: Configurable ring buffer for message history
- **Graceful Shutdown**: Proper cleanup and connection handling
- **Docker Support**: Containerized deployment ready
- **Authentication**: Optional API key authentication
- **Observability**: Health checks, statistics, and monitoring

## Quick Start

### Local Development

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Start the service**:
   ```bash
   # Development mode with auto-reload
   npm run dev
   
   # Production mode
   npm start
   ```

3. **Service will be available at**:
   - WebSocket: `ws://localhost:8080/ws`
   - REST API: `http://localhost:8080`

### Docker Deployment

1. **Build the image**:
   ```bash
   docker build -t plivo-pubsub .
   ```

2. **Run the container**:
   ```bash
   docker run -p 8080:8080 plivo-pubsub
   ```

3. **With custom configuration**:
   ```bash
   docker run -p 8080:8080 \
     -e PORT=8080 \
     -e QUEUE_MAX=200 \
     -e RING_BUFFER_SIZE=500 \
     plivo-pubsub
   ```

## Configuration

Configure the service using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Server port |
| `WS_PATH` | `/ws` | WebSocket endpoint path |
| `QUEUE_MAX` | `100` | Maximum messages per subscriber queue |
| `RING_BUFFER_SIZE` | `100` | Size of topic message ring buffer |
| `SLOW_CONSUMER_THRESHOLD` | `50` | Drop threshold before disconnecting slow consumers |
| `GRACEFUL_TIMEOUT_MS` | `5000` | Graceful shutdown timeout |
| `API_KEY` | `null` | Optional API key for authentication |
| `LOG_LEVEL` | `info` | Logging level |
| `HEARTBEAT_INTERVAL` | `30000` | Heartbeat logging interval (ms) |

## API Reference

### WebSocket API

Connect to `ws://localhost:8080/ws` for real-time messaging.

#### Message Types

**Subscribe to a topic**:
```json
{
  "type": "subscribe",
  "topic": "orders",
  "client_id": "s1",
  "last_n": 5,
  "request_id": "uuid-optional"
}
```

**Unsubscribe from a topic**:
```json
{
  "type": "unsubscribe",
  "topic": "orders",
  "client_id": "s1",
  "request_id": "uuid-optional"
}
```

**Publish a message**:
```json
{
  "type": "publish",
  "topic": "orders",
  "message": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "payload": { "order_id": "ORD-123", "amount": 99.5 }
  },
  "request_id": "uuid-optional"
}
```

**Ping**:
```json
{
  "type": "ping",
  "request_id": "uuid-optional"
}
```

#### Response Types

**Acknowledgment**:
```json
{
  "type": "ack",
  "request_id": "...",
  "topic": "orders",
  "status": "ok",
  "ts": "2025-01-25T10:00:00Z"
}
```

**Event (delivered to subscribers)**:
```json
{
  "type": "event",
  "topic": "orders",
  "message": { "id": "...", "payload": ... },
  "ts": "2025-01-25T10:00:00Z"
}
```

**Error**:
```json
{
  "type": "error",
  "request_id": "...",
  "error": { "code": "BAD_REQUEST", "message": "..." },
  "ts": "2025-01-25T10:00:00Z"
}
```

**Pong**:
```json
{
  "type": "pong",
  "request_id": "...",
  "ts": "2025-01-25T10:00:00Z"
}
```

### REST API

#### Topic Management

**Create a topic**:
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"orders"}' \
  http://localhost:8080/topics
```

**List topics**:
```bash
curl http://localhost:8080/topics
```

**Delete a topic**:
```bash
curl -X DELETE http://localhost:8080/topics/orders
```

#### Observability

**Health check**:
```bash
curl http://localhost:8080/health
```

**Statistics**:
```bash
curl http://localhost:8080/stats
```

**Debug information**:
```bash
curl http://localhost:8080/debug
```

## Design Assumptions

### Backpressure Policy
- **Drop Oldest**: When a subscriber's queue is full, the oldest message is dropped
- **Slow Consumer Detection**: If drops exceed `SLOW_CONSUMER_THRESHOLD`, the subscriber is disconnected
- **Queue Size**: Each subscriber has a bounded queue (default: 100 messages)

### Message Replay
- **Ring Buffer**: Each topic maintains a circular buffer of recent messages
- **Replay Order**: Messages are replayed in chronological order (oldest first)
- **Configurable Size**: Ring buffer size is configurable via `RING_BUFFER_SIZE`

### Graceful Shutdown
1. Stop accepting new WebSocket connections
2. Attempt to flush existing subscriber queues
3. Notify all subscribers of topic deletions
4. Close all connections after timeout
5. Exit cleanly

### Concurrency
- **Single-threaded**: Leverages Node.js event loop for concurrency
- **Non-blocking**: Uses `setImmediate` for async operations
- **Race Condition Prevention**: Per-subscriber sending locks prevent concurrent sends

## Testing

### Manual Testing with curl and wscat

1. **Start the service**:
   ```bash
   npm start
   ```

2. **Create a topic**:
   ```bash
   curl -X POST -H "Content-Type: application/json" \
     -d '{"name":"test"}' \
     http://localhost:8080/topics
   ```

3. **Subscribe to the topic** (in another terminal):
   ```bash
   wscat -c ws://localhost:8080/ws
   > {"type":"subscribe","topic":"test","client_id":"client1","last_n":5}
   ```

4. **Publish a message** (in another terminal):
   ```bash
   wscat -c ws://localhost:8080/ws
   > {"type":"publish","topic":"test","message":{"id":"550e8400-e29b-41d4-a716-446655440000","payload":{"test":"data"}}}
   ```

5. **Check statistics**:
   ```bash
   curl http://localhost:8080/stats
   ```

### Test Scripts

See the `tests/` directory for automated test scripts and examples.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   WebSocket     │    │   REST API       │    │   Topic         │
│   Handler       │    │   Routes         │    │   Registry      │
├─────────────────┤    ├──────────────────┤    ├─────────────────┤
│ • Message       │    │ • POST /topics   │    │ • Topic Map     │
│   Processing    │    │ • DELETE /topics │    │ • Ring Buffers  │
│ • Validation    │    │ • GET /topics    │    │ • Subscribers   │
│ • Fan-out       │    │ • GET /health    │    │ • Statistics    │
│ • Backpressure  │    │ • GET /stats     │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Subscriber    │
                    │   Management    │
                    ├─────────────────┤
                    │ • Queue Logic   │
                    │ • Backpressure  │
                    │ • Connection    │
                    │   Handling      │
                    └─────────────────┘
```

## Error Handling

### WebSocket Errors
- `BAD_REQUEST`: Invalid message format or missing required fields
- `TOPIC_NOT_FOUND`: Attempting to publish/subscribe to non-existent topic
- `SLOW_CONSUMER`: Subscriber queue overflow, connection will be closed

### REST API Errors
- `400 Bad Request`: Invalid request body or parameters
- `401 Unauthorized`: Missing or invalid API key (if enabled)
- `404 Not Found`: Topic not found
- `409 Conflict`: Topic already exists
- `500 Internal Server Error`: Unexpected server error

## Performance Considerations

- **Memory Usage**: All data is stored in memory, monitor usage for large deployments
- **Connection Limits**: No built-in connection limits, consider load balancing for scale
- **Message Throughput**: Optimized for high-frequency, low-latency messaging
- **Backpressure**: Automatic handling prevents memory exhaustion

## Security

- **API Key Authentication**: Optional header-based authentication
- **WebSocket Security**: API key validation on connection upgrade
- **Input Validation**: Comprehensive message and parameter validation
- **Error Information**: Sanitized error messages to prevent information leakage

## Monitoring

- **Health Endpoint**: Basic service health and uptime
- **Statistics**: Per-topic message counts and subscriber counts
- **Debug Endpoint**: Detailed internal state (development only)
- **Logging**: Structured logging with configurable levels
- **Heartbeat**: Periodic status logging

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions, please create an issue in the repository.
