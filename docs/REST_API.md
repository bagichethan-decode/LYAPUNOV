# LYAPUNOV REST API

The REST API provides a local HTTP interface to the existing LYAPUNOV reliability engine.

## Configuration

Required environment variables:
- LYAPUNOV_DB_URL
- LYAPUNOV_DB_USER
- LYAPUNOV_DB_PASSWORD

Optional environment variables:
- LYAPUNOV_API_HOST (default: 127.0.0.1)
- LYAPUNOV_API_PORT (default: 8081)

The API is unauthenticated in this development milestone. Keep it bound to localhost. Never expose it to public networks.

## Endpoints

### Health
GET /health

Returns API and database availability.

### Enqueue event
POST /api/v1/events

Content-Type: application/json

Example request:
{"aggregateType":"ORDER","aggregateId":"order-1001","eventType":"ORDER_CREATED","payload":{"orderId":"order-1001","amount":1250}}

### Dead-letter events
GET /api/v1/events/dead

Optional query parameters: limit and offset.

### Workers
GET /api/v1/workers

Returns worker heartbeat information.

## Reliability guarantees

- Persistence uses existing LYAPUNOV repositories.
- Delivery semantics remain at-least-once.
- Consumers must implement idempotency.
- Privileged retry and skip operations are not exposed.
- Authentication and authorization are required before public deployment.