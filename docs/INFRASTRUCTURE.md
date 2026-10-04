# LYAPUNOV Infrastructure

Docker Compose infrastructure for LYAPUNOV.

## PostgreSQL
- Image: postgres:16-alpine
- Database: lyapunov
- Port: 5432
- Transactional outbox table: outbox_events

## Kafka
- Image: apache/kafka:3.7.0
- Port: 9092
- Topic: lyapunov.events
- Partitions: 3

## Verified
- PostgreSQL container running and healthy
- Kafka container running
- Kafka producer and consumer tested successfully
- Test event test-001 successfully consumed

