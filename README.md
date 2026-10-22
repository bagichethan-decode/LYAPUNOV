# LYAPUNOV

### Distributed Event Reliability & Fault-Verification Platform

> A distributed event delivery system that tests message reliability, ordering, and recovery under real-world failures.

[![Java 21](https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk\&logoColor=white)](https://www.oracle.com/java/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-336791?logo=postgresql\&logoColor=white)](https://www.postgresql.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%2B-231F20?logo=apachekafka\&logoColor=white)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker\&logoColor=white)](https://www.docker.com/)
[![Linux](https://img.shields.io/badge/Linux-Systems%20Tooling-FCC624?logo=linux\&logoColor=black)](https://www.linux.org/)

---

## Overview

Modern backend systems often use asynchronous events to keep services in sync.

The difficult part is what happens when something fails between those services.

A database transaction may succeed while a message broker is temporarily unavailable. A worker may crash during delivery. A network connection may disappear halfway through a request. A broker may restart while events are being processed.

These failures can leave distributed systems in an inconsistent state even when individual services appear healthy.

**LYAPUNOV** is being built to explore and verify these failure scenarios.

The project combines a **transactional event delivery engine** with an independent **fault-injection and verification system**. Instead of testing only the normal path, LYAPUNOV deliberately introduces failures and checks whether the system recovers while preserving its defined delivery guarantees.

The name comes from **Aleksandr Lyapunov**, whose work on stability examines how systems behave when they are disturbed.

---

## The Problem

A common backend design performs two independent operations:

```text
Application
     â”‚
     â”œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º PostgreSQL
     â”‚                    â”‚
     â”‚                 COMMIT âœ“
     â”‚
     â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â–º Kafka
                          â”‚
                       FAILURE âœ—
```

If the database update succeeds but the event publish fails, the database and downstream services can disagree.

This is commonly referred to as the **dual-write problem**.

LYAPUNOV uses the **Transactional Outbox Pattern** to make the database transaction the durable source of the event.

```mermaid
flowchart LR
    A[Application] --> B[(PostgreSQL)]
    B --> C[Outbox Events]
    C --> D[LYAPUNOV Event Engine]
    D --> E[Apache Kafka]
    E --> F[Downstream Services]
```

The application writes its business change and the corresponding outbox event in the same database transaction.

The LYAPUNOV engine then reads those persisted events and attempts to deliver them to Kafka.

---

## What LYAPUNOV Does

The project has three main parts.

### 1. Event Delivery Engine

The core Java service responsible for reading durable outbox events and publishing them to Kafka.

It is designed around:

* PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`
* Concurrent event processing
* Worker leases
* Retry and exponential backoff
* Per-aggregate ordering
* Kafka acknowledgements
* Dead-letter handling
* Crash recovery

The initial delivery model is **at-least-once delivery**.

That distinction matters.

A crash after Kafka accepts an event but before the database state is updated can result in the same event being published again.

LYAPUNOV treats this as a behavior to measure and verify rather than pretending that duplicates cannot happen.

---

### 2. Fault Injection Engine

The system will deliberately introduce failures while events are being processed.

Planned failure scenarios include:

* Process termination
* Kafka connectivity failures
* PostgreSQL connectivity failures
* Network interruptions
* Delayed responses
* Worker failures
* Poison or malformed events

The purpose of the fault injector is simple:

> **Break the system in controlled ways and observe what happens next.**

---

### 3. Independent Verification Engine

A separate consumer observes the resulting Kafka stream and verifies the behavior of the system.

It will check:

* Missing events
* Duplicate deliveries
* Per-aggregate ordering
* Dead-letter behavior
* Recovery time
* Throughput during failures
* Successful recovery after process crashes

The verifier is intentionally independent from the delivery engine.

The system should not simply say:

> "I delivered everything correctly."

The verifier should be able to examine the resulting stream and determine whether that statement is actually true.

---

# System Architecture

```mermaid
flowchart TB

    G[Workload Generator]

    DB[(PostgreSQL)]
    OUT[Outbox Events]

    ENGINE[LYAPUNOV Event Engine]

    KAFKA[(Apache Kafka)]

    VERIFY[Independent Invariant Verifier]

    REPORT[Experiment Report]

    CHAOS[Chaos / Fault Injector]

    G --> DB
    DB --> OUT
    OUT --> ENGINE
    ENGINE --> KAFKA
    KAFKA --> VERIFY
    VERIFY --> REPORT

    CHAOS -. Process failures .-> ENGINE
    CHAOS -. Network failures .-> ENGINE
    CHAOS -. Broker failures .-> KAFKA
    CHAOS -. Database failures .-> DB
```

---

## Event Lifecycle

A simplified event lifecycle looks like this:

```text
CREATED
   â”‚
   â–¼
PERSISTED
   â”‚
   â–¼
CLAIMED
   â”‚
   â–¼
PUBLISHED
   â”‚
   â–¼
ACKNOWLEDGED
   â”‚
   â–¼
COMPLETED
```

If the worker fails before completion:

```text
CLAIMED
   â”‚
   â”‚  process crash
   â–¼
LEASE EXPIRES
   â”‚
   â–¼
RE-CLAIMED
   â”‚
   â–¼
RETRIED
   â”‚
   â–¼
PUBLISHED
```

The important part is that the event remains durable in PostgreSQL until the delivery process has successfully completed its required acknowledgement flow.

---

# Reliability Model

LYAPUNOV is built around **at-least-once delivery**.

The system is not designed to make the unrealistic claim that an event can never be delivered twice.

Instead, the verification layer measures what happens during failure and recovery.

For an aggregate with monotonically increasing sequence numbers:

```text
Expected:

1 â†’ 2 â†’ 3 â†’ 4 â†’ 5 â†’ 6 â†’ 7 â†’ ...

```

The verifier checks for:

```text
Missing sequence
1 â†’ 2 â†’ 4 â†’ 5
        â†‘
      missing 3

Ordering violation
1 â†’ 2 â†’ 4 â†’ 3 â†’ 5
            â†‘
        out of order

Duplicate replay
1 â†’ 2 â†’ 3 â†’ 3 â†’ 4
            â†‘
        duplicate
```

Duplicates may occur during crash recovery under at-least-once delivery.

Missing events and ordering violations are treated as correctness failures for the relevant experiment.

---

# Core Technical Components

## PostgreSQL Outbox

The application writes the business mutation and event record within the same database transaction.

Conceptually:

```sql
BEGIN;

UPDATE business_table
SET ...

INSERT INTO outbox_events (...);

COMMIT;
```

The event is therefore persisted together with the business state change.

---

## Concurrent Polling

Multiple workers will use PostgreSQL row locking to safely process available events concurrently.

The planned polling mechanism uses:

```sql
SELECT ...
FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT ?;
```

`SKIP LOCKED` allows workers to avoid waiting on rows already claimed by another worker.

---

## Worker Leases

Workers will use time-bounded ownership of events or partitions.

If a worker disappears unexpectedly, its ownership can expire and another worker can recover the work.

This is important for handling failures such as:

```text
Worker A
   â”‚
   â”œâ”€â”€ claims work
   â”‚
   Xâ”€â”€ process crashes
   â”‚
   â–¼
Lease expires
   â”‚
   â–¼
Worker B
   â”‚
   â””â”€â”€ recovers work
```

---

## Kafka Delivery

The event engine will publish events to Kafka using explicit producer acknowledgement settings.

Per-aggregate ordering will be maintained by consistently routing events belonging to the same aggregate to the same Kafka partition.

For example:

```text
user-101 â†’ partition 2
user-205 â†’ partition 0
user-314 â†’ partition 1
user-101 â†’ partition 2
```

This allows the sequence of events belonging to one aggregate to remain ordered within that partition.

---

## Retry and Backoff

Transient failures should not immediately become permanent failures.

The engine will use retry attempts with exponential backoff and jitter.

Conceptually:

```text
Attempt 1
   â”‚
   â””â”€â”€ failure
        â”‚
        â–¼
      wait
        â”‚
Attempt 2
   â”‚
   â””â”€â”€ failure
        â”‚
        â–¼
      longer wait
        â”‚
Attempt 3
   â”‚
   â””â”€â”€ success
```

Persistent failures can eventually be routed to the Dead-Letter Queue.

---

# Chaos Testing

Normal tests answer:

> Does the system work?

LYAPUNOV's chaos experiments ask:

> What happens when the system stops working normally?

Planned experiments include:

### Process Failure

```text
Generate events
      â†“
Start relay
      â†“
Process events
      â†“
SIGKILL relay
      â†“
Recover
      â†“
Verify stream
```

### Network Failure

```text
Producer
   â”‚
   X
   â”‚
Kafka

Network connection interrupted
        â†“
Retry
        â†“
Recovery
        â†“
Verification
```

### Broker Failure

The experiment will temporarily make Kafka unavailable while events continue to accumulate in the durable outbox.

The engine should recover once the broker becomes available again.

### Database Failure

Database connectivity interruptions will be introduced to observe how the polling and recovery logic behaves.

---

# Verification

Every experiment should produce measurable results.

The verifier will compare:

```text
Expected Events
       â”‚
       â–¼
Database / Workload Record
       â”‚
       â”‚
       â–¼
Actual Kafka Stream
       â”‚
       â–¼
Invariant Checks
```

The main invariants are:

| Invariant        | What it checks                                         |
| ---------------- | ------------------------------------------------------ |
| Completeness     | No expected event disappears from the resulting stream |
| Ordering         | Events for the same aggregate remain in sequence       |
| Duplicate replay | Measures repeated deliveries during recovery           |
| DLQ isolation    | Poison events do not block unrelated events            |
| Recovery         | Measures how quickly normal processing resumes         |

---

# Experiment Report

Actual benchmark values will be generated by the project.

The final report will follow a format similar to:

```text
============================================================
                 LYAPUNOV EXPERIMENT REPORT
============================================================

Workload
------------------------------------------------------------
Events generated:          <measured>
Events committed:          <measured>
Events delivered:          <measured>

Invariant Verification
------------------------------------------------------------
Missing events:            <measured>
Ordering violations:      <measured>
Duplicate deliveries:     <measured>
DLQ events:                <measured>

Fault Injection
------------------------------------------------------------
Process failures:          <measured>
Network failures:          <measured>
Broker failures:           <measured>

Recovery
------------------------------------------------------------
Recovery time:             <measured>
Peak throughput:           <measured>
Fault-state throughput:    <measured>

============================================================
```

No benchmark number will be included in the documentation until it has actually been measured.

---

# Technology Stack

| Component           | Technology              |
| ------------------- | ----------------------- |
| Language            | Java 21                 |
| Database            | PostgreSQL 16+          |
| Message Broker      | Apache Kafka 3.7+       |
| Containers          | Docker / Docker Compose |
| Systems Tooling     | Linux                   |
| Build               | Maven Wrapper           |
| Unit Testing        | JUnit 5                 |
| Integration Testing | Testcontainers          |
| Version Control     | Git                     |

---

# Project Structure

The repository will evolve toward a structure similar to:

```text
lyapunov/
â”‚
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ main/
â”‚   â”‚   â””â”€â”€ java/
â”‚   â”‚       â””â”€â”€ com/
â”‚   â”‚           â””â”€â”€ lyapunov/
â”‚   â”‚
â”‚   â””â”€â”€ test/
â”‚       â””â”€â”€ java/
â”‚           â””â”€â”€ com/
â”‚               â””â”€â”€ lyapunov/
â”‚
â”œâ”€â”€ docker/
â”‚
â”œâ”€â”€ scripts/
â”‚   â”œâ”€â”€ chaos/
â”‚   â””â”€â”€ benchmark/
â”‚
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ architecture/
â”‚   â””â”€â”€ experiments/
â”‚
â”œâ”€â”€ pom.xml
â”œâ”€â”€ README.md
â””â”€â”€ .gitignore
```

Directories and components will be added as they become part of the implementation.

---

# Current Implementation

LYAPUNOV is under active development. The current implementation work focuses on
building the durable event foundation and independently checking event correctness.

### Implemented foundation

- Java project with Maven Wrapper.
- Docker Compose configuration for PostgreSQL and Apache Kafka.
- PostgreSQL outbox event model and repository.
- Event verification logic with unit tests.
- Aggregate-key collision protection in the verifier.

### Current engineering priorities

- Strengthen per-aggregate event ordering guarantees.
- Validate fault-injection experiments and their generated reports.
- Expand reliability testing and recovery scenarios.
- Verify database and broker integration in a working Docker environment.

### Reliability principle

A successful publish operation alone does not prove that an event stream is correct.
LYAPUNOV aims to verify completeness, duplicate delivery, and ordering independently.

Benchmark results will be documented only after they have been measured and validated.

---
# Development Roadmap

### Phase 1 â€” Foundation

* [x] Project concept and architecture
* [ ] Java 21 Maven project
* [ ] Maven Wrapper
* [ ] Docker Compose environment
* [ ] PostgreSQL setup
* [ ] Kafka setup

### Phase 2 â€” Event Engine

* [ ] Outbox schema
* [ ] Event persistence
* [ ] Basic polling
* [ ] Kafka producer
* [ ] Kafka acknowledgements
* [ ] Event completion handling

### Phase 3 â€” Reliability

* [ ] Concurrent workers
* [ ] `SKIP LOCKED`
* [ ] Worker leases
* [ ] Retry policy
* [ ] Exponential backoff
* [ ] Per-aggregate ordering
* [ ] Dead-Letter Queue

### Phase 4 â€” Verification

* [ ] Workload generator
* [ ] Independent Kafka consumer
* [ ] Sequence tracking
* [ ] Missing-event detection
* [ ] Duplicate detection
* [ ] Ordering verification
* [ ] Recovery-time measurement

### Phase 5 â€” Chaos Engineering

* [ ] Process crash injection
* [ ] Network failure injection
* [ ] Kafka failure scenarios
* [ ] PostgreSQL failure scenarios
* [ ] Poison-event experiments
* [ ] Automated experiment runs

### Phase 6 â€” Benchmarking

* [ ] Throughput benchmarks
* [ ] Latency measurements
* [ ] Recovery measurements
* [ ] Duplicate-replay analysis
* [ ] Reproducible experiment reports

---

# Engineering Goals

LYAPUNOV is being built around a few simple principles:

### Correctness before performance

A fast event system that loses or reorders data is not useful.

### Failures are test cases

Crashes, network interruptions, and broker failures are treated as part of the system's operating environment.

### Measure instead of assume

Reliability claims should come from reproducible experiments rather than assumptions.

### Keep verification independent

The component responsible for delivering events should not be the only component deciding whether delivery was correct.

### Start simple

The first version focuses on a well-defined PostgreSQL â†’ Kafka event pipeline before introducing unnecessary distributed complexity.

---

# Why This Project?

Most event-driven systems are evaluated on their happy path.

LYAPUNOV focuses on the harder question:

> **What happens to event correctness when the system is under failure?**

The project is an attempt to answer that question with an actual implementation, controlled failures, independent verification, and measurable results.

---

# Status

**Early development**

The repository currently contains the project foundation. Implementation, experiments, and benchmark results will be added incrementally.

---

## License 

License information will be added with the first open-source release.





---

## Current Implementation Status

LYAPUNOV is being developed incrementally, with correctness and recoverability as primary engineering goals.

The current foundation includes:

- Java and Maven Wrapper project setup.
- Docker Compose configuration for PostgreSQL and Apache Kafka.
- PostgreSQL outbox event persistence and repository components.
- Event verification logic with unit tests.
- Aggregate-key collision protection in the verifier.
- Initial fault-injection experiment reports.

### Current Priorities

1. Validate per-aggregate ordering behavior.
2. Improve the reliability of experiment generation and reporting.
3. Expand recovery and failure-injection tests.
4. Validate database and Kafka integration with Docker running.

Implementation status and benchmark claims should reflect verified code and test results, not planned functionality.
