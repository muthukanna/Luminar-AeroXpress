# ✈️ Luminar AeroXpress

**Luminar AeroXpress** is a full-stack airline management platform built to simulate real-world aviation workflows. It streamlines flight scheduling, ticket booking, passenger services, and operational management with a modern, scalable architecture.

Under the hood it is a production-style, event-driven system built with **Java 21, Spring Boot, Kafka, Redis and PostgreSQL**. It is designed around one hard problem: booking a seat safely when many users compete for it, payments can fail, and services can go down.

> Focus: backend architecture and distributed-systems patterns (Saga, Outbox, idempotency, seat locking, resilience, observability), not UI.

---

## Table of contents

1. [Architecture](#architecture)
2. [Microservices](#microservices)
3. [Booking flow](#booking-flow)
4. [Key design decisions](#key-design-decisions)
5. [Kafka events](#kafka-events)
6. [Tech stack](#tech-stack)
7. [Repository layout](#repository-layout)
8. [Getting started](#getting-started)
9. [API overview](#api-overview)
10. [Resilience and observability](#resilience-and-observability)
11. [Testing](#testing)
12. [Roadmap](#roadmap)
13. [Interview talking points](#interview-talking-points)

---

## Architecture

```
                    React / Angular
                          │
                          ▼
               ┌─────────────────────┐
               │     API Gateway     │
               │ JWT · Rate limit ·  │
               │ CORS · Routing      │
               └──────────┬──────────┘
                          │
   ┌───────────┬──────────┼───────────┬────────────┐
   ▼           ▼          ▼           ▼            ▼
Identity    Flight     Booking     Payment      Ticket
Service     Service    Service     Service      Service
                          │           │            │
                          └───────────┼────────────┘
                                      ▼
                                    Kafka
                                      │
                       ┌──────────────┴──────────────┐
                       ▼                             ▼
                 Notification                   Operations
                   Service                       Service

        PostgreSQL (per service) · Redis · Kafka
```

Each service owns its own database (or schema). No service reads another service's tables.

---

## Microservices

Seven services plus an API gateway.

| # | Service | Port | Storage | Responsibility |
|---|---------|------|---------|----------------|
| 0 | `api-gateway` | 8080 | Redis | Routing, JWT validation, rate limiting, CORS, correlation IDs |
| 1 | `identity-service` | 8081 | PostgreSQL | Registration, login, JWT, refresh tokens, roles, passenger profiles |
| 2 | `flight-service` | 8082 | PostgreSQL, Redis | Airports, aircraft, seat maps, flights, schedules, search |
| 3 | `booking-service` | 8083 | PostgreSQL, Redis | PNR, seat inventory and locking, booking state machine, Saga orchestrator |
| 4 | `payment-service` | 8084 | PostgreSQL | Mock payment gateway, idempotency, refunds |
| 5 | `ticket-service` | 8085 | PostgreSQL | Ticket generation, PDF, online check-in, boarding pass |
| 6 | `notification-service` | 8086 | PostgreSQL | Email/SMS driven by Kafka events |
| 7 | `operations-service` | 8087 | PostgreSQL | Flight status, gates, baggage tracking, loyalty miles, admin views |

### 1. identity-service
- `POST /auth/register`, `/auth/login`, `/auth/refresh`, `/auth/logout`
- Roles: `PASSENGER`, `ADMIN`
- BCrypt password hashing, JWT signing, refresh token rotation
- Owns passenger profile data (name, contact, passport)

### 2. flight-service
- Flight search by origin, destination, date, passenger count
- Admin CRUD for flights, aircraft, and seat configuration
- Cached search results in Redis
- Publishes `FlightDelayed` and `FlightCancelled`

### 3. booking-service (core)
- Creates bookings and generates PNRs
- Seat inventory with optimistic locking and a unique constraint
- Temporary seat holds in Redis with a TTL
- Orchestrates the booking Saga and compensates on failure
- Outbox table for reliable event publishing
- Scheduled job that expires stale holds

### 4. payment-service
- `POST /payments` with an `Idempotency-Key` header
- Mock provider that can return success or failure (to exercise compensation paths)
- Refunds
- Resilience4j circuit breaker, timeout, and retry on provider calls

### 5. ticket-service
- Generates ticket numbers and PDFs on `BookingConfirmed`
- Online check-in and boarding pass
- Idempotent Kafka consumer

### 6. notification-service
- Pure Kafka consumer
- Booking confirmation, payment receipt, ticket, cancellation, delay emails
- Retry topic and dead-letter topic

### 7. operations-service
- Flight status lifecycle
- Baggage tracking
- Loyalty miles calculation
- Admin reporting endpoints

---

## Booking flow

```
Passenger → Gateway → Flight search
                           │
                           ▼
                  Select seat (Booking)
                           │
             Redis SET NX EX  → seat HELD
                           │
               Booking = PAYMENT_PENDING
                           │
                           ▼
                        Payment
                     ┌─────┴─────┐
                  SUCCESS      FAILED
                     │            │
             Booking CONFIRMED   Cancel booking
                     │           Release seat
            BookingConfirmed      (compensation)
               event (Kafka)
                     │
        ┌────────────┼─────────────┐
        ▼            ▼             ▼
     Ticket    Notification   Loyalty miles
```

### Booking states

`INITIATED → HELD → PAYMENT_PENDING → CONFIRMED`
with `CANCELLED` and `EXPIRED` as terminal failure states.

### Seat states

`AVAILABLE → HELD → BOOKED` (or `HELD → AVAILABLE` on failure or expiry), plus `BLOCKED` for admin use.

---

## Key design decisions

| Problem | Solution |
|---------|----------|
| Two users pick the same seat | Atomic Redis `SET NX EX` lock, plus optimistic locking (`@Version`) and `UNIQUE(flight_id, seat_no)` in the database |
| Abandoned bookings hold seats forever | 10-minute TTL on the lock, plus a scheduled cleanup job |
| Transaction spanning several services | Saga with compensating actions (orchestrated by Booking) |
| DB commit succeeds but Kafka publish fails | Outbox pattern: business row and event row committed in one transaction, a publisher relays to Kafka |
| Duplicate Kafka delivery | Consumers are idempotent using an event ID and a `processed_events` table |
| Payment retried after a timeout | `Idempotency-Key` stored with the result; repeat calls return the original result |
| Payment service is down | Timeout, limited retry, circuit breaker; booking stays `PAYMENT_PENDING` and is reconciled later |
| Redis is a cache, not truth | The database is always the source of truth for bookings and seats |
| Following one request across services | OpenTelemetry trace context propagated over HTTP and Kafka headers, viewed in Jaeger |

---

## Kafka events

| Event | Producer | Consumers |
|-------|----------|-----------|
| `BookingCreated` | booking | operations |
| `BookingConfirmed` | booking | ticket, notification, operations |
| `BookingCancelled` | booking | notification, payment (refund) |
| `SeatHeld` / `SeatReleased` | booking | flight (cache invalidation) |
| `PaymentCompleted` | payment | booking |
| `PaymentFailed` | payment | booking |
| `PaymentRefunded` | payment | notification |
| `TicketGenerated` | ticket | notification |
| `CheckInCompleted` | ticket | notification, operations |
| `FlightDelayed` / `FlightCancelled` | flight, operations | booking, notification |
| `BaggageCheckedIn` / `BaggageArrived` | operations | notification |

Every event carries `eventId`, `eventType`, `occurredAt`, `traceId`, and a versioned payload.

Failed messages are retried with backoff and then routed to a dead-letter topic (`<topic>.DLT`).

---

## Tech stack

**Backend:** Java 21, Spring Boot 3, Spring Cloud Gateway, Spring Security, Spring Data JPA
**Messaging:** Apache Kafka
**Data:** PostgreSQL, Redis
**Resilience:** Resilience4j (circuit breaker, retry, timeout, bulkhead)
**Observability:** Spring Boot Actuator, Micrometer, Prometheus, Grafana, OpenTelemetry, Jaeger
**API docs:** springdoc-openapi (Swagger UI)
**Testing:** JUnit 5, Mockito, Testcontainers, WireMock, REST Assured
**DevOps:** Docker, Docker Compose, Kubernetes, GitHub Actions or Jenkins, Terraform (optional AWS deployment)

---

## Repository layout

```
airline-platform/
├── api-gateway/
├── identity-service/
├── flight-service/
├── booking-service/
├── payment-service/
├── ticket-service/
├── notification-service/
├── operations-service/
├── common-events/          # shared event DTOs and schemas
├── docker-compose.yml
├── k8s/                    # Deployments, Services, ConfigMaps, HPA, Ingress
├── terraform/              # optional AWS infrastructure
└── README.md
```

---

## Getting started

### Prerequisites
- JDK 21
- Maven 3.9+
- Docker and Docker Compose

### Run infrastructure

```bash
docker compose up -d postgres redis kafka jaeger prometheus grafana mailpit
```

### Build

```bash
mvn clean install
```

### Run services

```bash
# from each service directory
mvn spring-boot:run
```

Or run everything containerized:

```bash
docker compose up --build
```

### Local endpoints

| Tool | URL |
|------|-----|
| API Gateway | http://localhost:8080 |
| Swagger UI (per service) | http://localhost:808x/swagger-ui.html |
| Jaeger | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Mailpit (test emails) | http://localhost:8025 |

---

## API overview

All calls go through the gateway.

```
POST   /api/auth/register
POST   /api/auth/login
POST   /api/auth/refresh-token

GET    /api/flights/search?from=BLR&to=DEL&date=2026-10-15&pax=2
GET    /api/flights/{id}

POST   /api/bookings
GET    /api/bookings/{pnr}
DELETE /api/bookings/{pnr}

POST   /api/payments                 (Idempotency-Key header required)
POST   /api/payments/{id}/refund

POST   /api/checkin/{pnr}
GET    /api/boarding-pass/{pnr}

POST   /api/admin/flights            (ROLE_ADMIN)
PUT    /api/admin/flights/{id}       (ROLE_ADMIN)
```

### Example: create a booking

```http
POST /api/bookings
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "flightId": 101,
  "passengerId": 501,
  "seat": "12A"
}
```

---

## Resilience and observability

- **Circuit breaker, retry, timeout** on Booking → Payment calls
- **Rate limiting** at the gateway, backed by Redis
- **Correlation/trace IDs** on every request, propagated through Kafka headers
- **Structured JSON logs** with `traceId`, `service`, and `bookingId`
- **Metrics:** request rate, error rate, P95 latency, JVM, DB pool, Kafka consumer lag
- **Tracing:** full request path visible in Jaeger

---

## Testing

- **Unit tests:** JUnit 5 and Mockito for domain logic and state machines
- **Integration tests:** Testcontainers for PostgreSQL, Kafka, and Redis
- **Contract/stub tests:** WireMock for inter-service HTTP calls
- **API tests:** REST Assured
- **Concurrency test:** many threads booking the same seat, asserting exactly one succeeds
- **Failure tests:** payment failure triggers compensation; duplicate Kafka message is processed once

---

## Roadmap

- [ ] **Phase 1:** identity-service, flight-service, Docker Compose with PostgreSQL
- [ ] **Phase 2:** booking-service, seat inventory and locking
- [ ] **Phase 3:** payment-service, idempotency and refunds
- [ ] **Phase 4:** Kafka events, consumers, retry and DLT
- [ ] **Phase 5:** Saga orchestration and Outbox pattern
- [ ] **Phase 6:** API gateway, JWT, rate limiting, circuit breaker, Redis
- [ ] **Phase 7:** ticket, notification, and operations services
- [ ] **Phase 8:** Prometheus, Grafana, OpenTelemetry, Jaeger
- [ ] **Phase 9:** Kubernetes manifests and CI/CD
- [ ] **Phase 10 (optional):** AWS deployment with Terraform (EKS, RDS, ElastiCache, MSK, ECR)

---

## Interview talking points

- Why an API gateway? A single entry point that handles cross-cutting concerns and hides internal topology.
- Why Kafka instead of REST for post-booking work? Several services react to one event, with durability, retries, and loose coupling.
- How is double booking prevented? An atomic Redis lock for the hold, plus optimistic locking and a unique constraint as the database safety net.
- How is payment failure handled? A Saga compensating step cancels the booking and releases the seat.
- Why the Outbox pattern? It keeps the database and Kafka consistent without distributed transactions.
- How are duplicate messages handled? Idempotent consumers keyed on event ID.
- How is payment made idempotent? An idempotency key maps to the stored result.
- What if the payment service is down? Timeout, retry, and circuit breaker, with the booking held in `PAYMENT_PENDING` for later reconciliation.
- How do you trace a request? OpenTelemetry context propagation and Jaeger.
- How do you scale booking? Stateless pods, shared state in the DB and Redis, Kubernetes HPA.

---

## License

MIT
