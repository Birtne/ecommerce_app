# NovaMart - Centralized E-commerce Platform

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&pause=900&center=true&vCenter=true&width=900&lines=NovaMart%3A+Taobao-style+Centralized+Commerce+Platform;Go+%2B+Hertz+%2B+PostgreSQL+%2B+Redis+%2B+NATS;Idempotency%2C+Outbox%2C+Replay+Jobs%2C+Audit%2C+Metrics" alt="typing animation" />
</p>

<p align="center">
  <img alt="Go" src="https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white">
  <img alt="React" src="https://img.shields.io/badge/React-18-20232A?logo=react">
  <img alt="TypeScript" src="https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="NATS" src="https://img.shields.io/badge/NATS-Messaging-27AAE1">
</p>

Taobao-style centralized e-commerce project, with completed core trade loop and reliability-focused engineering safeguards.

## Highlights

- User registration/login with token sessions
- Product browsing with Redis cache
- Cart add/remove/query
- Order placement with strict idempotency key
- Order history list/detail with filters + cursor pagination
- Outbox event publishing + replay jobs + failed retry
- Admin audit logs + CSV export
- Health and Prometheus metrics endpoints
- CI quality gates and nightly trend checks

## Tech Stack

- Frontend: React + TypeScript + Vite
- Backend: Go + CloudWeGo/Hertz
- Data: PostgreSQL + Redis
- Messaging: NATS
- Infra: Docker Compose

## Project Structure

```text
ecommerce_app/
├── backend/      # Go APIs, domain, repository, services, migrations
├── frontend/     # React storefront
├── infra/        # Docker Compose + monitoring config
├── scripts/      # CI/nightly health and trend checks
└── docs/         # API contract, architecture, roadmap, runbooks
```

## Quick Start

1. Start infra:

```bash
cd ecommerce_app
make up
```

2. Start backend:

```bash
cd ecommerce_app
make backend
```

3. Start frontend:

```bash
cd ecommerce_app
make frontend
```

4. Open:

- Frontend: `http://localhost:5173`
- Backend Health: `http://localhost:8080/health`
- API Base: `http://localhost:8080/api/v1`

## API Example

```bash
curl http://localhost:8080/health
curl http://localhost:8080/api/v1/products
```

Place order with idempotency key:

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Authorization: Bearer <token>" \
  -H "Idempotency-Key: demo-001" \
  -H "Content-Type: application/json" \
  -d '{"address":"Shanghai Pudong Road 1"}'
```

## Quality Gates

```bash
cd ecommerce_app
make lint
make test
make integration
make build
make scope-check
make frontend-drift-check
```

## Monitoring and Reliability

- Outbox health: `GET /health/outbox`
- Metrics: `GET /metrics`
- Replay threshold config: `docs/REPLAY_SOAK_THRESHOLDS.json`
- Cross-job trend config: `docs/CROSS_JOB_SCORING_CONFIG.json`
- Alerting runbook: `docs/ALERTING_RUNBOOK.md`

## Docs

- Architecture: `docs/ARCHITECTURE.md`
- API contract: `docs/API_CONTRACT.md`
- Roadmap: `docs/ROADMAP.md`
- Iteration notes: `docs/ITERATION_NOTES.md`

## Roadmap (Current Focus)

- Consolidate reliability and replay pipeline guardrails
- Keep CI/nightly regression stable
- Expand from trade loop into payment/fulfillment modules
