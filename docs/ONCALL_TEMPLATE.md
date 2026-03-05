# On-Call Handover Template (iter-08)

## Shift Info
- Primary on-call:
- Secondary on-call:
- Shift window:
- Escalation manager:

## Current Health Snapshot
- Order success rate (5m):
- Outbox oldest pending age:
- Dead-letter backlog:
- Replay jobs in running/failed:

## Open Alerts
| Alert | Severity | Since | Owner | Status |
|---|---|---|---|---|

## Active Incidents
- Incident ID:
- Impact:
- Mitigation in progress:
- Next checkpoint:

## Risk Notes for Next Shift
- 

## Mandatory Checks at Shift Start
1. Grafana dashboard `Ecommerce Backend SLO` loads and data fresh.
2. Alertmanager routes and receivers reachable.
3. No stuck replay jobs in `running` beyond lease window.
4. Audit log export endpoint operational for incident forensics.

## Drill Verification Record
- Drill date/time:
- Trigger type: `warning` / `critical`
- Route test command result:
- Grafana panel checked:
- Recovery verification timestamp:
- Automated script output (`scripts/monitoring_drill_check.sh`):
