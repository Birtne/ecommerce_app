.PHONY: up down backend frontend test integration build lint ci scope-check frontend-drift-check

GO_ENV=GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.org

up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down

backend:
	cd backend && $(GO_ENV) go run ./cmd/server

frontend:
	cd frontend && npm run dev

test:
	cd backend && $(GO_ENV) go test ./...
	cd frontend && npm run build

integration:
	docker compose -f infra/docker-compose.yml up -d
	cd backend && $(GO_ENV) go test -tags=integration ./integration -v

build:
	cd backend && $(GO_ENV) go build ./cmd/server
	cd frontend && npm run build

lint:
	cd backend && test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './_disabled/*' -not -path './_legacy/*' -not -path './third_party/*'))"
	cd backend && $(GO_ENV) go vet ./...
	cd frontend && npm run build

scope-check:
	./scripts/git_scope_guard.sh
	./scripts/frontend_drift_guard.sh

frontend-drift-check:
	./scripts/frontend_drift_guard.sh

ci: scope-check lint test build
