# NovaMart 电商平台

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&pause=900&center=true&vCenter=true&width=900&lines=NovaMart+%7C+大型中心化电商项目;交易闭环+可靠性护栏+可观测性;Go+%2B+React+%2B+PostgreSQL+%2B+Redis+%2B+NATS" alt="typing animation" />
</p>

<p align="center">
  <img alt="Go" src="https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white">
  <img alt="React" src="https://img.shields.io/badge/React-18-20232A?logo=react">
  <img alt="TS" src="https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="NATS" src="https://img.shields.io/badge/NATS-Messaging-27AAE1">
</p>

## 一眼看懂

| 模块 | 状态 |
|---|---|
| 用户注册/登录 | ✅ |
| 商品列表 + 缓存 | ✅ |
| 购物车 | ✅ |
| 下单幂等 | ✅ |
| 订单列表/详情/筛选 | ✅ |
| Outbox + 重放任务 | ✅ |
| 审计日志 + 导出 | ✅ |
| Prometheus 指标 | ✅ |
| 支付/履约/售后 | ⏳ |

## 业务流（图）

```mermaid
flowchart LR
  A[用户登录] --> B[浏览商品]
  B --> C[加入购物车]
  C --> D[提交订单 Idempotency-Key]
  D --> E[(PostgreSQL)]
  E --> F[Outbox 事件]
  F --> G[NATS]
  F --> H[重放任务/失败重试]
  H --> I[审计日志]
```

## 架构图（图）

```mermaid
graph TD
  UI[Frontend React] --> API[Backend Go Hertz]
  API --> PG[(PostgreSQL)]
  API --> R[(Redis)]
  API --> N[NATS]
  API --> M[/metrics]
  API --> O[/health/outbox]
```

## 3 步启动

```bash
cd ecommerce_app
make up
make backend
make frontend
```

访问地址：
- 前端：`http://localhost:5173`
- 健康检查：`http://localhost:8080/health`
- API：`http://localhost:8080/api/v1`

## 常用命令

```bash
# 质量门禁
make lint
make test
make integration
make build
make scope-check
make frontend-drift-check
```

## 目录地图

```text
ecommerce_app/
├── backend/   # API + 服务 + 存储 + 迁移
├── frontend/  # React 商城前端
├── infra/     # docker-compose + 监控配置
├── scripts/   # CI / Nightly 脚本
└── docs/      # 架构、接口、路线图、运行手册
```

## 文档入口

- 架构：`docs/ARCHITECTURE.md`
- 接口：`docs/API_CONTRACT.md`
- 路线图：`docs/ROADMAP.md`
- 迭代记录：`docs/ITERATION_NOTES.md`
