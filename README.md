# NovaMart 电商平台

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&height=220&color=0:1D4ED8,50:059669,100:F97316&text=NovaMart&fontSize=64&fontColor=ffffff&animation=fadeIn&desc=大型中心化电商项目%20%7C%20交易闭环%20+%20可靠性护栏&descAlignY=68" alt="NovaMart Banner" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&pause=850&center=true&vCenter=true&width=980&lines=Go+%2B+React+%2B+PostgreSQL+%2B+Redis+%2B+NATS;订单幂等+Outbox+重放任务+审计日志+Prometheus;可跑通交易链路+可观测+可回放+可回归" alt="typing animation" />
</p>

<p align="center">
  <img alt="Go" src="https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white">
  <img alt="React" src="https://img.shields.io/badge/React-18-20232A?logo=react">
  <img alt="TypeScript" src="https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="NATS" src="https://img.shields.io/badge/NATS-Messaging-27AAE1">
  <img alt="Status" src="https://img.shields.io/badge/Repo-Public-brightgreen">
</p>

## 视觉总览

| 交易链路 | 可靠性 | 可观测 | 后续拓展 |
|---|---|---|---|
| 注册/登录 | 下单幂等 | `/metrics` | 支付 |
| 商品/购物车 | Outbox | `/health/outbox` | 履约 |
| 下单/订单详情 | Replay 重放 | 审计日志导出 | 售后 |

## 交易时序图

```mermaid
sequenceDiagram
  participant U as 用户
  participant FE as 前端
  participant BE as 后端 API
  participant DB as PostgreSQL
  participant OB as Outbox
  participant MQ as NATS

  U->>FE: 选择商品并下单
  FE->>BE: POST /orders + Idempotency-Key
  BE->>DB: 订单写入 + 购物车清空(同事务)
  BE->>OB: 写入待发布事件
  BE-->>FE: 返回 order_id
  OB->>MQ: 发布 ecom.order.created
  MQ-->>BE: 下游消费/回放
```

## 架构拓扑图

```mermaid
flowchart TB
  subgraph Client
    FE[React Storefront]
  end

  subgraph Server[Go Hertz Backend]
    API[HTTP API]
    SVC[Domain Services]
    REP[Repository]
  end

  subgraph Data
    PG[(PostgreSQL)]
    RD[(Redis)]
    NA[(NATS)]
  end

  subgraph Ops
    MET[/metrics/]
    HL[/health/outbox/]
    CI[CI + Nightly Gates]
  end

  FE --> API
  API --> SVC --> REP
  REP --> PG
  SVC --> RD
  SVC --> NA
  API --> MET
  API --> HL
  CI --> API
```

## 能力看板

```text
[交易闭环]
注册登录  ██████████ 100%
商品浏览  ██████████ 100%
购物车    ██████████ 100%
订单下单  ██████████ 100%

[可靠性]
幂等键保护        ██████████ 100%
Outbox 发布       ██████████ 100%
失败重放/重试      ██████████ 100%
跨任务趋势门禁      ████████░░  80%

[业务扩展]
支付             ███░░░░░░░  30%
履约物流          ██░░░░░░░░  20%
售后             ██░░░░░░░░  20%
```

## 快速启动（3 步）

```bash
cd ecommerce_app
make up
make backend
make frontend
```

访问地址：
- 前端：`http://localhost:5173`
- 健康检查：`http://localhost:8080/health`
- API 基地址：`http://localhost:8080/api/v1`

## 常用操作

```bash
# 质量门禁
make lint
make test
make integration
make build
make scope-check
make frontend-drift-check
```

## 路线图（形象版）

```mermaid
gantt
  title NovaMart 里程碑
  dateFormat  YYYY-MM-DD
  axisFormat  %m/%d

  section 基础交易
  用户/商品/购物车/订单闭环 :done, m1, 2026-01-01, 45d

  section 可靠性
  幂等 + Outbox + Replay    :done, m2, 2026-02-01, 40d
  CI/Nightly 趋势门禁        :active, m3, 2026-03-01, 35d

  section 业务拓展
  支付与履约                 :m4, 2026-04-01, 60d
  售后与运营                 :m5, 2026-06-15, 60d
```

## 目录

```text
ecommerce_app/
├── backend/   # API + 领域服务 + 存储 + 迁移
├── frontend/  # React 商城前端
├── infra/     # docker-compose + 监控配置
├── scripts/   # CI / Nightly 自动化脚本
└── docs/      # 架构、接口、路线图、运行手册
```

## 文档入口

- 架构设计：`docs/ARCHITECTURE.md`
- API 合同：`docs/API_CONTRACT.md`
- 路线图：`docs/ROADMAP.md`
- 迭代记录：`docs/ITERATION_NOTES.md`
