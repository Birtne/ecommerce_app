# NovaMart

<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&height=250&color=0:FB923C,50:F97316,100:EA580C&text=NovaMart&fontSize=72&fontColor=ffffff&animation=twinkling&desc=%E7%94%B5%E5%95%86%E6%A9%99%20E-commerce%20Platform&descAlignY=70" alt="NovaMart Banner" />
</p>

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=24&pause=900&color=F97316&center=true&vCenter=true&width=1000&lines=%E8%B4%AD%E7%89%A9%E8%BD%A6+%E2%9E%9C+%E4%B8%8B%E5%8D%95+%E2%9E%9C+Outbox+%E2%9E%9C+NATS+%E2%9E%9C+%E9%87%8D%E6%94%BE;%E4%BA%A4%E6%98%93%E9%97%AD%E7%8E%AF+%7C+%E5%8F%AF%E8%A7%82%E6%B5%8B+%7C+%E5%8F%AF%E5%9B%9E%E5%BD%92" alt="typing animation" />
</p>

<p align="center">
  <img alt="Go" src="https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white">
  <img alt="React" src="https://img.shields.io/badge/React-20232A?logo=react">
  <img alt="TS" src="https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white">
  <img alt="Postgres" src="https://img.shields.io/badge/PostgreSQL-336791?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white">
  <img alt="NATS" src="https://img.shields.io/badge/NATS-000000">
</p>

```mermaid
flowchart LR
  A[👤 用户] --> B[🛍️ 前端商城]
  B --> C[⚙️ Go API]
  C --> D[(🐘 PostgreSQL)]
  C --> E[(⚡ Redis)]
  C --> F[📤 Outbox]
  F --> G[📡 NATS]
  F --> H[🔁 Replay Job]
  H --> I[📋 Audit Log]
```

```mermaid
sequenceDiagram
  participant U as 👤
  participant FE as 🛍️
  participant API as ⚙️
  participant DB as 🐘
  participant MQ as 📡

  U->>FE: 下单
  FE->>API: POST /orders + Idempotency-Key
  API->>DB: 订单事务写入
  API-->>FE: order_id
  API->>MQ: ecom.order.created
```

<p align="center">
  <img src="https://github-readme-stats.vercel.app/api/pin/?username=Birtne&repo=ecommerce_app&theme=solarized-light&border_color=FB923C&title_color=EA580C&icon_color=F97316" alt="repo card" />
</p>

```bash
cd ecommerce_app
make up && make backend && make frontend
```

```text
http://localhost:5173
http://localhost:8080/health
http://localhost:8080/api/v1
```
