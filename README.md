# orderflow — an event-driven order pipeline on Kafka

A small but realistic Kafka demo in **Node.js + TypeScript** with a **React** front end.
Orders flow through four services that talk only through Kafka topics, with the
reliability patterns you need in real systems: keyed partitioning, consumer groups,
retries with exponential backoff, dead-letter queues, DLQ replay and idempotent
consumers. Inventory is real state in **Postgres** (transactional outbox, compacted
changelog topic), and a gateway streams what happens in Kafka to the browser over
**Server-Sent Events**. The APIs are protected with **OAuth2/OIDC** (Keycloak, JWTs
checked against its JWKS; the React app signs in with the code flow + PKCE), and
runtime behaviour, in the services and in the UI, is controlled by **feature flags**
(LaunchDarkly, with a local fallback that needs no account).

```bash
docker compose up -d   # Kafka (KRaft) + Kafka UI + Keycloak + Postgres
npm run dev            # installs dependencies on first run, then starts the backend services,
                       # the real-time gateway and the React app
```

Open **<http://localhost:5173>**, sign in as `alice` / `alice` (or `bob` / `bob`), and
watch stock and orders update live as you place orders. See [The web app](#the-web-app).

Or use the API directly. Get a token and create an order:

```bash
TOKEN=$(curl -s -d grant_type=password -d client_id=orderflow-cli -d username=alice -d password=alice \
  localhost:8081/realms/orderflow/protocol/openid-connect/token | node -pe 'JSON.parse(require("fs").readFileSync(0)).access_token')

curl -s -X POST localhost:3000/orders -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"customerId":"alice","customerTier":"gold","country":"DE","items":[{"sku":"SKU-KEYBOARD","quantity":1,"unitPrice":79.99}]}'
```

Or just run `npm run load -- --wait`: the load generator gets its own token.

| UI       | URL                     | Login                                     |
| -------- | ----------------------- | ----------------------------------------- |
| Web app  | <http://localhost:5173> | `alice`/`alice`, `bob`/`bob`              |
| Kafka UI | <http://localhost:8080> | –                                         |
| Keycloak | <http://localhost:8081> | `admin`/`admin`                           |
| Postgres | `localhost:5432`        | `orderflow` (user, password and database) |

For a guided tour of happy path, outage, poison messages, DLQ replay, duplicates,
authentication, the live dashboard and feature flags, see
**[docs/demo-script.md](docs/demo-script.md)**.

## Documentation

| Page                                             | Read it to…                                                                                                                  |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| **[docs/architecture.md](docs/architecture.md)** | See the whole system in nine diagrams: components, Kafka topology, order flow, live path, security …                         |
| **[docs/demo-guide.md](docs/demo-guide.md)**     | Present the project: the pitch, demo tracks (15/30/60 min), every concept with where it lives in the code and how to show it |
| [docs/demo-script.md](docs/demo-script.md)       | Run the demo step by step: ten scenarios with exact commands                                                                 |
| [docs/ld-flags.md](docs/ld-flags.md)             | Understand and set up feature flags, including LaunchDarkly                                                                  |

## Architecture

![OrderFlow architecture: browser, services, Kafka, Postgres and Keycloak](docs/images/orderflow-architecture.png)

The whole system at a glance. Solid arrows carry data, dotted arrows are sign-in, key
distribution and configuration. The four backend services never call each other: they only
exchange events through Kafka. More views (topics and consumer groups, one order end to end,
the live path to the browser, inventory internals, what every consumer does, security, feature
flags, code structure) are in **[docs/architecture.md](docs/architecture.md)**.

```mermaid
flowchart TB
    subgraph browser["Browser"]
        web["<b>React app</b><br/>Vite, TypeScript<br/>TanStack Query, oidc-client-ts"]
    end

    proxy["<b>Vite dev server</b>  :5173<br/>/api/* proxy, so no CORS"]

    subgraph apis["HTTP services (verify the user's JWT)"]
        order["<b>order-service</b>  :3000<br/>REST API + order saga"]
        inventory["<b>inventory-service</b>  :3001<br/>stock API + outbox relay"]
        gateway["<b>gateway-service</b>  :3002<br/>stock read model<br/>SSE stream + UI flags"]
    end

    subgraph workers["Worker services (no HTTP)"]
        payment["<b>payment-service</b><br/>charges orders"]
        notification["<b>notification-service</b><br/>logs notifications"]
    end

    subgraph tools["CLI tools"]
        load["<b>load-generator</b><br/>npm run load"]
        replay["<b>dlq-replay</b><br/>npm run dlq:replay"]
    end

    kafka[("<b>Kafka</b>  :9092<br/>the event backbone")]
    pg[("<b>Postgres</b>  :5432<br/>stock, reservations, outbox")]
    keycloak{{"<b>Keycloak</b>  :8081<br/>OAuth2 / OIDC"}}
    flagsrc[/"Feature flags<br/>feature-flags.json or LaunchDarkly"/]

    web -- "HTTP + SSE" --> proxy
    proxy -- "/api/order-service<br/>/api/inventory-service<br/>/api/gateway-service" --> apis
    web -. "sign in<br/>(code flow + PKCE)" .-> keycloak
    keycloak -. "public keys (JWKS)" .-> apis

    apis <-- "publish and consume events" --> kafka
    workers <-- "publish and consume events" --> kafka
    inventory <-- "SQL transactions" --> pg

    load -- "REST + client credentials" --> order
    load -- "poison messages" --> kafka
    replay <-- "dead letters" --> kafka

    flagsrc -. "flags" .-> apis
    flagsrc -. "flags" .-> workers

    classDef external fill:#e8f0fe,stroke:#4a6fa5,color:#222
    classDef store fill:#fff4dd,stroke:#d4a017,color:#222
    class keycloak,flagsrc external
    class kafka,pg store
```

### The event flow in detail

```mermaid
flowchart LR
    browser(["React app<br/>(browser)"])
    client([HTTP client /<br/>load-generator])
    order["<b>order-service</b><br/>Fastify API + order saga"]
    payment["<b>payment-service</b><br/>chaos + fraud check via flags"]
    inventory["<b>inventory-service</b><br/>stock API + outbox"]
    pg[("Postgres<br/>stock, reservations, outbox")]
    notification["<b>notification-service</b><br/>logs notifications"]
    gateway["<b>gateway-service</b><br/>stock read model + SSE"]
    replayTool([dlq-replay])
    keycloak{{"Keycloak<br/>OAuth2 / OIDC"}}
    flags{{"LaunchDarkly or<br/>feature-flags.json"}}

    oc[["orders.created"]]
    pay[["payments.completed<br/>payments.failed"]]
    inv[["inventory.reserved<br/>inventory.rejected<br/>inventory.released"]]
    stock[["inventory.stock-levels (compacted)<br/>inventory.stock-low"]]
    dlq[["orders.created.dlq"]]

    client -- "POST /orders<br/>GET /orders/:id" --> order
    browser -- "POST /orders" --> order
    browser -- "adjust stock" --> inventory
    gateway -- "SSE: stock, feed, flags" --> browser
    order -- "publish<br/>key = orderId" --> oc
    oc --> payment
    oc --> inventory
    payment --> pay
    inventory --> inv
    inventory <--> pg
    inventory -- "via outbox<br/>key = SKU" --> stock
    pay --> notification
    inv --> notification
    pay -- "PENDING → CONFIRMED / CANCELLED" --> order
    inv --> order
    pay -. "payment failed:<br/>release stock" .-> inventory
    oc --> gateway
    pay --> gateway
    inv --> gateway
    stock --> gateway
    payment -.-> dlq
    inventory -. "poison message or<br/>retries exhausted" .-> dlq
    dlq -.-> replayTool
    replayTool -. "republish original<br/>key, value, headers" .-> oc
    keycloak -. "JWT" .-> client
    keycloak -. "login (PKCE)" .-> browser
    keycloak -. "JWKS" .-> order
    flags -. "flags" .-> payment
    flags -. "UI flags per user" .-> gateway

    classDef topic fill:#fff4dd,stroke:#d4a017,color:#333
    classDef dead fill:#fde2e2,stroke:#c0392b,color:#333
    classDef ext fill:#e8f0fe,stroke:#4a6fa5,color:#333
    class oc,pay,inv,stock topic
    class dlq dead
    class keycloak,flags,pg ext
```

Topics are yellow, the dead-letter queue is red, and external systems are blue.
Every service reads feature flags (only the payment and gateway edges are drawn).
Each service has its own consumer group (named after the service; the gateway adds an
instance id), and every topic has 3 partitions, keyed by `orderId` or, for the stock
topics, by SKU. Every consumer follows the same rules: a message it can't decode, or
one that still fails after its retries, goes to `<topic>.dlq`. The diagram shows only
`orders.created.dlq` to keep it readable. notification-service and order-service
ignore `inventory.released`; inventory-service itself reacts to `payments.failed`.

### What happens to one order

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant O as order-service
    participant K as Kafka
    participant P as payment-service
    participant I as inventory-service
    participant N as notification-service

    C->>O: POST /orders
    O->>O: save order (PENDING)
    O->>K: orders.created (key = orderId)
    O-->>C: 202 Accepted + order
    par separate consumer groups
        K->>P: orders.created
        P->>K: payments.completed | payments.failed
    and
        K->>I: orders.created
        I->>K: inventory.reserved | inventory.rejected
    end
    K->>O: payment + inventory outcomes
    O->>O: PENDING → CONFIRMED (both succeeded)<br/>or CANCELLED (either failed)
    K->>N: every outcome
    N->>N: log customer notification
    C->>O: GET /orders/:id
```

## Repository layout

| Path                            | What it is                                                                                   |
| ------------------------------- | -------------------------------------------------------------------------------------------- |
| `packages/contracts`            | Event envelope, zod schemas per event type, topic names, `createEvent` / `decodeEvent`       |
| `packages/kafka-utils`          | kafkajs client, producer, consumer wrapper (retries, backoff, DLQ, idempotency), logging     |
| `packages/auth`                 | JWT verification via JWKS (`jose`), Fastify plugin with `requireScope()`, client credentials |
| `packages/feature-flags`        | `FeatureFlags` interface: LaunchDarkly provider and a local JSON/env provider                |
| `services/order-service`        | Fastify API (`POST /orders`, `GET /orders/:id`) and the order state machine                  |
| `services/payment-service`      | Charges orders; has the chaos toggle                                                         |
| `services/inventory-service`    | Stock in Postgres: reserves/releases (all-or-nothing), transactional outbox, stock API       |
| `services/notification-service` | Logs a customer notification for every outcome                                               |
| `apps/web`                      | React dashboard (Vite): live stock, orders and activity, sign-in with Keycloak               |
| `packages/stream-types`         | Types of the gateway's stream, shared by the gateway and the web app (no runtime code)       |
| `services/gateway-service`      | Kafka to browser: consumes the topics, keeps a stock read model, streams it over SSE         |
| `tools/load-generator`          | Generates orders over HTTP, duplicates, or poison messages; waits and summarizes the results |
| `tools/dlq-replay`              | Inspects DLQs and replays records to their original topic                                    |
| `infra/keycloak`                | Importable Keycloak realm (clients, scopes, demo users)                                      |
| `feature-flags.json`            | Local flag values, used when `LD_SDK_KEY` is not set                                         |
| `docs/architecture.md`          | Diagrams of the whole system                                                                 |
| `docs/demo-guide.md`            | How to present the project, with every concept mapped to code                                |
| `docs/demo-script.md`           | Step-by-step demo commands                                                                   |
| `docs/ld-flags.md`              | Feature flags, LaunchDarkly setup and targeting                                              |

## Topics

| Topic                    | Partitions | Key     | Producer          | Consumer groups                                              |
| ------------------------ | ---------- | ------- | ----------------- | ------------------------------------------------------------ |
| `orders.created`         | 3          | orderId | order-service     | `payment-service`, `inventory-service`                       |
| `payments.completed`     | 3          | orderId | payment-service   | `order-service`, `notification-service`                      |
| `payments.failed`        | 3          | orderId | payment-service   | `order-service`, `notification-service`, `inventory-service` |
| `inventory.reserved`     | 3          | orderId | inventory-service | `order-service`, `notification-service`                      |
| `inventory.rejected`     | 3          | orderId | inventory-service | `order-service`, `notification-service`                      |
| `inventory.released`     | 3          | orderId | inventory-service | `gateway-service-<id>-feed`                                  |
| `inventory.stock-levels` | 3          | SKU     | inventory-service | `gateway-service-<id>-state` (**compacted**)                 |
| `inventory.stock-low`    | 3          | SKU     | inventory-service | `gateway-service-<id>-feed`                                  |
| `<topic>.dlq`            | 1          | orderId | consumer wrapper  | read by `tools/dlq-replay` (`dlq-replay.<topic>`)            |

Topics are declared once in [`packages/contracts/src/topics.ts`](packages/contracts/src/topics.ts)
and created by the services at startup; auto-creation is disabled on the broker.
Because every order event is keyed by `orderId` (and every stock event by SKU), all
events for one order, or one SKU, land on the same partition and are processed in order. Each service has its own consumer group, so
every service sees every event, and running more instances of a service spreads
the 3 partitions across them.

## Inventory: stock in Postgres

inventory-service keeps real state in Postgres (`stock`, `reservations`, `outbox`;
migrations run at startup). It publishes more than the order-level outcome:

| Topic                    | Key   | Event                 | Meaning                                                                       |
| ------------------------ | ----- | --------------------- | ----------------------------------------------------------------------------- |
| `inventory.stock-levels` | SKU   | `stock.level.changed` | Latest level of one SKU (**log-compacted**); `version` orders changes per SKU |
| `inventory.stock-low`    | SKU   | `stock.low`           | A SKU just dropped to or below its low-stock threshold                        |
| `inventory.released`     | order | `inventory.released`  | A reservation was given back (the order's payment failed)                     |

- **Transactional outbox.** Every operation is one Postgres transaction that changes
  stock _and_ inserts the resulting events into `outbox`. A relay publishes them to
  Kafka in order and marks them sent (at-least-once; consumers de-duplicate by
  eventId). A crash can't lose an event or emit one for a change that rolled back.
- **Idempotent by construction.** One `reservations` row per order records the
  outcome (`reserved`, `rejected`, `released`, `cancelled`), so a redelivered
  `orders.created` never reserves twice, and a rejection stays a rejection even if
  stock arrives before the redelivery. Output eventIds derive from the input event.
- **Compensation.** On `payments.failed` the reservation is released and the stock
  comes back. If that event is processed _before_ `orders.created` (they run in
  parallel), the order is marked `cancelled` and the late reservation is skipped.
- **State as a changelog.** `inventory.stock-levels` is compacted, so the newest
  record per SKU is the current level. A snapshot of every SKU is published at each
  startup, which rebuilds the topic if Kafka was reset.

Stock API (port `3001`, needs `inventory:read` / `inventory:write`):

```bash
curl -s localhost:3001/inventory -H "authorization: Bearer $TOKEN"
curl -s -X POST localhost:3001/inventory/SKU-WEBCAM/adjust -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"delta":40,"reason":"restock","note":"delivery"}'
```

`reason` is `restock` (delta > 0), `shrinkage` (delta < 0) or `correction`. Stock can't
go below zero (`409`).

## Real-time gateway (Kafka to the browser)

A browser can't speak the Kafka protocol, so `gateway-service` (port `3002`) consumes the
topics and pushes what happened to React over **Server-Sent Events**. It is read-only:
commands (placing orders, adjusting stock) still go to order-service and inventory-service.

```bash
curl -sN localhost:3002/stream -H "authorization: Bearer $TOKEN"   # needs the stream:read scope
curl -s localhost:3002/snapshot -H "authorization: Bearer $TOKEN"  # the same state as one JSON document
curl -s localhost:3002/flags -H "authorization: Bearer $TOKEN"     # the web app's feature flags for this user
```

| Frame      | Sent when                                                    | Payload                                      |
| ---------- | ------------------------------------------------------------ | -------------------------------------------- |
| `snapshot` | once, when a client connects or reconnects                   | `{ stock, feed, resumed, serverTime }`       |
| `stock`    | a SKU's level changes                                        | the SKU's new level and what changed         |
| `feed`     | an order, payment or reservation event, or a low-stock alert | a short `summary` plus the event data        |
| `flags`    | right after the snapshot, and whenever a flag value changes  | the user's [web app flags](docs/ld-flags.md) |

- **State and events are handled differently.** Stock is _state_: the gateway rebuilds the
  table by reading the compacted `inventory.stock-levels` topic from the beginning, and
  every connect gets the whole table, so a reconnecting client can never miss a stock
  change. The feed is _events_: a bounded in-memory ring buffer (500 entries) gives new
  clients the latest 50 and lets a reconnecting client **resume**. Each `snapshot` and
  `feed` frame carries an SSE `id`, the browser sends it back as `Last-Event-ID`, and the
  gateway replays only what was missed (`resumed: true`). An id from an earlier gateway run,
  or one that was already evicted, falls back to a fresh snapshot (`resumed: false`).
- **Every instance sees every event.** Each gateway instance uses its own consumer groups
  (`gateway-service-<instanceId>-state` / `-feed`) instead of a shared one. A shared group
  would split the partitions between instances, so each instance would only see part of
  the events its own clients need. Set `GATEWAY_INSTANCE_ID` to keep the group names
  stable across restarts.
- **Who sees what.** The token needs `stream:read` to connect. Then `inventory:read`
  (or `inventory:write`) selects stock and low-stock alerts, and `orders:read` selects
  order events: only the user's own orders (the event `actor`), or all of them with
  `admin`. A token with `stream:read` but no data scope gets `403`.
- **Slow clients** that let more than 1 MB queue up are dropped and resume on reconnect.
- **Heartbeats and dead connections.** Idle streams get a `ping` event every 15 s. It is a real
  event (not a `:` comment) because browser SSE libraries don't expose comments, and the web app
  needs to _see_ it: if nothing arrives for 45 s the connection is declared dead (a proxy can
  leave a broken one open) and the app reconnects, resuming from its last event id. On shutdown
  the gateway ends every open stream right away, so clients reconnect to the next instance
  instead of waiting for a timeout.
- **What a restart loses.** Stock is rebuilt from Kafka, but the activity feed lives in the
  gateway's memory, so after a restart it starts empty and fills again from then on.
- Browsers can't set headers on `EventSource`, so the React app uses `fetch` streaming
  (for example `@microsoft/fetch-event-source`) with the `Authorization` header. Tokens
  never go in the URL. CORS allows the origins in `WEB_ORIGIN`.

## The web app

`apps/web` is a React + TypeScript dashboard built with Vite. It shows stock levels, your
orders and a live activity feed, all updating the moment something happens in Kafka.

```bash
npm run dev            # starts everything, including the web app on http://localhost:5173
npm run dev -- --only web   # just the web app (backend already running)
npm run dev -- --skip web   # backend only
```

| Piece           | How it works                                                                                                                                                                                                                                                                        |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sign-in**     | OAuth2 authorization code flow with **PKCE** against Keycloak (`oidc-client-ts`, public client `orderflow-web`, no secret). Tokens live in `sessionStorage` and are renewed with the refresh token. Behind the small `useAuth()` interface, so the identity provider can be swapped |
| **Live data**   | One long-lived SSE connection to the gateway (`@microsoft/fetch-event-source`). It sends a fresh bearer token on every reconnect and resumes from `Last-Event-ID`; the header shows **Live** / **Reconnecting**                                                                     |
| **Stock table** | Fed by `snapshot` + `stock` frames. Rows flash when a live update arrives; users with `inventory:write` also get an _Adjust stock_ form                                                                                                                                             |
| **Orders**      | Placing an order is a normal REST `POST` to order-service. The orders table refetches (debounced) when the stream says one of your orders moved                                                                                                                                     |
| **No CORS**     | The Vite dev server proxies `/api/order-service`, `/api/inventory-service` and `/api/gateway-service` to ports 3000 to 3002, so the browser only talks to one origin                                                                                                                |
| **Permissions** | The UI reads the token's scopes to hide what you can't do (the services still enforce them): no `orders:write` hides the order form, no `inventory:write` hides stock adjustment                                                                                                    |

Things to try: place an order for 2 laptops (over the card limit, so payment declines and the
reserved stock is released: watch the stock go down and back up), restock `SKU-GPU` from the
adjust form, or open a second window as `bob` and see that each user only sees their own orders
while stock updates reach both. Restart the gateway (`touch services/gateway-service/src/index.ts`): the header shows
_Reconnecting…_ for a few seconds and then _Live_ again on its own, with the stock table rebuilt.

Configuration (all optional, `VITE_*` variables): `VITE_OIDC_AUTHORITY`, `VITE_OIDC_CLIENT_ID`,
`VITE_OIDC_SCOPE`, and `VITE_ORDER_API` / `VITE_INVENTORY_API` / `VITE_GATEWAY_API` to point at
the services directly instead of through the dev proxy (the gateway allows the origins in
`WEB_ORIGIN`; order-service and inventory-service don't enable CORS). `npm run build` also
produces a static bundle in `apps/web/dist`. The dev proxy is not part of it, so a deployment needs
a reverse proxy with the same `/api/...` routes. The stream connection is only authenticated when it
opens, so a very long-lived connection keeps working after the access token expires.

## Event envelope

Every message value is a JSON envelope, validated with zod on the way out
(`createEvent`) and on the way in (`decodeEvent`):

```json
{
  "eventId": "0a3d306f-2538-4183-9530-7b3017d1db33",
  "type": "order.created",
  "version": 1,
  "occurredAt": "2026-09-28T10:01:59.260Z",
  "correlationId": "smoke-1",
  "actor": { "sub": "862ea51e-9ea5-49f7-b730-589f6213adc6", "clientId": "orderflow-cli" },
  "data": {
    "orderId": "f43d58d0-856a-4a56-9f96-6b5c5c89401f",
    "customerId": "alice",
    "customerTier": "gold",
    "country": "DE",
    "items": [{ "sku": "SKU-MOUSE", "quantity": 1, "unitPrice": 25 }],
    "totalAmount": 25,
    "currency": "USD"
  }
}
```

- **eventId** is unique per event and is what consumers de-duplicate on.
- **correlationId** comes from the `x-correlation-id` request header (or is generated)
  and is copied onto every downstream event, so you can grep one order's whole
  journey across all service logs.
- **actor** is who placed the order: the token's `sub` and client (`azp`). Services
  copy it onto every event they emit in reaction. It's identity only. **Access
  tokens never go into events.** The schema is strict, so an actor with any extra
  field (such as a token) is rejected.
- **version** is checked per type; an unknown version is rejected into the DLQ
  instead of being half-understood.
- `event-id`, `event-type`, `event-version` and `correlation-id` are also set as
  Kafka headers, so you can filter them in Kafka UI.

## Reliability patterns

All of these live in [`packages/kafka-utils/src/consumer.ts`](packages/kafka-utils/src/consumer.ts)
(`processMessage`), so every service gets them for free:

1. **Validation.** A message that isn't valid JSON, isn't an envelope, has the wrong
   type for the topic, or has an invalid payload is a _poison message_. It goes
   straight to `<topic>.dlq` (retrying cannot help) and the partition keeps moving.
2. **Idempotency.** Kafka consumers get at-least-once delivery: after a crash or a
   rebalance, messages are delivered again, and producers can write an event twice
   when they retry. Before calling a handler the wrapper checks whether that
   `eventId` was already processed by this consumer group and skips it if so. The
   services also derive their outgoing eventIds from the incoming one
   (`deriveEventId`), so re-processing an event re-emits the _same_ eventIds and
   de-duplication keeps working all the way downstream.
3. **Retries with exponential backoff.** If a handler throws, the wrapper retries
   in-process: `initial × 2^(n-1)`, capped, ±20% jitter, and it heartbeats while
   waiting so the consumer isn't kicked out of the group. A `NonRetryableError`
   skips the retries.
4. **Dead-letter queue.** After `max-retry-attempts` retries (a feature flag, default
   3, resolved per message) the message goes to
   `<topic>.dlq` as a record with the error, the attempt count, the consumer group,
   and the original topic/partition/offset/key/headers/value. The original offset is
   then committed. If the DLQ itself can't be written, the offset is _not_ committed
   and Kafka redelivers, so nothing is silently dropped.
5. **Replay.** `npm run dlq:replay` republishes records exactly as they were (same
   key and headers, so the same eventId) and adds `replayed-from`/`replay-count`
   headers. Consumer groups that already processed the event skip it (see 2).
6. **Graceful shutdown.** On SIGINT/SIGTERM consumers disconnect and leave their
   group, so partitions are reassigned right away instead of after the session
   timeout.

The order saga in [`order.ts`](services/order-service/src/order.ts) is a pure
function: `PENDING` until both answers arrive, then `CONFIRMED` if payment completed
**and** stock was reserved, or `CANCELLED` as soon as either fails. Final states
stay final, and applying the same event twice does nothing.

## Security: OAuth2 / OIDC

Keycloak runs in docker-compose and imports
[`infra/keycloak/realm-export.json`](infra/keycloak/realm-export.json) (realm
`orderflow`). order-service verifies every bearer token locally against Keycloak's
JWKS with [`jose`](https://github.com/panva/jose). It checks the signature, `iss`,
`aud = order-service`, `exp` and `sub`, and never calls Keycloak per request.

| Client              | Type                       | Scopes                                                                        | Used by                                                 |
| ------------------- | -------------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------- |
| `order-service`     | resource server            | –                                                                             | the token audience (`aud`)                              |
| `load-generator`    | confidential, client creds | `orders:read` `orders:write`                                                  | `tools/load-generator`                                  |
| `dlq-replay`        | confidential, client creds | `admin`                                                                       | `tools/dlq-replay` (operator)                           |
| `inventory-service` | resource server            | –                                                                             | the stock API's token audience (`aud`)                  |
| `gateway-service`   | resource server            | –                                                                             | the stream's token audience (`aud`)                     |
| `orderflow-cli`     | public, password grant     | `orders:read` `orders:write` `inventory:read` `inventory:write` `stream:read` | humans with `curl` (users `alice`/`alice`, `bob`/`bob`) |

Each scope adds `order-service` to the token's `aud`. A client can only get the
scopes it is assigned; for example, load-generator asking for `admin` gets
`invalid_scope`.

| Route                        | Requires                                                    |
| ---------------------------- | ----------------------------------------------------------- |
| `POST /orders`               | `orders:write`                                              |
| `GET /orders/:id`            | `orders:read` **and** being the order's creator, or `admin` |
| `GET /orders`                | `orders:read` (own orders) or `admin` (all orders)          |
| `POST /orders/:id/republish` | `orders:write` and being the creator, or `admin`            |
| `GET /health`                | nothing                                                     |

The stock API (inventory-service, `aud = inventory-service`) is separate: `GET /inventory`
and `GET /inventory/:sku` need `inventory:read` (or `inventory:write`), and
`POST /inventory/:sku/adjust` needs `inventory:write`. An order token can't be used for stock.

The gateway (`aud = gateway-service`) needs `stream:read` for `GET /stream` and
`GET /snapshot`; what they contain depends on `orders:read` / `inventory:read`.

Errors follow RFC 6750:

| Status | When                                                    | Body `error`              | `WWW-Authenticate`                               |
| ------ | ------------------------------------------------------- | ------------------------- | ------------------------------------------------ |
| 401    | no token                                                | `invalid_request`         | `Bearer realm="orderflow"`                       |
| 401    | malformed, bad signature, expired, wrong `iss` or `aud` | `invalid_token`           | `… error="invalid_token", error_description="…"` |
| 403    | valid token without the required scope                  | `insufficient_scope`      | `… error="insufficient_scope", scope="…"`        |
| 403    | valid token, but someone else's order                   | `access_denied`           | –                                                |
| 503    | Keycloak's JWKS unreachable (can't verify)              | `temporarily_unavailable` | – (`Retry-After: 5`)                             |

The tools use the OAuth2 client-credentials flow with a cached token
(`ClientCredentialsTokenProvider`). dlq-replay refuses to run unless its token
carries `admin`. It uses that token to show each dead-lettered order's current
status, and stamps replayed messages with a `replayed-by` header.

## Feature flags

| Flag                       | Type    | Default | Effect                                                                             |
| -------------------------- | ------- | ------- | ---------------------------------------------------------------------------------- |
| `payment-failure-rate`     | number  | `0`     | Chaos: share of payment gateway calls that throw (replaces `PAYMENT_FAILURE_RATE`) |
| `payment-consumer-enabled` | boolean | `true`  | `false` pauses the payment consumer live (lag builds up)                           |
| `fraud-check-enabled`      | boolean | `false` | Adds a fraud-check step in payment-service                                         |
| `notification-channel`     | string  | `email` | `email` / `sms` / `push` / `slack`                                                 |
| `max-retry-attempts`       | number  | `3`     | Retries before a message is dead-lettered (every consumer)                         |
| `live-updates-enabled`     | boolean | `true`  | **Web app:** `false` switches from the SSE stream to polling every 5 s, live       |
| `new-inventory-dashboard`  | boolean | `false` | **Web app:** stock as a grid of cards instead of a table                           |
| `bulk-adjust-enabled`      | boolean | `false` | **Web app:** adds a one-click "restock all low items" action                       |
| `activity-feed-size`       | number  | `50`    | **Web app:** how many entries the live activity feed shows (5 to 200)              |

The four web app flags are evaluated by gateway-service **per signed-in user** (context
kind `user`: key = the token's `sub`, name = the username), so LaunchDarkly can target
them at individual people. The browser never talks to LaunchDarkly and never sees an SDK
key: it gets the values from `GET /flags`, and the gateway pushes a `flags` frame down
every open stream the moment a flag changes (the page updates without a reload).

With `LD_SDK_KEY` set, flags come from LaunchDarkly and are evaluated per order
(context kind `order`: key orderId, attributes `customerTier`, `country`). Without
a key, they come from `feature-flags.json`, which you can edit live, plus `FLAG_*`
env vars. Every flag has a safe default, so the pipeline keeps working if
LaunchDarkly is down or misconfigured. See **[docs/ld-flags.md](docs/ld-flags.md)**.

## Configuration

Everything has a sensible default. To override values, copy `.env.example` to `.env`
(`npm run dev` loads it) or set environment variables.

| Variable                           | Default                                                   | Used by           | Meaning                                                                                     |
| ---------------------------------- | --------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------- |
| `KAFKA_BROKERS`                    | `localhost:9092`                                          | all               | Comma-separated bootstrap brokers                                                           |
| `ORDER_SERVICE_PORT`               | `3000`                                                    | order-service     | HTTP port                                                                                   |
| `INVENTORY_SERVICE_PORT`           | `3001`                                                    | inventory-service | HTTP port of the stock API                                                                  |
| `GATEWAY_SERVICE_PORT`             | `3002`                                                    | gateway-service   | HTTP port of the stream                                                                     |
| `WEB_ORIGIN`                       | `http://localhost:5173`                                   | gateway-service   | Allowed browser origin(s) for CORS, comma-separated                                         |
| `GATEWAY_INSTANCE_ID`              | random                                                    | gateway-service   | Suffix of the gateway's consumer groups                                                     |
| `GATEWAY_AUTH_AUDIENCE`            | `gateway-service`                                         | gateway-service   | Expected `aud` for the stream                                                               |
| `DATABASE_URL`                     | `postgres://orderflow:orderflow@localhost:5432/orderflow` | inventory-service | Postgres connection string                                                                  |
| `INVENTORY_AUTH_AUDIENCE`          | `inventory-service`                                       | inventory-service | Expected `aud` for the stock API                                                            |
| `PAYMENT_DECLINE_RATE`             | `0`                                                       | payment-service   | Probability that a card is randomly declined (→ `payments.failed`)                          |
| `PAYMENT_CARD_LIMIT`               | `2000`                                                    | payment-service   | Orders above this amount are always declined                                                |
| `CONSUMER_INITIAL_RETRY_MS`        | `200`                                                     | all consumers     | First backoff delay                                                                         |
| `CONSUMER_MAX_RETRY_MS`            | `5000`                                                    | all consumers     | Backoff cap                                                                                 |
| `LOG_LEVEL`                        | `info`                                                    | all               | pino level (`debug` shows every publish)                                                    |
| `LOG_FORMAT`                       | pretty                                                    | all               | `json` for newline-delimited JSON (also when `NODE_ENV=production`)                         |
| `KAFKAJS_DEBUG`                    | unset                                                     | all               | Set to anything to see kafkajs internals                                                    |
| `AUTH_ISSUER`                      | `http://localhost:8081/realms/orderflow`                  | all               | Expected `iss`; JWKS and token URLs are derived from it (`AUTH_JWKS_URI`, `AUTH_TOKEN_URL`) |
| `AUTH_AUDIENCE`                    | `order-service`                                           | order-service     | Expected `aud`                                                                              |
| `LOADGEN_CLIENT_ID` / `_SECRET`    | demo values                                               | load-generator    | Client credentials (demo secret from the realm file)                                        |
| `DLQ_REPLAY_CLIENT_ID` / `_SECRET` | demo values                                               | dlq-replay        | Client credentials (demo secret from the realm file)                                        |
| `LD_SDK_KEY`                       | unset                                                     | all               | LaunchDarkly server-side SDK key; unset = local flags                                       |
| `FEATURE_FLAGS_FILE`               | `./feature-flags.json`                                    | all               | Local flag file (watched for changes)                                                       |
| `FLAG_<KEY>`                       | unset                                                     | all               | Local flag override, e.g. `FLAG_PAYMENT_FAILURE_RATE=0.5`                                   |
| `KAFKA_UI_IMAGE`                   | kafbat v1.4.2                                             | docker compose    | Override the Kafka UI image                                                                 |
| `KEYCLOAK_IMAGE`                   | `quay.io/keycloak/keycloak:26.4`                          | docker compose    | Override the Keycloak image                                                                 |
| `POSTGRES_IMAGE` / `POSTGRES_PORT` | `postgres:17-alpine` / `5432`                             | docker compose    | Override the Postgres image or host port                                                    |

## HTTP API (order-service)

| Method & path                | Description                                                                                                      |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `POST /orders`               | Body `{ customerId, items: [{ sku, quantity, unitPrice }], customerTier?, country?, currency? }` → `202` + order |
| `GET /orders/:id`            | Current order state, payment/inventory status and an event history                                               |
| `GET /orders?limit=20`       | Most recent orders                                                                                               |
| `POST /orders/:id/republish` | **Demo only**: republishes the original `orders.created` event (same eventId)                                    |
| `GET /health`                | Liveness                                                                                                         |

All routes except `/health` need a bearer token; see [Security](#security-oauth2--oidc).
`customerTier` is `standard` (default), `gold` or `platinum`, and `country` is an ISO
code (default `US`). Both feed feature-flag targeting.

Stock (see [`inventory.ts`](services/inventory-service/src/inventory.ts)):
`SKU-KEYBOARD`, `SKU-MOUSE`, `SKU-MONITOR`, `SKU-LAPTOP`, `SKU-HEADSET`, `SKU-WEBCAM`
(only 10 in stock) and `SKU-GPU` (always out of stock).

## Tools

```bash
npm run load -- --help
npm run load                                        # 20 orders at 5/s
npm run load -- -n 200 -r 50 --scenario mixed --wait # includes out-of-stock + over-limit orders, prints a summary
npm run load -- -n 5 --duplicate --wait              # publishes every orders.created twice
npm run load -- --poison 5                           # writes 5 malformed messages to orders.created
npm run load -- --tier standard --country BR --wait  # force the flag-targeting attributes

npm run dlq:replay                                   # how many records each DLQ holds
npm run dlq:replay -- -t orders.created.dlq --dry-run --reason all  # needs the admin scope (client "dlq-replay")
npm run dlq:replay -- -t orders.created.dlq --group payment-service

npm run lag                                          # consumer group offsets and lag
```

## Scripts

| Script                                  | What it does                                                                        |
| --------------------------------------- | ----------------------------------------------------------------------------------- |
| `npm run dev`                           | Runs all services and the web app with hot reload (`--only payment` / `--skip web`) |
| `npm run build`                         | `tsc -b` for every workspace, then the Vite bundle (output in `*/dist`)             |
| `npm start -w @orderflow/order-service` | Runs one built service with plain `node`                                            |
| `npm run lint` / `lint:fix`             | ESLint (type-aware) + Prettier                                                      |
| `npm run typecheck`                     | Typechecks every workspace, including tests                                         |
| `npm test` / `test:watch`               | Vitest unit tests (backend, then the web app's jsdom tests); no Kafka needed        |
| `npm run infra:up` / `infra:down`       | `docker compose up -d --wait` / `docker compose down -v` (wipes Kafka data)         |

In development, workspace packages are imported straight from their TypeScript
sources through a custom `@orderflow/source` export condition, so you don't need a
build step. `npm run build` + `npm start` run the compiled JavaScript instead.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs lint, typecheck,
tests and build. It then runs an end-to-end smoke test against a real Kafka broker
and Keycloak: unauthenticated calls get 401, orders settle, duplicates are skipped,
and poison messages reach the DLQ. It needs **no secrets**: the demo realm carries
its own demo credentials, and without `LD_SDK_KEY` the local flag provider is used.

## Requirements

- Node.js **22.12+** (`.nvmrc` pins 22)
- Docker with Compose v2 (Kafka, Keycloak and Postgres run in containers)

## Deliberate simplifications

This is a demo, so some production concerns are simplified on purpose:

- **Orders are in memory.** Orders and the consumers' processed eventIds are lost when
  a service restarts (inventory is the exception: stock and reservations are in
  Postgres, and its reservation table makes handling idempotent across restarts).
  In production you would keep processed eventIds in the same database transaction
  as the side effect (or use Redis `SET NX` with a TTL), shared by every instance.
- **Outbox only in inventory-service.** order-service still saves the order and then
  publishes, and it rolls back if the publish fails. Applying the same outbox there
  (or CDC) would make that atomic too.
- **Partial compensation.** A failed payment releases the stock reservation. If
  payment succeeds but stock is rejected, the order is cancelled and a warning notes
  that a refund is needed; a full saga would publish a compensating refund command.
- **Schemas live in code** (zod), not in a schema registry.
- **Retries block the partition** while they back off. That's fine for short,
  transient failures. For long outages, use retry topics with delayed
  consumption instead.
- **Demo identity setup.** Keycloak runs in dev mode (in-memory, re-imported on every
  start, plain HTTP). The client secrets and user passwords in the realm file are
  public demo values. The `orderflow-cli` password grant exists only so you can
  `curl` as a user; real front ends use the authorization code flow with PKCE.
- **Ownership is the token's `sub`.** Orders are owned by whoever created them.
  `customerId` is just business data.
- **Local flags have no targeting.** Per-tier or per-country rules need LaunchDarkly.
- **kafkajs** is used as requested; it is stable but no longer actively developed.
  For new production work, consider `@confluentinc/kafka-javascript`.
