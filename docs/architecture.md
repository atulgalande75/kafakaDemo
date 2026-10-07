# Architecture

This page shows the whole system in diagrams, from the big picture down to the
details that matter in a demo. Every diagram is [Mermaid](https://mermaid.js.org/), so
it renders on GitHub and in most Markdown viewers. For the concepts behind the design
and how to present them, see [demo-guide.md](demo-guide.md). For step-by-step commands,
see [demo-script.md](demo-script.md).

| #   | Diagram                                                   | Answers                                                      |
| --- | --------------------------------------------------------- | ------------------------------------------------------------ |
| 1   | [System overview](#1-system-overview)                     | What runs, on which port, and who talks to whom?             |
| 2   | [Kafka topology](#2-kafka-topology)                       | Which topics exist, and who produces and consumes each one?  |
| 3   | [One order, end to end](#3-one-order-end-to-end)          | What happens when an order succeeds, and when payment fails? |
| 4   | [From Kafka to the browser](#4-from-kafka-to-the-browser) | How does a change in Kafka reach the page, safely?           |
| 5   | [Inside inventory-service](#5-inside-inventory-service)   | How are stock and events kept consistent?                    |
| 6   | [What every consumer does](#6-what-every-consumer-does)   | How are retries, duplicates and bad messages handled?        |
| 7   | [Security](#7-security)                                   | Who can call what, and how is a token checked?               |
| 8   | [Feature flags](#8-feature-flags)                         | Where do flag values come from, and who reads them?          |
| 9   | [Code structure](#9-code-structure)                       | How are the packages, services and apps related?             |

---

## 1. System overview

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

**Reading it.** Solid arrows carry data, dotted arrows are checks and configuration. The
four backend services never call each other: they only exchange events through Kafka.
The browser only ever talks to one origin (the Vite dev server), which forwards to the
three services that have an HTTP API.

| Component            | Port | Role                                                                           |
| -------------------- | ---- | ------------------------------------------------------------------------------ |
| Web app (Vite)       | 5173 | React dashboard and the dev proxy                                              |
| order-service        | 3000 | Accepts orders, runs the order state machine                                   |
| inventory-service    | 3001 | Owns stock in Postgres, publishes stock events through an outbox               |
| gateway-service      | 3002 | Turns Kafka events into a live stream for browsers; serves the web app's flags |
| payment-service      | –    | Consumer and producer only (no HTTP)                                           |
| notification-service | –    | Consumer only (no HTTP)                                                        |
| Kafka                | 9092 | Event backbone (single node, KRaft mode, no ZooKeeper)                         |
| Kafka UI             | 8080 | Browse topics, messages, consumer groups                                       |
| Keycloak             | 8081 | Identity provider (realm `orderflow`)                                          |
| Postgres             | 5432 | Durable state of inventory-service                                             |

---

## 2. Kafka topology

```mermaid
flowchart LR
    order["order-service"]
    payment["payment-service"]
    inventory["inventory-service"]
    notification["notification-service"]
    gateway["gateway-service"]

    oc[["<b>orders.created</b>"]]
    pay[["<b>payments.completed</b><br/><b>payments.failed</b>"]]
    inv[["<b>inventory.reserved</b><br/><b>inventory.rejected</b><br/><b>inventory.released</b>"]]
    stock[["<b>inventory.stock-levels</b> (compacted)<br/><b>inventory.stock-low</b>"]]
    dlq[["<b>&lt;topic&gt;.dlq</b><br/>a dead-letter topic for every topic"]]

    order -- "key = orderId" --> oc
    oc -- "group payment-service" --> payment
    oc -- "group inventory-service" --> inventory
    payment --> pay
    inventory --> inv
    inventory -- "key = SKU" --> stock

    pay -- "group inventory-service<br/>(release stock)" --> inventory
    pay -- "group order-service" --> order
    inv -- "group order-service" --> order
    pay -- "group notification-service" --> notification
    inv -- "group notification-service" --> notification

    oc -. "per-instance groups" .-> gateway
    pay -.-> gateway
    inv -.-> gateway
    stock -.-> gateway

    oc -. "undecodable, or failed<br/>after all retries" .-> dlq

    classDef topic fill:#fff4dd,stroke:#d4a017,color:#222
    classDef dead fill:#fde2e2,stroke:#c0392b,color:#222
    class oc,pay,inv,stock topic
    class dlq dead
```

| Rule                                    | Why                                                                                                          |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Key = `orderId` (order topics)          | All events for one order land on one partition, so they are consumed in order                                |
| Key = SKU (stock topics)                | All changes to one SKU stay in order; compaction keeps the newest record per SKU                             |
| One consumer group per service          | Every service sees every event; more instances of one service share its partitions                           |
| Gateway: one group set **per instance** | Every gateway instance must see every event to serve its own connected browsers                              |
| 3 partitions, 1 for each `.dlq`         | Parallelism for business topics; dead letters are rare and read by hand                                      |
| Topics are created by the services      | Broker auto-creation is off, so the layout is explicit in [`topics.ts`](../packages/contracts/src/topics.ts) |

---

## 3. One order, end to end

```mermaid
sequenceDiagram
    autonumber
    actor U as User (browser)
    participant O as order-service
    participant K as Kafka
    participant P as payment-service
    participant I as inventory-service
    participant G as gateway-service

    U->>O: POST /orders (Bearer token)
    O->>O: save order as PENDING
    O->>K: orders.created (key = orderId)
    O-->>U: 202 Accepted

    par separate consumer groups
        K->>P: orders.created
        P->>K: payments.completed or payments.failed
    and
        K->>I: orders.created
        I->>I: reserve stock (one DB transaction)
        I->>K: inventory.reserved or inventory.rejected, plus stock events
    end

    K->>O: payment and inventory outcomes
    O->>O: PENDING becomes CONFIRMED or CANCELLED

    K-->>G: every event
    G-->>U: SSE frames (feed, stock)

    opt payment failed after stock was reserved
        K->>I: payments.failed
        I->>I: release the reservation (stock goes back up)
        I->>K: inventory.released, plus stock events
        K-->>G: events
        G-->>U: stock row goes back up, order shows CANCELLED
    end
```

**Order state machine.** An order is `PENDING` until both answers arrive. It becomes
`CONFIRMED` if payment completed **and** stock was reserved, and `CANCELLED` as soon as
either fails. Final states stay final, and applying the same event twice changes nothing
([`order.ts`](../services/order-service/src/order.ts)).

**The race.** Payment and inventory run in parallel, so `payments.failed` can reach
inventory-service _before_ it has reserved anything. It records the order as `cancelled`,
and the late reservation is skipped ([`inventory.ts`](../services/inventory-service/src/inventory.ts)).

---

## 4. From Kafka to the browser

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant W as React app
    participant KC as Keycloak
    participant G as gateway-service
    participant K as Kafka

    U->>W: open http://localhost:5173
    W->>KC: redirect (authorization code + PKCE challenge)
    U->>KC: sign in
    KC-->>W: code, then access token + refresh token

    W->>G: GET /flags (Bearer token)
    G-->>W: the user's feature flags
    Note over W: live-updates-enabled decides:<br/>stream (default) or polling

    W->>G: GET /stream (Bearer token, Last-Event-ID if resuming)
    G-->>W: snapshot (full stock table + recent feed)
    G-->>W: flags
    loop while connected
        K-->>G: events
        G-->>W: stock, feed, flags frames
        G-->>W: ping every 15 s
    end

    Note over W,G: Nothing for 45 s means the connection is dead,<br/>so the app reconnects and sends its last event id
    W->>G: GET /stream (Last-Event-ID)
    G-->>W: snapshot with resumed = true and only the missed feed entries
```

How the gateway builds what it sends ([`hub.ts`](../services/gateway-service/src/hub.ts)):

```mermaid
flowchart LR
    kstate[["inventory.stock-levels<br/>(compacted, read from the start)"]]
    kfeed[["order and alert topics<br/>(read from now on)"]]

    subgraph hub["Hub (in memory)"]
        stock["<b>Stock table</b><br/>latest level per SKU<br/>older versions ignored"]
        ring["<b>Feed ring buffer</b><br/>last 500 entries, each with<br/>a sequence number = SSE id"]
        filter{"Visibility<br/>per user"}
    end

    kstate --> stock
    kfeed --> ring
    stock --> filter
    ring --> filter
    filter -- "inventory:read: stock + alerts<br/>orders:read: own orders (admin: all)" --> sse["SSE connection<br/>of that user"]

    classDef topic fill:#fff4dd,stroke:#d4a017,color:#222
    class kstate,kfeed topic
```

**Stock is state, the feed is events.** Stock is rebuilt from the compacted topic and sent in
full on every connect, so a client can never miss a stock change. The feed is a bounded
buffer: a reconnecting client sends `Last-Event-ID` and gets exactly the entries it missed. An
id from an older gateway run, or one already evicted, falls back to a fresh snapshot. A
restarted gateway starts with an empty feed, because the buffer is in memory.

---

## 5. Inside inventory-service

```mermaid
flowchart TB
    subgraph in["Inputs"]
        oc[["orders.created"]]
        pf[["payments.failed"]]
        api["REST: POST /inventory/:sku/adjust<br/>(scope inventory:write)"]
    end

    subgraph svc["inventory-service"]
        handlers["Handlers<br/>(Kafka consumer wrapper)"]
        store["InventoryStore"]
        relay["Outbox relay<br/>polls + woken after each write"]
    end

    subgraph pgdb["Postgres"]
        direction TB
        stock["stock<br/>available, version"]
        res["reservations<br/>one row per order:<br/>reserved, rejected,<br/>released, cancelled"]
        outbox["outbox<br/>events not yet sent"]
    end

    subgraph out["Outputs (Kafka)"]
        e1[["inventory.reserved / rejected / released"]]
        e2[["inventory.stock-levels<br/>(compacted, key = SKU)"]]
        e3[["inventory.stock-low"]]
    end

    oc --> handlers
    pf --> handlers
    api --> store
    handlers --> store
    store -- "ONE transaction:<br/>change stock + record outcome<br/>+ write the events" --> pgdb
    outbox --> relay
    relay -- "publish in order,<br/>then mark as sent" --> out
    store -. "at startup: publish a snapshot<br/>of every SKU" .-> outbox

    classDef topic fill:#fff4dd,stroke:#d4a017,color:#222
    class e1,e2,e3,oc,pf topic
```

| Guarantee                  | How it is achieved                                                                                                                            |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| No lost or invented events | The stock change and its events commit together (transactional outbox). A crash can't leave one without the other                             |
| At-least-once publishing   | The relay marks rows as sent after Kafka acknowledges. A crash in between means a resend, with the same event id                              |
| Safe to process twice      | One `reservations` row per order records the outcome. Output event ids are derived from the input event id, so duplicates collapse downstream |
| No overselling             | Stock rows are locked in SKU order inside the transaction; a reservation is all-or-nothing                                                    |
| Order of changes per SKU   | Each change bumps `version`; consumers ignore a stock event older than what they have                                                         |
| Table-like topic           | `inventory.stock-levels` is log-compacted: only the newest record per SKU is kept, so a new consumer can rebuild the table from the beginning |

---

## 6. What every consumer does

Every service reads Kafka through the same wrapper
([`consumer.ts`](../packages/kafka-utils/src/consumer.ts)), so these rules apply everywhere.

```mermaid
flowchart TB
    msg(["Message arrives"]) --> decode{"Valid JSON,<br/>valid envelope,<br/>right type and version?"}
    decode -- "no: poison message" --> dlq[["write to &lt;topic&gt;.dlq<br/>(with error and original record)"]]
    decode -- "yes" --> seen{"eventId already<br/>processed by this group?"}
    seen -- "yes: duplicate" --> skip(["skip it"])
    seen -- "no" --> run["run the handler"]
    run -- "ok" --> mark["remember the eventId"]
    run -- "throws" --> retry{"retryable and<br/>attempts left?<br/>(max-retry-attempts flag)"}
    retry -- "yes" --> wait["wait: exponential backoff<br/>with jitter, keep heartbeating"]
    wait --> run
    retry -- "no" --> dlq
    mark --> commit(["commit the offset"])
    dlq --> dlqok{"DLQ write succeeded?"}
    dlqok -- "yes" --> commit
    dlqok -- "no" --> redeliver(["do NOT commit:<br/>Kafka redelivers, nothing is lost"])
    skip --> commit

    dlq -.-> tool["npm run dlq:replay<br/>republish with the same key and headers"]
    tool -.-> run

    classDef dead fill:#fde2e2,stroke:#c0392b,color:#222
    class dlq dead
```

---

## 7. Security

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller (browser, curl or tool)
    participant K as Keycloak
    participant S as A service (order, inventory or gateway)

    C->>K: get a token (login with PKCE, password grant, or client credentials)
    K-->>C: signed JWT (scopes, audience, subject)

    Note over S,K: At start-up and when a new key id appears,<br/>the service fetches Keycloak's public keys (JWKS) and caches them

    C->>S: request with Authorization Bearer token
    Note over S: Checked locally, on every request,<br/>with no call to Keycloak:<br/>1 signature, 2 issuer, audience and expiry,<br/>3 required scope, 4 ownership
    alt all checks pass
        S-->>C: 2xx response
    else no token, or token invalid
        S-->>C: 401 with WWW-Authenticate
    else valid token, but missing scope or not the owner
        S-->>C: 403
    end
```

| Who calls               | Keycloak client  | How it gets a token                                    |
| ----------------------- | ---------------- | ------------------------------------------------------ |
| A person in the web app | `orderflow-web`  | Authorization code flow with PKCE                      |
| A person with `curl`    | `orderflow-cli`  | Password grant (demo only, never for a real front end) |
| The load generator      | `load-generator` | Client credentials (a service identity)                |
| The DLQ replay operator | `dlq-replay`     | Client credentials, with the `admin` scope             |

| Scope             | Allows                                                                      | Where                      |
| ----------------- | --------------------------------------------------------------------------- | -------------------------- |
| `orders:write`    | `POST /orders`                                                              | order-service              |
| `orders:read`     | Read own orders (and see them on the stream)                                | order-service, gateway     |
| `inventory:read`  | `GET /inventory`; stock table and alerts on the stream                      | inventory-service, gateway |
| `inventory:write` | `POST /inventory/:sku/adjust`                                               | inventory-service          |
| `stream:read`     | Open `/stream`, `/snapshot`, `/flags` (the data scopes decide what you see) | gateway-service            |
| `admin`           | Read any order, replay DLQs                                                 | order-service, dlq-replay  |

Each scope adds its service to the token's `aud`, so a token meant for one service is
rejected by another. Events carry the user's identity (`actor`: `sub` and client) but **never
the token**. The gateway uses that `actor` to show each user only their own orders.

| Failure                                          | Response                          |
| ------------------------------------------------ | --------------------------------- |
| No token                                         | `401`, `WWW-Authenticate: Bearer` |
| Bad signature, expired, wrong issuer or audience | `401 invalid_token`               |
| Valid token, missing scope                       | `403 insufficient_scope`          |
| Valid token, someone else's order                | `403 access_denied`               |
| Keycloak's keys unreachable                      | `503` with `Retry-After`          |

---

## 8. Feature flags

```mermaid
flowchart LR
    ld{{"LaunchDarkly<br/>(when LD_SDK_KEY is set)"}}
    file[/"feature-flags.json<br/>watched, edit live"/]
    env[/"FLAG_* env vars"/]
    cli["npm run flag -- key value"]

    subgraph pkg["packages/feature-flags: one interface, two providers"]
        iface["FeatureFlags.get(key, context)<br/>never throws, falls back to a safe default"]
    end

    ld --> iface
    file --> iface
    env --> iface
    cli -- "edits" --> file

    subgraph backend["Backend flags (order and service contexts)"]
        f1["payment-failure-rate<br/>(chaos)"]
        f2["payment-consumer-enabled<br/>(kill switch)"]
        f3["fraud-check-enabled"]
        f4["notification-channel"]
        f5["max-retry-attempts<br/>(every consumer)"]
    end

    subgraph ui["Web app flags (user context)"]
        u1["live-updates-enabled"]
        u2["new-inventory-dashboard"]
        u3["bulk-adjust-enabled"]
        u4["activity-feed-size"]
    end

    iface --> backend
    iface --> gw["gateway-service<br/>evaluates for the signed-in user"]
    gw --> ui
    ui -- "GET /flags<br/>flags frame on /stream<br/>flags in /snapshot" --> web["React app<br/>(never sees an SDK key)"]
```

Every flag has a **safe default** that means normal, healthy behaviour. It is served
when LaunchDarkly is down, the flag is missing, or the value is invalid, so a flag outage
can never take the pipeline down. A flag change reaches running services within about a
second (local file) or as fast as LaunchDarkly streams it, and it reaches open browser
pages through the `flags` frame, without a reload.

---

## 9. Code structure

```mermaid
flowchart BT
    subgraph packages["packages (shared libraries)"]
        contracts["<b>contracts</b><br/>event schemas (zod), topic names,<br/>createEvent and decodeEvent"]
        kafkautils["<b>kafka-utils</b><br/>producer, consumer wrapper,<br/>retries, DLQ, idempotency"]
        flags["<b>feature-flags</b><br/>LaunchDarkly + local provider"]
        auth["<b>auth</b><br/>JWT verification (JWKS),<br/>Fastify plugin, scopes"]
        stream["<b>stream-types</b><br/>types of the SSE stream<br/>(no runtime code)"]
    end

    subgraph services["services"]
        subgraph http["HTTP services"]
            order["order-service"]
            inventory["inventory-service"]
            gateway["gateway-service"]
        end
        subgraph workers["worker services"]
            payment["payment-service"]
            notification["notification-service"]
        end
    end

    subgraph tools["tools"]
        load["load-generator"]
        replay["dlq-replay"]
    end

    web["<b>apps/web</b><br/>React dashboard"]

    kafkautils --> contracts
    flags --> contracts

    services -- "all five" --> kafkautils
    services -- "all five" --> flags
    http -- "verify tokens" --> auth
    tools --> kafkautils
    tools -- "client credentials" --> auth
    gateway --> stream
    web -- "types only" --> stream
```

Every service also depends on `contracts` (directly or through `kafka-utils`). The web
app imports only **types** from `stream-types`, so no server code ends up in the browser.

| Path                                                              | What to read first                                                                              |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [`packages/contracts`](../packages/contracts/src)                 | `topics.ts`, `events.ts`, `envelope.ts`: the whole vocabulary of the system                     |
| [`packages/kafka-utils`](../packages/kafka-utils/src)             | `consumer.ts` (`processMessage`): validation, idempotency, retries, DLQ                         |
| [`services/inventory-service`](../services/inventory-service/src) | `inventory.ts`, `outbox.ts`, `migrations.ts`                                                    |
| [`services/gateway-service`](../services/gateway-service/src)     | `hub.ts` (read model, visibility, resume), `app.ts` (SSE endpoint), `ui-flags.ts`               |
| [`apps/web`](../apps/web/src)                                     | `live/LiveProvider.tsx` (stream, polling, watchdog), `live/reducer.ts`, `auth/AuthProvider.tsx` |
| [`infra/keycloak`](../infra/keycloak/realm-export.json)           | The realm: clients, scopes, audiences, demo users                                               |
