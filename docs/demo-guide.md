# Demo guide

How to present this project, and the concepts it teaches. It answers three questions:

1. **What is this, in one minute?** ([The pitch](#the-pitch))
2. **How do I run a demo?** ([Before you start](#before-you-start), [Pick a track](#pick-a-track))
3. **Which concepts does it show, where do they live in the code, and how do I show them?**
   ([Concept map](#concept-map) and [The concepts in plain language](#the-concepts-in-plain-language))

Related pages: [demo-script.md](demo-script.md) has the exact commands for every scenario,
[architecture.md](architecture.md) has the diagrams, and [ld-flags.md](ld-flags.md) covers
feature flags in depth.

---

## The pitch

> Customers place orders in a web page. Behind it, five small services never call each
> other. They only publish and read **events on Kafka**: payment, stock and notifications
> all react to the same "order created" event. Stock lives in a real database, and a gateway
> pushes every change to the browser in real time, so the page updates the moment something
> happens. Everything is protected by a standard login, and behaviour can be changed at
> runtime with **feature flags**, without a deploy.

What makes it more than a toy: it has the parts real systems need and tutorials usually skip.
Retries with backoff, dead-letter queues and replay, duplicate protection, an outbox so the
database and Kafka never disagree, per-user security, and a front end that survives dropped
connections.

**Three ideas to leave people with:**

1. **Events decouple services.** Adding a new consumer needs no change to the producer.
2. **Reliability is designed in.** Every message is validated, de-duplicated, retried and, if
   it still fails, parked in a dead-letter topic where nothing is lost.
3. **Real-time is a pipeline, not a trick.** Kafka → read model → stream → UI, with state and
   events handled differently on purpose.

---

## Before you start

**You need:** Node.js 22.12 or newer, and Docker. (If `docker compose` is not available on your
machine but `docker-compose` is, use that in every command in these docs.)

```bash
docker compose up -d --wait     # Kafka, Kafka UI, Keycloak, Postgres
npm run dev                     # four services + gateway + web app (installs on first run)
```

| What           | Where                                              | Login                                 |
| -------------- | -------------------------------------------------- | ------------------------------------- |
| **Web app**    | <http://localhost:5173>                            | `alice` / `alice`, or `bob` / `bob`   |
| Kafka UI       | <http://localhost:8080>                            | none                                  |
| Keycloak admin | <http://localhost:8081>                            | `admin` / `admin`                     |
| Postgres       | `docker exec orderflow-postgres psql -U orderflow` | none                                  |
| APIs           | order `:3000`, inventory `:3001`, gateway `:3002`  | Bearer token (see the script's setup) |

**A good setup for the room:** the web app on the main screen, Kafka UI and a terminal
(T2) on a second window or screen. Open a private browser window for `bob`, so you can show
two users side by side.

**Reset to a clean state** between demos (stock goes back to its seed values):

```bash
# stop npm run dev (Ctrl+C), then
npm run infra:down && docker compose up -d --wait
npm run flag -- --reset
npm run dev
```

**Things that trip people up:**

| Symptom                                   | Cause and fix                                                                                           |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `401 Token expired` in `curl`             | Access tokens last 5 minutes. Run the `ALICE=…` line from the script's setup again                      |
| Web app says "Sign-in failed"             | Keycloak is not up yet or was recreated. Wait for it to be healthy and click _Try again_                |
| `address already in use` on 5173 or 3000+ | Another copy of the stack is running. Stop it first (the web app needs exactly port 5173 for the login) |
| Stock numbers differ from the script      | The script assumes a fresh state (`SKU-WEBCAM` = 10, `SKU-GPU` = 0). Reset as above                     |
| Header shows **Polling**                  | The `live-updates-enabled` flag is off. `npm run flag -- --reset`                                       |
| Kafka UI looks empty                      | Topics are created when the services start; wait for `npm run dev` to print `joined consumer group`     |

---

## Pick a track

Scenario numbers refer to [demo-script.md](demo-script.md).

| Time          | Audience                                   | Run these scenarios                                                                    | Story                                                       |
| ------------- | ------------------------------------------ | -------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **15 min**    | Managers, product people, first look       | 8 (dashboard, including the declined order) and 10 (flags)                             | "Watch a business event flow through the system, live"      |
| **30 min**    | Developers new to Kafka                    | 1 (happy path), 4 (poison → DLQ), 6 (duplicates), 8 (dashboard)                        | "Events, partitions, and what happens when things go wrong" |
| **60 min**    | Engineers who will build something similar | 1, 3, 4, 5, 6, 8, 9, 10                                                                | "A production-minded event-driven system, end to end"       |
| **Deep dive** | Security or platform teams                 | 2 (auth), 7 (flags, LaunchDarkly), 9 (outbox), plus [architecture.md](architecture.md) | "Identity, configuration and consistency"                   |

**Rhythm that works:** show the _working_ thing first (a live order), then _break_ something
(kill-switch flag, poison message, restart a service), then show how the system recovers.
Each scenario in the script follows that shape. Say the concept's name out loud when it
appears, so people can look it up later.

---

## Concept map

Each row: what the idea is, where to find it, and where to show it.

### Messaging and events

| Concept                       | In one line                                                                          | Where in the code                                                                                                                                            | Show it in |
| ----------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| **Event-driven architecture** | Services react to events instead of calling each other                               | All services; [`topics.ts`](../packages/contracts/src/topics.ts)                                                                                             | 1          |
| **Choreography saga**         | A business process (order → payment + stock → confirm/cancel) without a central boss | [`order.ts`](../services/order-service/src/order.ts) state machine                                                                                           | 1, 8       |
| **Topics, partitions, keys**  | Same key → same partition → events for one order stay in order                       | [`producer.ts`](../packages/kafka-utils/src/producer.ts), topic table in the README                                                                          | 1          |
| **Consumer groups**           | Each service gets every event; more instances share the work                         | `ConsumerGroups` in `topics.ts`, [`consumer.ts`](../packages/kafka-utils/src/consumer.ts)                                                                    | 3          |
| **Consumer lag**              | How far behind a consumer is; builds up while it is paused                           | `npm run lag`, the `payment-consumer-enabled` flag                                                                                                           | 3          |
| **Event envelope and schema** | Every message has the same wrapper; invalid ones are rejected (zod)                  | [`envelope.ts`](../packages/contracts/src/envelope.ts), [`events.ts`](../packages/contracts/src/events.ts), [`codec.ts`](../packages/contracts/src/codec.ts) | 1, 4       |
| **Correlation id**            | One id follows a request through every service and log                               | `x-correlation-id` header → `correlationId` in every event                                                                                                   | 1          |
| **Versioned events**          | An unknown version is rejected instead of half-understood                            | `version` in the envelope, `decodeEvent`                                                                                                                     | 4          |

### Reliability

| Concept                     | In one line                                                               | Where in the code                                                                                                       | Show it in |
| --------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------- |
| **At-least-once delivery**  | Kafka may deliver a message twice, so consumers must cope                 | [`consumer.ts`](../packages/kafka-utils/src/consumer.ts) (`processMessage`)                                             | 6          |
| **Idempotent consumers**    | Processing the same event twice has the same effect as once               | [`idempotency.ts`](../packages/kafka-utils/src/idempotency.ts), `deriveEventId` in `codec.ts`, the `reservations` table | 6, 9       |
| **Retries with backoff**    | Transient failures are retried, waiting longer each time, with jitter     | [`backoff.ts`](../packages/kafka-utils/src/backoff.ts)                                                                  | 5          |
| **Dead-letter queue (DLQ)** | A message that can't be processed is parked with its error, never dropped | `publishToDlq` in `consumer.ts`                                                                                         | 4, 5       |
| **DLQ replay**              | An operator re-sends parked messages after the cause is fixed             | [`tools/dlq-replay`](../tools/dlq-replay/src)                                                                           | 5          |
| **Poison messages**         | Malformed input goes straight to the DLQ (retrying can't help)            | `npm run load -- --poison 5`                                                                                            | 4          |
| **Graceful shutdown**       | Consumers leave their group cleanly so partitions move at once            | [`shutdown.ts`](../packages/kafka-utils/src/shutdown.ts)                                                                | 3, 8       |
| **Chaos engineering**       | Inject failures on purpose to prove the safety nets work                  | `payment-failure-rate` flag                                                                                             | 5          |

### Data consistency

| Concept                            | In one line                                                                   | Where in the code                                                                                                            | Show it in |
| ---------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------- |
| **Transactional outbox**           | Change data and record the event in one DB transaction; publish afterwards    | [`outbox.ts`](../services/inventory-service/src/outbox.ts), [`inventory.ts`](../services/inventory-service/src/inventory.ts) | 9          |
| **Compensation**                   | Undo a step when a later one fails (release stock when payment fails)         | `release()` in `inventory.ts`, the `payments.failed` handler                                                                 | 8, 9       |
| **Log compaction / changelog**     | A topic that keeps only the newest record per key, so it behaves like a table | `inventory.stock-levels` (see `TOPIC_CONFIG` in `topics.ts`)                                                                 | 9          |
| **Optimistic ordering by version** | A per-SKU counter lets consumers ignore stale updates                         | `version` on stock events, `applyStock` in [`hub.ts`](../services/gateway-service/src/hub.ts)                                | 9          |
| **Row locking, no overselling**    | Stock rows are locked in order inside the transaction                         | `lockStock` in `inventory.ts`                                                                                                | 9          |
| **Schema migrations**              | Database changes are versioned and run once                                   | [`migrations.ts`](../services/inventory-service/src/migrations.ts)                                                           | 9          |

### Real-time to the browser

| Concept                          | In one line                                                                       | Where in the code                                                                                      | Show it in |
| -------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ---------- |
| **Read model (CQRS)**            | Writes go to the order and inventory APIs; reads come from a separate, live model | [`hub.ts`](../services/gateway-service/src/hub.ts)                                                     | 8          |
| **Server-Sent Events**           | One-way push over plain HTTP, with automatic reconnect                            | [`app.ts`](../services/gateway-service/src/app.ts), [`sse.ts`](../services/gateway-service/src/sse.ts) | 8          |
| **State vs events**              | State (stock) is sent whole on connect; events (feed) are replayed from a buffer  | `snapshot()` in `hub.ts`                                                                               | 8          |
| **Resume with `Last-Event-ID`**  | A reconnecting client gets exactly what it missed                                 | `parseLastEventId` in `hub.ts`                                                                         | 8          |
| **Per-instance consumer groups** | Every gateway instance needs every event, so groups are not shared                | `index.ts` of gateway-service                                                                          | 8          |
| **Heartbeats and a watchdog**    | The server pings; the page reconnects if it hears nothing for 45 s                | [`LiveProvider.tsx`](../apps/web/src/live/LiveProvider.tsx)                                            | 8          |
| **Backpressure**                 | A client that falls too far behind is dropped and resumes later                   | `maxBufferedBytes` in `app.ts`                                                                         | –          |
| **Reducer-based UI state**       | All stream frames go through one pure function that is easy to test               | [`reducer.ts`](../apps/web/src/live/reducer.ts)                                                        | –          |

### Security

| Concept                            | In one line                                                               | Where in the code                                                        | Show it in |
| ---------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ---------- |
| **OAuth2 / OpenID Connect**        | Log in with an identity provider, call APIs with its tokens               | [`realm-export.json`](../infra/keycloak/realm-export.json)               | 2, 8       |
| **Authorization code + PKCE**      | The safe login flow for browser apps: no secret in the page               | [`AuthProvider.tsx`](../apps/web/src/auth/AuthProvider.tsx)              | 8          |
| **JWT verified locally (JWKS)**    | Services check the signature with cached public keys; no call per request | [`verifier.ts`](../packages/auth/src/verifier.ts)                        | 2          |
| **Scopes and audiences**           | What a token may do, and which service it is for                          | [`scopes.ts`](../packages/auth/src/scopes.ts), the realm's client scopes | 2          |
| **Ownership checks**               | You can only read your own orders (unless admin)                          | `assertCanAccess` in [`app.ts`](../services/order-service/src/app.ts)    | 2, 8       |
| **Identity in events, not tokens** | Events carry who acted (`actor`), never the token itself                  | `actorSchema` in `envelope.ts`                                           | 2          |
| **401 vs 403**                     | 401: who are you? 403: you may not do that                                | `sendAuthError` in [`plugin.ts`](../packages/auth/src/plugin.ts)         | 2          |
| **Service identities**             | Tools authenticate as themselves with client credentials                  | `ClientCredentialsTokenProvider`                                         | 2, 5       |

### Feature flags

| Concept                            | In one line                                                                     | Where in the code                                                                                                                    | Show it in |
| ---------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| **Runtime configuration**          | Change behaviour without a deploy; running services notice within a second      | [`local.ts`](../packages/feature-flags/src/local.ts), `npm run flag`                                                                 | 7, 10      |
| **Kill switch**                    | Pause a consumer live; lag builds up; resume to catch up                        | `payment-consumer-enabled`                                                                                                           | 3          |
| **Safe defaults**                  | A flag outage can never take the system down                                    | [`definitions.ts`](../packages/feature-flags/src/definitions.ts), [`launchdarkly.ts`](../packages/feature-flags/src/launchdarkly.ts) | 7          |
| **Targeting**                      | Different values for different orders or users (needs LaunchDarkly)             | `order` and `user` contexts                                                                                                          | 7, 10      |
| **UI flags served by the backend** | The browser never holds an SDK key; the gateway evaluates per user              | [`ui-flags.ts`](../services/gateway-service/src/ui-flags.ts)                                                                         | 10         |
| **Graceful degradation**           | Switching `live-updates-enabled` off moves every page from streaming to polling | `LiveProvider.tsx`                                                                                                                   | 10         |

### Engineering practice

| Concept                              | In one line                                                                                | Where in the code                                                                     |
| ------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| **Monorepo with shared packages**    | One repo, npm workspaces, small libraries for the shared parts                             | `packages/`, [`architecture.md`](architecture.md#9-code-structure)                    |
| **Types shared without code**        | The browser imports only types from the gateway's contract                                 | [`packages/stream-types`](../packages/stream-types/src/index.ts)                      |
| **Tests without infrastructure**     | Unit tests need no Kafka; an in-process Postgres (PGlite) tests the real SQL               | `*.test.ts`, [`test-db.ts`](../services/inventory-service/src/test-db.ts), `npm test` |
| **CI with an end-to-end smoke test** | Real Kafka and Keycloak in CI; checks auth, saga, duplicates, DLQ and the stream           | [`ci.yml`](../.github/workflows/ci.yml)                                               |
| **Structured logging**               | pino logs with ids on every line (readable in development, `LOG_FORMAT=json` for machines) | [`logger.ts`](../packages/kafka-utils/src/logger.ts)                                  |

---

## The concepts in plain language

For people who are new to the ideas. Each has a sentence you can say out loud.

**Event.** A fact that already happened: "order 42 was created". It is not a request. Because it
is a fact, any number of services can react to it and none of them can "say no" to it.

**Topic, partition, key.** A topic is a named log of events. It is split into partitions so it can
be read in parallel. The key decides the partition: all events with the same key go to the same
one, in order. _"We key by order id, so everything about one order is processed in order."_

**Consumer group.** A group is a team that shares a topic: each partition goes to one member. A
different group gets its own full copy. _"Payment and inventory each read every order, because
they are different groups. If we run two payment instances, they split the orders between them."_

**At-least-once and idempotency.** After a crash Kafka re-sends what wasn't confirmed, so a
consumer must be safe to run twice on the same event. Here every event has an id, and each
service remembers what it handled. Inventory also stores one row per order, so even a replayed
event can't reserve stock twice. _"Duplicates aren't a bug to prevent, they're a fact to handle."_

**Dead-letter queue.** Where a message goes when it can't be processed (it is malformed, or still
fails after all retries). It keeps the original message and the error. Because nothing is dropped,
an operator can fix the cause and replay. _"Never lose a message, never block the line."_

**Backoff with jitter.** When a call fails, wait before retrying, and wait longer each time, with
a little randomness so many consumers don't retry in lock-step.

**Transactional outbox.** The classic trap: update the database, then publish to Kafka. A crash
in between leaves them out of sync. Instead the event is written to an `outbox` table _in the same
transaction_ as the change, and a relay publishes it afterwards. _"Either both happened or neither
did."_

**Compaction.** Normally Kafka keeps events for a time. A compacted topic keeps only the newest
record per key, forever. So `inventory.stock-levels` is effectively a table: read it from the start
and you have the current stock of every SKU. _"A topic that acts like a table."_

**State vs events.** Some data is a current value (stock level), some is a history (what
happened). Sending the current value in full is simple and can't miss anything; a history needs a
buffer and a way to resume. This project uses each where it fits.

**Server-Sent Events.** The browser opens one HTTP request and the server keeps writing to it.
Compared with WebSockets it is one-way, works through ordinary proxies, and reconnects by itself,
which is exactly what a live dashboard needs. Commands still go through normal REST calls.

**OAuth2, OIDC, JWT, PKCE.** The user logs in at Keycloak (the identity provider), never at our
app. Keycloak gives the app a signed token (a JWT). Each service verifies the signature itself
with Keycloak's public keys, so there is no call to Keycloak per request. PKCE lets a browser app
do this safely without a client secret.

**Scope and audience.** A scope is a permission (`orders:write`). The audience says which service
the token is for. A token for the order service is rejected by the inventory service.

**Feature flag.** A setting you can change while the system runs. Types seen here: a _kill
switch_ (pause payments), a _chaos knob_ (fail 30% of payments), a _rollout_ (new UI for one
person first), and a _degradation lever_ (stop streaming, start polling). The golden rule here:
every flag has a safe default, so losing the flag service changes nothing.

---

## Questions you may get

| Question                                                     | Answer                                                                                                                                                                                  |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Why Kafka and not a simple queue?                            | Kafka keeps events (replay, new consumers can read history), keeps order per key, and lets several services each read everything. A queue deletes a message once one consumer takes it. |
| Is this exactly-once?                                        | No, and it says so. Delivery is at-least-once; _processing_ is made effectively-once by idempotent consumers and the outbox.                                                            |
| What if the gateway runs on several machines?                | Each instance has its own consumer groups, so each sees every event and serves its own clients. A user is served by whichever instance their connection reached.                        |
| What happens to the activity feed when the gateway restarts? | It starts empty (it is held in memory). Stock is rebuilt from the compacted topic. Persisting the feed would be a small change if it mattered.                                          |
| Why not call the payment service directly?                   | Then order-service would need to know about it, wait for it, and fail when it is down. With events, a slow or missing consumer just builds up lag and catches up later.                 |
| Can a token leak into Kafka?                                 | No. Events carry the user's identity (`sub`, client) only. The event schema is strict, so an extra field such as a token is rejected.                                                   |
| Why is order data not in a database?                         | Deliberate simplification to keep the demo small: orders are in memory in order-service. Inventory (the interesting part) is in Postgres. See the README's simplifications.             |
| Is this production-ready?                                    | The patterns are; the setup is not (single Kafka node, Keycloak in dev mode, demo secrets, in-memory orders). The README lists every shortcut.                                          |
| Why did you drop Auth0?                                      | It is a hosted proprietary service. Keycloak is open source and runs offline, which keeps the demo and CI self-contained. The login code sits behind one interface if that changes.     |

---

## Cheat sheet

```bash
# Start / stop
docker compose up -d --wait         # infrastructure
npm run dev                         # everything (add: -- --skip web, or -- --only payment)
npm run infra:down                  # stop infrastructure and wipe all data

# Generate traffic
npm run load -- -n 100 -r 20 --scenario mixed --wait   # orders, with business failures
npm run load -- -n 5 --duplicate --wait                # every event published twice
npm run load -- --poison 5                             # malformed messages
npm run lag                                            # consumer group lag

# Flags (take effect in about a second)
npm run flag                                           # show
npm run flag -- payment-consumer-enabled false         # pause payments (kill switch)
npm run flag -- payment-failure-rate 0.5               # chaos
npm run flag -- new-inventory-dashboard true           # web app: cards
npm run flag -- live-updates-enabled false             # web app: polling
npm run flag -- --reset

# Dead letters
npm run dlq:replay                                     # counts per DLQ
npm run dlq:replay -- -t orders.created.dlq --dry-run --reason all

# Restart one service (the dev runner watches the source)
touch services/inventory-service/src/index.ts

# Look inside Postgres
docker exec orderflow-postgres psql -U orderflow -c "select sku, available, version from stock order by sku"
```

**Which order states mean what:** `PENDING` (waiting for payment and stock), `CONFIRMED` (both
succeeded), `CANCELLED` (either failed). Cancellations for `Insufficient stock for SKU-GPU` and
`exceeds card limit` are normal business outcomes, not errors, so they never reach a DLQ.

**Business rules you can trigger on purpose:** `SKU-GPU` is always out of stock; orders over
2000 are declined by payment; `SKU-WEBCAM` runs out quickly (10 in stock); with the fraud flag on,
a standard-tier order over 1000 is declined.
