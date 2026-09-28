# Demo script

A 30–40 minute walkthrough of the order pipeline. Each scenario lists what to run,
what to watch, and the point it makes. The scenarios build on each other, but you
can run any of them on its own after [Setup](#0-setup).

| #   | Scenario                                                                | Shows                                                |
| --- | ----------------------------------------------------------------------- | ---------------------------------------------------- |
| 1   | [Happy path](#1-happy-path)                                             | Keys, partitions, correlation, the order saga        |
| 2   | [Authentication and authorization](#2-authentication-and-authorization) | JWTs, scopes, ownership, 401 vs 403, actor in events |
| 3   | [Outage and lag catch-up](#3-outage-and-lag-catch-up)                   | Kill-switch flag, lag, catch-up, scaling out         |
| 4   | [Poison message → DLQ](#4-poison-message--dlq)                          | Validation, DLQ records, the operator tool           |
| 5   | [Chaos → retries → DLQ → replay](#5-chaos--retries--dlq--replay)        | Chaos flag, backoff, retry-count flag, replay        |
| 6   | [Duplicate delivery](#6-duplicate-delivery)                             | Idempotent consumers                                 |
| 7   | [Feature flags and LaunchDarkly](#7-feature-flags-and-launchdarkly)     | Fraud check, channels, targeting, LD outage          |

**Windows used below**

| Window  | Runs                                                                                 |
| ------- | ------------------------------------------------------------------------------------ |
| **T1**  | `npm run dev`: all four services                                                     |
| **T2**  | Commands (`curl`, `npm run load`, `npm run flag`, `npm run dlq:replay`)              |
| Browser | Kafka UI <http://localhost:8080>, Keycloak <http://localhost:8081> (`admin`/`admin`) |

The flags in this script are changed with `npm run flag -- <key> <value>`, which
edits `feature-flags.json`. Every service logs `feature flag changed` within about a
second, with no restart. If you've set `LD_SDK_KEY`, flip the flags in LaunchDarkly
instead (see [section 7](#7-feature-flags-and-launchdarkly)).

---

## 0. Setup

```bash
# T2 – Kafka (KRaft, no ZooKeeper), Kafka UI and Keycloak; waits until all are healthy
docker compose up -d --wait

# T1 – all four services (the first run also runs `npm install`)
npm run dev
```

Each service logs `joined consumer group` with its partition assignment, for
example `{"orders.created":[0,1,2]}`. It also logs `LD_SDK_KEY not set - using local
feature flags` with the current flag values.

Then define two shell helpers in **T2**. Access tokens live for 5 minutes; just run
the `ALICE=…` lines again when you get a `Token expired` 401.

```bash
KC=http://localhost:8081/realms/orderflow/protocol/openid-connect/token
jsonfield() { node -pe "JSON.parse(require('fs').readFileSync(0)).$1"; }
user_token() { curl -s -d grant_type=password -d client_id=orderflow-cli -d username=$1 -d password=$1 $KC | jsonfield access_token; }

ALICE=$(user_token alice)
BOB=$(user_token bob)
# The operator client (scope "admin") that dlq-replay also uses
ADMIN=$(curl -s -d grant_type=client_credentials -d client_id=dlq-replay -d client_secret=dlq-replay-demo-secret $KC | jsonfield access_token)
```

**Kafka UI → Topics.** Point out the five business topics (3 partitions each) and
one `.dlq` topic per business topic. The services created them at startup from
the definitions in `packages/contracts/src/topics.ts`.

To start over at any point: stop the services, run `npm run infra:down`,
`docker compose up -d --wait` and `npm run flag -- --reset`.

---

## 1. Happy path

**Goal:** follow one order end to end, then look at the partitions and keys.

```bash
curl -s -X POST localhost:3000/orders \
  -H "authorization: Bearer $ALICE" \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-happy-1' \
  -d '{"customerId":"alice","customerTier":"gold","country":"DE","items":[{"sku":"SKU-KEYBOARD","quantity":1,"unitPrice":79.99},{"sku":"SKU-MOUSE","quantity":2,"unitPrice":24.5}]}'
```

The response is `202 Accepted` with `"status":"PENDING"`: the API only records the
order and publishes an event. Copy the `id` and then:

```bash
curl -s localhost:3000/orders/<id> -H "authorization: Bearer $ALICE"
```

The order is now `CONFIRMED`, with `payment.status = COMPLETED`,
`inventory.status = RESERVED`, `createdBy = { sub, clientId: "orderflow-cli" }`, and a
`history` of the three events that got it there.

**Watch the logs:** search for `demo-happy-1`. The same `correlationId` appears in
every service: order created → `payment completed` and `inventory reserved` (in
parallel, in separate consumer groups) → `order CONFIRMED` → two notifications
(`[email] …`).

**Kafka UI → Topics → `orders.created` → Messages:**

- The **key** is the orderId. Every event for this order (on all five topics) has
  the same key, so it lands on the same partition number and stays in order.
- The **headers** include `event-id`, `event-type` and `correlation-id`.
- The **value** is the envelope: `eventId, type, version, occurredAt, correlationId, actor, data`.

Now some volume, including orders that fail for business reasons. The load
generator gets its own token with OAuth2 client credentials (client
`load-generator`):

```bash
npm run load -- -n 100 -r 20 --scenario mixed --wait
```

The summary shows mostly `CONFIRMED`, plus `CANCELLED` orders for
`Insufficient stock for SKU-GPU` and `Amount … exceeds card limit`. It also shows
end-to-end latency. A cancellation is a normal business outcome, not an error, so
nothing goes to a DLQ.

**Talking point:** order-service never calls payment or inventory directly. They
don't know about each other, and new consumers can be added without changing
any of them.

---

## 2. Authentication and authorization

**Goal:** every API call carries a JWT from Keycloak. order-service verifies it
locally (signature via JWKS, `iss`, `aud`, `exp`), then checks scopes and
ownership, and only identity (never the token) travels on in events.

**Look at a token** (paste it into <https://jwt.io>, or decode it inline):

```bash
echo $ALICE | cut -d. -f2 | base64 -d 2>/dev/null; echo
```

Point out `iss` (the realm), `aud: "order-service"`, `azp: "orderflow-cli"`,
`sub` (alice's user id), `scope: "orders:write orders:read"` and `exp`.

**The status codes:**

```bash
# 401 – no token. WWW-Authenticate: Bearer realm="orderflow"
curl -si -X POST localhost:3000/orders | head -5

# 401 – not a valid token (error="invalid_token")
curl -si localhost:3000/orders -H 'authorization: Bearer not.a.jwt' | grep -iE '^HTTP|www-auth'

# 403 – valid token, missing scope: the admin token has "admin" but not "orders:write"
curl -si -X POST localhost:3000/orders -H "authorization: Bearer $ADMIN" \
  -H 'content-type: application/json' -d '{}' | grep -iE '^HTTP|www-auth'
```

The last one returns
`WWW-Authenticate: Bearer realm="orderflow", error="insufficient_scope", scope="orders:write"`.
The difference matters: **401** means "we don't know who you are", **403** means "we
know, and you aren't allowed".

**Ownership:** alice creates an order, then bob tries to read it.

```bash
ORDER=$(curl -s -X POST localhost:3000/orders -H "authorization: Bearer $ALICE" \
  -H 'content-type: application/json' \
  -d '{"customerId":"alice","items":[{"sku":"SKU-MOUSE","quantity":1,"unitPrice":24.5}]}' | jsonfield id)

curl -s -o /dev/null -w "alice: %{http_code}\n" localhost:3000/orders/$ORDER -H "authorization: Bearer $ALICE"   # 200
curl -s -o /dev/null -w "bob:   %{http_code}\n" localhost:3000/orders/$ORDER -H "authorization: Bearer $BOB"     # 403 access_denied
curl -s -o /dev/null -w "admin: %{http_code}\n" localhost:3000/orders/$ORDER -H "authorization: Bearer $ADMIN"   # 200 (admin scope)
curl -s localhost:3000/orders -H "authorization: Bearer $BOB"                                                      # [] – bob only sees his own
```

**Scopes are assigned per client.** The load generator can't give itself admin:

```bash
curl -s -d grant_type=client_credentials -d client_id=load-generator \
  -d client_secret=load-generator-demo-secret -d scope=admin $KC
# {"error":"invalid_scope","error_description":"Invalid scopes: admin"}
```

**Identity in events, not tokens:** in Kafka UI, open any message on
`payments.completed`. The envelope has
`"actor": {"sub": "…", "clientId": "orderflow-cli"}`, copied from the order. There's
no token anywhere, and the `actor` schema is strict, so an event carrying extra
fields would be rejected into the DLQ.

**Bonus – Keycloak down:** run `docker compose stop keycloak`. Existing tokens keep
working, because order-service verifies them locally with the cached JWKS. New
tokens can't be issued, though, so `npm run load` fails with
`Cannot reach the identity provider`. Bring it back with
`docker compose start keycloak`.

---

## 3. Outage and lag catch-up

**Goal:** a consumer that stops loses nothing. Its work waits in Kafka as _lag_
and is processed when it comes back.

1. **Flip the kill switch.** No restart and no deploy:

   ```bash
   npm run flag -- payment-consumer-enabled false
   ```

   T1 shows `feature flag changed {"flag":"payment-consumer-enabled","from":true,"to":false}`
   in every service, then payment-service logs
   `consumer PAUSED - messages will queue up as lag`. The consumer stays in its
   group; it just stops fetching.

2. **Keep placing orders:**

   ```bash
   npm run load -- -n 50 -r 10
   curl -s 'localhost:3000/orders?limit=3' -H "authorization: Bearer $ADMIN"
   ```

   The orders are `PENDING` with `inventory.status = RESERVED`. Inventory kept
   working because it has its own consumer group; only payment is behind.

3. **Show the lag:**

   ```bash
   npm run lag
   ```

   `payment-service` has `LAG` > 0 on all three `orders.created` partitions.
   `inventory-service` has lag 0. In Kafka UI, **Consumers → payment-service**
   shows the same numbers.

4. **Resume:**

   ```bash
   npm run flag -- payment-consumer-enabled true
   ```

   `consumer RESUMED`, then a burst of `payment completed` lines followed by
   `order CONFIRMED`. Run `npm run lag` again and every lag is 0.

**Variant – a real crash:** stop T1 and run `npm run dev -- --skip payment` there
instead. Now payment-service is really gone: `npm run lag` shows no active members
for its group. Start it in another terminal with `npm run dev -- --only payment`
and watch it catch up from its committed offsets.

**Bonus – scaling out:** with everything running, start a second payment instance in
another terminal with `npm run dev -- --only payment`. Both instances log
`joined consumer group` after a rebalance, and the 3 partitions are split between
them (for example `[0,1]` and `[2]`). A fourth instance would sit idle: partitions
are the unit of parallelism.

---

## 4. Poison message → DLQ

**Goal:** one malformed message must not crash a consumer or block its
partition.

```bash
npm run load -- --poison 5
```

This writes five messages straight to `orders.created`, bypassing the API: plain
text, JSON that isn't an envelope, an envelope with an invalid payload, an
unsupported `version: 99`, and binary garbage.

**Watch T1:** payment-service and inventory-service each log
`invalid message -> DLQ` with a precise reason, such as
`Message value is not valid JSON` or `Unsupported version 99 for "order.created"`.
Neither retries: retrying bad data can't help.

```bash
npm run dlq:replay                     # depth per DLQ: orders.created.dlq = 10
npm run dlq:replay -- -t orders.created.dlq --dry-run --reason invalid-message
```

The listing starts with `🔐 authorized as "dlq-replay" (scope: admin)`. Replaying
into production topics is an operator action, so the tool gets a client-credentials
token and refuses to run without the `admin` scope. Try it with the wrong client:

```bash
DLQ_REPLAY_CLIENT_ID=load-generator DLQ_REPLAY_CLIENT_SECRET=load-generator-demo-secret \
  npm run dlq:replay -- -t orders.created.dlq --dry-run
# Client "load-generator" does not have the "admin" scope required to replay DLQs
```

There are 10 records because 5 messages × 2 consumer groups, and each group
dead-letters independently. Each record shows the group, the reason, the original
partition/offset and the error.

**Kafka UI → `orders.created.dlq` → Messages:** the value is a DLQ record with
`reason`, `error {name, message, stack}`, `attempts`, `consumerGroup`, and
`original {topic, partition, offset, key, headers, value}`. The value is stored
as base64 for the binary message. The headers `dlq-reason`, `dlq-error` and
`dlq-consumer-group` make records easy to filter.

Prove the pipeline is still healthy:

```bash
npm run load -- -n 10 --wait
```

All 10 orders are `CONFIRMED`. The bad messages were set aside, and their offsets
were committed.

---

## 5. Chaos → retries → DLQ → replay

**Goal:** transient failures are retried with exponential backoff. When retries
run out, the message is parked in the DLQ, and after the problem is fixed it is
replayed without side effects for services that already processed it.

1. **Turn chaos on** so every simulated gateway call fails:

   ```bash
   npm run flag -- payment-failure-rate 1
   ```

   payment-service logs `CHAOS ON: 100% of gateway calls will fail`. This flag
   replaces the old `PAYMENT_FAILURE_RATE` environment variable, and no restart is
   needed.

2. `npm run load -- -n 3 -r 1`

3. **Watch T1:** each message goes through
   `handler failed, retrying {attempt: 1, retryInMs: ~200}` → `~400` → `~800`, then
   `handler failed permanently -> DLQ` after 4 attempts. The retry count comes from
   the `max-retry-attempts` flag (default 3), and the delays from
   `CONSUMER_*_RETRY_MS`. Inventory reserved stock for all three orders as usual,
   so they stay `PENDING`.

   > Change the retry budget live: `npm run flag -- max-retry-attempts 1` and place
   > one more order. It is dead-lettered after 2 attempts. Set it back with
   > `npm run flag -- max-retry-attempts 3`.
   >
   > Try `npm run flag -- payment-failure-rate 0.5` as well. Most messages now
   > succeed on a retry (`attempt: 2` or `attempt: 3` on `payment completed`). Only
   > about 6% (0.5⁴) exhaust their retries.

4. **Inspect the DLQ:**

   ```bash
   npm run dlq:replay -- -t orders.created.dlq --dry-run --group payment-service
   ```

   Three `processing-failed by payment-service after 4 attempt(s) … PaymentGatewayError … | order PENDING`
   records. The tool used its admin token to look up each order's current status.
   (The poison records from scenario 4 are filtered out because they are
   `invalid-message`.)

5. **Fix the outage:** `npm run flag -- payment-failure-rate 0` (`chaos off`).

6. **Replay:**

   ```bash
   npm run dlq:replay -- -t orders.created.dlq --group payment-service
   ```

   The records are republished to `orders.created` with their original key, value
   and headers, plus `replayed-from`, `replay-count` and `replayed-by: dlq-replay`.

**Watch the logs:**

- payment-service: `payment completed` ×3, then order-service: `order CONFIRMED` ×3.
- inventory-service: `duplicate event skipped (already processed)` ×3. The replay
  went to a shared topic, so inventory saw the events again. It recognized their
  eventIds and did **not** reserve stock twice.

Run the replay command again and it prints `Nothing to replay`. The tool tracks
its progress in its own consumer group (`dlq-replay.orders.created.dlq`), so each
record is replayed once.

---

## 6. Duplicate delivery

**Goal:** Kafka gives consumers _at-least-once_ delivery, so every consumer must
be idempotent.

Duplicates happen in real life when a producer retries after a lost ack, or when
a consumer crashes after doing the work but before committing its offset. We
simulate the first case: order-service publishes the **exact same**
`orders.created` event (same `eventId`) a second time. The endpoint is allowed for
the order's owner or an admin.

```bash
ORDER=$(curl -s -X POST localhost:3000/orders -H "authorization: Bearer $ALICE" \
  -H 'content-type: application/json' \
  -d '{"customerId":"alice","items":[{"sku":"SKU-WEBCAM","quantity":1,"unitPrice":89}]}' | jsonfield id)

curl -s -X POST localhost:3000/orders/$ORDER/republish -H "authorization: Bearer $ALICE"
curl -s localhost:3000/orders/$ORDER -H "authorization: Bearer $ALICE"
```

**Watch the logs:**

- order-service: `re-published orders.created (duplicate delivery demo)`
- payment-service **and** inventory-service: `duplicate event skipped (already processed)`,
  with the same `eventId` as the first delivery.
- There is only one `payment completed` and one `inventory reserved` for this order.
  The customer isn't charged twice, stock is decremented once, and only one set of
  notifications goes out.

The order's `history` has exactly three entries. Kafka UI shows the event twice in
`orders.created`, at two different offsets with the same `event-id` header.

At scale:

```bash
npm run load -- -n 20 --duplicate --wait
```

All 20 are `CONFIRMED` even though 40 `orders.created` messages were written.

**Talking points: defense in depth**

1. The consumer wrapper skips any `eventId` the consumer group has already
   processed.
2. Services derive their _outgoing_ eventIds from the incoming one
   (`deriveEventId`). If a handler runs twice, for example after a crash before the
   offset commit, it re-emits the same eventIds, and the next hop de-duplicates
   them too.
3. The order state machine ignores an event it has already applied, and
   inventory keeps at most one reservation per order.

In this demo the processed-eventId store is in memory. In production it lives in
the service's database, written in the same transaction as the side effect.

---

## 7. Feature flags and LaunchDarkly

**Goal:** change behaviour at runtime, per order, without deploys, and stay safe
when the flag service is down. The flag reference is in [ld-flags.md](ld-flags.md).

**Fraud check.** Turn it on, then send a standard-tier and a gold-tier order for the
same laptop (1499 USD; the standard tier limit is 1000):

```bash
npm run flag -- fraud-check-enabled true
curl -s -X POST localhost:3000/orders -H "authorization: Bearer $ALICE" -H 'content-type: application/json' \
  -d '{"customerId":"alice","customerTier":"standard","items":[{"sku":"SKU-LAPTOP","quantity":1,"unitPrice":1499}]}'
curl -s -X POST localhost:3000/orders -H "authorization: Bearer $ALICE" -H 'content-type: application/json' \
  -d '{"customerId":"alice","customerTier":"gold","items":[{"sku":"SKU-LAPTOP","quantity":1,"unitPrice":1499}]}'
```

payment-service logs `fraud check {"passed":false}` for the standard order, which is
cancelled with `Fraud check failed: 1499 USD exceeds the standard tier limit of 1000`.
The gold order is confirmed. Orders from country `ZZ` (unknown) always fail the
check. Turn it off again with `npm run flag -- fraud-check-enabled false`.

**Notification channel:**

```bash
npm run flag -- notification-channel sms
npm run load -- -n 2 --wait
```

notification-service switches from `[email] Payment received …` to `[sms] …` on the
next message.

**With LaunchDarkly (optional).** Create the five flags as described in
[ld-flags.md](ld-flags.md#setting-up-launchdarkly), put your server-side SDK key in
`.env` (`LD_SDK_KEY=sdk-…`), and restart T1. Each service logs
`connected to LaunchDarkly`. Then:

- **Targeting by order attributes:** turn `fraud-check-enabled` on only for
  `customerTier` is `standard`, and run
  `npm run load -- -n 10 --tier standard --scenario mixed --wait` vs `--tier gold`.
  Or set `payment-failure-rate` to `1` only for `country` is `BR` and run
  `npm run load -- -n 5 --country BR`: only those orders hit retries and the DLQ.
- **Kill switch from the dashboard:** serve `false` for `payment-consumer-enabled`.
  Within a second payment-service logs `feature flag changed` and
  `consumer PAUSED`.
- **LaunchDarkly down:** stop T1, set `LD_SDK_KEY=sdk-invalid` (or cut the
  network), and start it again. Each service logs
  `LaunchDarkly unavailable - serving safe defaults until it connects`, and
  `npm run load -- -n 10 --wait` still confirms every order. Every flag has a safe
  default, so a flag outage never takes the pipeline down.

Clean up with `npm run flag -- --reset`.

---

## Cleanup

```bash
# Ctrl+C in T1
npm run flag -- --reset
npm run infra:down     # stops Kafka, Kafka UI and Keycloak and deletes the Kafka volume
```
