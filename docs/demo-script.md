# Demo script

A 20–30 minute walkthrough of the order pipeline. Each scenario lists what to run,
what to watch, and the point it makes. The scenarios build on each other, but you
can run any of them on its own after [Setup](#0-setup).

**Terminals used below**

| Terminal | Runs                                                         |
| -------- | ------------------------------------------------------------ |
| **T1**   | Order, inventory and notification services                   |
| **T2**   | payment-service by itself, so we can stop and reconfigure it |
| **T3**   | Commands (`curl`, load generator, DLQ tool)                  |
| Browser  | Kafka UI at <http://localhost:8080>                          |

> Tip: `npm run dev` with no flags runs all four services in one terminal. Two
> terminals are used here only so that payment-service can be stopped and
> restarted on its own.

---

## 0. Setup

```bash
# T3 – start Kafka (KRaft, no ZooKeeper) and Kafka UI; waits until healthy
docker compose up -d --wait

# T1 – everything except payment (the first run also runs `npm install`)
npm run dev -- --skip payment

# T2 – payment-service on its own
npm run dev -- --only payment
```

Each service logs `joined consumer group` with its partition assignment, for
example `{"orders.created":[0,1,2]}`.

**Kafka UI → Topics.** Point out the five business topics (3 partitions each) and
one `.dlq` topic per business topic. The services created them at startup from
the definitions in `packages/contracts/src/topics.ts`.

To start over at any point: stop the services, then run `npm run infra:down` and
`docker compose up -d --wait`.

---

## 1. Happy path

**Goal:** follow one order end to end, then look at the partitions and keys.

```bash
# T3
curl -s -X POST localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-happy-1' \
  -d '{"customerId":"alice","items":[{"sku":"SKU-KEYBOARD","quantity":1,"unitPrice":79.99},{"sku":"SKU-MOUSE","quantity":2,"unitPrice":24.5}]}'
```

The response is `202 Accepted` with `"status":"PENDING"`: the API only records the
order and publishes an event. Copy the `id` and then:

```bash
curl -s localhost:3000/orders/<id>
```

The order is now `CONFIRMED`, with `payment.status = COMPLETED`,
`inventory.status = RESERVED`, and a `history` of the three events that got it
there.

**Watch the logs:** search for `demo-happy-1`. The same `correlationId` appears in
every service: order created → `payment completed` and `inventory reserved` (in
parallel, in separate consumer groups) → `order CONFIRMED` → two notifications.

**Kafka UI → Topics → `orders.created` → Messages:**

- The **key** is the orderId. Every event for this order (on all five topics) has
  the same key, so it lands on the same partition number and stays in order.
- The **headers** include `event-id`, `event-type` and `correlation-id`.
- The **value** is the envelope: `eventId, type, version, occurredAt, correlationId, data`.

Now some volume, including orders that fail for business reasons:

```bash
npm run load -- -n 100 -r 20 --scenario mixed --wait
```

The summary shows mostly `CONFIRMED`, plus `CANCELLED` orders for
`Insufficient stock for SKU-GPU` and `Amount … exceeds card limit`. It also shows
end-to-end latency. A cancellation is a normal business outcome, not an error, so
nothing goes to a DLQ. (Set `PAYMENT_DECLINE_RATE=0.1` to add random card
declines as well.)

**Talking point:** order-service never calls payment or inventory directly. They
don't know about each other, and you could add a fraud-check service tomorrow
without changing any of them.

---

## 2. Outage and lag catch-up

**Goal:** a consumer that is down loses nothing. Its work waits in Kafka as
_lag_ and is processed when it comes back.

1. **T2:** press `Ctrl+C` to stop payment-service. It logs `shutting down` and
   leaves the consumer group cleanly, so the broker doesn't wait for a session
   timeout.
2. **T3:** keep placing orders:

   ```bash
   npm run load -- -n 50 -r 10
   curl -s 'localhost:3000/orders?limit=3'
   ```

   The orders are `PENDING` with `inventory.status = RESERVED`. Inventory kept
   working because it has its own consumer group; only payment is behind.

3. **Show the lag:**

   ```bash
   npm run lag
   ```

   `payment-service` has `LAG` > 0 on all three `orders.created` partitions and no
   active members. `inventory-service` has lag 0. In Kafka UI, go to
   **Consumers → payment-service** to see the same numbers.

4. **T2:** start payment-service again:

   ```bash
   npm run dev -- --only payment
   ```

   It resumes from its committed offsets and works through the backlog: a burst of
   `payment completed` lines, followed by `order CONFIRMED` in T1. Run `npm run lag`
   again and every lag is 0.

**Bonus – scaling out:** open a fourth terminal and run a second instance with
`npm run dev -- --only payment`. Both instances log `joined consumer group`
after a rebalance, and the 3 partitions are split between them (for example
`[0,1]` and `[2]`). A third instance would take the last partition, and a fourth
would sit idle: partitions are the unit of parallelism.

---

## 3. Poison message → DLQ

**Goal:** one malformed message must not crash a consumer or block its
partition.

```bash
npm run load -- --poison 5
```

This writes five messages straight to `orders.created`, bypassing the API: plain
text, JSON that isn't an envelope, an envelope with an invalid payload, an
unsupported `version: 99`, and binary garbage.

**Watch T1/T2:** payment-service and inventory-service each log
`invalid message -> DLQ` with a precise reason, such as
`Message value is not valid JSON` or `Unsupported version 99 for "order.created"`.
Neither retries: retrying bad data can't help.

```bash
npm run dlq:replay                     # depth per DLQ: orders.created.dlq = 10
npm run dlq:replay -- -t orders.created.dlq --dry-run --reason invalid-message
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

## 4. Chaos → retries → DLQ → replay

**Goal:** transient failures are retried with exponential backoff. When retries
run out, the message is parked in the DLQ, and after the problem is fixed it is
replayed without side effects for services that already processed it.

1. **T2:** `Ctrl+C`, then restart payment-service with the chaos toggle on. Every
   simulated gateway call will fail:

   ```bash
   PAYMENT_FAILURE_RATE=1 npm run dev -- --only payment
   ```

   It logs `CHAOS ON: 100% of gateway calls will fail`.

   (On Windows, or to keep it permanent, put `PAYMENT_FAILURE_RATE=1` in `.env`
   instead.)

2. **T3:** `npm run load -- -n 3 -r 1`

3. **Watch T2:** each message goes through
   `handler failed, retrying {attempt: 1, retryInMs: ~200}` → `~400` → `~800`, then
   `handler failed permanently -> DLQ` after 4 attempts. The delays double, with
   jitter, and are set by `CONSUMER_*_RETRY_MS` and `CONSUMER_MAX_RETRIES`.
   Inventory reserved stock for all three orders as usual, so they stay `PENDING`.

   > Try `PAYMENT_FAILURE_RATE=0.5` as well. Most messages now succeed on a retry,
   > which you can see as `attempt: 2` or `attempt: 3` on `payment completed`. Only
   > about 6% (0.5⁴) exhaust their retries.

4. **Inspect the DLQ:**

   ```bash
   npm run dlq:replay -- -t orders.created.dlq --dry-run --group payment-service
   ```

   Three `processing-failed by payment-service after 4 attempt(s) … PaymentGatewayError`
   records. (The poison records from scenario 3 are filtered out because they are
   `invalid-message`.)

5. **Fix the outage.** **T2:** `Ctrl+C`, then `npm run dev -- --only payment`
   (chaos off).

6. **Replay:**

   ```bash
   npm run dlq:replay -- -t orders.created.dlq --group payment-service
   ```

   The records are republished to `orders.created` with their original key, value
   and headers, plus `replayed-from` and `replay-count` headers.

**Watch the logs:**

- payment-service: `payment completed` ×3, then order-service: `order CONFIRMED` ×3.
- inventory-service: `duplicate event skipped (already processed)` ×3. The replay
  went to a shared topic, so inventory saw the events again. It recognized their
  eventIds and did **not** reserve stock twice.

Run the replay command again and it prints `Nothing to replay`. The tool tracks
its progress in its own consumer group (`dlq-replay.orders.created.dlq`), so each
record is replayed once.

---

## 5. Duplicate delivery

**Goal:** Kafka gives consumers _at-least-once_ delivery, so every consumer must
be idempotent.

Duplicates happen in real life when a producer retries after a lost ack, or when
a consumer crashes after doing the work but before committing its offset. We
simulate the first case: order-service publishes the **exact same**
`orders.created` event (same `eventId`) a second time.

```bash
ORDER=$(curl -s -X POST localhost:3000/orders -H 'content-type: application/json' \
  -d '{"customerId":"dave","items":[{"sku":"SKU-WEBCAM","quantity":1,"unitPrice":89}]}' \
  | node -pe 'JSON.parse(require("fs").readFileSync(0)).id')

curl -s -X POST localhost:3000/orders/$ORDER/republish
curl -s localhost:3000/orders/$ORDER
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

## Cleanup

```bash
# Ctrl+C in T1 and T2
npm run infra:down     # stops Kafka and Kafka UI and deletes the Kafka volume
```
