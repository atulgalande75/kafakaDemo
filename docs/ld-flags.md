# Feature flags

The services read five feature flags through the `FeatureFlags` interface in
[`packages/feature-flags`](../packages/feature-flags/src). There are two providers
behind it:

| Provider         | Used when                       | Where values come from                                                    |
| ---------------- | ------------------------------- | ------------------------------------------------------------------------- |
| **LaunchDarkly** | `LD_SDK_KEY` is set             | Your LaunchDarkly project (server-side SDK, streaming updates)            |
| **Local**        | `LD_SDK_KEY` is empty (default) | `feature-flags.json` (watched for edits) + `FLAG_*` environment variables |

Tests, CI and a fresh clone all use the local provider. No LaunchDarkly account or
secret is needed to run anything in this repo.

## The flags

| Key                        | Type    | Safe default | Used by                  | What it does                                                                                                                                  |
| -------------------------- | ------- | ------------ | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `payment-failure-rate`     | number  | `0`          | payment-service          | **Chaos.** Probability (0–1) that the simulated gateway throws a transient error. Retried, then DLQ'd.                                        |
| `payment-consumer-enabled` | boolean | `true`       | payment-service          | **Kill switch.** `false` pauses the payment consumer (it stays in its group; lag builds up). `true` resumes it.                               |
| `fraud-check-enabled`      | boolean | `false`      | payment-service          | Adds a fraud check before charging: amount over the tier limit (standard 1000, gold 5000, platinum none) or country `ZZ` → `payments.failed`. |
| `notification-channel`     | string  | `"email"`    | notification-service     | `email`, `sms`, `push` or `slack`. Shown in every notification log line.                                                                      |
| `max-retry-attempts`       | number  | `3`          | every consumer (wrapper) | Integer 0–10. Retries after the first attempt before a message goes to `<topic>.dlq`.                                                         |

`payment-failure-rate` replaces the old `PAYMENT_FAILURE_RATE` environment
variable, and `max-retry-attempts` replaces `CONSUMER_MAX_RETRIES`. payment-service
logs a warning if `PAYMENT_FAILURE_RATE` is still set.

### Safe defaults and failure modes

Every flag's default describes normal, healthy behaviour: no chaos, consumer on,
no fraud check, email, 3 retries. The default is served whenever:

- `LD_SDK_KEY` is not set and nothing overrides the flag locally;
- LaunchDarkly is unreachable at start-up. Services wait up to 5 seconds, log
  `LaunchDarkly unavailable - serving safe defaults until it connects`, and start
  anyway;
- LaunchDarkly rejects the key (for example, a 401/403 for a wrong key or a
  client-side ID);
- the flag doesn't exist in LaunchDarkly yet;
- a value has the wrong type or is out of range (for example, a failure rate of
  `7`). A warning is logged.

If the connection drops after start-up, the SDK keeps serving the last values it
received. `get()` never throws.

### Change logging

Both providers log every change:

```
INFO: feature flag changed {"flag":"payment-consumer-enabled","from":true,"to":false,"provider":"local"}
```

With LaunchDarkly, changes arrive over the SDK's streaming connection within a
second or so. The logged `from`/`to` are evaluated for the service's own context.
If only the order-level targeting rules changed, the log says `feature flag updated
(targeting rules changed)`.

## Contexts

Most flags are evaluated **per order**, so LaunchDarkly can target by customer
attributes. Service-wide switches use a service context.

| Context kind | Key          | Attributes                | Used for                                                                                                                                                        |
| ------------ | ------------ | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `order`      | orderId      | `customerTier`, `country` | `payment-failure-rate`, `fraud-check-enabled`, `max-retry-attempts` (on `orders.created`), `notification-channel` and `max-retry-attempts` elsewhere (key only) |
| `service`    | service name | –                         | `payment-consumer-enabled`, start-up snapshot and change logs                                                                                                   |

`customerTier` (`standard` / `gold` / `platinum`) and `country` (ISO code) are part
of the order: `POST /orders` accepts them, and they travel in `orders.created`.
Outcome events carry only the orderId, so notification-service evaluates with the
orderId as the key and no attributes. Use percentage rollouts or individual
targets there rather than tier rules.

## Setting up LaunchDarkly

1. In a LaunchDarkly project and environment, create the five flags with **exactly**
   these keys and types:

   | Key                        | Flag type | Variations to create            | Off / fallthrough |
   | -------------------------- | --------- | ------------------------------- | ----------------- |
   | `payment-failure-rate`     | Number    | `0`, `0.3`, `1`                 | `0`               |
   | `payment-consumer-enabled` | Boolean   | `true`, `false`                 | `true`            |
   | `fraud-check-enabled`      | Boolean   | `true`, `false`                 | `false`           |
   | `notification-channel`     | String    | `email`, `sms`, `push`, `slack` | `email`           |
   | `max-retry-attempts`       | Number    | `0`, `1`, `3`, `5`              | `3`               |

   Make each flag's "off" variation its safe default, so turning a flag off is
   always the safe move.

2. Copy the environment's **server-side SDK key** (it starts with `sdk-`; not the
   client-side ID or the mobile key) into `.env`:

   ```bash
   LD_SDK_KEY=sdk-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   ```

3. Restart `npm run dev`. Each service logs `using LaunchDarkly (LD_SDK_KEY is set)`,
   then `connected to LaunchDarkly` and a snapshot of the flag values.

The two context kinds, `order` and `service`, appear in LaunchDarkly automatically
the first time they're evaluated.

### Targeting ideas for the demo

- **Fraud checks only where it matters:** turn `fraud-check-enabled` on with a rule
  "context kind `order`, `customerTier` is one of `standard`". Gold and platinum
  customers skip the check.
- **Chaos for one country:** set `payment-failure-rate` to `1` for `order` contexts
  where `country` is `BR`. Only Brazilian orders go through retries and into the DLQ
  (`npm run load -- --country BR`).
- **Gentler retries for VIPs:** serve `max-retry-attempts = 5` to
  `customerTier = platinum`.
- **Channel rollout:** roll `notification-channel = sms` out to 25% of orders.
- **Kill switch:** target the `service` context `payment-service` and serve `false`
  for `payment-consumer-enabled` to pause payments without a deploy.

## Local provider

Without `LD_SDK_KEY`, values come from these layers (later layers win):

1. safe defaults (above)
2. `feature-flags.json` in the repo root (or `FEATURE_FLAGS_FILE`), checked every second
3. `FLAG_<KEY>` environment variables, for example `FLAG_PAYMENT_FAILURE_RATE=0.5` or
   `FLAG_NOTIFICATION_CHANNEL=sms`

Edit `feature-flags.json` while the services run and every service logs
`feature flag changed` within about a second, just like toggling a flag in
LaunchDarkly. Local values apply to every context, since there is no targeting.
Invalid JSON is ignored with a warning and the previous values stay in effect.

```json
{
  "payment-failure-rate": 0,
  "payment-consumer-enabled": true,
  "fraud-check-enabled": false,
  "notification-channel": "email",
  "max-retry-attempts": 3
}
```

## Adding a flag

1. Add its key and type to `FlagValues`, and its definition (type, bounds or
   allowed values, safe default, description) to `FLAGS` in
   [`definitions.ts`](../packages/feature-flags/src/definitions.ts).
2. Add it to `feature-flags.json` and to the tables above.
3. Read it with `flags.get('<key>', orderContext(...))`, or use `watchFlag()` to react
   to changes.
