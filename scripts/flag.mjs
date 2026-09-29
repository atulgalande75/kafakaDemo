// Shows or changes local feature flags in feature-flags.json. Running services
// pick up changes within ~1 second (only when LD_SDK_KEY is not set).
//
//   npm run flag                                   # show current values
//   npm run flag -- payment-failure-rate 1
//   npm run flag -- payment-consumer-enabled false
//   npm run flag -- notification-channel sms
//   npm run flag -- --reset                        # back to the safe defaults
import { readFileSync, writeFileSync } from 'node:fs';

const DEFAULTS = {
  'payment-failure-rate': 0,
  'payment-consumer-enabled': true,
  'fraud-check-enabled': false,
  'notification-channel': 'email',
  'max-retry-attempts': 3,
};

const file = process.env.FEATURE_FLAGS_FILE ?? new URL('../feature-flags.json', import.meta.url);
const read = () => {
  try {
    return JSON.parse(readFileSync(file, 'utf8'));
  } catch {
    return {};
  }
};
const write = (flags) => writeFileSync(file, `${JSON.stringify(flags, null, 2)}\n`);

const [key, raw] = process.argv.slice(2);

if (key === '--reset') {
  write(DEFAULTS);
  console.log('feature flags reset to safe defaults');
} else if (key) {
  if (!(key in DEFAULTS)) {
    console.error(`Unknown flag "${key}". Known flags: ${Object.keys(DEFAULTS).join(', ')}`);
    process.exit(1);
  }
  if (raw === undefined) {
    console.error(`Usage: npm run flag -- ${key} <value>`);
    process.exit(1);
  }
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    value = raw; // bare strings such as sms
  }
  if (typeof value !== typeof DEFAULTS[key]) {
    console.error(`"${key}" expects a ${typeof DEFAULTS[key]}, got ${JSON.stringify(value)}`);
    process.exit(1);
  }
  const flags = { ...DEFAULTS, ...read(), [key]: value };
  write(flags);
  console.log(`${key} = ${JSON.stringify(value)}`);
}

if (process.env.LD_SDK_KEY) {
  console.warn('Note: LD_SDK_KEY is set - services read flags from LaunchDarkly, not this file.');
}
console.table({ ...DEFAULTS, ...read() });
