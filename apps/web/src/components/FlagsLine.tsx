import { useLive } from '../live/LiveProvider';

const onOff = (value: boolean) => (value ? 'on' : 'off');

/** Shows which feature flags are active for this user, so a flag change is visible in a demo. */
export function FlagsLine() {
  const { flags } = useLive();
  return (
    <footer className="flagline muted" aria-label="Feature flags">
      Feature flags: live updates {onOff(flags.liveUpdates)} &middot; card view{' '}
      {onOff(flags.newInventoryDashboard)} &middot; bulk restock {onOff(flags.bulkAdjust)} &middot;
      feed size {flags.activityFeedSize}
    </footer>
  );
}
