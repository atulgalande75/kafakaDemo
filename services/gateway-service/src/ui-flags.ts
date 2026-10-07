import { userContext, type FeatureFlags } from '@orderflow/feature-flags';
import type { UiFlags } from '@orderflow/stream-types';

export interface FlagUser {
  sub: string;
  username?: string;
}

/** Evaluates the web app's flags for one user (so LaunchDarkly can target by user). Never throws. */
export async function evaluateUiFlags(flags: FeatureFlags, user: FlagUser): Promise<UiFlags> {
  const context = userContext(user.sub, user.username);
  const [liveUpdates, newInventoryDashboard, bulkAdjust, activityFeedSize] = await Promise.all([
    flags.get('live-updates-enabled', context),
    flags.get('new-inventory-dashboard', context),
    flags.get('bulk-adjust-enabled', context),
    flags.get('activity-feed-size', context),
  ]);
  return { liveUpdates, newInventoryDashboard, bulkAdjust, activityFeedSize };
}

export const sameUiFlags = (a: UiFlags, b: UiFlags) =>
  a.liveUpdates === b.liveUpdates &&
  a.newInventoryDashboard === b.newInventoryDashboard &&
  a.bulkAdjust === b.bulkAdjust &&
  a.activityFeedSize === b.activityFeedSize;
