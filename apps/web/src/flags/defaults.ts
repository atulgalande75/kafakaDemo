import type { UiFlags } from '@orderflow/stream-types';

/** Used until the gateway answers and whenever it can't be reached: normal, healthy behaviour. */
export const DEFAULT_UI_FLAGS: UiFlags = {
  liveUpdates: true,
  newInventoryDashboard: false,
  bulkAdjust: false,
  activityFeedSize: 50,
};

export const sameFlags = (a: UiFlags, b: UiFlags) =>
  a.liveUpdates === b.liveUpdates &&
  a.newInventoryDashboard === b.newInventoryDashboard &&
  a.bulkAdjust === b.bulkAdjust &&
  a.activityFeedSize === b.activityFeedSize;
