import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { FlagsLine } from './FlagsLine';

const hoisted = vi.hoisted(() => ({ flags: undefined as object | undefined }));
vi.mock('../live/LiveProvider', () => ({ useLive: () => ({ flags: hoisted.flags }) }));

describe('FlagsLine', () => {
  it('shows the active flags', () => {
    hoisted.flags = { ...DEFAULT_UI_FLAGS, newInventoryDashboard: true, activityFeedSize: 20 };
    render(<FlagsLine />);
    expect(screen.getByLabelText('Feature flags')).toHaveTextContent(
      'live updates on · card view on · bulk restock off · feed size 20',
    );
  });
});
