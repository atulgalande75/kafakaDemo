import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { initialLiveState } from '../live/reducer';
import { feedEntry } from '../test/fixtures';
import { ActivityFeed } from './ActivityFeed';

const hoisted = vi.hoisted(() => ({ live: undefined as object | undefined }));
vi.mock('../live/LiveProvider', () => ({ useLive: () => hoisted.live }));

describe('ActivityFeed', () => {
  it('tells the user when nothing has happened yet', () => {
    hoisted.live = initialLiveState;
    render(<ActivityFeed />);
    expect(screen.getByText(/nothing yet/i)).toBeInTheDocument();
  });

  it('lists entries in the given order and filters by kind', async () => {
    hoisted.live = {
      ...initialLiveState,
      feed: [
        feedEntry({ seq: 3, kind: 'stock-alert', type: 'stock.low', summary: 'Webcam is low' }),
        feedEntry({ seq: 2, type: 'payment.failed', summary: 'Payment failed' }),
        feedEntry({ seq: 1, type: 'order.created', summary: 'Order placed' }),
      ],
    };
    const user = userEvent.setup();
    render(<ActivityFeed />);
    expect(screen.getAllByRole('listitem').map((li) => li.textContent)).toEqual([
      expect.stringContaining('Webcam is low'),
      expect.stringContaining('Payment failed'),
      expect.stringContaining('Order placed'),
    ]);

    await user.click(screen.getByRole('button', { name: 'Alerts' }));
    expect(screen.getAllByRole('listitem')).toHaveLength(1);
    expect(screen.getByText('Webcam is low')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Orders' }));
    expect(screen.queryByText('Webcam is low')).not.toBeInTheDocument();
    expect(screen.getAllByRole('listitem')).toHaveLength(2);
  });

  it('shows only as many entries as the activity-feed-size flag allows', () => {
    hoisted.live = {
      ...initialLiveState,
      flags: { ...DEFAULT_UI_FLAGS, activityFeedSize: 5 },
      feed: Array.from({ length: 12 }, (_, i) =>
        feedEntry({ seq: 12 - i, summary: `entry ${12 - i}` }),
      ),
    };
    render(<ActivityFeed />);
    const items = screen.getAllByRole('listitem');
    expect(items).toHaveLength(5);
    expect(items[0]).toHaveTextContent('entry 12'); // the newest ones are kept
    expect(items[4]).toHaveTextContent('entry 8');
  });
});
