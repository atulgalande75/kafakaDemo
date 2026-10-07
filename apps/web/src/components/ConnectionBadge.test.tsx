import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ConnectionBadge } from './ConnectionBadge';

describe('ConnectionBadge', () => {
  it.each([
    ['connecting', 'Connecting…'],
    ['live', 'Live'],
    ['polling', 'Polling'],
    ['reconnecting', 'Reconnecting…'],
    ['error', 'Disconnected'],
  ] as const)('labels %s', (status, label) => {
    render(<ConnectionBadge status={status} />);
    expect(screen.getByRole('status')).toHaveTextContent(label);
  });

  it('explains polling on hover', () => {
    render(<ConnectionBadge status="polling" />);
    expect(screen.getByRole('status')).toHaveAttribute(
      'title',
      expect.stringMatching(/switched off/),
    );
  });
});
