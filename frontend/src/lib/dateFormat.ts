export function formatBadgeDateTime(ts?: number | null): string {
  const n = Number(ts || 0);
  if (!Number.isFinite(n) || n <= 0) return '—';
  try {
    return new Intl.DateTimeFormat('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    })
      .format(new Date(n * 1000))
      .replace(' am', ' AM')
      .replace(' pm', ' PM');
  } catch {
    return '—';
  }
}
