export function formatSampleRateKhz(sampleRate?: number | null): string | null {
  const value = Number(sampleRate || 0);
  if (!Number.isFinite(value) || value <= 0) return null;
  const khz = value / 1000;
  const rounded = Math.round(khz * 10) / 10;
  const display = Number.isInteger(rounded) ? String(Math.trunc(rounded)) : rounded.toFixed(1).replace(/\.0$/, '');
  return `${display}kHz`;
}

export function formatAudioSpec(bitDepth?: number | null, sampleRate?: number | null): string | null {
  const depth = Number(bitDepth || 0);
  const rate = formatSampleRateKhz(sampleRate);
  const parts: string[] = [];
  if (Number.isFinite(depth) && depth > 0) parts.push(`${Math.trunc(depth)}bit`);
  if (rate) parts.push(rate);
  return parts.length > 0 ? parts.join('/') : null;
}
