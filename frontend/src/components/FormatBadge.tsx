import { cn } from '@/lib/utils';

interface FormatBadgeProps {
  format: string;
  className?: string;
  size?: 'sm' | 'md';
}

const formatClasses: Record<string, string> = {
  flac: 'format-badge-flac',
  mp3: 'format-badge-mp3',
  m4a: 'format-badge-m4a',
  aac: 'format-badge-m4a',
  alac: 'format-badge-flac',
  wav: 'format-badge-flac',
  ogg: 'format-badge-mp3',
  opus: 'format-badge-mp3',
};

export function FormatBadge({ format, className, size = 'md' }: FormatBadgeProps) {
  const normalizedFormat = format.toLowerCase().replace('.', '');
  const formatClass = formatClasses[normalizedFormat] || '';
  const sizeClass = size === 'sm' ? 'text-[10px] px-1.5 py-0' : '';

  return (
    <span className={cn('format-badge', formatClass, sizeClass, className)}>
      {format.toUpperCase()}
    </span>
  );
}
