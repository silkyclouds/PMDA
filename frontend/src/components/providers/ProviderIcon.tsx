import { CircleHelp, Fingerprint, HardDrive, ImageIcon, Search } from 'lucide-react';
import type { SimpleIcon } from 'simple-icons';
import {
  siAnthropic,
  siBandcamp,
  siBandsintown,
  siDiscogs,
  siGoogle,
  siLastdotfm,
  siMusicbrainz,
  siOllama,
  siWikipedia,
} from 'simple-icons';

import { getProviderMeta, type ProviderIconKey } from '@/lib/providerMeta';
import { cn } from '@/lib/utils';

interface ProviderIconProps {
  provider?: string | null;
  className?: string;
  size?: number;
}

const SIMPLE_ICONS: Partial<Record<ProviderIconKey, SimpleIcon>> = {
  bandcamp: siBandcamp,
  discogs: siDiscogs,
  lastfm: siLastdotfm,
  musicbrainz: siMusicbrainz,
  wikipedia: siWikipedia,
  anthropic: siAnthropic,
  google: siGoogle,
  ollama: siOllama,
  bandsintown: siBandsintown,
};

/** OpenAI logomark path — simple-icons v16 doesn't export siOpenai */
const OPENAI_PATH =
  'M22.282 9.821a5.985 5.985 0 0 0-.516-4.91 6.046 6.046 0 0 0-6.51-2.9A6.065 6.065 0 0 0 4.981 4.18a5.998 5.998 0 0 0-3.998 2.9 6.05 6.05 0 0 0 .743 7.097 5.98 5.98 0 0 0 .51 4.911 6.051 6.051 0 0 0 6.515 2.9A5.985 5.985 0 0 0 13.26 24a6.056 6.056 0 0 0 5.772-4.206 5.99 5.99 0 0 0 3.997-2.9 6.056 6.056 0 0 0-.747-7.073zM13.26 22.43a4.476 4.476 0 0 1-2.876-1.04l.141-.081 4.779-2.758a.795.795 0 0 0 .392-.681v-6.737l2.02 1.168a.071.071 0 0 1 .038.052v5.583a4.504 4.504 0 0 1-4.494 4.494zM3.6 18.304a4.47 4.47 0 0 1-.535-3.014l.142.085 4.783 2.759a.771.771 0 0 0 .78 0l5.843-3.369v2.332a.08.08 0 0 1-.033.062L9.74 19.95a4.5 4.5 0 0 1-6.14-1.646zM2.34 7.896a4.485 4.485 0 0 1 2.366-1.973V11.6a.766.766 0 0 0 .388.676l5.815 3.355-2.02 1.168a.076.076 0 0 1-.071 0l-4.83-2.786A4.504 4.504 0 0 1 2.34 7.872zm16.597 3.855l-5.833-3.387L15.119 7.2a.076.076 0 0 1 .071 0l4.83 2.791a4.494 4.494 0 0 1-.676 8.105v-5.678a.79.79 0 0 0-.407-.667zm2.01-3.023l-.141-.085-4.774-2.782a.776.776 0 0 0-.785 0L9.409 9.23V6.897a.066.066 0 0 1 .028-.061l4.83-2.787a4.5 4.5 0 0 1 6.68 4.66zm-12.64 4.135l-2.02-1.164a.08.08 0 0 1-.038-.057V6.075a4.5 4.5 0 0 1 7.375-3.453l-.142.08L8.704 5.46a.795.795 0 0 0-.393.681zm1.097-2.365l2.602-1.5 2.607 1.5v2.999l-2.597 1.5-2.607-1.5z';

function SimpleIconGlyph({ icon, size = 12, className, label }: { icon: SimpleIcon; size?: number; className?: string; label?: string }) {
  return (
    <svg
      role="img"
      aria-label={label || icon.title}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      className={className}
    >
      <title>{label || icon.title}</title>
      <path d={icon.path} />
    </svg>
  );
}

function SvgPathIcon({ path, size = 12, className, label }: { path: string; size?: number; className?: string; label: string }) {
  return (
    <svg
      role="img"
      aria-label={label}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      className={className}
    >
      <title>{label}</title>
      <path d={path} />
    </svg>
  );
}

export function ProviderIcon({ provider, className, size = 12 }: ProviderIconProps) {
  const meta = getProviderMeta(provider);
  const simpleIcon = SIMPLE_ICONS[meta.iconKey];
  if (simpleIcon) {
    return <SimpleIconGlyph icon={simpleIcon} size={size} className={cn('shrink-0', className)} label={meta.label} />;
  }
  const iconClass = cn('shrink-0', className);
  switch (meta.iconKey) {
    case 'openai':
      return <SvgPathIcon path={OPENAI_PATH} size={size} className={iconClass} label={meta.label} />;
    case 'acoustid':
      return <Fingerprint aria-label="AcoustID" className={iconClass} size={size} />;
    case 'audiodb':
      return <ImageIcon aria-label="TheAudioDB" className={iconClass} size={size} />;
    case 'fanart':
      return <ImageIcon aria-label="Fanart.tv" className={iconClass} size={size} />;
    case 'serper':
      return <Search aria-label="Serper" className={iconClass} size={size} />;
    case 'local':
      return <HardDrive aria-label="Local" className={iconClass} size={size} />;
    case 'media_cache':
      return <HardDrive aria-label="Media cache" className={iconClass} size={size} />;
    default:
      return <CircleHelp aria-label="Unknown provider" className={iconClass} size={size} />;
  }
}
