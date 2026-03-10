import { CircleHelp, Fingerprint, HardDrive, ImageIcon, Search, Sparkles } from 'lucide-react';
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

function SimpleIconGlyph({ icon, size = 12, className }: { icon: SimpleIcon; size?: number; className?: string }) {
  return (
    <svg
      aria-hidden="true"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      className={className}
    >
      <path d={icon.path} />
    </svg>
  );
}

export function ProviderIcon({ provider, className, size = 12 }: ProviderIconProps) {
  const meta = getProviderMeta(provider);
  const simpleIcon = SIMPLE_ICONS[meta.iconKey];
  if (simpleIcon) {
    return <SimpleIconGlyph icon={simpleIcon} size={size} className={cn('shrink-0', className)} />;
  }
  const iconClass = cn('shrink-0', className);
  switch (meta.iconKey) {
    case 'openai':
      return <Sparkles aria-hidden="true" className={iconClass} size={size} />;
    case 'acoustid':
      return <Fingerprint aria-hidden="true" className={iconClass} size={size} />;
    case 'audiodb':
    case 'fanart':
      return <ImageIcon aria-hidden="true" className={iconClass} size={size} />;
    case 'serper':
      return <Search aria-hidden="true" className={iconClass} size={size} />;
    case 'local':
    case 'media_cache':
      return <HardDrive aria-hidden="true" className={iconClass} size={size} />;
    default:
      return <CircleHelp aria-hidden="true" className={iconClass} size={size} />;
  }
}
