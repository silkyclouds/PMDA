import { useEffect, useMemo, useState, type SyntheticEvent } from 'react';
import { Music } from 'lucide-react';

type ArtworkSourcePhase = 'album' | 'artist' | 'none';

interface AlbumArtworkProps {
  albumThumb?: string | null;
  artistId?: number | null;
  alt: string;
  size?: number;
  imageClassName?: string;
  fallbackClassName?: string;
  iconClassName?: string;
}

function normalizeUrl(value?: string | null): string | null {
  const txt = String(value || '').trim();
  return txt || null;
}

export function AlbumArtwork({
  albumThumb,
  artistId,
  alt,
  size = 512,
  imageClassName = 'w-full h-full object-cover animate-in fade-in-0 duration-300',
  fallbackClassName = 'w-full h-full flex items-center justify-center',
  iconClassName = 'w-10 h-10 text-muted-foreground',
}: AlbumArtworkProps) {
  const albumUrl = normalizeUrl(albumThumb);
  const artistUrl = useMemo(() => {
    const id = Number(artistId || 0);
    if (!Number.isFinite(id) || id <= 0) return null;
    const px = Math.max(64, Math.min(2048, Math.floor(size)));
    return `/api/library/files/artist/${id}/image?size=${px}`;
  }, [artistId, size]);

  const [phase, setPhase] = useState<ArtworkSourcePhase>('none');
  const [src, setSrc] = useState<string | null>(null);

  useEffect(() => {
    if (albumUrl) {
      setPhase('album');
      setSrc(albumUrl);
      return;
    }
    if (artistUrl) {
      setPhase('artist');
      setSrc(artistUrl);
      return;
    }
    setPhase('none');
    setSrc(null);
  }, [albumUrl, artistUrl]);

  const onError = () => {
    if (phase === 'album' && artistUrl) {
      setPhase('artist');
      setSrc(artistUrl);
      return;
    }
    setPhase('none');
    setSrc(null);
  };

  const onLoad = (e: SyntheticEvent<HTMLImageElement>) => {
    const img = e.currentTarget;
    if (phase === 'album' && artistUrl && img.naturalWidth <= 2 && img.naturalHeight <= 2) {
      setPhase('artist');
      setSrc(artistUrl);
    }
  };

  if (src) {
    return (
      <img
        src={src}
        alt={alt}
        loading="lazy"
        decoding="async"
        className={imageClassName}
        onLoad={onLoad}
        onError={onError}
      />
    );
  }

  return (
    <div className={fallbackClassName}>
      <Music className={iconClassName} />
    </div>
  );
}
