import { useMemo } from 'react';
import { MapContainer, TileLayer, Marker, Circle } from 'react-leaflet';
import L from 'leaflet';
import { latLngBounds } from 'leaflet';

import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png';
import iconUrl from 'leaflet/dist/images/marker-icon.png';
import shadowUrl from 'leaflet/dist/images/marker-shadow.png';

import type { ArtistConcertEvent } from '@/lib/api';

// Leaflet's default marker assets don't resolve correctly in Vite without explicit URLs.
// Configure them once at module load.
try {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  delete (L.Icon.Default.prototype as any)._getIconUrl;
  L.Icon.Default.mergeOptions({
    iconRetinaUrl,
    iconUrl,
    shadowUrl,
  });
} catch {
  // no-op
}

function toNumber(x?: string): number | null {
  const s = (x || '').trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

export function ConcertsMiniMap(props: {
  events: ArtistConcertEvent[];
  home?: { lat: number; lon: number; radiusKm?: number } | null;
  className?: string;
}) {
  const { events, home, className } = props;

  const points = useMemo(() => {
    return (events || [])
      .map((ev) => {
        const v = ev.venue;
        const lat = toNumber(v?.latitude);
        const lon = toNumber(v?.longitude);
        if (lat == null || lon == null) return null;
        const venueName = (v?.name || '').trim();
        const city = (v?.city || '').trim();
        const region = (v?.region || '').trim();
        const country = (v?.country || '').trim();
        const where = [city, region, country].filter(Boolean).join(', ');
        return {
          key: `${ev.provider || 'p'}:${ev.id || ''}:${lat.toFixed(5)}:${lon.toFixed(5)}`,
          lat,
          lon,
          label: [venueName || 'Venue', where].filter(Boolean).join(' · '),
        };
      })
      .filter(Boolean) as Array<{ key: string; lat: number; lon: number; label: string }>;
  }, [events]);

  const bounds = useMemo(() => {
    const coords: Array<[number, number]> = points.map((p) => [p.lat, p.lon]);
    if (home && Number.isFinite(home.lat) && Number.isFinite(home.lon)) {
      coords.push([home.lat, home.lon]);
    }
    if (coords.length === 0) return null;
    return latLngBounds(coords);
  }, [points, home]);

  if (!bounds) return null;

  return (
    <div className={className}>
      <MapContainer
        bounds={bounds}
        boundsOptions={{ padding: [18, 18] }}
        className="h-44 w-full rounded-xl overflow-hidden border border-border/60"
        scrollWheelZoom={false}
        dragging={false}
        doubleClickZoom={false}
        zoomControl={false}
        attributionControl={false}
      >
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
        {home && Number.isFinite(home.lat) && Number.isFinite(home.lon) && home.radiusKm && home.radiusKm > 0 ? (
          <Circle
            center={[home.lat, home.lon]}
            radius={Math.max(1000, home.radiusKm * 1000)}
            pathOptions={{ color: 'hsl(var(--primary))', weight: 1, fillColor: 'hsl(var(--primary))', fillOpacity: 0.08 }}
          />
        ) : null}
        {points.slice(0, 60).map((p) => (
          <Marker key={p.key} position={[p.lat, p.lon]} />
        ))}
      </MapContainer>
    </div>
  );
}
