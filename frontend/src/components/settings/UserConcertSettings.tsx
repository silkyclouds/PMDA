import { useEffect, useMemo, useState } from 'react';
import { Loader2, MapPin, Save } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

function getApiErrorMessage(error: unknown): string {
  const bodyError = (error as { body?: { error?: unknown } } | null)?.body?.error;
  if (typeof bodyError === 'string' && bodyError.trim()) return bodyError.trim();
  if (error instanceof Error && error.message.trim()) return error.message;
  return 'Request failed';
}

type Props = {
  compact?: boolean;
};

export function UserConcertSettings({ compact = false }: Props) {
  const { user, refreshSession } = useAuth();
  const [enabled, setEnabled] = useState<boolean>(Boolean(user?.concerts_filter_enabled ?? false));
  const [lat, setLat] = useState<string>(String(user?.concerts_home_lat ?? ''));
  const [lon, setLon] = useState<string>(String(user?.concerts_home_lon ?? ''));
  const [radiusKm, setRadiusKm] = useState<string>(String(user?.concerts_radius_km ?? '150'));
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setEnabled(Boolean(user?.concerts_filter_enabled ?? false));
    setLat(String(user?.concerts_home_lat ?? ''));
    setLon(String(user?.concerts_home_lon ?? ''));
    setRadiusKm(String(user?.concerts_radius_km ?? '150'));
  }, [user?.concerts_filter_enabled, user?.concerts_home_lat, user?.concerts_home_lon, user?.concerts_radius_km]);

  const hasChanges = useMemo(
    () => (
      enabled !== Boolean(user?.concerts_filter_enabled ?? false)
      || lat !== String(user?.concerts_home_lat ?? '')
      || lon !== String(user?.concerts_home_lon ?? '')
      || radiusKm !== String(user?.concerts_radius_km ?? '150')
    ),
    [enabled, lat, lon, radiusKm, user?.concerts_filter_enabled, user?.concerts_home_lat, user?.concerts_home_lon, user?.concerts_radius_km],
  );

  const saveConcertSettings = async () => {
    setSaving(true);
    try {
      await api.updateAuthProfile({
        concerts_filter_enabled: enabled,
        concerts_home_lat: lat,
        concerts_home_lon: lon,
        concerts_radius_km: radiusKm,
      });
      await refreshSession();
      toast.success('Concert preferences updated');
    } catch (error) {
      toast.error(getApiErrorMessage(error));
    } finally {
      setSaving(false);
    }
  };

  const useMyLocation = () => {
    if (!navigator?.geolocation?.getCurrentPosition) {
      toast.error('Geolocation is not available in this browser.');
      return;
    }
    navigator.geolocation.getCurrentPosition(
      (position) => {
        const nextLat = position.coords?.latitude;
        const nextLon = position.coords?.longitude;
        if (!Number.isFinite(nextLat) || !Number.isFinite(nextLon)) {
          toast.error('Could not read your location.');
          return;
        }
        setLat(String(nextLat));
        setLon(String(nextLon));
        setEnabled(true);
        toast.success('Location captured');
      },
      () => toast.error('Location permission denied.'),
      { enableHighAccuracy: false, maximumAge: 60_000, timeout: 10_000 },
    );
  };

  return (
    <Card className={compact ? '' : 'border-cyan-500/20 bg-cyan-500/[0.04]'}>
      <CardHeader>
        <CardTitle>Concerts</CardTitle>
        <CardDescription>
          Filter upcoming concerts around your location on artist pages.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <Label htmlFor="concerts-filter-enabled" className="text-sm font-medium">
                Enable location filter
              </Label>
              <p className="text-xs text-muted-foreground">
                When enabled, artist pages only keep concerts inside your radius.
              </p>
            </div>
            <Switch id="concerts-filter-enabled" checked={enabled} onCheckedChange={setEnabled} />
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <div className="space-y-2">
            <Label htmlFor="concerts-home-lat">Latitude</Label>
            <Input
              id="concerts-home-lat"
              inputMode="decimal"
              placeholder="50.535"
              value={lat}
              onChange={(event) => setLat(event.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="concerts-home-lon">Longitude</Label>
            <Input
              id="concerts-home-lon"
              inputMode="decimal"
              placeholder="5.567"
              value={lon}
              onChange={(event) => setLon(event.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="concerts-radius-km">Radius (km)</Label>
            <Input
              id="concerts-radius-km"
              type="number"
              min={1}
              max={2000}
              placeholder="150"
              value={radiusKm}
              onChange={(event) => setRadiusKm(event.target.value)}
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" variant="outline" className="gap-2" onClick={useMyLocation}>
            <MapPin className="h-4 w-4" />
            Use my location
          </Button>
          <Button
            type="button"
            variant="ghost"
            onClick={() => {
              setLat('');
              setLon('');
            }}
          >
            Clear
          </Button>
        </div>

        <div className="flex justify-end">
          <Button type="button" className="gap-2" onClick={saveConcertSettings} disabled={!hasChanges || saving}>
            {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            Save concert preferences
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
