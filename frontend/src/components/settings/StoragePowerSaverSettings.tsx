import { HardDrive, Info, ShieldCheck } from 'lucide-react';

import type { PMDAConfig } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

type Props = {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
};

function PathSetting({
  label,
  value,
  placeholder,
  help,
  onChange,
}: {
  label: string;
  value: string | undefined;
  placeholder: string;
  help: string;
  onChange: (value: string) => void;
}) {
  return (
    <div className="space-y-2 rounded-lg border border-border/60 bg-background/30 p-3">
      <div className="space-y-1">
        <Label>{label}</Label>
        <p className="text-[11px] leading-5 text-muted-foreground">{help}</p>
      </div>
      <Input
        value={value ?? ''}
        placeholder={placeholder}
        className="font-mono text-xs"
        onChange={(event) => onChange(event.target.value)}
      />
    </div>
  );
}

export function StoragePowerSaverSettings({ config, updateConfig }: Props) {
  const enabled = Boolean(config.STORAGE_POWER_SAVER_ENABLED);
  const provider = String(config.STORAGE_PROVIDER || 'unraid').trim() || 'unraid';
  const maxDevices = Number(config.STORAGE_MAX_ACTIVE_DEVICES ?? 1);

  return (
    <Card className="border-emerald-500/20 bg-emerald-500/[0.035]">
      <CardHeader>
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div className="space-y-2">
            <CardTitle className="flex items-center gap-2 text-base">
              <HardDrive className="h-4 w-4 text-emerald-300" />
              Disk-aware power saver scan
            </CardTitle>
            <CardDescription>
              Optional Unraid mode that scans one source disk at a time through a read-only <span className="font-mono">/host_mnt</span> mount while keeping PMDA paths canonical.
            </CardDescription>
          </div>
          <Badge variant={enabled ? 'default' : 'secondary'} className={enabled ? 'bg-emerald-500/20 text-emerald-100' : ''}>
            {enabled ? 'On' : 'Off'}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-start justify-between gap-4 rounded-lg border border-border/60 bg-background/40 p-3">
          <div className="space-y-1">
            <Label className="flex items-center gap-2">
              <ShieldCheck className={enabled ? 'h-4 w-4 text-emerald-300' : 'h-4 w-4 text-muted-foreground'} />
              Enable disk-aware scan
            </Label>
            <p className="text-xs leading-5 text-muted-foreground">
              Off is the default. When on, PMDA refuses to start a scan if <span className="font-mono">/host_mnt/disk*</span> or the share mapping is invalid.
            </p>
          </div>
          <Switch checked={enabled} onCheckedChange={(checked) => updateConfig({ STORAGE_POWER_SAVER_ENABLED: checked })} />
        </div>

        <div className="grid gap-3 md:grid-cols-3">
          <div className="space-y-2 rounded-lg border border-border/60 bg-background/30 p-3">
            <Label>Storage provider</Label>
            <Input
              value={provider}
              disabled
              className="font-mono text-xs"
              onChange={() => undefined}
            />
            <p className="text-[11px] text-muted-foreground">v1 is intentionally Unraid-only.</p>
          </div>
          <div className="space-y-2 rounded-lg border border-border/60 bg-background/30 p-3">
            <Label>Max active source disks</Label>
            <Input
              type="number"
              min={1}
              max={1}
              value={Number.isFinite(maxDevices) ? maxDevices : 1}
              onChange={() => updateConfig({ STORAGE_MAX_ACTIVE_DEVICES: 1 })}
            />
            <p className="text-[11px] text-muted-foreground">v1 strict mode is fixed at one disk.</p>
          </div>
          <div className="space-y-2 rounded-lg border border-border/60 bg-background/30 p-3">
            <Label>Spindown policy</Label>
            <Input
              value={String(config.STORAGE_SPINDOWN_POLICY || 'none')}
              disabled
              className="font-mono text-xs"
              onChange={() => undefined}
            />
            <p className="text-[11px] text-muted-foreground">PMDA does not force spindown in v1.</p>
          </div>
        </div>

        <div className="grid gap-3 lg:grid-cols-3">
          <PathSetting
            label="Host /mnt mount in container"
            value={config.UNRAID_HOST_MNT_ROOT || '/host_mnt'}
            placeholder="/host_mnt"
            help="Container path where the Unraid host /mnt is mounted read-only."
            onChange={(value) => updateConfig({ UNRAID_HOST_MNT_ROOT: value || '/host_mnt' })}
          />
          <PathSetting
            label="User share host root"
            value={config.UNRAID_USER_SHARE_HOST_ROOT || '/host_mnt/user/MURRAY/Music'}
            placeholder="/host_mnt/user/MURRAY/Music"
            help="Host-side user share root equivalent to PMDA's canonical /music root."
            onChange={(value) => updateConfig({ UNRAID_USER_SHARE_HOST_ROOT: value || '/host_mnt/user/MURRAY/Music' })}
          />
          <PathSetting
            label="Canonical container root"
            value={config.UNRAID_CONTAINER_SHARE_ROOT || '/music'}
            placeholder="/music"
            help="PMDA-visible root used in cache, UI, moves and library paths."
            onChange={(value) => updateConfig({ UNRAID_CONTAINER_SHARE_ROOT: value || '/music' })}
          />
        </div>

        <div className="flex gap-2 rounded-lg border border-amber-500/25 bg-amber-500/[0.06] p-3 text-xs leading-5 text-amber-100/90">
          <Info className="mt-0.5 h-4 w-4 shrink-0" />
          <p>
            This minimizes source disk wakeups during discovery, tags, ffprobe/fingerprint and matching. Writes to <span className="font-mono">Music_matched</span>, dupes or incompletes can still wake a destination disk unless those targets are on cache/pool SSD.
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
