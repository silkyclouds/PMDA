import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';
import { Loader2, Save, Upload, X } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';

function getInitials(username: string): string {
  const clean = String(username || '').trim();
  if (!clean) return 'U';
  return clean.slice(0, 2).toUpperCase();
}

function getApiErrorMessage(error: unknown): string {
  const bodyError = (error as { body?: { error?: unknown } } | null)?.body?.error;
  if (typeof bodyError === 'string' && bodyError.trim()) return bodyError.trim();
  if (error instanceof Error && error.message.trim()) return error.message;
  return 'Request failed';
}

type Props = {
  compact?: boolean;
};

export function ProfileSharingSettings({ compact = false }: Props) {
  const { user, refreshSession } = useAuth();
  const [acceptShares, setAcceptShares] = useState<boolean>(Boolean(user?.accept_shares ?? true));
  const [avatarDataUrl, setAvatarDataUrl] = useState<string | null>(user?.avatar_data_url ?? null);
  const [saving, setSaving] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setAcceptShares(Boolean(user?.accept_shares ?? true));
    setAvatarDataUrl(user?.avatar_data_url ?? null);
  }, [user?.accept_shares, user?.avatar_data_url]);

  const hasChanges = useMemo(
    () => Boolean(acceptShares !== Boolean(user?.accept_shares ?? true) || (avatarDataUrl ?? null) !== (user?.avatar_data_url ?? null)),
    [acceptShares, avatarDataUrl, user?.accept_shares, user?.avatar_data_url],
  );

  const saveProfile = async () => {
    setSaving(true);
    try {
      await api.updateAuthProfile({
        accept_shares: acceptShares,
        avatar_data_url: avatarDataUrl,
      });
      await refreshSession();
      toast.success('Profile updated');
    } catch (error) {
      toast.error(getApiErrorMessage(error));
    } finally {
      setSaving(false);
    }
  };

  const onSelectAvatar = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith('image/')) {
      toast.error('Please choose an image file');
      event.target.value = '';
      return;
    }
    if (file.size > 256 * 1024) {
      toast.error('Avatar image must be 256 KB or smaller');
      event.target.value = '';
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      if (!result.startsWith('data:image/')) {
        toast.error('Unsupported avatar image');
        return;
      }
      setAvatarDataUrl(result);
    };
    reader.onerror = () => toast.error('Failed to read avatar image');
    reader.readAsDataURL(file);
    event.target.value = '';
  };

  return (
    <Card className={compact ? '' : 'border-teal-500/20 bg-teal-500/[0.04]'}>
      <CardHeader>
        <CardTitle>Profile & Sharing</CardTitle>
        <CardDescription>
          Manage how other PMDA users see you and whether they can send you recommendations.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center">
          <div className="flex items-center gap-4">
            <Avatar className="h-20 w-20 rounded-2xl border border-border/60">
              {avatarDataUrl ? <AvatarImage src={avatarDataUrl} alt={user?.username || 'User avatar'} /> : null}
              <AvatarFallback className="rounded-2xl text-lg font-semibold">{getInitials(user?.username || '')}</AvatarFallback>
            </Avatar>
            <div className="space-y-1">
              <div className="text-base font-semibold">{user?.username || 'Unknown user'}</div>
              <div className="text-sm text-muted-foreground">
                {user?.is_admin ? 'Administrator' : 'Library user'}
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 md:ml-auto">
            <input
              ref={inputRef}
              type="file"
              accept="image/png,image/jpeg,image/webp,image/gif"
              className="hidden"
              onChange={onSelectAvatar}
            />
            <Button type="button" variant="outline" className="gap-2" onClick={() => inputRef.current?.click()}>
              <Upload className="h-4 w-4" />
              Upload avatar
            </Button>
            <Button
              type="button"
              variant="outline"
              className="gap-2"
              disabled={!avatarDataUrl}
              onClick={() => setAvatarDataUrl(null)}
            >
              <X className="h-4 w-4" />
              Remove
            </Button>
          </div>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/40 p-4">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <Label htmlFor="accept-shares" className="text-sm font-medium">
                Accept recommendations from other PMDA users
              </Label>
              <p className="text-xs text-muted-foreground">
                If disabled, your account is hidden from the share dialog and other users cannot send you recommendations.
              </p>
            </div>
            <Switch id="accept-shares" checked={acceptShares} onCheckedChange={setAcceptShares} />
          </div>
        </div>

        <div className="flex justify-end">
          <Button type="button" onClick={() => void saveProfile()} disabled={!hasChanges || saving} className="gap-2">
            {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            Save profile
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
