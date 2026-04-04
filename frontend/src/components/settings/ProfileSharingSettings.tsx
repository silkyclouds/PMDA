import { useEffect, useMemo, useRef, useState, type ChangeEvent } from 'react';
import { Loader2, Save, Upload, X } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { PasswordInput } from '@/components/ui/password-input';
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

async function readImageDataUrl(file: File): Promise<string> {
  return await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      if (!result.startsWith('data:image/')) {
        reject(new Error('Unsupported avatar image'));
        return;
      }
      resolve(result);
    };
    reader.onerror = () => reject(new Error('Failed to read avatar image'));
    reader.readAsDataURL(file);
  });
}

async function optimizeAvatarDataUrl(file: File): Promise<string> {
  const input = await readImageDataUrl(file);
  const image = await new Promise<HTMLImageElement>((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error('Failed to decode avatar image'));
    img.src = input;
  });
  const side = 512;
  const canvas = document.createElement('canvas');
  canvas.width = side;
  canvas.height = side;
  const ctx = canvas.getContext('2d');
  if (!ctx) return input;
  const srcSide = Math.min(image.naturalWidth, image.naturalHeight);
  const sx = Math.max(0, Math.floor((image.naturalWidth - srcSide) / 2));
  const sy = Math.max(0, Math.floor((image.naturalHeight - srcSide) / 2));
  ctx.clearRect(0, 0, side, side);
  ctx.drawImage(image, sx, sy, srcSide, srcSide, 0, 0, side, side);
  const webp = canvas.toDataURL('image/webp', 0.84);
  if (webp.length <= 1024 * 1024) return webp;
  return canvas.toDataURL('image/jpeg', 0.82);
}

type Props = {
  compact?: boolean;
};

export function ProfileSharingSettings({ compact = false }: Props) {
  const { user, refreshSession, logout } = useAuth();
  const [acceptShares, setAcceptShares] = useState<boolean>(Boolean(user?.accept_shares ?? true));
  const [shareLikedPublic, setShareLikedPublic] = useState<boolean>(Boolean(user?.share_liked_public ?? false));
  const [shareRecommendationsPublic, setShareRecommendationsPublic] = useState<boolean>(Boolean(user?.share_recommendations_public ?? false));
  const [avatarDataUrl, setAvatarDataUrl] = useState<string | null>(user?.avatar_data_url ?? null);
  const [saving, setSaving] = useState(false);
  const [avatarBusy, setAvatarBusy] = useState(false);
  const [passwordBusy, setPasswordBusy] = useState(false);
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setAcceptShares(Boolean(user?.accept_shares ?? true));
    setShareLikedPublic(Boolean(user?.share_liked_public ?? false));
    setShareRecommendationsPublic(Boolean(user?.share_recommendations_public ?? false));
    setAvatarDataUrl(user?.avatar_data_url ?? null);
  }, [user?.accept_shares, user?.avatar_data_url, user?.share_liked_public, user?.share_recommendations_public]);

  const hasChanges = useMemo(
    () => Boolean(
      acceptShares !== Boolean(user?.accept_shares ?? true)
      || shareLikedPublic !== Boolean(user?.share_liked_public ?? false)
      || shareRecommendationsPublic !== Boolean(user?.share_recommendations_public ?? false)
      || (avatarDataUrl ?? null) !== (user?.avatar_data_url ?? null)
    ),
    [acceptShares, avatarDataUrl, shareLikedPublic, shareRecommendationsPublic, user?.accept_shares, user?.avatar_data_url, user?.share_liked_public, user?.share_recommendations_public],
  );

  const saveProfile = async () => {
    setSaving(true);
    try {
      await api.updateAuthProfile({
        accept_shares: acceptShares,
        share_liked_public: shareLikedPublic,
        share_recommendations_public: shareRecommendationsPublic,
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

  const hasPasswordDraft = Boolean(currentPassword || newPassword || confirmPassword);

  const changePassword = async () => {
    if (!currentPassword || !newPassword || !confirmPassword) {
      toast.error('Fill in current password, new password, and confirmation');
      return;
    }
    if (newPassword !== confirmPassword) {
      toast.error('Passwords do not match');
      return;
    }
    setPasswordBusy(true);
    try {
      await api.changeAuthPassword({
        current_password: currentPassword,
        new_password: newPassword,
        new_password_confirm: confirmPassword,
      });
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
      toast.success('Password changed. Please sign in again.');
      await logout();
    } catch (error) {
      toast.error(getApiErrorMessage(error));
    } finally {
      setPasswordBusy(false);
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
    setAvatarBusy(true);
    try {
      const optimized = await optimizeAvatarDataUrl(file);
      setAvatarDataUrl(optimized);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Failed to process avatar image');
    } finally {
      setAvatarBusy(false);
    }
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
              {avatarBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
              {avatarBusy ? 'Processing avatar…' : 'Upload avatar'}
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

        <div className="grid gap-4 lg:grid-cols-2">
          <div className="rounded-xl border border-border/60 bg-background/40 p-4">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <Label htmlFor="share-liked-public" className="text-sm font-medium">
                  Let others browse your liked page
                </Label>
                <p className="text-xs text-muted-foreground">
                  If enabled, other users can open your liked albums, artists and labels from social badges and the liked page user picker.
                </p>
              </div>
              <Switch id="share-liked-public" checked={shareLikedPublic} onCheckedChange={setShareLikedPublic} />
            </div>
          </div>

          <div className="rounded-xl border border-border/60 bg-background/40 p-4">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <Label htmlFor="share-recommendations-public" className="text-sm font-medium">
                  Let others browse your recommendations
                </Label>
                <p className="text-xs text-muted-foreground">
                  If enabled, other users can browse recommendations you received or sent when you choose to make them public.
                </p>
              </div>
              <Switch
                id="share-recommendations-public"
                checked={shareRecommendationsPublic}
                onCheckedChange={setShareRecommendationsPublic}
              />
            </div>
          </div>
        </div>

        <div className="flex justify-end">
          <Button type="button" onClick={() => void saveProfile()} disabled={!hasChanges || saving} className="gap-2">
            {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
            Save profile
          </Button>
        </div>

        <div className="rounded-xl border border-border/60 bg-background/40 p-4 space-y-4">
          <div className="space-y-1">
            <Label className="text-sm font-medium">Change password</Label>
            <p className="text-xs text-muted-foreground">
              Confirm your current password, choose a new one, then sign in again with the updated credential.
            </p>
          </div>
          <div className="grid gap-4 lg:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="current-password">Current password</Label>
              <PasswordInput
                id="current-password"
                value={currentPassword}
                onChange={(event) => setCurrentPassword(event.target.value)}
                autoComplete="current-password"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="new-password">New password</Label>
              <PasswordInput
                id="new-password"
                value={newPassword}
                onChange={(event) => setNewPassword(event.target.value)}
                autoComplete="new-password"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="confirm-password">Confirm new password</Label>
              <PasswordInput
                id="confirm-password"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                autoComplete="new-password"
              />
            </div>
          </div>
          <div className="flex justify-end">
            <Button
              type="button"
              onClick={() => void changePassword()}
              disabled={!hasPasswordDraft || passwordBusy}
              className="gap-2"
            >
              {passwordBusy ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              Update password
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
