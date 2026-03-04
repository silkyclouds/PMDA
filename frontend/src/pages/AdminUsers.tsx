import { useCallback, useEffect, useMemo, useState } from 'react';
import { Loader2, RefreshCw, Save, UserPlus } from 'lucide-react';
import { toast } from 'sonner';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { PasswordInput } from '@/components/ui/password-input';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';

interface UserDraft {
  username: string;
  is_admin: boolean;
  can_download: boolean;
  is_active: boolean;
  password: string;
}

function toDraft(user: api.AuthUser): UserDraft {
  return {
    username: user.username,
    is_admin: Boolean(user.is_admin),
    can_download: Boolean(user.can_download),
    is_active: Boolean(user.is_active),
    password: '',
  };
}

function getApiErrorMessage(error: unknown): string {
  const bodyError = (error as { body?: { error?: unknown } } | null)?.body?.error;
  if (typeof bodyError === 'string' && bodyError.trim()) return bodyError.trim();
  if (error instanceof Error && error.message.trim()) return error.message;
  return 'Request failed';
}

function formatLastLogin(value?: number | null): string {
  if (!value || value <= 0) return 'Never';
  try {
    return new Date(value * 1000).toLocaleString();
  } catch {
    return String(value);
  }
}

export default function AdminUsersPage() {
  const auth = useAuth();
  const [users, setUsers] = useState<api.AuthUser[]>([]);
  const [drafts, setDrafts] = useState<Record<number, UserDraft>>({});
  const [loading, setLoading] = useState(true);
  const [savingId, setSavingId] = useState<number | null>(null);
  const [createBusy, setCreateBusy] = useState(false);
  const [newUser, setNewUser] = useState({
    username: '',
    password: '',
    passwordConfirm: '',
    is_admin: false,
    can_download: false,
  });

  const loadUsers = useCallback(async () => {
    setLoading(true);
    try {
      const res = await api.getAdminUsers();
      const rows = Array.isArray(res.users) ? res.users : [];
      setUsers(rows);
      setDrafts(
        rows.reduce<Record<number, UserDraft>>((acc, u) => {
          acc[u.id] = toDraft(u);
          return acc;
        }, {}),
      );
    } catch (error) {
      toast.error(getApiErrorMessage(error));
      setUsers([]);
      setDrafts({});
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  const updateDraft = useCallback((userId: number, patch: Partial<UserDraft>) => {
    setDrafts((prev) => ({
      ...prev,
      [userId]: {
        ...(prev[userId] ?? {
          username: '',
          is_admin: false,
          can_download: false,
          is_active: true,
          password: '',
        }),
        ...patch,
      },
    }));
  }, []);

  const saveUser = useCallback(
    async (userId: number) => {
      const draft = drafts[userId];
      if (!draft) return;
      setSavingId(userId);
      try {
        const payload: api.AdminUserUpdateRequest = {
          username: draft.username.trim(),
          is_admin: Boolean(draft.is_admin),
          can_download: Boolean(draft.can_download),
          is_active: Boolean(draft.is_active),
        };
        if (draft.password.trim()) {
          payload.password = draft.password;
        }
        await api.updateAdminUser(userId, payload);
        toast.success('User updated');
        await loadUsers();
      } catch (error) {
        toast.error(getApiErrorMessage(error));
      } finally {
        setSavingId(null);
      }
    },
    [drafts, loadUsers],
  );

  const onCreateUser = useCallback(async () => {
    if (!newUser.username.trim() || !newUser.password) {
      toast.error('Username and password are required');
      return;
    }
    if (newUser.password !== newUser.passwordConfirm) {
      toast.error('Passwords do not match');
      return;
    }
    setCreateBusy(true);
    try {
      await api.createAdminUser({
        username: newUser.username.trim(),
        password: newUser.password,
        password_confirm: newUser.passwordConfirm,
        is_admin: newUser.is_admin,
        can_download: newUser.can_download,
      });
      toast.success('User created');
      setNewUser({ username: '', password: '', passwordConfirm: '', is_admin: false, can_download: false });
      await loadUsers();
    } catch (error) {
      toast.error(getApiErrorMessage(error));
    } finally {
      setCreateBusy(false);
    }
  }, [loadUsers, newUser]);

  const sortedUsers = useMemo(() => {
    return [...users].sort((a, b) => Number(b.is_admin) - Number(a.is_admin) || a.username.localeCompare(b.username));
  }, [users]);

  if (!auth.isAdmin) {
    return (
      <div className="container py-8">
        <Card>
          <CardHeader>
            <CardTitle>Users</CardTitle>
            <CardDescription>Admin access required.</CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  return (
    <div className="container space-y-6 py-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between gap-3">
          <div>
            <CardTitle>User Management</CardTitle>
            <CardDescription>Manage PMDA users, roles and download permission.</CardDescription>
          </div>
          <Button type="button" variant="outline" onClick={() => void loadUsers()} disabled={loading}>
            {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
            Refresh
          </Button>
        </CardHeader>
        <CardContent className="space-y-4">
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading users…</p>
          ) : sortedUsers.length === 0 ? (
            <p className="text-sm text-muted-foreground">No users found.</p>
          ) : (
            sortedUsers.map((u) => {
              const draft = drafts[u.id] ?? toDraft(u);
              const isSelf = Number(auth.user?.id) === Number(u.id);
              return (
                <div key={u.id} className="rounded-lg border p-4">
                  <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                    <p className="text-sm font-medium">#{u.id} · {u.username}</p>
                    <p className="text-xs text-muted-foreground">Last login: {formatLastLogin(u.last_login_at)}</p>
                  </div>
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-5">
                    <div className="space-y-1">
                      <Label htmlFor={`username-${u.id}`}>Username</Label>
                      <Input
                        id={`username-${u.id}`}
                        value={draft.username}
                        onChange={(e) => updateDraft(u.id, { username: e.target.value })}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor={`password-${u.id}`}>Reset password</Label>
                      <PasswordInput
                        id={`password-${u.id}`}
                        value={draft.password}
                        onChange={(e) => updateDraft(u.id, { password: e.target.value })}
                        placeholder="Leave empty"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor={`admin-${u.id}`}>Admin</Label>
                      <div className="flex h-10 items-center">
                        <Switch
                          id={`admin-${u.id}`}
                          checked={Boolean(draft.is_admin)}
                          onCheckedChange={(checked) => updateDraft(u.id, { is_admin: Boolean(checked) })}
                          disabled={isSelf}
                        />
                      </div>
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor={`download-${u.id}`}>Can download</Label>
                      <div className="flex h-10 items-center">
                        <Switch
                          id={`download-${u.id}`}
                          checked={Boolean(draft.can_download)}
                          onCheckedChange={(checked) => updateDraft(u.id, { can_download: Boolean(checked) })}
                        />
                      </div>
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor={`active-${u.id}`}>Active</Label>
                      <div className="flex h-10 items-center justify-between gap-2">
                        <Switch
                          id={`active-${u.id}`}
                          checked={Boolean(draft.is_active)}
                          onCheckedChange={(checked) => updateDraft(u.id, { is_active: Boolean(checked) })}
                        />
                        <Button
                          type="button"
                          size="sm"
                          onClick={() => void saveUser(u.id)}
                          disabled={savingId === u.id}
                        >
                          {savingId === u.id ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                          Save
                        </Button>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Create User</CardTitle>
          <CardDescription>Add a new account and assign permissions.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="new-username">Username</Label>
              <Input
                id="new-username"
                value={newUser.username}
                onChange={(e) => setNewUser((prev) => ({ ...prev, username: e.target.value }))}
                minLength={3}
                maxLength={48}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="new-password">Password</Label>
              <PasswordInput
                id="new-password"
                value={newUser.password}
                onChange={(e) => setNewUser((prev) => ({ ...prev, password: e.target.value }))}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="new-password-confirm">Confirm password</Label>
              <PasswordInput
                id="new-password-confirm"
                value={newUser.passwordConfirm}
                onChange={(e) => setNewUser((prev) => ({ ...prev, passwordConfirm: e.target.value }))}
              />
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-6">
            <div className="flex items-center gap-2">
              <Switch
                id="new-is-admin"
                checked={Boolean(newUser.is_admin)}
                onCheckedChange={(checked) => setNewUser((prev) => ({ ...prev, is_admin: Boolean(checked) }))}
              />
              <Label htmlFor="new-is-admin">Admin</Label>
            </div>
            <div className="flex items-center gap-2">
              <Switch
                id="new-can-download"
                checked={Boolean(newUser.can_download)}
                onCheckedChange={(checked) => setNewUser((prev) => ({ ...prev, can_download: Boolean(checked) }))}
              />
              <Label htmlFor="new-can-download">Can download albums</Label>
            </div>
          </div>
          <Button type="button" onClick={() => void onCreateUser()} disabled={createBusy}>
            {createBusy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <UserPlus className="mr-2 h-4 w-4" />}
            Create user
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
