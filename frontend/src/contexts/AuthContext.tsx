import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';

import * as api from '@/lib/api';

interface AuthContextValue {
  isLoading: boolean;
  bootstrapRequired: boolean;
  user: api.AuthUser | null;
  isAdmin: boolean;
  canDownload: boolean;
  login: (username: string, password: string) => Promise<void>;
  bootstrapAdmin: (username: string, password: string, passwordConfirm: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshSession: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function apiStatusCode(error: unknown): number | null {
  const status = (error as { response?: { status?: unknown } } | null)?.response?.status;
  const numeric = Number(status);
  return Number.isFinite(numeric) ? numeric : null;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isLoading, setIsLoading] = useState(true);
  const [bootstrapRequired, setBootstrapRequired] = useState(false);
  const [user, setUser] = useState<api.AuthUser | null>(null);

  useEffect(() => {
    api.installGlobalAuthFetchInterceptor();
  }, []);

  const refreshSession = useCallback(async () => {
    const token = api.getAuthToken();
    if (!token) {
      setUser(null);
      return;
    }
    try {
      const me = await api.me();
      setUser(me.user ?? null);
    } catch (error) {
      const status = apiStatusCode(error);
      if (status === 401 || status === 403) {
        api.clearAuthToken();
      }
      setUser(null);
    }
  }, []);

  const initialize = useCallback(async () => {
    setIsLoading(true);
    try {
      const status = await api.getAuthBootstrapStatus();
      const required = Boolean(status?.bootstrap_required);
      setBootstrapRequired(required);
      if (required) {
        api.clearAuthToken();
        setUser(null);
        return;
      }
      await refreshSession();
    } catch {
      setBootstrapRequired(false);
      await refreshSession();
    } finally {
      setIsLoading(false);
    }
  }, [refreshSession]);

  useEffect(() => {
    void initialize();
  }, [initialize]);

  const login = useCallback(async (username: string, password: string) => {
    const payload = await api.login({ username, password });
    api.setAuthToken(payload.token);
    setBootstrapRequired(false);
    setUser(payload.user ?? null);
  }, []);

  const bootstrapAdmin = useCallback(
    async (username: string, password: string, passwordConfirm: string) => {
      await api.bootstrapAdmin({ username, password, password_confirm: passwordConfirm });
      await login(username, password);
    },
    [login],
  );

  const logout = useCallback(async () => {
    try {
      await api.logout();
    } catch {
      // best-effort logout
    }
    api.clearAuthToken();
    setUser(null);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      isLoading,
      bootstrapRequired,
      user,
      isAdmin: Boolean(user?.is_admin),
      canDownload: Boolean(user?.can_download),
      login,
      bootstrapAdmin,
      logout,
      refreshSession,
    }),
    [bootstrapAdmin, bootstrapRequired, isLoading, login, logout, refreshSession, user],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return ctx;
}
