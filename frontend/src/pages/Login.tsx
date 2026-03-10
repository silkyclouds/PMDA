import { FormEvent, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { Loader2, LogIn } from 'lucide-react';

import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { PasswordInput } from '@/components/ui/password-input';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';

function extractApiError(error: unknown): string {
  const msg = (error as { body?: { error?: unknown } } | null)?.body?.error;
  if (typeof msg === 'string' && msg.trim()) return msg;
  if (error instanceof Error && error.message.trim()) return error.message;
  return 'Login failed';
}

export default function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const auth = useAuth();

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [rememberMe, setRememberMe] = useState(true);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const nextPath = useMemo(() => {
    const raw = new URLSearchParams(location.search).get('next') || '/library';
    return raw.startsWith('/') ? raw : '/library';
  }, [location.search]);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsSubmitting(true);
    try {
      await auth.login(username.trim(), password, rememberMe);
      navigate(nextPath, { replace: true });
    } catch (err) {
      setError(extractApiError(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-stone-50 to-zinc-100 p-6 md:p-10">
      <div className="mx-auto flex min-h-[80vh] w-full max-w-md items-center">
        <Card className="w-full shadow-sm">
          <CardHeader className="space-y-4">
            <div className="flex justify-center">
              <img
                src="/pmda-logo-mute-v1-transparent-cropped.png"
                alt="PMDA"
                width={1024}
                height={514}
                className="h-auto w-56 max-w-[85%] object-contain"
                loading="eager"
                decoding="async"
              />
            </div>
          </CardHeader>
          <CardContent>
            <form onSubmit={onSubmit} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="username">Username</Label>
                <Input
                  id="username"
                  autoComplete="username"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="admin"
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <PasswordInput
                  id="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
              </div>
              <div className="flex items-center gap-2">
                <Checkbox
                  id="remember-me"
                  checked={rememberMe}
                  onCheckedChange={(next) => setRememberMe(next === true)}
                />
                <Label htmlFor="remember-me" className="cursor-pointer text-sm font-normal">
                  Remember me
                </Label>
              </div>
              {error ? <p className="text-sm text-destructive">{error}</p> : null}
              <Button type="submit" className="w-full" disabled={isSubmitting}>
                {isSubmitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <LogIn className="mr-2 h-4 w-4" />}
                Sign In
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
