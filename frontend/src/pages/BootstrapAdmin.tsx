import { FormEvent, useState } from 'react';
import { Loader2, ShieldPlus } from 'lucide-react';

import { useAuth } from '@/contexts/AuthContext';
import { Card, CardContent, CardDescription, CardHeader } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { PasswordInput } from '@/components/ui/password-input';
import { Button } from '@/components/ui/button';

function extractApiError(error: unknown): string {
  const msg = (error as { body?: { error?: unknown } } | null)?.body?.error;
  if (typeof msg === 'string' && msg.trim()) return msg;
  if (error instanceof Error && error.message.trim()) return error.message;
  return 'Bootstrap failed';
}

export default function BootstrapAdminPage() {
  const auth = useAuth();

  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [passwordConfirm, setPasswordConfirm] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsSubmitting(true);
    try {
      await auth.bootstrapAdmin(username.trim(), password, passwordConfirm);
    } catch (err) {
      setError(extractApiError(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-amber-50 via-orange-50 to-stone-100 p-6 md:p-10">
      <div className="mx-auto flex min-h-[80vh] w-full max-w-lg items-center">
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
            <CardDescription>
              First run detected. Create the initial administrator account.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={onSubmit} className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="bootstrap-username">Admin username</Label>
                <Input
                  id="bootstrap-username"
                  autoComplete="username"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="admin"
                  minLength={3}
                  maxLength={48}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="bootstrap-password">Password</Label>
                <PasswordInput
                  id="bootstrap-password"
                  autoComplete="new-password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="bootstrap-password-confirm">Confirm password</Label>
                <PasswordInput
                  id="bootstrap-password-confirm"
                  autoComplete="new-password"
                  value={passwordConfirm}
                  onChange={(e) => setPasswordConfirm(e.target.value)}
                  required
                />
              </div>
              {error ? <p className="text-sm text-destructive">{error}</p> : null}
              <Button type="submit" className="w-full" disabled={isSubmitting}>
                {isSubmitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <ShieldPlus className="mr-2 h-4 w-4" />}
                Create Admin Account
              </Button>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
