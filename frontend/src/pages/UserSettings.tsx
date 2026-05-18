import { ProfileSharingSettings } from '@/components/settings/ProfileSharingSettings';
import { UserConcertSettings } from '@/components/settings/UserConcertSettings';

export default function UserSettings() {
  return (
    <div className="space-y-6 p-6">
      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">User settings</div>
        <h1 className="text-3xl font-semibold tracking-tight">Personal profile and preferences</h1>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Manage your avatar, sharing preferences, password, and concert radius without touching the global PMDA setup.
        </p>
      </div>
      <ProfileSharingSettings />
      <UserConcertSettings />
    </div>
  );
}
