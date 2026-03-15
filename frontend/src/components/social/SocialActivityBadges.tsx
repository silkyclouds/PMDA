import { useEffect, useState } from 'react';
import { Heart, Share2 } from 'lucide-react';
import { useLocation, useNavigate } from 'react-router-dom';

import * as api from '@/lib/api';
import { withBackLinkState } from '@/lib/backNavigation';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

type Props = {
  entityType: 'artist' | 'album' | 'label' | 'genre' | 'playlist' | 'track';
  entityId?: number | null;
  entityKey?: string | null;
  className?: string;
  compact?: boolean;
};

function initials(username: string): string {
  const clean = String(username || '').trim();
  return clean ? clean.slice(0, 2).toUpperCase() : 'U';
}

function UserChip({
  user,
  kind,
  compact,
  onClick,
}: {
  user: api.SocialUser;
  kind: 'liked' | 'recommended';
  compact: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        'inline-flex items-center gap-2 rounded-full border border-border/70 bg-background/55 transition-colors hover:bg-muted/40',
        compact ? 'px-2 py-1 text-[10px]' : 'px-2.5 py-1.5 text-[11px]'
      )}
      title={kind === 'liked' ? `Open ${user.username}'s liked items` : `Open ${user.username}'s recommendations`}
    >
      <Avatar className={cn('rounded-full border border-border/60', compact ? 'h-5 w-5' : 'h-6 w-6')}>
        {user.avatar_data_url ? <AvatarImage src={user.avatar_data_url} alt={user.username} /> : null}
        <AvatarFallback className={cn('rounded-full text-[9px] font-semibold', compact ? 'text-[8px]' : 'text-[9px]')}>
          {initials(user.username)}
        </AvatarFallback>
      </Avatar>
      <span className="truncate max-w-[12rem]">{user.username}</span>
    </button>
  );
}

export function SocialActivityBadges({ entityType, entityId, entityKey, className, compact = false }: Props) {
  const navigate = useNavigate();
  const location = useLocation();
  const [data, setData] = useState<api.SocialContextResponse | null>(null);

  useEffect(() => {
    let cancelled = false;
    const numericId = Number(entityId || 0);
    const cleanKey = String(entityKey || '').trim();
    if (numericId <= 0 && !cleanKey) {
      setData(null);
      return;
    }
    void (async () => {
      try {
        const res = await api.getSocialContext({
          entity_type: entityType,
          entity_id: numericId > 0 ? numericId : undefined,
          entity_key: cleanKey || undefined,
        });
        if (!cancelled) setData(res);
      } catch {
        if (!cancelled) setData(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [entityId, entityKey, entityType]);

  const likedBy = data?.liked_by || [];
  const recommendedBy = data?.recommended_by || [];
  if (likedBy.length === 0 && recommendedBy.length === 0) return null;

  return (
    <div className={cn('space-y-2', className)}>
      <div className={cn(compact ? 'text-[9px]' : 'text-[10px]', 'font-medium uppercase tracking-[0.2em] text-muted-foreground/85')}>
        Local activity
      </div>
      <div className="flex flex-wrap items-start gap-2">
        {likedBy.length > 0 ? (
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge variant="outline" className={cn(compact ? 'text-[9px]' : 'text-[10px]')}>
              <Heart className="mr-1 h-3 w-3" />
              Liked locally by
            </Badge>
            {likedBy.map((entry) => (
              <UserChip
                key={`liked-by-${entry.id}`}
                user={entry}
                kind="liked"
                compact={compact}
                onClick={() => navigate(`/library/liked?user=${entry.id}`, { state: withBackLinkState(location) })}
              />
            ))}
          </div>
        ) : null}
        {recommendedBy.length > 0 ? (
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge variant="outline" className={cn(compact ? 'text-[9px]' : 'text-[10px]')}>
              <Share2 className="mr-1 h-3 w-3" />
              Recommended locally by
            </Badge>
            {recommendedBy.map((entry) => (
              <UserChip
                key={`recommended-by-${entry.id}`}
                user={entry}
                kind="recommended"
                compact={compact}
                onClick={() => navigate(`/library/recommendations?user=${entry.id}`, { state: withBackLinkState(location) })}
              />
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}
