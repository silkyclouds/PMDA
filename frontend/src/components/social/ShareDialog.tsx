import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Loader2, Send, Share2 } from 'lucide-react';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { useToast } from '@/hooks/use-toast';
import { Button } from '@/components/ui/button';
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Textarea } from '@/components/ui/textarea';

type ShareDialogProps = {
  entityType: api.LikeEntityType;
  entityId?: number | null;
  entityKey?: string | null;
  entityLabel: string;
  entitySubtitle?: string | null;
  parentRecommendationId?: number | null;
  trigger?: ReactNode;
  onShared?: (count: number) => void;
};

function getInitials(username: string): string {
  const clean = String(username || '').trim();
  return clean ? clean.slice(0, 2).toUpperCase() : 'U';
}

export function ShareDialog({
  entityType,
  entityId,
  entityKey,
  entityLabel,
  entitySubtitle,
  parentRecommendationId,
  trigger,
  onShared,
}: ShareDialogProps) {
  const { user } = useAuth();
  const { toast } = useToast();
  const [open, setOpen] = useState(false);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const [users, setUsers] = useState<api.SocialUser[]>([]);
  const [selectedUserIds, setSelectedUserIds] = useState<number[]>([]);
  const [message, setMessage] = useState('');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    const run = async () => {
      setLoadingUsers(true);
      try {
        const res = await api.getSocialUsers();
        if (cancelled) return;
        const all = Array.isArray(res.users) ? res.users : [];
        setUsers(all.filter((candidate) => candidate.is_active && candidate.id !== user?.id));
      } catch (error) {
        if (!cancelled) {
          toast({
            title: 'Share unavailable',
            description: error instanceof Error ? error.message : 'Failed to load users',
            variant: 'destructive',
          });
        }
      } finally {
        if (!cancelled) setLoadingUsers(false);
      }
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [open, toast, user?.id]);

  const selectedCount = selectedUserIds.length;
  const cleanEntityKey = String(entityKey || '').trim();
  const canSubmit =
    !submitting
    && selectedCount > 0
    && ((Number(entityId || 0) > 0) || Boolean(cleanEntityKey));

  const subtitle = useMemo(() => {
    const pieces = [entityLabel, entitySubtitle].map((value) => String(value || '').trim()).filter(Boolean);
    return pieces.join(' · ');
  }, [entityLabel, entitySubtitle]);

  const toggleUser = (candidateId: number) => {
    setSelectedUserIds((prev) => (
      prev.includes(candidateId)
        ? prev.filter((id) => id !== candidateId)
        : [...prev, candidateId]
    ));
  };

  const reset = () => {
    setSelectedUserIds([]);
    setMessage('');
  };

  const handleShare = async () => {
    if (!canSubmit) return;
    setSubmitting(true);
    try {
      const res = await api.shareLibraryEntity({
        entity_type: entityType,
        entity_id: Number(entityId || 0) > 0 ? Number(entityId) : undefined,
        entity_key: cleanEntityKey || undefined,
        recipient_user_ids: selectedUserIds,
        message: message.trim() || undefined,
        parent_recommendation_id: Number(parentRecommendationId || 0) > 0 ? Number(parentRecommendationId) : undefined,
      });
      const count = Number(res.count || selectedCount || 0);
      toast({
        title: 'Recommendation sent',
        description: count === 1
          ? `${entityLabel} was shared with 1 user.`
          : `${entityLabel} was shared with ${count} users.`,
      });
      onShared?.(count);
      setOpen(false);
      reset();
    } catch (error) {
      toast({
        title: 'Share failed',
        description: error instanceof Error ? error.message : 'Could not send recommendation',
        variant: 'destructive',
      });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(next) => {
      setOpen(next);
      if (!next) reset();
    }}>
      {trigger ? (
        <DialogTrigger asChild>{trigger}</DialogTrigger>
      ) : (
        <DialogTrigger asChild>
          <Button type="button" size="sm" variant="outline" className="h-8 gap-2">
            <Share2 className="h-4 w-4" />
            Share
          </Button>
        </DialogTrigger>
      )}
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Share2 className="h-4 w-4 text-primary" />
            Share recommendation
          </DialogTitle>
          <DialogDescription>
            Recommend this item to other PMDA users.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="rounded-2xl border border-border/60 bg-muted/20 p-4">
            <div className="text-sm font-medium">{entityLabel}</div>
            {subtitle && subtitle !== entityLabel ? (
              <div className="mt-1 text-xs text-muted-foreground">{subtitle}</div>
            ) : null}
          </div>

          <div className="space-y-2">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
              Send to
            </div>
            <ScrollArea className="h-48 rounded-2xl border border-border/60 bg-background/40 p-3">
              {loadingUsers ? (
                <div className="flex items-center gap-2 px-1 py-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading users…
                </div>
              ) : users.length === 0 ? (
                <div className="px-1 py-2 text-sm text-muted-foreground">
                  No other active users available.
                </div>
              ) : (
                <div className="space-y-2">
                  {users.map((candidate) => (
                    <label
                      key={`share-user-${candidate.id}`}
                      className="flex cursor-pointer items-center gap-3 rounded-xl border border-transparent px-2 py-2 transition-colors hover:border-border/60 hover:bg-muted/30"
                    >
                      <Checkbox
                        checked={selectedUserIds.includes(candidate.id)}
                        onCheckedChange={() => toggleUser(candidate.id)}
                      />
                      <Avatar className="h-9 w-9 rounded-xl border border-border/60">
                        {candidate.avatar_data_url ? <AvatarImage src={candidate.avatar_data_url} alt={candidate.username} /> : null}
                        <AvatarFallback className="rounded-xl text-xs font-semibold">{getInitials(candidate.username)}</AvatarFallback>
                      </Avatar>
                      <div className="min-w-0">
                        <div className="truncate text-sm font-medium">{candidate.username}</div>
                        <div className="truncate text-xs text-muted-foreground">
                          {candidate.is_admin ? 'Administrator' : 'Library user'}
                        </div>
                      </div>
                    </label>
                  ))}
                </div>
              )}
            </ScrollArea>
          </div>

          <div className="space-y-2">
            <div className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
              Message
            </div>
            <Textarea
              value={message}
              onChange={(event) => setMessage(event.target.value)}
              placeholder="Optional note. Example: This reminded me of something you already like."
              className="min-h-[110px]"
            />
          </div>
        </div>

        <DialogFooter className="gap-2 sm:justify-between">
          <div className="text-xs text-muted-foreground">
            {selectedCount > 0 ? `${selectedCount} recipient${selectedCount > 1 ? 's' : ''} selected` : 'Pick at least one user'}
          </div>
          <Button type="button" className="gap-2" disabled={!canSubmit} onClick={() => void handleShare()}>
            {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            Send recommendation
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
