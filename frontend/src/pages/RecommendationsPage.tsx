import { useCallback, useEffect, useMemo, useState } from 'react';
import { Heart, Inbox, Loader2, MessageSquareShare, Send, Share2 } from 'lucide-react';
import { useLocation, useNavigate } from 'react-router-dom';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { AlbumArtwork } from '@/components/library/AlbumArtwork';
import { ShareDialog } from '@/components/social/ShareDialog';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useToast } from '@/hooks/use-toast';
import { withBackLinkState } from '@/lib/backNavigation';
import { formatBadgeDateTime } from '@/lib/dateFormat';

function openEntity(
  navigate: ReturnType<typeof useNavigate>,
  location: ReturnType<typeof useLocation>,
  item: api.RecommendationItem,
) {
  const href = String(item.entity_href || '').trim();
  if (!href) return;
  if (href.startsWith('/')) {
    navigate(`${href}${location.search || ''}`, { state: withBackLinkState(location) });
    return;
  }
  window.open(href, '_blank', 'noopener,noreferrer');
}

export default function RecommendationsPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth();
  const { toast } = useToast();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<'received' | 'sent'>('received');
  const [data, setData] = useState<api.RecommendationListResponse | null>(null);
  const [likingId, setLikingId] = useState<number | null>(null);
  const [visibleUsers, setVisibleUsers] = useState<api.SocialUser[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string>('me');
  const requestedUserId = useMemo(() => {
    const value = new URLSearchParams(location.search).get('user');
    return value && /^\d+$/.test(value) ? value : 'me';
  }, [location.search]);

  useEffect(() => {
    setSelectedUserId(requestedUserId);
  }, [requestedUserId]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await api.getRecommendations(selectedUserId === 'me' ? undefined : Number(selectedUserId));
      setData(res);
    } catch (err) {
      setData(null);
      setError(err instanceof Error ? err.message : 'Failed to load recommendations');
    } finally {
      setLoading(false);
    }
  }, [selectedUserId]);

  const loadUsers = useCallback(async () => {
    try {
      const res = await api.getSocialUsers('recommendations');
      setVisibleUsers(Array.isArray(res.users) ? res.users : []);
    } catch {
      setVisibleUsers([]);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  useEffect(() => {
    const qs = new URLSearchParams(location.search);
    if (selectedUserId === 'me') {
      qs.delete('user');
    } else {
      qs.set('user', selectedUserId);
    }
    const nextSearch = qs.toString() ? `?${qs.toString()}` : '';
    if (nextSearch !== location.search) {
      navigate({ pathname: location.pathname, search: nextSearch }, { replace: true });
    }
  }, [location.pathname, location.search, navigate, selectedUserId]);

  const items = useMemo(
    () => (tab === 'received' ? (data?.received || []) : (data?.sent || [])),
    [data?.received, data?.sent, tab],
  );

  const handleLike = async (recommendationId: number) => {
    if (likingId) return;
    setLikingId(recommendationId);
    try {
      await api.likeRecommendation(recommendationId);
      toast({ title: 'Recommendation liked', description: 'The sender has been notified.' });
      await load();
    } catch (err) {
      toast({
        title: 'Like failed',
        description: err instanceof Error ? err.message : 'Could not like this recommendation',
        variant: 'destructive',
      });
    } finally {
      setLikingId(null);
    }
  };

  return (
    <div className="container py-6 space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h1 className="text-3xl font-bold tracking-tight">Recommendations</h1>
          <p className="text-sm text-muted-foreground">
            Albums, artists, labels and playlists shared between PMDA users.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Select value={selectedUserId} onValueChange={setSelectedUserId}>
            <SelectTrigger className="w-[240px]">
              <SelectValue placeholder="Choose a user" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="me">My recommendations</SelectItem>
              {visibleUsers.map((candidate) => (
                <SelectItem key={`recommend-user-${candidate.id}`} value={String(candidate.id)}>
                  {candidate.username}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {Number(data?.unread_count || 0) > 0 ? (
            <Badge variant="outline" className="text-[11px]">
              {data?.unread_count} unread
            </Badge>
          ) : null}
          <Button type="button" variant="outline" size="sm" onClick={() => void load()}>
            Refresh
          </Button>
        </div>
      </div>

      <Tabs value={tab} onValueChange={(next) => setTab(next as 'received' | 'sent')}>
        <TabsList className="grid w-full max-w-md grid-cols-2">
          <TabsTrigger value="received" className="gap-2">
            <Inbox className="h-4 w-4" />
            Received
          </TabsTrigger>
          <TabsTrigger value="sent" className="gap-2">
            <Send className="h-4 w-4" />
            Sent
          </TabsTrigger>
        </TabsList>
      </Tabs>

      {data?.owner?.username ? (
        <div className="text-sm text-muted-foreground">
          Viewing recommendations for <span className="font-medium text-foreground">{selectedUserId === 'me' ? (user?.username || data.owner.username) : data.owner.username}</span>
        </div>
      ) : null}

      {loading ? (
        <div className="flex items-center justify-center py-24 text-muted-foreground">
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
          Loading recommendations…
        </div>
      ) : error ? (
        <Card className="border-destructive/40">
          <CardContent className="p-6 text-sm text-destructive">{error}</CardContent>
        </Card>
      ) : items.length === 0 ? (
        <Card>
          <CardContent className="p-8 text-sm text-muted-foreground">
            {tab === 'received' ? 'No recommendations received yet.' : 'You have not recommended anything yet.'}
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {items.map((item) => (
            <Card key={`recommendation-${item.recommendation_id}`} className="border-border/70">
              <CardContent className="p-5">
                <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                  <div className="flex min-w-0 gap-4">
                    <div className="h-24 w-24 shrink-0 overflow-hidden rounded-2xl bg-muted">
                      {item.entity_thumb ? (
                        <img src={item.entity_thumb} alt={item.entity_label} className="h-full w-full object-cover" />
                      ) : null}
                    </div>
                    <div className="min-w-0 space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant="outline" className="text-[10px] uppercase">
                          {item.entity_type}
                        </Badge>
                        <Badge variant="outline" className="text-[10px]">
                          {tab === 'received' ? `From ${item.sender_username}` : `To ${item.recipient_username}`}
                        </Badge>
                        {item.liked_by_recipient ? (
                          <Badge variant="outline" className="text-[10px]">
                            Liked
                          </Badge>
                        ) : null}
                      </div>
                      <div>
                        <div className="text-base font-semibold">{item.entity_label}</div>
                        {item.entity_subtitle ? (
                          <div className="text-sm text-muted-foreground">{item.entity_subtitle}</div>
                        ) : null}
                      </div>
                      {item.message ? (
                        <div className="rounded-2xl border border-border/60 bg-muted/20 px-4 py-3 text-sm text-muted-foreground">
                          “{item.message}”
                        </div>
                      ) : null}
                      <div className="text-xs text-muted-foreground">
                        {formatBadgeDateTime(item.created_at)}
                      </div>
                    </div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <Button type="button" size="sm" variant="outline" onClick={() => openEntity(navigate, location, item)}>
                      Open
                    </Button>
                    {tab === 'received' && !item.liked_by_recipient ? (
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        className="gap-2"
                        disabled={likingId === item.recommendation_id}
                        onClick={() => void handleLike(item.recommendation_id)}
                      >
                        {likingId === item.recommendation_id ? <Loader2 className="h-4 w-4 animate-spin" /> : <Heart className="h-4 w-4" />}
                        Like
                      </Button>
                    ) : null}
                    <ShareDialog
                      entityType={item.entity_type}
                      entityId={item.entity_id}
                      entityKey={item.entity_key}
                      entityLabel={item.entity_label}
                      entitySubtitle={item.entity_subtitle}
                      parentRecommendationId={item.recommendation_id}
                      trigger={(
                        <Button type="button" size="sm" variant="outline" className="gap-2">
                          <MessageSquareShare className="h-4 w-4" />
                          Recommend back
                        </Button>
                      )}
                    />
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
