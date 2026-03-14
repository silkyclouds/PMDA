import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';

import * as api from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';
import { useToast } from '@/hooks/use-toast';

function resolveNotificationHref(item: api.UserNotificationItem): string | null {
  const payloadHref = typeof item.payload?.entity_href === 'string' ? item.payload.entity_href.trim() : '';
  if (payloadHref.startsWith('/')) return payloadHref;
  if (item.recommendation_id) return '/library/recommendations';
  if (item.entity_type === 'album' && item.entity_id) return `/library/album/${item.entity_id}`;
  if (item.entity_type === 'artist' && item.entity_id) return `/library/artist/${item.entity_id}`;
  if (item.entity_type === 'playlist' && item.entity_id) return `/library/playlists/${item.entity_id}`;
  if (item.entity_type === 'label' && item.entity_key) return `/library/label/${encodeURIComponent(item.entity_key)}`;
  if (item.entity_type === 'genre' && item.entity_key) return `/library/genre/${encodeURIComponent(item.entity_key)}`;
  return null;
}

export function SocialNotificationsBridge() {
  const { user } = useAuth();
  const { toast } = useToast();
  const navigate = useNavigate();
  const seenRef = useRef<Set<number>>(new Set());

  useEffect(() => {
    seenRef.current = new Set();
  }, [user?.id]);

  useEffect(() => {
    if (!user) return;
    let cancelled = false;

    const poll = async () => {
      try {
        const res = await api.getNotifications(20, true);
        if (cancelled) return;
        for (const item of res.notifications || []) {
          const notificationId = Number(item.notification_id || 0);
          if (notificationId <= 0 || seenRef.current.has(notificationId)) continue;
          seenRef.current.add(notificationId);
          const href = resolveNotificationHref(item);
          toast({
            title: item.title || 'New notification',
            description: item.body || 'You have a new activity in PMDA.',
            duration: 7000,
          });
          void api.markNotificationRead(notificationId).catch(() => undefined);
          if (href) {
            // Keep the route discoverable from the toast text without forcing navigation.
            void href;
          }
        }
      } catch {
        // Best effort only.
      }
    };

    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 30_000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [navigate, toast, user]);

  return null;
}
