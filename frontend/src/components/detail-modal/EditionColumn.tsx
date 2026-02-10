import { useState } from 'react';
import { Crown, Copy, Check, Disc3, Database } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { RadioGroupItem } from '@/components/ui/radio-group';
import { Label } from '@/components/ui/label';
import { FormatBadge } from '@/components/FormatBadge';
import { cn } from '@/lib/utils';
import type { Edition } from '@/lib/api';

interface EditionColumnProps {
  edition: Edition;
  index: number;
  isSelected: boolean;
  totalEditions: number;
}

export function EditionColumn({ edition, index, isSelected, totalEditions }: EditionColumnProps) {
  const [copied, setCopied] = useState(false);
  const [copiedMbid, setCopiedMbid] = useState(false);
  const path = edition.path || edition.folder || '';

  const copyPath = async () => {
    if (!path) return;
    await navigator.clipboard.writeText(path);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const formatSize = (bytes: number) => {
    if (!bytes) return '0 MB';
    const mb = bytes / (1024 * 1024);
    return `${mb.toFixed(1)} MB`;
  };

  const bonusCount = edition.tracks?.filter(t => t.is_bonus).length || 0;

  return (
    <div 
      className={cn(
        "flex-1 min-w-0 flex-shrink-0 p-4 rounded-xl border transition-all overflow-hidden",
        isSelected 
          ? "border-primary bg-primary/5 ring-2 ring-primary/20" 
          : "border-border bg-muted/30"
      )}
      style={{ flex: '1 1 0%' }}
    >
      {/* Selection radio */}
      <div className="flex items-center gap-2 mb-3">
        <RadioGroupItem 
          value={index.toString()} 
          id={`edition-${index}`}
        />
        <Label htmlFor={`edition-${index}`} className="cursor-pointer flex-1">
          {isSelected ? (
            <span className="inline-flex items-center gap-1 text-xs font-semibold text-primary bg-primary/10 px-2 py-0.5 rounded-full">
              <Crown className="w-3 h-3" />
              Keep
            </span>
          ) : (
            <span className="text-xs font-medium text-destructive/80 bg-destructive/10 px-2 py-0.5 rounded-full">
              Delete
            </span>
          )}
        </Label>
      </div>

      {/* Cover */}
      <div className="cover-image w-full aspect-square mb-3">
        {edition.thumb_data ? (
          <img
            src={edition.thumb_data}
            alt=""
            className="w-full h-full object-cover"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center bg-muted">
            <Disc3 className="w-8 h-8 text-muted-foreground/50" />
          </div>
        )}
      </div>

      {/* Title and badges */}
      <div className="space-y-2">
        <h4 className="font-medium text-foreground text-sm leading-tight line-clamp-2" title={edition.title_raw}>
          {edition.title_raw}
        </h4>

        <div className="flex items-center gap-2 flex-wrap">
          <FormatBadge format={edition.fmt} />
          {edition.track_count && (
            <span className="text-xs text-muted-foreground">
              {edition.track_count} tracks
            </span>
          )}
          {bonusCount > 0 && (
            <span className="text-xs text-warning font-medium">
              +{bonusCount} bonus
            </span>
          )}
        </div>

        {/* Technical specs: backend always sends real values (derived from tracks/ffprobe when needed) */}
        <div className="flex flex-wrap gap-x-2 gap-y-1 text-xs text-muted-foreground">
          <span className="font-medium">{formatSize(edition.size)}</span>
          <span>{edition.br} kbps</span>
          <span>{(edition.sr / 1000).toFixed(1)} kHz</span>
          <span>{edition.bd}-bit</span>
        </div>

        {/* MusicBrainz ID */}
        {edition.musicbrainz_id && (
          <div className="flex items-center gap-1.5 mt-2 p-2 rounded-md bg-blue-500/5 border border-blue-500/20">
            <Database className="w-3 h-3 text-blue-500 shrink-0" />
            <div className="flex-1 min-w-0">
              <span className="text-[10px] text-blue-600 dark:text-blue-400 font-medium">MusicBrainz ID:</span>
              {edition.match_verified_by_ai && (
                <span className="ml-1 text-[9px] text-green-600 dark:text-green-400 font-medium" title="Match chosen by AI verify">(verified by AI)</span>
              )}
              <code className="block text-[10px] text-blue-700 dark:text-blue-300 font-mono truncate mt-0.5" title={edition.musicbrainz_id}>
                {edition.musicbrainz_id}
              </code>
            </div>
            <Button
              size="icon"
              variant="ghost"
              className="h-5 w-5 flex-shrink-0"
              onClick={async () => {
                if (edition.musicbrainz_id) {
                  await navigator.clipboard.writeText(edition.musicbrainz_id);
                  setCopiedMbid(true);
                  setTimeout(() => setCopiedMbid(false), 2000);
                }
              }}
            >
              {copiedMbid ? (
                <Check className="w-2.5 h-2.5 text-success" />
              ) : (
                <Copy className="w-2.5 h-2.5" />
              )}
            </Button>
          </div>
        )}

        {/* Path */}
        {path && (
          <div className="flex items-center gap-1 mt-2">
            <code className="path-display flex-1 truncate text-[10px]" title={path}>
              {path}
            </code>
            <Button
              size="icon"
              variant="ghost"
              className="h-5 w-5 flex-shrink-0"
              onClick={copyPath}
            >
              {copied ? (
                <Check className="w-2.5 h-2.5 text-success" />
              ) : (
                <Copy className="w-2.5 h-2.5" />
              )}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
