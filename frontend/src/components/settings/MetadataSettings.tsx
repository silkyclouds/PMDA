import { useState, useEffect, useRef } from 'react';
import { Database, ExternalLink, Loader2, CheckCircle2, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { Switch } from '@/components/ui/switch';
import { PasswordInput } from '@/components/ui/password-input';
import { FieldTooltip } from '@/components/ui/field-tooltip';
import * as api from '@/lib/api';
import type { PMDAConfig } from '@/lib/api';
import { toast } from 'sonner';

const KNOWN_REQUIRED_TAGS = ['artist', 'album', 'date', 'genre', 'year', 'tracks'] as const;

function normalizeRequiredTags(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.every((x) => typeof x === 'string') ? (raw as string[]) : ['artist', 'album', 'date'];
  }
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw) as unknown;
      return Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === 'string') : ['artist', 'album', 'date'];
    } catch {
      return raw ? raw.split(',').map((s) => s.trim()).filter(Boolean) : ['artist', 'album', 'date'];
    }
  }
  return ['artist', 'album', 'date'];
}

function RequiredTagsCheckboxes({
  config,
  updateConfig,
}: {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
}) {
  const selected = normalizeRequiredTags(config.REQUIRED_TAGS);
  const toggle = (tag: string) => {
    const next = selected.includes(tag)
      ? selected.filter((t) => t !== tag)
      : [...selected, tag];
    updateConfig({ REQUIRED_TAGS: next.length > 0 ? next : ['artist', 'album', 'date'] });
  };
  return (
    <div className="flex flex-wrap gap-4 rounded-lg border border-border bg-muted/30 p-3">
      {KNOWN_REQUIRED_TAGS.map((tag) => (
        <label key={tag} className="flex items-center gap-2 cursor-pointer">
          <Checkbox
            checked={selected.includes(tag)}
            onCheckedChange={() => toggle(tag)}
            aria-label={`Required tag: ${tag}`}
          />
          <span className="text-sm font-medium">
            {tag === 'tracks' ? 'Track numbers & titles' : tag.charAt(0).toUpperCase() + tag.slice(1)}
          </span>
        </label>
      ))}
    </div>
  );
}

interface MetadataSettingsProps {
  config: Partial<PMDAConfig>;
  updateConfig: (updates: Partial<PMDAConfig>) => void;
  errors: Record<string, string>;
}

export function MetadataSettings({ config, updateConfig, errors }: MetadataSettingsProps) {
  const [testingMB, setTestingMB] = useState(false);
  const [mbTestResult, setMbTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const testMusicBrainz = async () => {
    if (!config.USE_MUSICBRAINZ) {
      toast.error('Please enable MusicBrainz first');
      return;
    }
    setTestingMB(true);
    setMbTestResult(null);
    try {
      // Pass USE_MUSICBRAINZ in request so backend can test even if config not saved yet
      const result = await api.testMusicBrainz(config.USE_MUSICBRAINZ);
      setMbTestResult(result);
      if (result.success) {
        toast.success('MusicBrainz connection successful');
      } else {
        toast.error(result.message || 'MusicBrainz connection failed');
      }
    } catch (error: any) {
      const message = error?.message || 'Failed to test MusicBrainz connection';
      setMbTestResult({ success: false, message });
      toast.error(message);
    } finally {
      setTestingMB(false);
    }
  };


  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-4">
        <div className="p-2 rounded-lg bg-primary/10">
          <Database className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h3 className="font-medium">Metadata</h3>
          <p className="text-sm text-muted-foreground">
            Configure metadata lookup and enrichment
          </p>
        </div>
      </div>

      <div className="space-y-4">
        {/* MusicBrainz Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5 flex-1">
              <div className="flex items-center gap-1.5">
                <Label>Use MusicBrainz</Label>
                <FieldTooltip content="Enable MusicBrainz metadata lookup for improved album identification, release-group information, and Box Set handling." />
              </div>
              <p className="text-xs text-muted-foreground">
                Enable MusicBrainz metadata lookup
              </p>
            </div>
            <Switch
              checked={config.USE_MUSICBRAINZ ?? false}
              onCheckedChange={(checked) => updateConfig({ USE_MUSICBRAINZ: checked })}
            />
          </div>

          {config.USE_MUSICBRAINZ && (
            <div className="space-y-3 pt-2 border-t border-border">
              <div className="p-3 rounded-lg bg-muted/50 space-y-2">
                <p className="text-sm font-medium">What does this do?</p>
                <p className="text-xs text-muted-foreground">
                  When enabled, PMDA queries MusicBrainz to enrich album metadata with release-group information, 
                  helping to identify related releases (e.g., different editions, remasters, box sets). 
                  This improves duplicate detection accuracy, especially for albums with multiple releases.
                </p>
              </div>

              <div className="p-3 rounded-lg bg-warning/5 border border-warning/20 space-y-2">
                <p className="text-sm font-medium text-warning">Performance Impact</p>
                <p className="text-xs text-muted-foreground">
                  MusicBrainz lookups add network requests per album during scanning, which can slow down the process. 
                  The impact depends on your library size and network latency. For large libraries (thousands of albums), 
                  expect scanning to take significantly longer. Consider enabling this only if you need improved accuracy 
                  for complex duplicate scenarios.
                </p>
              </div>

              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <Label>Test MusicBrainz Connection</Label>
                    <FieldTooltip content="Test the connection to MusicBrainz API to verify it's working correctly." />
                  </div>
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    onClick={testMusicBrainz}
                    disabled={testingMB || !config.USE_MUSICBRAINZ}
                    className="gap-1.5"
                  >
                    {testingMB ? (
                      <>
                        <Loader2 className="w-3 h-3 animate-spin" />
                        Testing...
                      </>
                    ) : (
                      <>
                        Test Connection
                      </>
                    )}
                  </Button>
                </div>
                {mbTestResult && (
                  <div className={`p-2 rounded-lg flex items-start gap-2 ${
                    mbTestResult.success 
                      ? 'bg-green-500/10 border border-green-500/20' 
                      : 'bg-red-500/10 border border-red-500/20'
                  }`}>
                    {mbTestResult.success ? (
                      <CheckCircle2 className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
                    ) : (
                      <XCircle className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
                    )}
                    <p className={`text-xs ${mbTestResult.success ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                      {mbTestResult.message}
                    </p>
                  </div>
                )}

                  <div className="space-y-4 pt-2 border-t border-border">
                  <div className="flex items-center justify-between rounded-lg border border-border p-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="mb-retry-not-found" className="text-sm font-medium">Re-check MusicBrainz for albums previously not found</Label>
                      <p className="text-xs text-muted-foreground">
                        When on, each scan will re-query MusicBrainz for artist+album pairs that were previously cached as "not found". Albums already found stay cached.
                      </p>
                    </div>
                    <Switch
                      id="mb-retry-not-found"
                      checked={config.MB_RETRY_NOT_FOUND ?? false}
                      onCheckedChange={(checked) => updateConfig({ MB_RETRY_NOT_FOUND: checked })}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-lg border border-border p-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="use-ai-mb-match" className="text-sm font-medium">Use AI to choose among multiple MusicBrainz candidates</Label>
                      <p className="text-xs text-muted-foreground">
                        When several release-groups match (e.g. same title, different editions), use AI to pick the best one from titles only.
                      </p>
                    </div>
                    <Switch
                      id="use-ai-mb-match"
                      checked={config.USE_AI_FOR_MB_MATCH ?? false}
                      onCheckedChange={(checked) => updateConfig({ USE_AI_FOR_MB_MATCH: checked })}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-lg border border-border p-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="use-ai-mb-verify" className="text-sm font-medium">Use AI to verify MusicBrainz match</Label>
                      <p className="text-xs text-muted-foreground">
                        Send artist, title, track count and track titles to AI to confirm or pick the correct release-group (e.g. &quot;Volume I&quot; vs &quot;volume i&quot;). Requires AI configured.
                      </p>
                    </div>
                    <Switch
                      id="use-ai-mb-verify"
                      checked={config.USE_AI_FOR_MB_VERIFY ?? false}
                      onCheckedChange={(checked) => updateConfig({ USE_AI_FOR_MB_VERIFY: checked })}
                    />
                  </div>
                  <div className="flex items-center justify-between rounded-lg border border-border p-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="use-ai-vision-cover" className="text-sm font-medium">Use AI vision for cover comparison</Label>
                      <p className="text-xs text-muted-foreground">
                        After AI picks a MusicBrainz candidate, compare local cover image to Cover Art Archive. Reject match if they differ.
                      </p>
                    </div>
                    <Switch
                      id="use-ai-vision-cover"
                      checked={config.USE_AI_VISION_FOR_COVER ?? false}
                      onCheckedChange={(checked) => updateConfig({ USE_AI_VISION_FOR_COVER: checked })}
                    />
                  </div>
                  {(config.USE_AI_VISION_FOR_COVER ?? false) && (
                    <div className="space-y-2 pl-3 border-l-2 border-border">
                      <Label htmlFor="openai-vision-model" className="text-sm">Vision model (optional)</Label>
                      <Input
                        id="openai-vision-model"
                        placeholder="gpt-4o-mini (default)"
                        value={config.OPENAI_VISION_MODEL || ''}
                        onChange={(e) => updateConfig({ OPENAI_VISION_MODEL: e.target.value })}
                      />
                    </div>
                  )}
                  <div className="flex items-center justify-between rounded-lg border border-border p-3">
                    <div className="space-y-0.5">
                      <Label htmlFor="use-web-search-mb" className="text-sm font-medium">Use web search for MusicBrainz</Label>
                      <p className="text-xs text-muted-foreground">
                        When no MB candidate or AI says NONE, search the web (Serper) and ask AI to suggest an MBID from results.
                      </p>
                    </div>
                    <Switch
                      id="use-web-search-mb"
                      checked={config.USE_WEB_SEARCH_FOR_MB ?? false}
                      onCheckedChange={(checked) => updateConfig({ USE_WEB_SEARCH_FOR_MB: checked })}
                    />
                  </div>
                  {(config.USE_WEB_SEARCH_FOR_MB ?? false) && (
                    <div className="space-y-2 pl-3 border-l-2 border-border">
                      <Label htmlFor="serper-api-key" className="text-sm">Serper API key</Label>
                      <PasswordInput
                        id="serper-api-key"
                        placeholder="Serper.dev API key"
                        value={config.SERPER_API_KEY || ''}
                        onChange={(e) => updateConfig({ SERPER_API_KEY: e.target.value })}
                      />
                      <p className="text-xs text-muted-foreground">
                        Get a key at serper.dev (2500 free searches/month).
                      </p>
                    </div>
                  )}
                  <div className="p-3 rounded-lg bg-blue-500/5 border border-blue-500/20 space-y-2">
                    <p className="text-sm font-medium text-blue-600 dark:text-blue-400">Contact Email (Optional)</p>
                    <p className="text-xs text-muted-foreground">
                      Your email address will be included in the User-Agent header sent to MusicBrainz. 
                      This helps MusicBrainz contact you if needed. Rate limit: 1 request per second (standard for all users).
                    </p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <Label htmlFor="musicbrainz-email">Contact Email</Label>
                      <FieldTooltip content="Your email address for MusicBrainz User-Agent identification. Optional - defaults to pmda@example.com if not provided." />
                      <a
                        href="https://musicbrainz.org/doc/MusicBrainz_API"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                      >
                        API doc <ExternalLink className="w-3 h-3" />
                      </a>
                    </div>
                    <Input
                      id="musicbrainz-email"
                      type="email"
                      placeholder="your-email@example.com"
                      value={config.MUSICBRAINZ_EMAIL || ''}
                      onChange={(e) => {
                        updateConfig({ MUSICBRAINZ_EMAIL: e.target.value });
                      }}
                      className="w-full"
                    />
                    <p className="text-xs text-muted-foreground">
                      This email is only used for identification in API requests. MusicBrainz may contact you at this address if needed.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

        {/* Discogs Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5 flex-1">
              <div className="flex items-center gap-1.5">
                <Label>Use Discogs</Label>
                <FieldTooltip content="Enable Discogs metadata provider for album enrichment. Requires authentication for cover art access." />
              </div>
              <p className="text-xs text-muted-foreground">
                Enable Discogs metadata lookup (60 req/min authenticated, 25 req/min unauthenticated)
              </p>
            </div>
            <Switch
              checked={config.USE_DISCOGS ?? false}
              onCheckedChange={(checked) => updateConfig({ USE_DISCOGS: checked })}
            />
          </div>

          {config.USE_DISCOGS && (
            <div className="space-y-3 pt-2 border-t border-border">
              <div className="p-3 rounded-lg bg-muted/50 space-y-2">
                <p className="text-sm font-medium">Authentication</p>
                <p className="text-xs text-muted-foreground">
                  User Token (recommended): Generate in Settings → Developers → "Generate new token" on Discogs.
                  Provides access to cover art and higher rate limits (60 req/min vs 25 req/min).
                </p>
              </div>

              <div className="space-y-2">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <Label htmlFor="discogs-token">User Token (Recommended)</Label>
                  <FieldTooltip content="Discogs user token for authenticated requests." />
                  <a
                    href="https://www.discogs.com/settings/developers"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                  >
                    Get token <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                <PasswordInput
                  id="discogs-token"
                  placeholder="Your Discogs user token"
                  value={config.DISCOGS_USER_TOKEN || ''}
                  onChange={(e) => {
                    updateConfig({ DISCOGS_USER_TOKEN: e.target.value });
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  Optional: OAuth Consumer Key/Secret for multi-user apps (advanced)
                </p>
              </div>
            </div>
          )}
        </div>

        {/* Last.fm Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5 flex-1">
              <div className="flex items-center gap-1.5">
                <Label>Use Last.fm</Label>
                <FieldTooltip content="Enable Last.fm metadata provider for album enrichment, tags, and cover art." />
              </div>
              <p className="text-xs text-muted-foreground">
                Enable Last.fm metadata lookup (requires API key and secret)
              </p>
            </div>
            <Switch
              checked={config.USE_LASTFM ?? false}
              onCheckedChange={(checked) => updateConfig({ USE_LASTFM: checked })}
            />
          </div>

          {config.USE_LASTFM && (
            <div className="space-y-3 pt-2 border-t border-border">
              <div className="p-3 rounded-lg bg-muted/50 space-y-2">
                <p className="text-sm font-medium">API Credentials</p>
                <p className="text-xs text-muted-foreground">
                  Create an API account to get your API key and secret.
                </p>
              </div>

              <div className="space-y-2">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <Label htmlFor="lastfm-api-key">API Key</Label>
                  <FieldTooltip content="Last.fm API key (required)" />
                  <a
                    href="https://www.last.fm/api/account/create"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                  >
                    Get API key & secret <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                <Input
                  id="lastfm-api-key"
                  placeholder="Your Last.fm API key"
                  value={config.LASTFM_API_KEY || ''}
                  onChange={(e) => {
                    updateConfig({ LASTFM_API_KEY: e.target.value });
                  }}
                />
              </div>

              <div className="space-y-2">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <Label htmlFor="lastfm-api-secret">API Secret</Label>
                  <FieldTooltip content="Last.fm API secret (required). Same page as API key." />
                </div>
                <PasswordInput
                  id="lastfm-api-secret"
                  placeholder="Your Last.fm API secret"
                  value={config.LASTFM_API_SECRET || ''}
                  onChange={(e) => {
                    updateConfig({ LASTFM_API_SECRET: e.target.value });
                  }}
                />
              </div>
            </div>
          )}
        </div>

        {/* Bandcamp Section */}
        <div className="space-y-4 p-4 rounded-lg border border-border">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5 flex-1">
              <div className="flex items-center gap-1.5">
                <Label>Use Bandcamp</Label>
                <FieldTooltip content="Enable Bandcamp as ultimate fallback (scraping-based; use at your own risk, last resort only)." />
              </div>
              <p className="text-xs text-muted-foreground">
                Ultimate fallback after MusicBrainz, Discogs, and Last.fm. No public API; uses scraping or undocumented access.
              </p>
            </div>
            <Switch
              checked={config.USE_BANDCAMP ?? false}
              onCheckedChange={(checked) => updateConfig({ USE_BANDCAMP: checked })}
            />
          </div>

          {config.USE_BANDCAMP && (
            <div className="p-3 rounded-lg bg-warning/5 border border-warning/20 space-y-2">
              <p className="text-sm font-medium text-warning">Terms of Service and use at your own risk</p>
              <p className="text-xs text-muted-foreground">
                Bandcamp has no public API for metadata or search. This provider relies on web scraping or undocumented endpoints.
                Bandcamp&apos;s Terms of Service explicitly prohibit scraping and automated data extraction. Using this option may violate those terms.
                Use at your own risk and only as a last resort when other providers (MusicBrainz, Discogs, Last.fm) have failed.
                Strict rate limiting (e.g. 5s between requests) is applied to reduce load and blocking risk.
              </p>
            </div>
          )}
        </div>

          {/* Incomplete Album Definition */}
          <div className="space-y-4 p-4 rounded-lg border border-border mt-4">
            <div>
              <h4 className="font-medium text-sm mb-2">Incomplete Album Definition</h4>
              <p className="text-xs text-muted-foreground mb-3">
                Select which tags are required for an album to be considered "complete".
                Albums missing any of these tags will be counted in statistics and shown in Tag Fixer.
              </p>
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label>Required Tags</Label>
                <FieldTooltip content="Tags that must be present for an album to be considered complete. At least one must be selected." />
              </div>
              <RequiredTagsCheckboxes config={config} updateConfig={updateConfig} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
