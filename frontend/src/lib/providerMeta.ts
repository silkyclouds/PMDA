export type ProviderId =
  | 'musicbrainz'
  | 'discogs'
  | 'bandcamp'
  | 'lastfm'
  | 'wikipedia'
  | 'acoustid'
  | 'audiodb'
  | 'fanart'
  | 'bandsintown'
  | 'searxng'
  | 'serper'
  | 'openai-api'
  | 'openai-codex'
  | 'anthropic'
  | 'google'
  | 'ollama'
  | 'local'
  | 'media_cache'
  | 'unknown';

export type ProviderIconKey =
  | 'musicbrainz'
  | 'discogs'
  | 'bandcamp'
  | 'lastfm'
  | 'wikipedia'
  | 'acoustid'
  | 'audiodb'
  | 'fanart'
  | 'bandsintown'
  | 'searxng'
  | 'serper'
  | 'openai'
  | 'anthropic'
  | 'google'
  | 'ollama'
  | 'local'
  | 'media_cache'
  | 'unknown';

export interface ProviderMeta {
  id: ProviderId;
  label: string;
  iconKey: ProviderIconKey;
  aliases: string[];
  externalBaseUrl?: string;
}

const PROVIDER_META: Record<ProviderId, ProviderMeta> = {
  musicbrainz: {
    id: 'musicbrainz',
    label: 'MusicBrainz',
    iconKey: 'musicbrainz',
    aliases: ['mb', 'mbid', 'music brainz'],
    externalBaseUrl: 'https://musicbrainz.org',
  },
  discogs: {
    id: 'discogs',
    label: 'Discogs',
    iconKey: 'discogs',
    aliases: [],
    externalBaseUrl: 'https://discogs.com',
  },
  bandcamp: {
    id: 'bandcamp',
    label: 'Bandcamp',
    iconKey: 'bandcamp',
    aliases: [],
    externalBaseUrl: 'https://bandcamp.com',
  },
  lastfm: {
    id: 'lastfm',
    label: 'Last.fm',
    iconKey: 'lastfm',
    aliases: ['last.fm', 'last_fm', 'last fm'],
    externalBaseUrl: 'https://last.fm',
  },
  wikipedia: {
    id: 'wikipedia',
    label: 'Wikipedia',
    iconKey: 'wikipedia',
    aliases: ['wikipedia:en', 'wikipedia-fr', 'wiki', 'wikipedia.org'],
    externalBaseUrl: 'https://wikipedia.org',
  },
  acoustid: {
    id: 'acoustid',
    label: 'AcoustID',
    iconKey: 'acoustid',
    aliases: ['acousticid', 'acoust-id'],
    externalBaseUrl: 'https://acoustid.org',
  },
  audiodb: {
    id: 'audiodb',
    label: 'TheAudioDB',
    iconKey: 'audiodb',
    aliases: ['theaudiodb', 'audio_db'],
    externalBaseUrl: 'https://theaudiodb.com',
  },
  fanart: {
    id: 'fanart',
    label: 'Fanart.tv',
    iconKey: 'fanart',
    aliases: ['fanart.tv', 'fanarttv'],
    externalBaseUrl: 'https://fanart.tv',
  },
  bandsintown: {
    id: 'bandsintown',
    label: 'Bandsintown',
    iconKey: 'bandsintown',
    aliases: [],
    externalBaseUrl: 'https://bandsintown.com',
  },
  searxng: {
    id: 'searxng',
    label: 'SearXNG',
    iconKey: 'searxng',
    aliases: ['searx', 'searx-ng', 'self-hosted-search'],
  },
  serper: {
    id: 'serper',
    label: 'Serper',
    iconKey: 'serper',
    aliases: ['google-serper', 'serper.dev'],
    externalBaseUrl: 'https://serper.dev',
  },
  'openai-api': {
    id: 'openai-api',
    label: 'OpenAI API',
    iconKey: 'openai',
    aliases: ['openai', 'api', 'openai_key'],
    externalBaseUrl: 'https://platform.openai.com',
  },
  'openai-codex': {
    id: 'openai-codex',
    label: 'OpenAI Codex',
    iconKey: 'openai',
    aliases: ['codex', 'chatgpt', 'openai-oauth', 'oauth'],
    externalBaseUrl: 'https://chatgpt.com',
  },
  anthropic: {
    id: 'anthropic',
    label: 'Anthropic',
    iconKey: 'anthropic',
    aliases: ['claude'],
    externalBaseUrl: 'https://anthropic.com',
  },
  google: {
    id: 'google',
    label: 'Google',
    iconKey: 'google',
    aliases: ['gemini', 'google-genai', 'google ai'],
    externalBaseUrl: 'https://ai.google.dev',
  },
  ollama: {
    id: 'ollama',
    label: 'Ollama',
    iconKey: 'ollama',
    aliases: ['ollama-local'],
    externalBaseUrl: 'https://ollama.com',
  },
  local: {
    id: 'local',
    label: 'Local',
    iconKey: 'local',
    aliases: ['filesystem', 'disk', 'folder'],
  },
  media_cache: {
    id: 'media_cache',
    label: 'Media cache',
    iconKey: 'media_cache',
    aliases: ['media cache', 'cache', 'ram_cache'],
  },
  unknown: {
    id: 'unknown',
    label: 'Unknown',
    iconKey: 'unknown',
    aliases: [],
  },
};

const ALIAS_TO_ID = new Map<string, ProviderId>();
for (const [id, meta] of Object.entries(PROVIDER_META) as Array<[ProviderId, ProviderMeta]>) {
  ALIAS_TO_ID.set(id, id);
  for (const alias of meta.aliases) ALIAS_TO_ID.set(alias.toLowerCase(), id);
}

export function normalizeProviderId(raw: string | null | undefined): ProviderId {
  const value = String(raw || '').trim().toLowerCase();
  if (!value) return 'unknown';
  if (value.includes('openai-codex')) return 'openai-codex';
  if (value.includes('openai-api')) return 'openai-api';
  if (value.startsWith('wikipedia')) return 'wikipedia';
  if (value.startsWith('last.fm') || value.startsWith('lastfm')) return 'lastfm';
  if (value.includes('musicbrainz') || value === 'mbid' || value === 'mb') return 'musicbrainz';
  if (value.includes('discogs')) return 'discogs';
  if (value.includes('bandcamp')) return 'bandcamp';
  if (value.includes('acoustid') || value.includes('acousticid')) return 'acoustid';
  if (value.includes('fanart')) return 'fanart';
  if (value.includes('audiodb') || value.includes('theaudiodb')) return 'audiodb';
  if (value.includes('bandsintown')) return 'bandsintown';
  if (value.includes('searxng') || value.includes('searx-ng') || value.includes('searx')) return 'searxng';
  if (value.includes('serper')) return 'serper';
  if (value.includes('ollama')) return 'ollama';
  if (value.includes('anthropic') || value.includes('claude')) return 'anthropic';
  if (value.includes('gemini') || value.includes('google')) return 'google';
  if (value.includes('openai') || value.includes('chatgpt') || value.includes('codex')) {
    if (value.includes('codex') || value.includes('oauth') || value.includes('chatgpt')) return 'openai-codex';
    return 'openai-api';
  }
  if (value === 'media_cache' || value === 'media cache' || value.includes('cache')) return 'media_cache';
  if (value === 'local' || value.includes('filesystem') || value.includes('folder') || value.includes('disk')) return 'local';
  return ALIAS_TO_ID.get(value) || 'unknown';
}

export function getProviderMeta(raw: string | null | undefined): ProviderMeta {
  const id = normalizeProviderId(raw);
  return PROVIDER_META[id] || PROVIDER_META.unknown;
}

export function providerLabel(raw: string | null | undefined): string {
  return getProviderMeta(raw).label;
}
