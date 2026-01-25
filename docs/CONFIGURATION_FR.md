# PMDA – Configuration et paramètres

Cette page liste chaque clé de configuration et variable d’environnement utilisée par PMDA, avec de courtes directives. Les valeurs peuvent être définies dans `config.json` ou via les variables d’environnement ; **l’env prime toujours sur la config**.

---

## Obligatoires

| Variable | Description | Exemple |
|----------|-------------|---------|
| `PLEX_HOST` | URL complète du serveur Plex (sans slash final). | `http://192.168.1.10:32400` |
| `PLEX_TOKEN` | Token API Plex. | Votre token dans les réglages Plex. |
| `PLEX_DB_PATH` | Dossier (ou chemin) où se trouve la base Plex. Avec Docker, utilisez le chemin **dans le conteneur**. | `/database` ou `/path/to/plex/data` |
| Section(s) | `SECTION_ID` (une seule) ou `SECTION_IDS` (séparés par des virgules). Si absent, PMDA détecte toutes les sections musique (type artiste). | `1` ou `1,2,3` |

---

## Chemins et mapping (bindings sans config manuelle)

L’objectif est de **ne pas obliger l’utilisateur à configurer les bindings à la main**. Au démarrage :

1. **Découverte** – PMDA interroge l’API Plex et récupère les chemins `<Location>` de la section musique (ex. `/music/matched`, `/music/unmatched`).
2. **Fusion** – Si tu as fourni un `PATH_MAP` (préfixe plus large, ex. `/music` → `/music/Music_matched`), il est appliqué ; sinon « chemin Plex = chemin dans le conteneur », donc tes montages Docker doivent utiliser les mêmes noms (ex. `-v /host/Music_matched:/music/matched`).
3. **Cross-check** – Pour chaque entrée, PMDA échantillonne des pistes en base, vérifie que les fichiers existent sur le disque via le mapping. Si ce n’est pas le cas, il tente de trouver le bon répertoire hôte (dossiers frères ou recherche) et **met à jour PATH_MAP + config.json** automatiquement.

Tu n’as qu’à monter les volumes ; PMDA s’aligne sur Plex et corrige les bindings si besoin.

| Variable | Description | Défaut / remarques |
|----------|-------------|---------------------|
| `PLEX_DB_FILE` | Nom du fichier de base Plex. | `com.plexapp.plugins.library.db` (sous `PLEX_DB_PATH`) |
| `PATH_MAP` | Optionnel. Préfixes Plex → hôte ; fusionnés avec la découverte Plex. Permet d’avoir des noms différents côté Plex et host (ex. `/music` → `/music/Music_dump`). Le cross-check peut le corriger automatiquement. | Découverte depuis Plex ; fallback conteneur = host |
| `DUPE_ROOT` | Dossier où sont déplacées les éditions « perdantes ». Avec Docker c’est souvent `/dupes` avec un montage. | `/dupes` |
| `PMDA_CONFIG_DIR` | Répertoire pour la copie de config, la base state, la base cache, les logs et `ai_prompt.txt`. | Répertoire du script (ou env) |

---

## Interface Web et ports

| Variable | Description | Défaut |
|----------|-------------|--------|
| `WEBUI_PORT` | Port d’écoute de l’app Flask (dans le conteneur avec Docker). | `5005` |
| `DISABLE_WEBUI` | Si défini à une valeur « vraie », l’interface web est désactivée. | (non défini) |

---

## Scan et performances

| Variable | Description | Défaut |
|----------|-------------|--------|
| `SCAN_THREADS` | Nombre de threads (workers) pour le scan. Utilisez `auto` ou laissez vide pour le nombre de CPU. | `auto` (nombre de CPU) ou `4` |
| `SKIP_FOLDERS` | Liste de préfixes de chemins séparés par des virgules ; les albums dont le dossier est sous l’un d’eux sont ignorés. | Vide |
| `CROSS_LIBRARY_DEDUPE` | Si true, la détection de doublons se fait sur toutes les sections configurées ; si false, par section uniquement. | `true` |
| `CROSSCHECK_SAMPLES` | Nombre de chemins échantillons pour la vérification des montages (auto-diagnostic). | `20` |

---

## OpenAI (optionnel)

| Variable | Description | Défaut |
|----------|-------------|--------|
| `OPENAI_API_KEY` | Clé API OpenAI pour le choix de la « meilleure édition » et les fusions. Laisser vide pour n’utiliser que l’heuristique. | Vide |
| `OPENAI_MODEL` | Nom du modèle. | `gpt-4` (PMDA peut basculer sur un modèle fonctionnel dans la même « échelle ») |
| `OPENAI_MODEL_FALLBACKS` | Liste de modèles de repli séparés par des virgules si le modèle principal est indisponible. | Vide |

---

## MusicBrainz (optionnel)

| Variable | Description | Défaut |
|----------|-------------|--------|
| `USE_MUSICBRAINZ` | Si true, PMDA utilise MusicBrainz pour les infos release-group et la gestion des Box Sets. | `false` |

---

## Comportement et préférence de format

| Variable | Description | Défaut |
|----------|-------------|--------|
| `FORMAT_PREFERENCE` | Liste d’extensions audio par ordre de préférence (meilleur en premier). Utilisée pour le score. | `["dsf","aif","aiff","wav","flac","m4a",...]` |
| `STATE_DB_FILE` | Chemin vers la base SQLite d’état (doublons, stats). | `{PMDA_CONFIG_DIR}/state.db` |
| `CACHE_DB_FILE` | Chemin vers la base SQLite de cache (infos audio FFmpeg, cache MB optionnel). | `{PMDA_CONFIG_DIR}/cache.db` |
| `PMDA_DEFAULT_MODE` | Mode par défaut quand aucun argument CLI n’est passé : `serve` (interface Web), `cli` ou `run` (scan CLI + dédupe). | Doit être défini dans Docker si pas de `--serve` / args CLI |

---

## Logs et notifications

| Variable | Description | Défaut |
|----------|-------------|--------|
| `LOG_LEVEL` | Niveau de log. | `INFO` |
| `LOG_FILE` | Chemin du fichier de log (rotation). | `{PMDA_CONFIG_DIR}/pmda.log` |
| `DISCORD_WEBHOOK` | URL de webhook Discord pour les notifications (ex. purge d’éditions invalides, doublon flou détecté). | Vide |

---

## Bonnes pratiques

1. **Docker**  
   Préférer les variables d’environnement pour les secrets et chemins ; utiliser `-e` et/ou un fichier env. Monter `PMDA_CONFIG_DIR` pour persister state et cache.

2. **PATH_MAP**  
   En général inutile : PMDA découvre les chemins depuis Plex et le cross-check valide (et corrige si besoin) les bindings. N’ajouter un préfixe que si tes dossiers hôte ne suivent pas les noms Plex (ex. `/music` → `/music/Music_dump`).

3. **Premier lancement**  
   Utiliser `LOG_LEVEL=DEBUG` en cas de problème, puis revenir à `INFO`.

4. **Sauvegarde**  
   Sauvegarder la base Plex et les dossiers musique avant de lancer une dédupe massive ; `DUPE_ROOT` conserve les copies déplacées jusqu’à suppression manuelle.

5. **Sections**  
   Utiliser `SECTION_IDS` lorsque vous avez plusieurs bibliothèques musique et souhaitez limiter ou ordonner celles qui sont scannées.

6. **Dossiers à ignorer**  
   Utiliser `SKIP_FOLDERS` pour les chemins à ne jamais prendre en compte (ex. archives, dossiers « ne pas toucher »). Les chemins sont comparés après résolution.

---

## Exemple config.json (minimal)

```json
{
  "PLEX_HOST": "http://192.168.1.10:32400",
  "PLEX_TOKEN": "VOTRE_TOKEN",
  "PLEX_DB_PATH": "/database",
  "OPENAI_API_KEY": "",
  "USE_MUSICBRAINZ": false,
  "SKIP_FOLDERS": []
}
```

Après un premier run avec Plex disponible, `PATH_MAP` sera fusionné et réécrit dans ce fichier. Vous pourrez alors ajuster `PATH_MAP`, `DUPE_ROOT`, `SECTION_ID` / `SECTION_IDS`, etc. selon vos besoins.
