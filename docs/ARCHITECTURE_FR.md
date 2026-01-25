# PMDA – Architecture et résumé technique

Ce document décrit ce que fait PMDA et comment il fonctionne en interne. Il s’adresse aux contributeurs et aux utilisateurs avancés.

---

## Ce que fait PMDA (résumé)

- **Scanne** une ou plusieurs bibliothèques Plex Music (structure artiste/album).
- **Découvre automatiquement** les sections, chemins et structure de la base Plex.
- **Mappe** les chemins des bibliothèques Plex vers les chemins hôte/container via `PATH_MAP` (généré depuis Plex ou défini par l’utilisateur).
- **Détecte les albums en double** grâce à la normalisation des titres, au recouvrement des pistes et, optionnellement, à MusicBrainz/OpenAI.
- **Choisit la « meilleure » édition** par groupe de doublons : via OpenAI (recommandé) ou une heuristique locale (format, bit depth, nombre de pistes, bitrate).
- **Déplace** les éditions perdantes vers un dossier `DUPE_ROOT` configurable et **supprime** leurs métadonnées dans Plex (corbeille + suppression).
- **Gère** le mode dry-run, le mode safe (sans suppression des métadonnées Plex), l’interface Web (Flask) et le mode CLI.
- **Optionnel** : MusicBrainz pour les infos release-group et les Box Sets ; notifications Discord.
- **Met en cache** les infos audio FFmpeg et les choix AI dans SQLite pour accélérer les runs suivants.

---

## Flux global

1. **Démarrage**  
   Chargement de `config.json` et des variables d’environnement, vérification de la connexion Plex, génération/fusion de `PATH_MAP` à partir de Plex, optionnellement self-diagnostic. Initialisation des bases state et cache.

2. **Scan**  
   Pour chaque artiste des sections configurées : récupération des ID d’albums dans la base Plex, résolution du dossier de chaque album via `PATH_MAP`, collecte des pistes et métadonnées audio (FFmpeg/cache). Regroupement par titre d’album normalisé (et désambiguïsation classique si besoin). Pour les groupes avec au moins 2 éditions, sélection de la « meilleure » édition (AI ou heuristique), puis sauvegarde des groupes en base state.

3. **Déduplication**  
   Pour chaque groupe choisi (un seul, une sélection, ou tout) : déplacement des dossiers perdants vers `DUPE_ROOT`, appels API Plex pour mise en corbeille et suppression des métadonnées, rafraîchissement du chemin et vidage de la corbeille. Mise à jour des stats (espace libéré, doublons supprimés).

4. **Interface Web**  
   Une SPA (template HTML dans le code). Endpoints : démarrage/pause/reprise/arrêt du scan, progression, liste des groupes de doublons (cartes), détail par groupe, déduplication (par artiste/album, sélection, ou tout), merge-and-dedupe.

5. **CLI**  
   Lance un scan complet dans le processus principal, puis la déduplication en mode CLI (options dry-run, safe-mode, tag-extra, verbose). Peut être lancé via `PMDA_DEFAULT_MODE=cli` (ou `run`) sans `--serve`.

---

## Composants principaux

| Composant | Rôle |
|-----------|------|
| **Config** | `config.json` + env ; `_get()`, `_parse_path_map()`, `_discover_path_map()` ; validation de `PLEX_*`, `SECTION_ID` / `SECTION_IDS`. |
| **Base Plex** | Lecture SQLite via `plex_connect()` ; requêtes artistes, albums, pistes, media parts et chemins. |
| **PATH_MAP** | Correspondance chemin Plex → chemin hôte ; utilisé par `container_to_host()`, `relative_path_under_known_roots()`, `build_dupe_destination()`. |
| **Scan** | `scan_artist_duplicates()` (worker par artiste), `scan_duplicates()` (regroupement par artiste), `choose_best()` (AI ou heuristique), `background_scan()`, `save_scan_to_db()` / `load_scan_from_db()`. |
| **Dedupe** | `perform_dedupe()` (déplacement + corbeille/suppression Plex), `safe_move()` (déplacement cross-device avec retries), `background_dedupe()`. |
| **State DB** | SQLite : `duplicates_best`, `duplicates_loser`, `stats` ; fichier `STATE_DB_FILE` (dans le répertoire config). |
| **Cache DB** | SQLite : `audio_cache` (path, mtime, bit_rate, sample_rate, bit_depth), cache MusicBrainz ; fichier `CACHE_DB_FILE`. |
| **OpenAI** | `choose_best()` envoie le résumé des éditions + optionnellement infos MusicBrainz ; prompt depuis `ai_prompt.txt` ; parse une ligne `index|rationale|pistes_extra`. |
| **Flask** | `/`, `/scan/*`, `/api/progress`, `/api/dedupe`, `/details/...`, `/dedupe/artist/...`, `/dedupe/all`, `/dedupe/selected`, `/api/edition_details`, `/api/dedupe_manual`. |

---

## Conventions importantes

- **Code en anglais**  
  Commentaires, identifiants, messages de commit et documentation technique en anglais. La doc utilisateur peut être dupliquée en français dans des fichiers dédiés.

- **Priorité de configuration**  
  La variable d’environnement prime sur `config.json` ; après démarrage, `PATH_MAP` est fusionné (découverte Plex + `PATH_MAP` utilisateur) et réécrit dans `config.json`.

- **Modes**  
  `--serve` → Web UI ; sinon CLI. Sans argument, `PMDA_DEFAULT_MODE` doit être défini (`serve`, `cli` ou `run`).

- **Cross-library**  
  `CROSS_LIBRARY_DEDUPE` (env, défaut true) indique si les doublons sont détectés entre toutes les sections configurées ou par section.

---

## Structure des fichiers (référence)

- `pmda.py` – Script principal unique : config, bases, logique de scan/dedupe, app Flask et template HTML.
- `config.json` – Config par défaut ; copie au runtime dans `PMDA_CONFIG_DIR`.
- `ai_prompt.txt` – Prompt AI par défaut ; copie au runtime dans `PMDA_CONFIG_DIR`.
- `requirements.txt` – Flask, requests, openai, musicbrainzngs.
- `static/` – Logo et assets pour l’interface Web.
- `docs/` – Markdown pour le wiki (architecture, guide utilisateur, configuration).
