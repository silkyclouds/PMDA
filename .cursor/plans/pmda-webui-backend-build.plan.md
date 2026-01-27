---
name: PMDA WebUI Backend Build
overview: Implémentation backend et données pour faire fonctionner l’UI Lovable (wizard tests, détails doublons, dédupe manuelle). La partie UI/UX Lovable est déjà livrée.
todos:
  - id: api-wizard
    content: Endpoints wizard (plex/check, plex/libraries, autodetect/paths, openai/check, openai/models)
    status: completed
  - id: config-persist
    content: Persistance config (DUPE_ROOT, PMDA_CONFIG_DIR dans PUT /api/config)
    status: completed
  - id: details-api
    content: Enrichir GET /details (path, size correct, track_count, tracks[], is_bonus)
    status: completed
  - id: duplicates-cards
    content: "Enrichir GET /api/duplicates (_build_card_list : size_mb, track_count, path)"
    status: completed
  - id: dedupe-manual
    content: API dédupe manuelle (keep_edition / editions_to_delete)
    status: completed
  - id: move-bonus
    content: (Optionnel) POST /api/duplicates/move_bonus_track
    status: cancelled
isProject: false
---

# Plan PMDA WebUI – Backend build (post-Lovable)

## État actuel

- **Lovable (terminé)** : liste seule avec toggle « Sans détails » / « Avec détails », modal détail (radios, pistes dépliables, bonus, Move to kept), wizard 6 étapes (Plex → Libraries → Paths → Scan → AI → Review), Config Directory dans Advanced Paths, tooltips corrigés, thème clair.
- **À faire** : backend pour que tous les appels UI répondent et que les données (taille, path, pistes) soient correctes.

---

## 1. Endpoints wizard (priorité 1)

Fichier : [pmda.py](pmda.py). Vérifier les URLs exactes dans [frontend/src/lib/api.ts](frontend/src/lib/api.ts).

| Endpoint | Frontend appelle | Implémentation backend |
|----------|------------------|------------------------|
| Test Plex | `GET /api/plex/check` ou équivalent | Requête `GET {PLEX_HOST}/library/sections` avec `X-Plex-Token`; retourner `{ success, message }`. Réutiliser la logique de check au démarrage. |
| Liste bibliothèques | `POST /api/autodetect/libraries` (api.ts l.226) | Nouvelle route ou réutiliser; GET `{PLEX_HOST}/library/sections`, parser XML, retourner `{ success, libraries: [ { id, name, type } ] }`. |
| Autodétection paths | `POST /api/autodetect/paths` (api.ts l.220) | Appeler `_discover_path_map(plex_host, plex_token, section_id)` pour chaque SECTION_ID; retourner `{ success, paths }`. |
| Test OpenAI | `POST /api/openai/check` (api.ts l.204) | Client OpenAI avec clé (body ou config), requête minimale; retourner `{ success, message }`. |
| Modèles OpenAI | `GET /api/openai/models` (api.ts l.211) | Liste statique ou API OpenAI list models; retourner `string[]`. |

---

## 2. Persistance config

- Dans **PUT /api/config** : ajouter `DUPE_ROOT` et `PMDA_CONFIG_DIR` à la liste `allowed`.
- Optionnel : après écriture, recharger la config depuis `CONFIG_PATH` pour les variables qui le permettent (documenter ce qui nécessite redémarrage).

---

## 3. Données doublons : taille, path, pistes, bonus

### GET /details/<artist>/<album_id>

- **Taille** : pour les losers utiliser `e.get("size")` (déjà en base); pour le best utiliser `folder_size(Path(e["folder"])) // (1024*1024)` ou valeur en base si ajoutée.
- **Path** : ajouter `"path": str(e["folder"]) `(et/ou `"folder"`) dans chaque édition du JSON.
- **track_count** : `len(get_tracks(plex_connect(), e["album_id"]))` pour chaque édition.
- **tracks** : pour chaque édition, liste `{ idx, title, dur, ... }` via `get_tracks()`; ajouter infos format/bitrate si disponibles.
- **is_bonus** : par piste, déduire (ex. piste dans un loser absente du best) et envoyer un booléen.

### GET /api/duplicates (_build_card_list)

- **size_mb** : `folder_size(Path(best["folder"])) // (1024*1024)`.
- **track_count** : `len(get_tracks(plex_connect(), best["album_id"]))` (attention perf si beaucoup de cartes; sinon laisser 0 ou optionnel).
- **path** : `str(best["folder"])` pour le mode « avec détails ».

---

## 4. Dédupe manuelle

- Étendre **POST /dedupe/artist/<artist>** (ou **POST /api/dedupe_manual**) pour accepter un body du type :
- `{ "album_id": number, "keep_edition_album_id": number }`  
ou
- `{ "album_id": number, "edition_ids_to_delete": [ album_id, ... ] }`.
- Retrouver le groupe (artist + album_id), construire `{ best: édition_à_garder, losers: [...] }`, appeler `perform_dedupe(g)`.

---

## 5. (Optionnel) Déplacement piste bonus

- **POST /api/duplicates/move_bonus_track** avec body `source_track_path` (ou identifiant piste), `destination_edition_album_id` (ou folder). Copier/déplacer le fichier vers le dossier de destination; rescan Plex si nécessaire.

---

## Ordre de build recommandé

1. **api-wizard** – Implémenter les 5 routes (plex check, libraries, autodetect paths, openai check, openai models) pour que le wizard ne affiche plus « Backend does not support … ».
2. **config-persist** – Ajouter DUPE_ROOT et PMDA_CONFIG_DIR dans PUT /api/config.
3. **details-api** – Enrichir `details()` : path, size, track_count, tracks, is_bonus.
4. **duplicates-cards** – Enrichir `_build_card_list` : size_mb, track_count, path.
5. **dedupe-manual** – Extension dédupe avec sélection d’édition à garder.
6. **move-bonus** – Optionnel selon priorité.

---

## Vérifications frontend (si besoin)

- Après implémentation des routes, vérifier que [api.ts](frontend/src/lib/api.ts) utilise les mêmes URLs (ex. `/api/plex/check` vs `/api/plex/testConnection`).
- Vérifier que le wizard envoie bien `SECTION_IDS` après l’étape Libraries (liste d’IDs cochés).
- Si le modal détail ou la liste attendent des champs avec des noms différents (ex. `size_mb` vs `size`), adapter soit le backend soit le frontend pour que les contrats correspondent.