# Prompt Lovable – Améliorations du modal de détail (doublons)

Le prompt ci‑dessous est à copier dans Lovable pour faire évoluer le **modal de détail** d'un groupe de doublons (composant `frontend/src/components/DetailModal.tsx`). Les données viennent de `GET /details/<artist>/<album_id>` : `{ artist, album, editions, rationale, merge_list }`. Chaque `edition` a `thumb_data`, `title_raw`, `size` (octets), `fmt`, `br`, `sr`, `bd`, `path`, `album_id`, `track_count`, `tracks` (array de `{ name, title, idx, duration, dur, format, bitrate, is_bonus, path? }`). La config (dont `PLEX_HOST`) est disponible via `api.getConfig()`.

---

## Texte du prompt à donner à Lovable

**Contexte:** Améliorer le modal de détail des doublons (DetailModal) dans l'app PMDA. Les exigences suivantes doivent être implémentées sans casser le reste de l'UI (sélection de l'édition à garder, déduplication, etc.).

**1. Largeur et défilement**

- Utiliser toute la largeur nécessaire pour afficher l'information correctement.
- Éviter absolument l'ascenseur horizontal : le contenu du modal doit rester dans la fenêtre (largeur adaptative, pas de `overflow-x` sur le modal).
- Ajuster le conteneur du modal (ex. `.modal-content` dans `frontend/src/index.css`, actuellement `max-w-4xl` et `overflow-auto`) : soit une largeur max plus grande (ex. `max-w-[95vw]` ou `min(max-w-6xl, 95vw)`), soit une largeur fluide qui évite le scroll horizontal. Les tableaux et blocs d'éditions doivent se plier à la largeur utile.

**2. Éditions côte à côte (comparaison)**

- Au lieu d'empiler les éditions verticalement (une au‑dessus de l'autre), les afficher **côte à côte** (layout horizontal : une colonne par édition).
- Chaque colonne représente une édition (cover, "Keep this edition" / "Will be deleted", titre, taille, format, bitrate, chemin, etc.).
- Les **pistes (tracks)** doivent être présentées dans un **tableau de comparaison unique** : une ligne par position logique (index / ligne), avec **une colonne par édition**. Ainsi on voit pour chaque "ligne" la piste de l'édition 1, celle de l'édition 2, etc. (si une édition n'a pas de piste à cette position, la cellule est vide ou "—"). Cela permet de comparer visuellement les pistes d'une édition à l'autre.

**3. Tracks bonus / présentes dans une seule édition (merge)**

- Les pistes **bonus** (présentes dans une édition et pas dans l'édition choisie comme "best") ou **absentes d'une édition** (présentes dans l'autre uniquement) doivent être **mises en avant** avec un **code couleur clair** (ex. fond pastel distinct pour bonus / "only in this edition").
- Ajouter un **bouton "Merge"** (ou "Déplacer vers l'édition gardée") sur les lignes concernées, permettant de **sélectionner les pistes** que l'utilisateur souhaite déplacer et **vers quelle édition** (l'édition gardée). Ce flux peut ouvrir un petit sous‑modal ou une section "Merge selection" : sélection des pistes (checkboxes) + choix de l'édition cible, puis appel à l'API existante de move/merge si elle existe (sinon afficher le bouton désactivé avec un tooltip "Backend merge non disponible").
- Réutiliser si possible la logique actuelle `moveBonusTrack` / `onMoveBonus` et le style "BONUS" déjà présent dans `DetailModal.tsx`.

**4. Nom des pistes + artiste + album dans les tableaux**

- Dans le tableau de comparaison des pistes, afficher pour chaque cellule d'édition :
  - le **nom de la piste** (track name / title),
  - et en sous‑ligne ou en texte secondaire : **nom de l'artiste** et **nom de l'album**.
- L'artiste et l'album sont disponibles au niveau du modal : `details.artist`, `details.album` ; par édition on a aussi `edition.title_raw` (album). Les afficher de façon lisible mais compacte (ex. "Artist · Album" en petit sous le nom de piste).

**5. Indication "bit‑perfect" / même édition**

- Lorsque, pour une même ligne de comparaison, les pistes de toutes les éditions comparées ont **exactement** : même **nom** (normalisé), même **durée** (ex. `duration` ou `dur`), même **bitrate**, et si disponible même **taille** (octets), afficher une **indication visuelle subtile** que la comparaison est "bit‑perfect" ou "same edition" (ex. petit badge ou icône + tooltip "Bit‑perfect match" / "Même édition").
- Ne pas surcharger l'UI : une simple pastille ou texte discret (couleur neutre/muted) suffit.

**6. Lien "Ouvrir dans Plex"**

- Dans le modal de détail, ajouter un **lien** (ex. "Voir dans Plex" / "Open in Plex") qui ouvre **Plex Web** dans un nouvel onglet, sur la page de l'**artiste** correspondant, avec l'**album** en contexte, pour que l'utilisateur puisse vérifier les doublons dans l'interface Plex.
- Construire l'URL ainsi :
  - Récupérer la base Plex via `api.getConfig()` → `PLEX_HOST` (ex. `http://192.168.3.2:32400`).
  - Pour un album, l'URL Plex Web typique est : `{PLEX_HOST}/web/index.html#!/library/metadata/{album_id}` (où `album_id` est l'id de l'édition, ex. `edition.album_id`). Pour cibler l'artiste en priorité tout en ayant l'album visible, on peut utiliser la même URL album (Plex affiche souvent le contexte parent artiste) ou, si la doc Plex le permet, une URL type `#!/library/metadata/{artist_id}` — en restant pragmatique : au minimum ouvrir la fiche de l'album via `edition.album_id` (ex. édition sélectionnée ou première édition).
- Placer le lien près du titre du modal (ex. à droite du "Artist – Album") ou en pied de section, avec une icône externe (lien s'ouvrant dans un nouvel onglet).

---

**Récap technique**

- Fichiers principaux : `DetailModal.tsx`, `index.css` (règles `.modal-content`).
- Données : `useDuplicateDetails(artist, albumId)` → `details.editions[]` avec `tracks[]` ; chaque track a `name`, `title`, `duration`, `dur`, `format`, `bitrate`, `is_bonus`.
- Config : `api.getConfig()` pour `PLEX_HOST`.
- Ne pas supprimer la sélection "Choose edition to keep" ni le bouton "Deduplicate" ; garder la cohérence avec l'API existante (`dedupeManual`, `moveBonusTrack`).
