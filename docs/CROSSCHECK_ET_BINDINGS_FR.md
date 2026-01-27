# Comment le cross-check et le PATH_MAP relient Plex aux dossiers réels

Ce document décrit comment PMDA fait le lien entre les chemins stockés dans la base Plex et les dossiers réels (ce que tu montes dans le conteneur), et comment fonctionne la vérification « Verify bindings ».

---

## 1. D’où viennent les chemins « Plex » ?

### Base de données Plex (`com.plexapp.plugins.library.db`)

- Plex enregistre, pour chaque fichier qu’il a scanné, un **chemin complet** dans la table `media_parts`, colonne `file`.
- Ce chemin est **tel que le serveur Plex le voit au moment du scan** :
  - Si Plex tourne dans un conteneur avec un volume monté, ce sera le chemin **dans ce conteneur** (ex. `/data/music/matched/Artist/Album/track.flac`).
  - Si Plex et PMDA utilisent la même convention (ex. racine musique en `/music`), tu peux avoir des lignes du type :  
    `/music/compilations/...`, `/music/matched/...`, `/music/unmatched/...`.

Donc : **les chemins dans la DB Plex = préfixe (racine de librairie) + sous-dossiers + fichier**. Le « Plex path » affiché dans l’UI (ex. `/music/compilations`) est ce **préfixe** qui correspond à une racine de librairie Plex.

---

## 2. C’est quoi le PATH_MAP ?

Le `PATH_MAP` est un dictionnaire (ou liste de paires) du type :

- **Clé (plex_root)** : préfixe de chemins tels qu’ils apparaissent dans la base Plex (ex. `/music/compilations`, `/music/matched`, `/music/unmatched`).
- **Valeur (host_root)** : chemin **dans le conteneur PMDA** où on doit trouver les mêmes fichiers.

Dans ton cas, après autodétecte ou config manuelle, tu as souvent la même chose des deux côtés :

- `/music/compilations` → `/music/compilations`
- `/music/matched` → `/music/matched`
- `/music/unmatched` → `/music/unmatched`

Donc : **Plex path = préfixe en base ; Host path = chemin côté conteneur PMDA où on va vérifier l’existence des fichiers.** Ce « host path » n’est pas le chemin physique sur la machine hôte, c’est le chemin **à l’intérieur du conteneur** (qui, lui, sera rendu possible par le bind Docker).

---

## 3. Comment le « bind » Docker intervient

Le vrai lien avec les dossiers de ton disque se fait **uniquement** au lancement du conteneur, avec un volume Docker :

- **Sur l’hôte** : par ex. `/mnt/user/MURRAY/Music` (tes dossiers réels : compilations, matched, unmatched, etc.).
- **Dans le conteneur** : par ex. `/music`.

Commande typique :

```text
-v "/mnt/user/MURRAY/Music:/music:ro"
```

Donc :

- **À l’intérieur du conteneur**, le chemin `/music/compilations` correspond au dossier réel **sur l’hôte** `/mnt/user/MURRAY/Music/compilations`.
- PMDA ne voit **que** le système de fichiers du conteneur : il ne connaît jamais directement `/mnt/user/...`. Il travaille uniquement avec des chemins conteneur comme `/music/...`.

En résumé : **Plex DB (chemins) → PATH_MAP (plex_root → host_root dans le conteneur) → le host_root est rendu réel par le bind Docker (ex. /music = /mnt/user/MURRAY/Music).**

---

## 4. Comment le cross-check analyse les dossiers (étape par étape)

La même logique est utilisée au démarrage (`_cross_check_bindings`) et quand tu cliques sur « Verify bindings » (`_run_path_verification`).

Pour **chaque** entrée du PATH_MAP `(plex_root, host_root)` :

1. **Échantillonnage en base**
   - Requête SQL sur `media_parts` : on récupère jusqu’à N chemins (ex. 20) dont `file` :
     - commence par `plex_root` (ex. `plex_root = '/music/compilations'` → `mp.file LIKE '/music/compilations/%'`) ;
     - et se termine par une extension audio (flac, mp3, m4a, etc.).
   - Ordre aléatoire pour avoir un échantillon varié.

2. **Pour chaque chemin Plex retourné** (ex. `/music/compilations/Artist/Album/01.flac`) :
   - On enlève le préfixe `plex_root` et on garde le **relatif** :  
     `rel = "Artist/Album/01.flac"`.
   - On construit le chemin **dans le conteneur** où ce fichier devrait être :  
     `dst_path = host_root + rel`  
     ex. `host_root = "/music/compilations"` →  
     `dst_path = "/music/compilations/Artist/Album/01.flac`.

3. **Vérification sur le disque (vu par PMDA)**
   - Le code fait `Path(dst_path).exists()` **dans le processus PMDA**, qui tourne **dans le conteneur**.
   - Donc on vérifie si le fichier existe bien à `/music/compilations/Artist/Album/01.flac` **tel que le conteneur le voit**.

4. **Résultat**
   - Si tous les échantillons existent pour cette entrée → binding OK.
   - Sinon → « X/Y sample(s) missing on disk » (X fichiers manquants sur Y testés).

Donc : **le cross-check ne « bind » rien lui-même** : il vérifie que les chemins construits avec le PATH_MAP (côté conteneur) existent bien sur le système de fichiers du conteneur. Le vrai « bind » des dossiers Plex vers les dossiers réels du host est fait par Docker ; le PATH_MAP dit seulement « préfixe Plex → répertoire correspondant dans le conteneur ».

---

## 5. Pourquoi « 20/20 missing on disk » alors que le bind est là ?

Si tu as bien monté `/mnt/user/MURRAY/Music` en `/music` dans le conteneur et que tu vois encore « 20/20 sample(s) missing on disk » pour `/music/compilations`, `/music/matched`, `/music/unmatched`, les causes possibles sont :

1. **Le conteneur n’a pas le volume monté**  
   Vérifier que le `docker run` contient bien :  
   `-v "/mnt/user/MURRAY/Music:/music:ro"` (ou le chemin host que tu utilises réellement).

2. **Casse ou structure différente sur le disque**  
   La DB Plex peut contenir par ex. `/music/compilations/...` alors que sur l’hôte le dossier s’appelle `Compilations` (majuscule). Dans le conteneur, après le bind, le chemin vu sera celui du host (ex. `/music/Compilations`). Si le PATH_MAP dit `host_root = /music/compilations`, le code cherchera `/music/compilations/...` et ne trouvera pas les fichiers sous `/music/Compilations/...`.

3. **Ce que Plex a en base ne correspond pas à /music**  
   Si Plex a scanné avec une autre racine (ex. `/data/music/matched`), les `plex_root` devraient être ces préfixes-là, et les `host_root` devraient pointer vers l’endroit **dans le conteneur** où ces arbres sont visibles (par ex. `/music/matched` si tu as monté la racine musique en `/music`).

En résumé : le cross-check **analyse** les dossiers en comparant la base Plex au système de fichiers **du conteneur** via le PATH_MAP ; le **binding** des dossiers de la librairie Plex aux dossiers réels du host est fait par le volume Docker, pas par le code de vérification.
