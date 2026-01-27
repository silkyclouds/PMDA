# Diagnostic des bindings – 26 janvier 2026 (serveur Unraid)

## Résumé

Le bind Docker **est correct** : `/mnt/user/MURRAY/Music` → `/music` dans le conteneur.  
Le problème vient du **décalage entre les chemins enregistrés par Plex dans sa base** et les **vrais noms de dossiers** sous `/music` sur le disque.

---

## Vérifications effectuées sur 192.168.3.2

### 1. Montages du conteneur PMDA_WEBUI

- `/mnt/user/MURRAY/Music` → `/music` (ro) ✅  
- Config, database Plex, dupes : OK.

### 2. Contenu de `/music` dans le conteneur

| Dossier vu dans le conteneur | Taille / contenu |
|-----------------------------|-------------------|
| **Compilations** (majuscule C) | Beaucoup d’artistes/albums (données réelles) |
| **compilations** (minuscule) | Vide (. et .. seulement) |
| **Music_matched** (M majuscule + underscore) | Données réelles (sous-dossiers 0, 1, 2…) |
| **matched** (minuscule) | Vide |
| **unmatched** (minuscule) | Vide |

### 3. Ce que contient la base Plex (`media_parts.file`)

- Exemples de chemins :  
  `/music/compilations/...`, `/music/matched/...`, `/music/unmatched/...`  
  (toujours en **minuscules** pour le premier niveau : `compilations`, `matched`, `unmatched`).

### 4. Test d’existence des fichiers

- Fichier exemple en base :  
  `/music/compilations/Aikana + Ital/Tribes 04.../Aikana + Ital - Tribes 04 - 04. - Boya.flac`
- Test dans le conteneur :
  - `/music/compilations/...` (minuscule) → **MISSING**
  - `/music/Compilations/...` (majuscule) → **EXISTS**

Donc : **Plex a enregistré des chemins avec des noms de dossiers qui ne correspondent pas aux noms réels** (casse ou nom différent).

---

## Cause du « 20/20 missing on disk »

Le PATH_MAP actuel (autodétecté depuis Plex) dit :

- Plex path `/music/compilations` → Host path (conteneur) `/music/compilations`
- Plex path `/music/matched` → Host path `/music/matched`
- Plex path `/music/unmatched` → Host path `/music/unmatched`

Le cross-check construit donc par exemple :

- `dst_path = /music/compilations/Artist/Album/file.flac`  
  alors que le fichier est réellement sous **`/music/Compilations/...`** (majuscule).  
  Sur un système sensible à la casse (Linux), ce sont deux chemins différents → fichier introuvable.

Même chose pour **matched** : la base dit `/music/matched/...`, les données sont dans **`/music/Music_matched/...`**.

---

## Solution : adapter le PATH_MAP aux vrais dossiers

Il faut que le **Host path** (côté conteneur) pointe vers les dossiers qui existent vraiment sous `/music` :

| Plex path (ce qui est en base) | Host path à utiliser (dans le conteneur) |
|--------------------------------|------------------------------------------|
| `/music/compilations`          | `/music/Compilations`                    |
| `/music/matched`               | `/music/Music_matched`                   |
| `/music/unmatched`             | À vérifier (actuellement `/music/unmatched` est vide ; si les fichiers sont ailleurs, mettre ce chemin) |

Dans le wizard (étape Paths), dans la zone Path Mapping, remplacer par exemple :

- ` /music/compilations=/music/compilations`  
  par  
  ` /music/compilations=/music/Compilations`
- ` /music/matched=/music/matched`  
  par  
  ` /music/matched=/music/Music_matched`

Pour **unmatched** : si les fichiers ne sont ni dans `/music/unmatched` ni dans un autre dossier sous `/music`, il faudra soit déplacer les fichiers, soit configurer Plex pour qu’il pointe vers le bon emplacement et resynchroniser la bibliothèque.

---

## Conclusion

- Le bind du dossier parent hôte vers `/music` est correct.
- Le cross-check ne trouve pas les fichiers parce que le **PATH_MAP** utilise les mêmes noms que la base Plex (`compilations`, `matched`, `unmatched`) alors que sur le disque les dossiers s’appellent **Compilations**, **Music_matched**, et éventuellement autre chose pour unmatched.
- Corriger le PATH_MAP (colonne « Host path » = chemin dans le conteneur) pour qu’il pointe vers ces vrais noms de dossiers règle le problème pour compilations et matched ; à compléter pour unmatched selon l’emplacement réel des fichiers.
