# Fix : Détection de duplicates pour le même dossier

**Date** : 27 janvier 2026  
**Problème** : Le même album dans le même dossier était détecté comme duplicate à chaque scan

## 🔍 Problème identifié

D'après le screenshot fourni, 3 "editions" du même album "Parallel Worlds - Transformation" étaient détectées, toutes avec le même chemin `/music/Plex_dump/12-2025/16-12/Parallel Worlds - Transformation/`.

**Cause probable** :
1. Plex peut avoir plusieurs `album_id` qui pointent vers le même dossier physique (duplicate entries dans Plex)
2. Chaque `album_id` créait une "edition" dans la liste
3. Le filtre existant (lignes 3310-3328) comparait les chemins avec `str(folder)` sans normalisation, ce qui pouvait manquer des cas où les chemins sont identiques mais représentés différemment

## ✅ Corrections apportées

### 1. Vérification en amont (lignes 3007, 3047-3062)

**Avant d'ajouter une edition** :
- Track des dossiers déjà vus avec `seen_folders: dict[str, int]`
- Normalise chaque dossier avec `Path(folder).resolve()` avant comparaison
- Si un dossier résolu a déjà été vu pour un autre `album_id`, skip l'album avec un warning
- Log explicite : "Album ID X points to the same folder as album ID Y"

**Code ajouté** :
```python
seen_folders: dict[str, int] = {}  # folder_path_resolved -> album_id
folder_resolved = Path(folder).resolve()
folder_str_resolved = str(folder_resolved)

if folder_str_resolved in seen_folders:
    existing_album_id = seen_folders[folder_str_resolved]
    logging.warning(
        "[Artist %s] Album ID %d points to the same folder as album ID %d: %s. "
        "This indicates duplicate Plex album entries. Skipping album ID %d to avoid false duplicates.",
        artist, aid, existing_album_id, folder_str_resolved, aid
    )
    skip_count += 1
    continue

seen_folders[folder_str_resolved] = aid
```

### 2. Amélioration du filtre de groupes (lignes 3329-3407)

**Normalisation des chemins** :
- Utilise `Path(folder).resolve()` pour normaliser tous les chemins avant comparaison
- Compare les chemins résolus au lieu de simples strings
- Gère les erreurs de résolution (fallback sur string si resolve échoue)

**Détection de doublons dans les groupes** :
- Vérifie si plusieurs editions ont le même `(album_id, folder_resolved)` combo
- Si oui, supprime les doublons (garde la première occurrence)
- Log d'erreur si des doublons sont trouvés

**Code amélioré** :
```python
# Normalize all folders using resolve() for accurate comparison
folders_resolved = set()
for e in ed_list:
    folder = e.get('folder')
    if folder:
        try:
            folder_resolved = str(Path(folder).resolve())
            folders_resolved.add(folder_resolved)
        except Exception as resolve_err:
            folders_resolved.add(str(folder))

if len(folders_resolved) == 1:
    # All editions in same folder - skip this group
    logging.warning(...)
    continue
```

## 🧪 Tests à effectuer

### Test 1 : Vérifier que le problème est résolu

1. [ ] Lancer un nouveau scan
2. [ ] Vérifier dans les logs qu'il n'y a plus de warnings sur "all editions share the same folder"
3. [ ] Vérifier que l'album "Parallel Worlds - Transformation" n'apparaît plus comme duplicate
4. [ ] Vérifier qu'aucun album dans le même dossier n'est détecté comme duplicate

### Test 2 : Vérifier les logs de diagnostic

1. [ ] Chercher dans les logs : `"points to the same folder as album ID"`
2. [ ] Vérifier que ces warnings apparaissent pour les albums problématiques
3. [ ] Vérifier que ces albums sont bien skippés (pas ajoutés aux editions)

### Test 3 : Vérifier que les vrais duplicates sont toujours détectés

1. [ ] S'assurer qu'on a un album qui existe vraiment en plusieurs exemplaires (dossiers différents)
2. [ ] Lancer un scan
3. [ ] Vérifier que ces vrais duplicates sont toujours détectés correctement

## 📊 Commandes de diagnostic

```bash
# Vérifier les logs pour les warnings de dossiers dupliqués
ssh root@192.168.3.2 "docker logs PMDA_WEBUI 2>&1 | grep -i 'points to the same folder\|share the same folder\|duplicate Plex album entries' | tail -20"

# Vérifier les logs pour un artiste spécifique
ssh root@192.168.3.2 "docker logs PMDA_WEBUI 2>&1 | grep -i 'Parallel Worlds' | tail -20"

# Vérifier qu'il n'y a plus de duplicates pour cet album
# (via l'interface web ou en vérifiant la base de données)
```

## 🔍 Vérification en base de données Plex

Pour diagnostiquer si Plex a vraiment plusieurs album_id pour le même dossier :

```bash
# Trouver les album_id pour "Parallel Worlds - Transformation"
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /database/com.plexapp.plugins.library.db \"SELECT id, title, parent_id FROM metadata_items WHERE metadata_type=9 AND title LIKE '%Parallel Worlds%Transformation%';\""

# Pour chaque album_id, vérifier le chemin du premier fichier
# (remplacer ALBUM_ID par chaque ID trouvé)
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /database/com.plexapp.plugins.library.db \"SELECT DISTINCT mp.file FROM metadata_items tr JOIN media_items mi ON mi.metadata_item_id = tr.id JOIN media_parts mp ON mp.media_item_id = mi.id WHERE tr.parent_id = ALBUM_ID LIMIT 1;\""
```

Si plusieurs album_id pointent vers le même dossier parent, c'est un problème Plex (duplicate entries). PMDA devrait maintenant les ignorer.

## ✅ Résumé des changements

- ✅ **Vérification en amont** : Skip les albums qui pointent vers un dossier déjà vu
- ✅ **Normalisation des chemins** : Utilise `Path.resolve()` pour comparer les chemins
- ✅ **Filtre amélioré** : Détecte et supprime les doublons dans les groupes
- ✅ **Logs détaillés** : Warnings et erreurs explicites pour diagnostiquer

Le problème devrait être résolu. Si des albums dans le même dossier sont encore détectés comme duplicates après ce fix, vérifier les logs pour comprendre pourquoi.
