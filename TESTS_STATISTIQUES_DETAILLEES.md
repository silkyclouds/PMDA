# Liste de tests - Statistiques détaillées du scan

**Date de déploiement** : 27 janvier 2026  
**Version** : `meaning/pmda:beta`  
**URL** : http://192.168.3.2:5005

## ✅ Déploiement terminé

- [x] Build Docker multi-platform (linux/amd64, linux/arm64)
- [x] Push vers Docker Hub
- [x] Déploiement sur serveur 192.168.3.2
- [x] Migration SQLite automatique réussie (nouvelles colonnes présentes)

## 📋 Tests à effectuer

### 1. Tests de base

#### 1.1 Accès Web UI
- [ ] Ouvrir http://192.168.3.2:5005
- [ ] Vérifier que l'interface se charge correctement
- [ ] Vérifier qu'aucune erreur dans la console du navigateur (F12 → Console)

#### 1.2 Vérification de la migration SQLite
- [x] Le conteneur démarre sans erreur ✅
- [x] Les logs ne montrent pas d'erreur SQLite ✅
- [x] La table `scan_history` contient les nouvelles colonnes ✅

**Commandes de vérification** :
```bash
# Vérifier que le conteneur tourne
ssh root@192.168.3.2 "docker ps | grep PMDA_WEBUI"

# Vérifier les logs
ssh root@192.168.3.2 "docker logs PMDA_WEBUI --tail 50"

# Vérifier la migration SQLite (depuis le conteneur)
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'PRAGMA table_info(scan_history);' | grep -E 'duplicate_groups|broken_albums|without'"
```

### 2. Tests des statistiques détaillées

#### 2.1 Lancer un scan
- [ ] Démarrer un nouveau scan depuis l'interface
- [ ] Observer la progression en temps réel
- [ ] Noter le temps de scan et le nombre d'artistes/albums traités

#### 2.2 Vérifier l'affichage en temps réel (ScanProgress)
- [ ] Pendant le scan, cliquer sur "Details" pour développer la section
- [ ] Vérifier que les statistiques s'affichent au fur et à mesure :
  - [ ] **Duplicate groups** (si > 0) - icône Package orange
  - [ ] **Total duplicates** (si > 0) - icône Music rouge
  - [ ] **Broken albums** (si > 0) - icône AlertTriangle rouge
  - [ ] **Missing albums** (si > 0) - icône Music jaune
  - [ ] **Without MB ID** (si > 0) - icône Database bleu
  - [ ] **Without Artist MB ID** (si > 0) - icône Database bleu
  - [ ] **Incomplete tags** (si > 0) - icône Tag violet
  - [ ] **Without album image** (si > 0) - icône Image gris
  - [ ] **Without artist image** (si > 0) - icône Image gris
- [ ] Vérifier que les statistiques s'incrémentent pendant le scan
- [ ] Vérifier que les valeurs sont formatées avec des séparateurs de milliers (ex: 1,234)

#### 2.3 Vérifier l'affichage dans l'historique (ScanDetails)
- [ ] Une fois le scan terminé, aller dans l'historique des scans (menu "Scan History")
- [ ] Ouvrir les détails d'un scan récent
- [ ] Vérifier la section "Detailed Statistics" :
  - [ ] Toutes les statistiques collectées sont affichées
  - [ ] Les valeurs correspondent à ce qui a été vu pendant le scan
  - [ ] Les icônes et couleurs sont correctes
  - [ ] La section "Basic Statistics" est toujours présente et fonctionne
  - [ ] La section "Detailed Statistics" n'apparaît que si au moins une statistique > 0

#### 2.4 Vérification en base de données
- [ ] Se connecter au conteneur : `ssh root@192.168.3.2 "docker exec -it PMDA_WEBUI sh"`
- [ ] Vérifier les données du dernier scan :
```sql
sqlite3 /config/state.db "SELECT scan_id, duplicate_groups_count, total_duplicates_count, broken_albums_count, albums_without_mb_id, albums_without_complete_tags, albums_without_album_image FROM scan_history ORDER BY scan_id DESC LIMIT 1;"
```
- [ ] Les valeurs doivent être cohérentes avec l'affichage dans l'UI
- [ ] Vérifier que les valeurs ne sont pas NULL (sauf pour les scans anciens)

### 3. Tests de régression

#### 3.1 Configuration via le wizard
- [ ] Ouvrir le wizard de configuration (Settings)
- [ ] Vérifier toutes les étapes :
  - [ ] Plex (PLEX_HOST, PLEX_TOKEN, SECTION_IDS)
  - [ ] Libraries
  - [ ] Paths (PATH_MAP, DUPE_ROOT)
  - [ ] Scan (SCAN_THREADS, AUTO_MOVE_DUPES)
  - [ ] AI (AI_PROVIDER, OPENAI_API_KEY, etc.)
  - [ ] Metadata (USE_MUSICBRAINZ, MUSICBRAINZ_CLIENT_ID, etc.)
  - [ ] Notifications (DISCORD_WEBHOOK)
- [ ] Sauvegarder la configuration
- [ ] Vérifier que le conteneur redémarre automatiquement
- [ ] Vérifier que les settings sont bien sauvegardés dans SQLite (table `settings`)

#### 3.2 Scan complet avec détection de duplicates
- [ ] Lancer un scan complet
- [ ] Vérifier que les duplicates sont détectés correctement
- [ ] Vérifier que les albums cassés sont identifiés
- [ ] Vérifier que les statistiques sont collectées pour chaque artiste

#### 3.3 Affichage des duplicates dans l'interface
- [ ] Vérifier que la liste des duplicates s'affiche correctement
- [ ] Vérifier que les détails d'un duplicate group s'ouvrent correctement
- [ ] Vérifier que les informations MusicBrainz s'affichent si disponibles
- [ ] Vérifier que les albums cassés sont marqués visuellement

#### 3.4 Historique des scans (affichage de base)
- [ ] Vérifier que l'historique des scans s'affiche correctement
- [ ] Vérifier que les statistiques de base (duration, albums scanned, etc.) sont présentes
- [ ] Vérifier que les actions (Dedupe, Restore) fonctionnent toujours

## 🔍 Points d'attention

### Configuration SQLite

**Table `settings` (configuration wizard)** :
- ✅ Aucun changement - Les settings du wizard sont toujours stockés dans la table `settings`
- ✅ Tout fonctionne comme avant - La sauvegarde via le wizard met à jour `settings` et `config.json`

**Table `scan_history` (statistiques de scan)** :
- ✅ Nouvelles colonnes ajoutées automatiquement au démarrage
- ⚠️ Les anciens scans auront `NULL` ou `0` pour les nouvelles colonnes (normal)
- ✅ Les nouveaux scans auront toutes les statistiques remplies

### Notes importantes

1. **Pas de nouvelle étape dans le wizard** : Les statistiques sont collectées automatiquement, aucune configuration supplémentaire n'est nécessaire.

2. **Migration SQLite** : La migration est automatique et s'est bien passée. Les nouvelles colonnes sont présentes dans `scan_history`.

3. **Premier scan** : Le premier scan après le déploiement va commencer à collecter les statistiques détaillées.

4. **Scans précédents** : Les scans effectués avant cette mise à jour n'auront pas ces statistiques (elles seront à 0 ou NULL).

## 📊 Commandes de vérification rapide

```bash
# Vérifier que le conteneur tourne
ssh root@192.168.3.2 "docker ps | grep PMDA_WEBUI"

# Vérifier les logs
ssh root@192.168.3.2 "docker logs PMDA_WEBUI --tail 50"

# Vérifier la migration SQLite (depuis le conteneur)
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'PRAGMA table_info(scan_history);' | grep -E 'duplicate_groups|broken_albums|without'"

# Vérifier un scan récent
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'SELECT scan_id, duplicate_groups_count, total_duplicates_count, broken_albums_count FROM scan_history ORDER BY scan_id DESC LIMIT 1;'"

# Vérifier les settings dans SQLite
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'SELECT key FROM settings ORDER BY key;'"
```

## ✅ Résumé

- **Déploiement** : ✅ Terminé
- **Migration SQLite** : ✅ Réussie
- **Conteneur** : ✅ Opérationnel
- **Tests manuels** : ⏳ À effectuer par l'utilisateur

Une fois tous les tests effectués, cocher les cases ci-dessus et noter toute anomalie ou observation.
