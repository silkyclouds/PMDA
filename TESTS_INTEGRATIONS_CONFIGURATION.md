# Tests - Configuration des intégrations et options avancées

**Date de déploiement** : 27 janvier 2026  
**Version** : `meaning/pmda:beta`  
**URL** : http://192.168.3.2:5005

## ✅ Déploiement terminé

- [x] Build Docker multi-platform (linux/amd64, linux/arm64)
- [x] Push vers Docker Hub
- [x] Déploiement sur serveur 192.168.3.2
- [x] Nouvelles options de configuration ajoutées

## 📋 Nouvelles fonctionnalités à tester

### 1. Section "Integrations" dans le wizard

Une nouvelle étape "Integrations" a été ajoutée dans le wizard de configuration, entre "Metadata" et "Notifications".

#### 1.1 Configuration Lidarr

- [ ] Ouvrir le wizard de configuration
- [ ] Aller à l'étape "Integrations"
- [ ] Vérifier la section "Lidarr" :
  - [ ] Champ "Lidarr URL" (ex: http://192.168.1.100:8686)
  - [ ] Champ "Lidarr API Key" (password field)
  - [ ] Switch "Automatically fix broken albums"
  - [ ] Bouton "Test Lidarr Connection"
  - [ ] Lien vers la documentation Lidarr

**Test de connexion Lidarr** :
- [ ] Entrer l'URL de votre instance Lidarr
- [ ] Entrer votre API Key (Settings → General → Security → API Key dans Lidarr)
- [ ] Cliquer sur "Test Lidarr Connection"
- [ ] Vérifier que le test réussit (message vert) ou échoue avec un message clair
- [ ] Vérifier que le message affiche la version de Lidarr si la connexion réussit

**Test AUTO_FIX_BROKEN_ALBUMS** :
- [ ] Activer le switch "Automatically fix broken albums"
- [ ] Vérifier que le switch est désactivé si URL ou API Key manquants
- [ ] Sauvegarder la configuration
- [ ] Lancer un scan avec des albums cassés détectés
- [ ] Vérifier dans les logs que les albums cassés sont envoyés à Lidarr (si MusicBrainz ID disponible)

#### 1.2 Configuration Autobrr

- [ ] Vérifier la section "Autobrr" :
  - [ ] Champ "Autobrr URL" (ex: http://192.168.1.100:7474)
  - [ ] Champ "Autobrr API Key" (password field)
  - [ ] Bouton "Test Autobrr Connection"
  - [ ] Lien vers la documentation Autobrr

**Test de connexion Autobrr** :
- [ ] Entrer l'URL de votre instance Autobrr
- [ ] Entrer votre API Key (Settings → API Keys dans Autobrr)
- [ ] Cliquer sur "Test Autobrr Connection"
- [ ] Vérifier que le test réussit (message vert) ou échoue avec un message clair

### 2. Seuils configurables pour albums cassés (Scan Settings)

Dans l'étape "Scan" du wizard, section "Advanced Options" :

- [ ] Développer "Advanced Options"
- [ ] Vérifier la nouvelle section "Broken Album Detection" :
  - [ ] Champ "Consecutive Missing Tracks Threshold" (nombre, défaut: 3)
  - [ ] Champ "Missing Tracks Percentage Threshold" (décimal 0.01-1.0, défaut: 0.20)
  - [ ] Tooltips explicatifs pour chaque champ

**Test des seuils** :
- [ ] Modifier "Consecutive Missing Tracks Threshold" à 5
- [ ] Modifier "Missing Tracks Percentage Threshold" à 0.15 (15%)
- [ ] Sauvegarder la configuration
- [ ] Lancer un scan
- [ ] Vérifier que les albums cassés sont détectés selon les nouveaux seuils
- [ ] Vérifier dans les logs que les seuils configurés sont utilisés

### 3. Définition d'albums incomplets (Metadata Settings)

Dans l'étape "Metadata" du wizard :

- [ ] Vérifier la nouvelle section "Incomplete Album Definition" :
  - [ ] Champ "Required Tags" (texte, défaut: "artist,album,date")
  - [ ] Tooltip expliquant les tags disponibles
  - [ ] Note indiquant les tags disponibles (artist, album, date, genre, year)

**Test des tags requis** :
- [ ] Modifier "Required Tags" à "artist,album,date,genre"
- [ ] Sauvegarder la configuration
- [ ] Lancer un scan
- [ ] Vérifier dans les statistiques détaillées que seuls les albums manquant un de ces 4 tags sont comptés comme "incomplets"
- [ ] Modifier à "artist,album" et vérifier que plus d'albums sont considérés comme complets

### 4. Sauvegarde dans SQLite

- [ ] Configurer toutes les nouvelles options :
  - [ ] Lidarr (URL, API Key, AUTO_FIX_BROKEN_ALBUMS)
  - [ ] Autobrr (URL, API Key)
  - [ ] Seuils albums cassés
  - [ ] Tags requis
- [ ] Sauvegarder la configuration
- [ ] Vérifier que le conteneur redémarre
- [ ] Vérifier en base de données que les settings sont sauvegardés :
```bash
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'SELECT key, value FROM settings WHERE key IN (\"LIDARR_URL\", \"LIDARR_API_KEY\", \"AUTO_FIX_BROKEN_ALBUMS\", \"AUTOBRR_URL\", \"AUTOBRR_API_KEY\", \"BROKEN_ALBUM_CONSECUTIVE_THRESHOLD\", \"BROKEN_ALBUM_PERCENTAGE_THRESHOLD\", \"REQUIRED_TAGS\") ORDER BY key;'"
```
- [ ] Vérifier que toutes les valeurs sont correctement sauvegardées

### 5. Test des intégrations fonctionnelles

#### 5.1 Test Lidarr - Ajout d'album cassé

- [ ] S'assurer que Lidarr est configuré et accessible
- [ ] Trouver un album cassé dans PMDA (via la page "Broken Albums")
- [ ] Vérifier que l'album a un MusicBrainz Release Group ID
- [ ] Cliquer sur "Send to Lidarr" pour un album cassé
- [ ] Vérifier que l'album apparaît dans Lidarr
- [ ] Vérifier que l'album est configuré pour être monitoré et recherché

#### 5.2 Test Lidarr - Ajout d'artiste

- [ ] Aller dans "Library Browser"
- [ ] Sélectionner un artiste
- [ ] Cliquer sur "Monitor in Lidarr" (étoile)
- [ ] Vérifier que l'artiste apparaît dans Lidarr
- [ ] Vérifier que l'artiste est configuré pour monitorer les albums manquants

#### 5.3 Test Autobrr - Création de filtre

- [ ] S'assurer qu'Autobrr est configuré et accessible
- [ ] Aller dans "Library Browser"
- [ ] Sélectionner un artiste
- [ ] Aller dans l'onglet "Similar Artists"
- [ ] Sélectionner plusieurs artistes similaires
- [ ] Cliquer sur "Add Selected to Autobrr"
- [ ] Vérifier qu'un filtre est créé dans Autobrr
- [ ] Vérifier que le filtre contient les noms des artistes

### 6. Tests de régression

- [ ] Vérifier que toutes les fonctionnalités existantes fonctionnent toujours :
  - [ ] Configuration Plex
  - [ ] Configuration Libraries
  - [ ] Configuration Paths
  - [ ] Configuration Scan (options existantes)
  - [ ] Configuration AI
  - [ ] Configuration Metadata (MusicBrainz)
  - [ ] Configuration Notifications (Discord)
  - [ ] Scan complet
  - [ ] Détection de duplicates
  - [ ] Historique des scans
  - [ ] Statistiques détaillées

## 🔍 Points d'attention

### Authentification API

**Lidarr** :
- Utilise le header `X-Api-Key` pour l'authentification
- Endpoint de test : `/api/v1/system/status`
- L'API Key se trouve dans Lidarr : Settings → General → Security → API Key

**Autobrr** :
- Utilise le header `X-API-Token` pour l'authentification
- Endpoints de test : `/api/healthz/liveness` ou `/api/config`
- L'API Key se trouve dans Autobrr : Settings → API Keys

### Seuils par défaut

- **Consecutive Missing Tracks Threshold** : 3 (si > 3 tracks consécutives manquantes, album cassé)
- **Missing Tracks Percentage Threshold** : 0.20 (20% - si > 20% de tracks manquantes, album cassé)
- **Required Tags** : ["artist", "album", "date"] (tags minimum pour considérer un album complet)

### Sauvegarde SQLite

Toutes les nouvelles options sont sauvegardées dans la table `settings` de SQLite :
- `LIDARR_URL` (string)
- `LIDARR_API_KEY` (string, masqué)
- `AUTOBRR_URL` (string)
- `AUTOBRR_API_KEY` (string, masqué)
- `AUTO_FIX_BROKEN_ALBUMS` (boolean)
- `BROKEN_ALBUM_CONSECUTIVE_THRESHOLD` (integer)
- `BROKEN_ALBUM_PERCENTAGE_THRESHOLD` (float)
- `REQUIRED_TAGS` (JSON array ou string séparée par virgules)

## 📊 Commandes de vérification

```bash
# Vérifier que le conteneur tourne
ssh root@192.168.3.2 "docker ps | grep PMDA_WEBUI"

# Vérifier les logs
ssh root@192.168.3.2 "docker logs PMDA_WEBUI --tail 50"

# Vérifier les settings dans SQLite
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db 'SELECT key, value FROM settings WHERE key LIKE \"%LIDARR%\" OR key LIKE \"%AUTOBRR%\" OR key LIKE \"%BROKEN%\" OR key LIKE \"%REQUIRED%\" ORDER BY key;'"

# Tester l'endpoint Lidarr (remplacer URL et KEY)
curl -X POST http://192.168.3.2:5005/api/lidarr/test \
  -H "Content-Type: application/json" \
  -d '{"url": "http://VOTRE_LIDARR:8686", "api_key": "VOTRE_KEY"}'

# Tester l'endpoint Autobrr (remplacer URL et KEY)
curl -X POST http://192.168.3.2:5005/api/autobrr/test \
  -H "Content-Type: application/json" \
  -d '{"url": "http://VOTRE_AUTOBRR:7474", "api_key": "VOTRE_KEY"}'
```

## ✅ Résumé des nouvelles options

### Intégrations
- ✅ **Lidarr** : URL, API Key, Auto-fix broken albums
- ✅ **Autobrr** : URL, API Key
- ✅ **Tests de connexion** : Boutons de test pour chaque intégration

### Détection albums cassés
- ✅ **Seuil consécutif** : Nombre de tracks consécutives manquantes (défaut: 3)
- ✅ **Seuil pourcentage** : Pourcentage de tracks manquantes (défaut: 0.20 = 20%)

### Définition albums incomplets
- ✅ **Tags requis** : Liste configurable de tags requis (défaut: artist,album,date)

Toutes ces options sont :
- ✅ Sauvegardées dans SQLite (table `settings`)
- ✅ Accessibles via le wizard de configuration
- ✅ Testables via des boutons de test
- ✅ Documentées avec des tooltips

Une fois tous les tests effectués, cocher les cases ci-dessus et noter toute anomalie ou observation.
