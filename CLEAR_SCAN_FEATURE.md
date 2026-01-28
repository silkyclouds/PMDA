# Feature : Clear Scan Results

**Date** : 27 janvier 2026  
**Fonctionnalité** : Nettoyer les résultats de scan corrompus ou obsolètes

## 🎯 Objectif

Permettre à l'utilisateur de nettoyer facilement les résultats de scan depuis l'interface web, notamment pour :
- Supprimer les résultats corrompus (duplicates détectés plusieurs fois dans le même dossier)
- Repartir sur une base propre avant un nouveau scan
- Nettoyer les caches optionnels (audio, MusicBrainz)

## ✅ Implémentation

### Backend (`pmda.py`)

**Nouvel endpoint** : `POST /api/scan/clear`

**Paramètres optionnels** (JSON body) :
- `clear_audio_cache` (boolean) : Nettoyer aussi le cache audio (format analysis)
- `clear_mb_cache` (boolean) : Nettoyer aussi le cache MusicBrainz

**Actions effectuées** :
1. Supprime toutes les entrées de `duplicates_best` et `duplicates_loser` dans `state.db`
2. Nettoie le state in-memory (`state["duplicates"]` et `state["scan_active_artists"]`)
3. Optionnellement, nettoie `audio_cache` et `musicbrainz_cache` dans `cache.db`

**Réponse** :
```json
{
  "status": "ok",
  "message": "Scan results cleared successfully",
  "cleared": {
    "duplicates_best": 42,
    "duplicates_loser": 15,
    "audio_cache": 1234,  // Si clear_audio_cache=true
    "musicbrainz_cache": 56  // Si clear_mb_cache=true
  }
}
```

### Frontend

#### API Client (`frontend/src/lib/api.ts`)

**Nouvelle fonction** : `clearScan(options?: ClearScanOptions)`

```typescript
interface ClearScanOptions {
  clear_audio_cache?: boolean;
  clear_mb_cache?: boolean;
}

interface ClearScanResult {
  status: string;
  message: string;
  cleared: {
    duplicates_best: number;
    duplicates_loser: number;
    audio_cache?: number;
    musicbrainz_cache?: number;
  };
}
```

#### Hook (`frontend/src/hooks/usePMDA.ts`)

**Ajout dans `useScanControls()`** :
- `clear`: fonction pour déclencher le clear
- `isClearing`: état de chargement

#### UI (`frontend/src/components/ScanProgress.tsx`)

**Nouveau bouton "Clear Scan"** :
- Visible uniquement quand aucun scan n'est en cours
- Bouton rouge avec icône `Trash2`
- Ouvre une dialog de confirmation avant de clear
- Affiche un toast de succès/erreur après l'action

**Dialog de confirmation** :
- Titre : "Clear Scan Results"
- Description : Explique que les résultats seront supprimés et qu'un nouveau scan sera nécessaire
- Boutons : "Cancel" et "Clear Results" (rouge)

## 📋 Utilisation

### Via l'interface web

1. Aller sur la page principale
2. S'assurer qu'aucun scan n'est en cours
3. Cliquer sur le bouton "Clear Scan" (icône poubelle) dans la section "Library Scan"
4. Confirmer dans la dialog
5. Les résultats sont immédiatement supprimés et l'interface se met à jour

### Via l'API (pour scripts/automatisation)

```bash
# Clear uniquement les résultats de scan
curl -X POST http://192.168.3.2:5005/api/scan/clear \
  -H "Content-Type: application/json" \
  -d '{}'

# Clear les résultats + cache audio
curl -X POST http://192.168.3.2:5005/api/scan/clear \
  -H "Content-Type: application/json" \
  -d '{"clear_audio_cache": true}'

# Clear tout (résultats + caches)
curl -X POST http://192.168.3.2:5005/api/scan/clear \
  -H "Content-Type: application/json" \
  -d '{"clear_audio_cache": true, "clear_mb_cache": true}'
```

## 🔍 Vérification

### Vérifier que les résultats sont bien supprimés

```bash
# Vérifier dans la base de données
ssh root@192.168.3.2 "docker exec PMDA_WEBUI sqlite3 /config/state.db \"SELECT COUNT(*) FROM duplicates_best; SELECT COUNT(*) FROM duplicates_loser;\""
```

Les deux requêtes doivent retourner `0` après un clear.

### Vérifier les logs

```bash
ssh root@192.168.3.2 "docker logs PMDA_WEBUI 2>&1 | grep -i 'scan results cleared' | tail -5"
```

## ⚠️ Notes importantes

1. **Action irréversible** : Une fois les résultats supprimés, ils ne peuvent pas être récupérés (sauf via l'historique des scans si disponible)

2. **Cache audio** : Si vous clear le cache audio, le prochain scan devra re-analyser tous les fichiers audio (plus lent)

3. **Cache MusicBrainz** : Si vous clear le cache MusicBrainz, le prochain scan devra re-interroger MusicBrainz pour tous les albums (plus lent, plus de requêtes réseau)

4. **Recommandation** : En général, clear uniquement les résultats de scan (`duplicates_best` et `duplicates_loser`) suffit. Ne clear les caches que si vous suspectez qu'ils sont corrompus.

## 🧪 Tests

1. [ ] Lancer un scan pour avoir des résultats
2. [ ] Vérifier que des duplicates sont affichés
3. [ ] Cliquer sur "Clear Scan" et confirmer
4. [ ] Vérifier que la liste des duplicates est vide
5. [ ] Vérifier dans la DB que les tables sont vides
6. [ ] Lancer un nouveau scan et vérifier que tout fonctionne

## 📝 Fichiers modifiés

- `pmda.py` : Nouvel endpoint `/api/scan/clear`
- `frontend/src/lib/api.ts` : Fonction `clearScan()`
- `frontend/src/hooks/usePMDA.ts` : Hook `useScanControls()` avec `clear` et `isClearing`
- `frontend/src/components/ScanProgress.tsx` : Bouton "Clear Scan" avec dialog de confirmation
- `frontend/src/pages/Index.tsx` : Passage de `onClear` et `isClearing` au composant `ScanProgress`
