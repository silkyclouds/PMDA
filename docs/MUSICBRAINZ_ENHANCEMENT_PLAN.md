# Plan d'Action : Amélioration de l'utilisation de MusicBrainz dans PMDA

## 📋 Résumé Exécutif

**Objectif** : Transformer MusicBrainz d'un simple outil d'analyse en un système complet de tagging automatique et d'enrichissement des métadonnées audio.

**Statut actuel** : MusicBrainz est utilisé uniquement en **lecture** pour améliorer la détection de doublons pendant le scan.

**Vision** : Permettre à PMDA de **taguer automatiquement** les fichiers audio avec les métadonnées MusicBrainz une fois qu'un album est identifié avec certitude.

---

## 🔍 Analyse de l'Existant

### Ce que PMDA fait actuellement avec MusicBrainz

1. **Lecture des tags MusicBrainz existants** (via `ffprobe`)
   - `musicbrainz_releasegroupid`
   - `musicbrainz_releaseid`
   - `musicbrainz_albumid`
   - `musicbrainz_originalreleaseid`

2. **Enrichissement pour la détection de doublons**
   - Récupération du `release-group` via l'API MusicBrainz
   - Identification du type (album, compilation, box set, etc.)
   - Utilisation de ces infos pour regrouper les albums liés

3. **Recherche fallback** si aucun ID n'est trouvé
   - Recherche par artiste + titre d'album
   - Comparaison du nombre de pistes

### Limitations actuelles

- ❌ **Aucune écriture de tags** : PMDA ne modifie jamais les fichiers audio
- ❌ **Aucun enrichissement automatique** : Les métadonnées manquantes ne sont pas complétées
- ❌ **Pas de réutilisation** : Les IDs MusicBrainz trouvés ne sont pas sauvegardés dans les fichiers
- ❌ **DB Plex en read-only** : Impossible de modifier les métadonnées dans Plex directement

---

## 🎯 Objectifs et Fonctionnalités Proposées

### Phase 1 : Auto-Tagging Basique (Priorité Haute)

**Objectif** : Taguer automatiquement les fichiers audio avec les IDs MusicBrainz une fois qu'un album est identifié.

#### Fonctionnalités

1. **Option de configuration** : `AUTO_TAG_MUSICBRAINZ` (bool, défaut: `false`)
   - Activable dans le wizard (section Metadata)
   - Avertissement : "Cette fonction modifie les fichiers audio. Assurez-vous d'avoir une sauvegarde."

2. **Tagging des IDs MusicBrainz**
   - Lorsqu'un album est identifié avec certitude (via ID existant ou recherche)
   - Écrire dans tous les fichiers de l'album :
     - `musicbrainz_releasegroupid` (prioritaire)
     - `musicbrainz_releaseid` (si disponible)
     - `musicbrainz_albumid` (si disponible)
     - `musicbrainz_albumartistid` (si disponible)
     - `musicbrainz_artistid` (par piste, si disponible)

3. **Modes de fonctionnement**
   - **Mode "Safe"** : Taguer uniquement les albums qui ont déjà un ID MusicBrainz partiel
   - **Mode "Confident"** : Taguer les albums identifiés avec une confiance élevée (>90%)
   - **Mode "Aggressive"** : Taguer tous les albums identifiés (avec confirmation utilisateur)

### Phase 2 : Enrichissement des Métadonnées (Priorité Moyenne)

**Objectif** : Compléter automatiquement les métadonnées manquantes avec les données MusicBrainz.

#### Fonctionnalités

1. **Enrichissement des tags de base**
   - `ARTIST` / `ALBUMARTIST` : Normalisation et correction
   - `ALBUM` : Titre canonique de MusicBrainz
   - `DATE` / `ORIGINALDATE` : Année de sortie
   - `GENRE` : Genres MusicBrainz (si manquant)
   - `DISCNUMBER` / `DISCTOTAL` : Informations multi-disques
   - `TRACKNUMBER` / `TRACKTOTAL` : Numérotation des pistes

2. **Cover Art** (optionnel)
   - Téléchargement depuis Cover Art Archive
   - Écriture dans les fichiers (si supporté par le format)
   - Stockage dans un dossier `covers/` (alternative)

3. **Tags avancés**
   - `LABEL` : Label de distribution
   - `CATALOGNUMBER` : Numéro de catalogue
   - `BARCODE` : Code-barres (si disponible)
   - `ASIN` : Amazon ASIN (si disponible)

### Phase 3 : Amélioration de la Détection (Priorité Moyenne)

**Objectif** : Utiliser les tags MusicBrainz pour améliorer la détection future.

#### Fonctionnalités

1. **Cache persistant des IDs**
   - Stocker les IDs MusicBrainz dans la DB SQLite après identification
   - Réutiliser lors des scans suivants (évite les requêtes API)

2. **Matching amélioré**
   - Utiliser les IDs MusicBrainz pour regrouper les albums même si les métadonnées diffèrent
   - Détection plus rapide des doublons (pas besoin de comparer tous les tags)

3. **Statistiques et rapports**
   - Nombre d'albums tagués
   - Taux de succès d'identification
   - Albums non identifiés (pour action manuelle)

---

## 🛠️ Implémentation Technique

### Bibliothèques Python Requises

1. **Mutagen** (nouveau)
   - Lecture/écriture de tags pour FLAC, MP3, OGG, M4A, etc.
   - Support natif des tags MusicBrainz
   - Installation : `pip install mutagen`

2. **musicbrainzngs** (déjà présent)
   - API MusicBrainz
   - Récupération des métadonnées complètes

3. **requests** (déjà présent)
   - Téléchargement des cover arts depuis Cover Art Archive

### Structure de Code Proposée

```
pmda.py
├── write_audio_tags()          # Nouvelle fonction principale
│   ├── get_mutagen_file()      # Détection du format et chargement
│   ├── write_mb_ids()          # Écriture des IDs MusicBrainz
│   ├── enrich_metadata()       # Enrichissement des métadonnées
│   └── write_cover_art()       # Écriture de la cover art
│
├── fetch_mb_release_full()     # Nouvelle fonction
│   └── Récupération complète d'une release (artistes, pistes, etc.)
│
└── auto_tag_album()            # Nouvelle fonction
    └── Orchestration du tagging pour un album complet
```

### Formats Audio Supportés

| Format | Bibliothèque | Tags MusicBrainz | Cover Art |
|--------|--------------|------------------|-----------|
| FLAC   | `mutagen.flac.FLAC` | ✅ Oui | ✅ Oui (embedded) |
| MP3    | `mutagen.id3.ID3` | ✅ Oui (TXXX frames) | ✅ Oui (APIC frame) |
| OGG    | `mutagen.oggvorbis.OggVorbis` | ✅ Oui | ✅ Oui |
| M4A    | `mutagen.mp4.MP4` | ✅ Oui | ✅ Oui |
| OPUS   | `mutagen.oggopus.OggOpus` | ✅ Oui | ✅ Oui |

### Exemple d'Implémentation

```python
from mutagen.flac import FLAC
from mutagen.id3 import ID3, TXXX, UFID
from mutagen.mp4 import MP4

def write_musicbrainz_ids(file_path: Path, mb_data: dict) -> bool:
    """
    Write MusicBrainz IDs to an audio file.
    
    Args:
        file_path: Path to the audio file
        mb_data: Dict with keys:
            - releasegroupid
            - releaseid
            - albumid
            - albumartistid
            - artistid (per track)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        ext = file_path.suffix.lower()
        
        if ext == '.flac':
            audio = FLAC(str(file_path))
            audio['MUSICBRAINZ_RELEASEGROUPID'] = mb_data['releasegroupid']
            audio['MUSICBRAINZ_RELEASEID'] = mb_data['releaseid']
            audio['MUSICBRAINZ_ALBUMID'] = mb_data['albumid']
            audio['MUSICBRAINZ_ALBUMARTISTID'] = mb_data['albumartistid']
            audio.save()
            
        elif ext in ['.mp3', '.m4a']:
            # MP3 uses ID3 frames
            if ext == '.mp3':
                audio = ID3(str(file_path))
                audio.add(TXXX(encoding=3, desc='MusicBrainz Release Group Id', 
                              text=mb_data['releasegroupid']))
                audio.add(UFID(owner='https://musicbrainz.org/', 
                              data=mb_data['releaseid'].encode()))
                audio.save()
            else:
                # M4A uses MP4 tags
                audio = MP4(str(file_path))
                audio['----:com.apple.iTunes:MusicBrainz Release Group Id'] = \
                    [mb_data['releasegroupid'].encode('utf-8')]
                audio.save()
                
        return True
    except Exception as e:
        logging.error(f"Failed to write MusicBrainz IDs to {file_path}: {e}")
        return False
```

### Gestion des Erreurs

1. **Fichiers en lecture seule**
   - Vérifier les permissions avant écriture
   - Logger l'erreur et continuer avec les autres fichiers

2. **Formats non supportés**
   - Détecter le format avant tentative d'écriture
   - Logger un avertissement et continuer

3. **Corruption de fichiers**
   - Backup automatique avant modification (optionnel)
   - Rollback en cas d'erreur

4. **Rate limiting MusicBrainz**
   - Respecter les limites de l'API (1 req/sec sans clé, plus avec clé)
   - Cache agressif pour éviter les requêtes répétées

---

## ⚙️ Configuration et Interface Utilisateur

### Nouveaux Paramètres de Configuration

```json
{
  "AUTO_TAG_MUSICBRAINZ": false,
  "MB_TAGGING_MODE": "safe",  // "safe" | "confident" | "aggressive"
  "MB_ENRICH_METADATA": false,
  "MB_ENRICH_COVER_ART": false,
  "MB_BACKUP_BEFORE_TAG": false
}
```

### Interface Wizard (Section Metadata)

```
┌─────────────────────────────────────────┐
│ Metadata Settings                       │
├─────────────────────────────────────────┤
│                                         │
│ ☑ Use MusicBrainz for duplicate        │
│   detection                             │
│                                         │
│ ┌─ Advanced Tagging ─────────────────┐ │
│ │ ☐ Auto-tag with MusicBrainz IDs   │ │
│ │                                    │ │
│ │ Mode: [Safe ▼]                    │ │
│ │   • Safe: Only tag albums with    │ │
│ │     partial MB IDs                │ │
│ │   • Confident: Tag albums with     │ │
│ │     high confidence match (>90%)  │ │
│ │   • Aggressive: Tag all           │ │
│ │     identified albums             │ │
│ │                                    │ │
│ │ ☐ Enrich missing metadata         │ │
│ │ ☐ Download and embed cover art    │ │
│ │ ☐ Create backup before tagging    │ │
│ └────────────────────────────────────┘ │
│                                         │
│ ⚠️ Warning: Auto-tagging modifies your  │
│    audio files. Ensure you have a      │
│    backup before enabling this option. │
└─────────────────────────────────────────┘
```

### Interface de Scan

- **Indicateur de progression** : "Tagging albums with MusicBrainz IDs..."
- **Statistiques** : "X albums tagged, Y files modified"
- **Logs détaillés** : Fichiers tagués, erreurs, avertissements

---

## 📊 Avantages et Bénéfices

### Pour l'Utilisateur

1. **Bibliothèque mieux organisée**
   - Tags cohérents et normalisés
   - Identification future plus rapide
   - Compatibilité avec d'autres outils (Picard, beets, etc.)

2. **Détection améliorée**
   - Les scans futurs seront plus rapides (IDs déjà présents)
   - Moins de requêtes API MusicBrainz
   - Meilleure détection des doublons

3. **Métadonnées complètes**
   - Albums avec toutes les informations
   - Cover arts intégrées
   - Informations de label, catalogue, etc.

### Pour PMDA

1. **Performance**
   - Moins de requêtes API (cache + tags existants)
   - Scans plus rapides

2. **Fiabilité**
   - Identification plus précise grâce aux IDs
   - Moins de faux positifs/négatifs

3. **Valeur ajoutée**
   - Fonctionnalité unique (peu de déduplicateurs font du tagging)
   - Différenciation par rapport à la concurrence

---

## ⚠️ Risques et Considérations

### Risques

1. **Modification de fichiers**
   - ⚠️ **Risque majeur** : Modification irréversible des fichiers audio
   - **Mitigation** : Option de backup automatique, mode "safe" par défaut

2. **Corruption de fichiers**
   - ⚠️ **Risque moyen** : Erreur lors de l'écriture peut corrompre un fichier
   - **Mitigation** : Tests exhaustifs, gestion d'erreurs robuste, backup

3. **Faux positifs**
   - ⚠️ **Risque faible** : Taguer un album avec le mauvais ID
   - **Mitigation** : Modes de confiance, validation manuelle optionnelle

4. **Performance**
   - ⚠️ **Risque faible** : Tagging de milliers de fichiers peut être lent
   - **Mitigation** : Traitement en parallèle, cache, option de désactivation

### Considérations Légales

- ✅ **MusicBrainz** : Données sous licence CC0 (domaine public)
- ✅ **Cover Art Archive** : Images sous licence CC (généralement)
- ⚠️ **Modification de fichiers** : Responsabilité de l'utilisateur (backup recommandé)

---

## 🚀 Plan de Déploiement

### Phase 1 : Infrastructure (Semaine 1-2)

1. **Ajout de Mutagen**
   - Ajout à `requirements.txt`
   - Tests unitaires pour chaque format (FLAC, MP3, OGG, M4A, OPUS)

2. **Fonctions de base**
   - `write_musicbrainz_ids()` : Écriture des IDs
   - `get_mutagen_file()` : Détection et chargement du format
   - Tests avec fichiers réels

3. **Configuration**
   - Ajout des paramètres dans `config.json`
   - Interface wizard (section Metadata)

### Phase 2 : Auto-Tagging Basique (Semaine 3-4)

1. **Intégration dans le scan**
   - Détection des albums identifiés
   - Appel de `write_musicbrainz_ids()` pour chaque fichier
   - Logging et statistiques

2. **Modes de confiance**
   - Implémentation des 3 modes (safe, confident, aggressive)
   - Calcul du score de confiance

3. **Tests et validation**
   - Tests sur bibliothèque réelle (backup obligatoire)
   - Validation des tags écrits

### Phase 3 : Enrichissement (Semaine 5-6)

1. **Enrichissement des métadonnées**
   - Récupération complète des données MusicBrainz
   - Écriture des tags manquants
   - Normalisation des artistes/albums

2. **Cover Art**
   - Intégration Cover Art Archive
   - Téléchargement et écriture
   - Gestion des erreurs (fichier trop gros, format non supporté)

3. **Tests finaux**
   - Tests end-to-end
   - Documentation utilisateur

---

## 📝 Checklist d'Implémentation

### Backend (Python)

- [ ] Ajouter `mutagen>=1.47.0` à `requirements.txt`
- [ ] Créer `write_musicbrainz_ids()` pour FLAC
- [ ] Créer `write_musicbrainz_ids()` pour MP3
- [ ] Créer `write_musicbrainz_ids()` pour OGG/M4A/OPUS
- [ ] Créer `fetch_mb_release_full()` pour récupérer toutes les métadonnées
- [ ] Créer `enrich_metadata()` pour compléter les tags manquants
- [ ] Créer `write_cover_art()` pour intégrer les covers
- [ ] Intégrer dans `scan_duplicates()` avec option de configuration
- [ ] Ajouter logging détaillé
- [ ] Gestion d'erreurs robuste
- [ ] Tests unitaires pour chaque format

### Frontend (React)

- [ ] Ajouter `AUTO_TAG_MUSICBRAINZ` dans `PMDAConfig` interface
- [ ] Ajouter `MB_TAGGING_MODE` dans `PMDAConfig` interface
- [ ] Ajouter `MB_ENRICH_METADATA` dans `PMDAConfig` interface
- [ ] Ajouter `MB_ENRICH_COVER_ART` dans `PMDAConfig` interface
- [ ] Mettre à jour `MetadataSettings.tsx` avec les nouvelles options
- [ ] Ajouter avertissements et tooltips
- [ ] Afficher les statistiques de tagging dans l'UI

### Configuration

- [ ] Ajouter les nouveaux paramètres dans `pmda.py` (chargement config)
- [ ] Ajouter validation des paramètres
- [ ] Documentation dans `CONFIGURATION.md` et `CONFIGURATION_FR.md`

### Documentation

- [ ] Guide utilisateur pour l'auto-tagging
- [ ] Avertissements sur les risques
- [ ] Guide de dépannage
- [ ] Exemples de configuration

---

## 🔗 Ressources et Références

### Documentation

- **Mutagen** : https://mutagen.readthedocs.io/
- **MusicBrainz API** : https://musicbrainz.org/doc/MusicBrainz_API
- **Cover Art Archive** : https://coverartarchive.org/
- **MusicBrainz Picard** : https://picard.musicbrainz.org/ (inspiration)

### Bibliothèques Python

- **mutagen** : https://pypi.org/project/mutagen/
- **musicbrainzngs** : https://python-musicbrainzngs.readthedocs.io/ (déjà utilisé)

### Standards de Tags

- **Vorbis Comment** (FLAC, OGG) : https://xiph.org/vorbis/doc/v-comment.html
- **ID3v2** (MP3) : https://id3.org/id3v2.3.0
- **MP4/M4A** : https://developer.apple.com/library/archive/documentation/QuickTime/QTFF/Metadata/Metadata.html

---

## ✅ Conclusion

Ce plan d'action transforme MusicBrainz d'un simple outil d'analyse en un système complet de tagging automatique. Les bénéfices sont significatifs :

- ✅ **Bibliothèque mieux organisée** : Tags cohérents et normalisés
- ✅ **Performance améliorée** : Scans plus rapides grâce aux IDs existants
- ✅ **Valeur ajoutée** : Fonctionnalité unique dans le domaine de la déduplication

Les risques sont maîtrisés grâce à :
- ✅ **Modes de confiance** : L'utilisateur contrôle le niveau de risque
- ✅ **Backup optionnel** : Protection contre les erreurs
- ✅ **Activation manuelle** : L'utilisateur doit explicitement activer le tagging

**Recommandation** : Commencer par la Phase 1 (Auto-Tagging Basique) pour valider le concept, puis étendre avec les phases suivantes selon les retours utilisateurs.
