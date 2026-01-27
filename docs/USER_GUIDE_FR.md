# PMDA – Guide utilisateur

Instructions claires et pas à pas pour utiliser PMDA avec Plex Music. Aucun code requis.

---

## Qu’est-ce que PMDA ?

PMDA (Plex Music Dedupe Assistant) scanne votre bibliothèque Plex Music, détecte les albums en double (même artiste et album, différentes éditions ou formats), choisit la meilleure version et déplace les autres dans un dossier « dupes » tout en nettoyant Plex pour qu’ils n’apparaissent plus.

---

## Avant de commencer

- Un **Plex Media Server** avec au moins une bibliothèque **Music** (type « artiste »).
- **Token Plex** : [Comment trouver votre token Plex](https://support.plex.tv/articles/204059436-finding-an-authentication-token-number/).
- **Chemin de la base Plex** : le dossier qui contient `com.plexapp.plugins.library.db` (sur la même machine ou un volume monté).
- **(Optionnel)** **Clé API OpenAI** – pour de meilleurs choix de « meilleure édition » et les pistes à fusionner.

---

## Lancer PMDA

### Avec Docker (recommandé)

1. Téléchargez l’image :  
   `docker pull meaning/pmda:latest`  
   (ou utilisez votre propre build.)

2. **Lancement minimal (wizard)** – aucune variable d’environnement obligatoire. Trois montages suffisent ; la configuration Plex se fait au premier démarrage via l’interface :

   ```bash
   docker run --rm --name pmda \
     -v "/chemin/vers/config:/config:rw" \
     -v "/chemin/vers/base/plex:/database:ro" \
     -v "/chemin/vers/dupes:/dupes:rw" \
     -p 5005:5005 \
     meaning/pmda:latest
   ```

   Ouvrez ensuite `http://localhost:5005` (ou votre machine:5005), allez dans **Paramètres**, et complétez la configuration Plex (voir [Premier lancement (wizard)](#premier-lancement-wizard) ci-dessous).

3. **Lancement classique** – avec variables d’environnement dès le départ :
   - `PLEX_HOST` – ex. `http://192.168.1.10:32400`
   - `PLEX_TOKEN` – votre token Plex
   - `PLEX_DB_PATH` – chemin **dans le conteneur** vers le dossier de la base Plex (défaut `/database`)
   - Montez le dossier de la base Plex, le config et les dupes (et les racines musique si besoin ; voir Configuration).

4. Choisissez le mode :
   - **Interface Web** : par défaut `serve` ; exposez le port (ex. `-p 5005:5005`).
   - **CLI (scan + dédupe)** : `PMDA_DEFAULT_MODE=cli` (pas de port nécessaire sauf pour l’UI plus tard).

Exemple (classique, avec variables) :

```bash
docker run --rm -it \
  -e PLEX_HOST="http://192.168.1.10:32400" \
  -e PLEX_TOKEN="votre-token-plex" \
  -e PLEX_DB_PATH="/database" \
  -e PMDA_CONFIG_DIR="/config" \
  -e PMDA_DEFAULT_MODE="serve" \
  -v "/chemin/vers/base/plex:/database:ro" \
  -v "/chemin/vers/config:/config:rw" \
  -v "/chemin/vers/musique:/music/matched:rw" \
  -v "/chemin/vers/dupes:/dupes:rw" \
  -p 5005:5005 \
  meaning/pmda:latest
```

Puis ouvrez `http://localhost:5005` dans votre navigateur.

#### Premier lancement (wizard)

Si vous avez démarré le conteneur en **minimal** (sans `PLEX_HOST` / `PLEX_TOKEN`), configurez Plex depuis l’interface :

1. Ouvrez l’interface (ex. `http://localhost:5005`) et allez dans **Paramètres**.
2. **Token Plex** : saisissez votre [token Plex](https://support.plex.tv/articles/204059436-finding-an-authentication-token-number/).
3. **Serveur Plex** :
   - Cliquez sur **Fetch my servers** pour lister les serveurs de votre compte Plex, puis choisissez-en un dans la liste ; ou
   - Cliquez sur **Discover on network** pour détecter les serveurs Plex sur votre réseau (pas de token nécessaire pour la découverte), puis choisissez-en un.
4. Le champ **Plex Server URL** est renseigné lorsque vous sélectionnez un serveur ; vous pouvez le modifier à la main si besoin.
5. Vérifiez que le dossier de la base Plex (contenant `com.plexapp.plugins.library.db`) est monté sur `/database` dans le conteneur. Sinon, l’interface vous le rappellera ; redémarrez le conteneur avec le bon montage puis sauvegardez.
6. Cliquez sur **Enregistrer** (et redémarrez le conteneur si vous avez ajouté ou modifié le montage de la base).
7. Vous pouvez utiliser **Autodetect** pour les Section IDs (bibliothèques musique), puis lancer un scan.

### Sans Docker

1. Installez Python 3.11+, FFmpeg et les dépendances :  
   `pip install -r requirements.txt`
2. Copiez et éditez `config.json` (voir Configuration).
3. Lancez :
   - Interface Web : `python pmda.py --serve`
   - CLI : définissez `PMDA_DEFAULT_MODE=cli` et lancez `python pmda.py`, ou utilisez les options CLI (ex. `--dry-run`, `--verbose`).

---

## Interface Web – Pas à pas

1. **Ouvrir l’interface**  
   Ouvrez l’URL et le port exposés (ex. `http://localhost:5005`).

2. **Premier scan**  
   Cliquez sur **New Scan** (ou **Resume** si un scan a été mis en pause). Attendez la fin de la barre de progression. Le tableau ou les cartes afficheront les groupes de doublons.

3. **Voir un groupe**  
   Cliquez sur une ligne ou une carte pour ouvrir la fenêtre de détail : vous verrez chaque édition (pochette, format, débit, etc.) et l’édition « gagnante » avec le raisonnement.

4. **Dédupliquer**  
   - **Un groupe** : utilisez **Deduplicate** sur cette ligne/carte.
   - **Plusieurs** : cochez les groupes puis **Deduplicate Selected**.
   - **Tout** : **Deduplicate ALL** (à utiliser avec précaution).

5. **Statistiques**  
   Les badges en haut indiquent : artistes totaux, albums totaux, doublons supprimés, groupes de doublons restants, espace libéré (Mo).

---

## Mode CLI

- **Exécution complète (scan + dédupe)** :  
  `PMDA_DEFAULT_MODE=cli` et lancez le conteneur/script sans `--serve`. Le scan de tous les artistes sera suivi de la dédupe (sauf en dry-run).
- **Dry-run (aucun déplacement de fichier, aucune suppression Plex)** :  
  Utilisez `--dry-run` en mode CLI.
- **Mode safe (déplacer les fichiers mais ne pas supprimer les métadonnées Plex)** :  
  Utilisez `--safe-mode`.

---

## Après la déduplication

- Les **dossiers perdants** sont déplacés dans le dossier configuré comme `DUPE_ROOT` (ex. `/dupes` dans le conteneur), en conservant la structure artiste/album quand c’est possible.
- **Plex** ne montrera plus ces albums (métadonnées mises à la corbeille et supprimées ; bibliothèque rafraîchie).
- Vous pouvez supprimer ou archiver le contenu de `DUPE_ROOT` manuellement une fois satisfait.

---

## Conseils

- **Première fois** : utilisez le **dry-run** en CLI ou lancez un scan dans l’interface et dédupliquez seulement quelques groupes pour valider le comportement.
- **PATH_MAP** : avec Docker, vérifiez que vos montages correspondent aux chemins de bibliothèque Plex ; PMDA peut découvrir les chemins depuis Plex et les fusionner avec votre `PATH_MAP`.
- **OpenAI** : pour les meilleurs choix « quelle édition garder » (y compris classique et pistes bonus), définissez `OPENAI_API_KEY` et éventuellement `OPENAI_MODEL` dans la config ou l’env.
- **Sauvegarde** : faites une sauvegarde de votre bibliothèque et/ou de la base Plex avant un « Deduplicate ALL » massif.

---

## Dépannage

| Problème | À vérifier |
|----------|------------|
| « No files found » pour des artistes | Montages de volumes et `PATH_MAP` : les chemins vus par Plex doivent être accessibles depuis le conteneur aux chemins mappés. |
| « Missing required config: PLEX_DB_PATH » / non configuré | Monter le dossier de la base Plex sur `/database` dans le conteneur et configurer serveur + token dans Paramètres (ou via variables d’env). Redémarrer après sauvegarde. |
| Le scan ne trouve jamais de doublons | Vérifiez avoir au moins deux « éditions » du même album (ex. MP3 et FLAC). Vérifiez `SKIP_FOLDERS` et les ID de section. |
| Échec dédupe / permission refusée | Droits en écriture sur les dossiers musique et sur `DUPE_ROOT` ; même utilisateur/permissions que Plex si besoin. |

Pour plus d’options et de variables, voir [CONFIGURATION_FR.md](CONFIGURATION_FR.md).
