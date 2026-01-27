# Déploiement beta – config “from scratch”

Image : **meaning/pmda:beta** (déjà buildée et poussée).

## 1. Sur le serveur (192.168.3.2) : lire le conteneur actuel (optionnel)

Pour voir ta config actuelle avant de passer en minimal :

```bash
ssh root@192.168.3.2

# Inspecter le conteneur existant (nom probable : PMDA_WEBUI ou pmda)
docker ps -a | grep -i pmda
docker inspect PMDA_WEBUI
# ou le nom exact du conteneur listé
```

Repère les 3 montages essentiels : **config**, **database** (base Plex), **dupes**. On ne garde que ceux-là pour le test “config neuve”.

---

## 2. Montages 100 % obligatoires (wizard-first)

| Montage hôte → conteneur | Rôle |
|--------------------------|------|
| `CONFIG` → `/config` | Persistance config / state / cache |
| `PLEX_DB` → `/database` | Dossier contenant `com.plexapp.plugins.library.db` |
| `DUPES` → `/dupes` | Dossier où vont les albums dédupliqués |

**Aucune variable d’environnement obligatoire** (PLEX_HOST, PLEX_TOKEN, PLEX_DB_PATH sont optionnels au premier lancement).

---

## 3. Config “neuve” : utiliser un dossier config dédié

Pour tester comme une **install from scratch**, utilise un **nouveau** dossier config (pas l’ancien) pour ne pas charger l’ancien `config.json` :

- Soit crée un dossier dédié, ex. : `/mnt/cache/appdata/PMDA_fresh`
- Soit vide l’ancien (attention : tu perds l’ancienne config) : `rm -f /mnt/cache/appdata/PMDA/config.json`

Exemple en “fresh” avec un dossier séparé :

```bash
mkdir -p /mnt/cache/appdata/PMDA_fresh
```

---

## 4. Commande Docker minimale (à exécuter sur le serveur)

Remplace les chemins par **tes** vrais chemins (même base Plex et dupes que d’habitude si tu veux ; seul le config change pour le test “neuve”).

```bash
# Arrêter et retirer l’ancien conteneur s’il existe
docker stop PMDA_WEBUI 2>/dev/null || true
docker rm PMDA_WEBUI 2>/dev/null || true

# Lancer avec les 4 montages + port (aucun -e)
docker run -d --name PMDA_WEBUI \
  --restart unless-stopped \
  -v "/mnt/cache/appdata/PMDA_fresh:/config:rw" \
  -v "/CHEMIN_VERS_BASE_PLEX:/database:ro" \
  -v "/CHEMIN_VERS_DUPES:/dupes:rw" \
  -v "/CHEMIN_VERS_MUSIQUE:/music:ro" \
  -p 5005:5005 \
  meaning/pmda:beta
```

À adapter :

- **CONFIG** : par ex. `/mnt/cache/appdata/PMDA_fresh` (config neuve) ou `/mnt/cache/appdata/PMDA` si tu veux repartir de l’ancienne config.
- **PLEX_DB** : le dossier qui contient `com.plexapp.plugins.library.db` (souvent sous `Plex Media Server/Library/Application Support/...`).
- **DUPES** : ex. `/mnt/user/MURRAY/Music/Music_dupes/Plex_dupes` (ou ton chemin dupes habituel).
- **MUSIQUE** : dossier parent de la librairie (ex. contient matched/unmatched/compilations). Indiquer `/music` comme « Path to parent folder » dans le wizard.

Exemple concret pour ton serveur (192.168.3.2) :

- Conteneur actuel : **PMDA_WEBUI**, un seul montage aujourd’hui : `/mnt/user/appdata/PMDA` → `/config`, port **5005**.
- Pour un test “config neuve”, on utilise un **nouveau** dossier config pour ne pas charger l’ancien `config.json`.

À faire : **remplace** `CHEMIN_BASE_PLEX` et `CHEMIN_DUPES` par tes vrais chemins (le dossier contenant `com.plexapp.plugins.library.db` et le dossier où mettre les dupes).

```bash
# Créer un dossier config "neuve" pour le test
mkdir -p /mnt/user/appdata/PMDA_fresh

# Arrêter / supprimer l’ancien conteneur
docker stop PMDA_WEBUI 2>/dev/null || true
docker rm PMDA_WEBUI 2>/dev/null || true

# Lancer en minimal : 4 montages + port, AUCUNE variable -e
docker run -d --name PMDA_WEBUI \
  --restart unless-stopped \
  -v "/mnt/user/appdata/PMDA_fresh:/config:rw" \
  -v "CHEMIN_BASE_PLEX:/database:ro" \
  -v "CHEMIN_DUPES:/dupes:rw" \
  -v "CHEMIN_MUSIQUE:/music:ro" \
  -p 5005:5005 \
  meaning/pmda:beta
```

Exemple si ta base Plex, tes dupes et ta musique sont par ex. :
- Base Plex : `/mnt/cache/appdata/PlexMediaServer/Library/Application Support/Plex Media Server`
- Dupes : `/mnt/user/Music/Music_dupes/Plex_dupes`
- Musique (parent des dossiers matched/unmatched/compilations) : `/mnt/user/Music`

alors remplace dans la commande :
- `CHEMIN_BASE_PLEX` par le chemin de la base Plex (guillemets si espaces),
- `CHEMIN_DUPES` par le dossier dupes,
- `CHEMIN_MUSIQUE` par le dossier parent de ta librairie musique.

---

## 5. Instructions pour tester (install from scratch)

1. **Ouvrir l’UI**  
   `http://192.168.3.2:5005`

2. **Paramètres (wizard)**  
   Aller dans **Settings**.

3. **Token Plex**  
   Saisir ton token Plex (support : [Finding your Plex token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-number/)).

4. **Serveur Plex**  
   - **Fetch my servers** : récupère la liste des serveurs de ton compte et remplit l’URL ; choisir un serveur dans la liste.  
   - Ou **Discover on network** : détecte les serveurs sur le LAN (sans token) ; choisir un serveur, l’URL se remplit.

5. **Enregistrer**  
   Cliquer sur **Save** dans les paramètres.

6. **Base Plex**  
   Si le message indique que la base n’est pas trouvée, vérifier que le dossier contenant `com.plexapp.plugins.library.db` est bien monté sur `/database` (voir la commande `docker run` ci‑dessus), puis redémarrer le conteneur et sauvegarder à nouveau.

7. **Bibliothèques (sections)**  
   Utiliser **Autodetect** pour les Section IDs (bibliothèques musique) si proposé, puis **Save**.

8. **Scan**  
   Lancer un **New Scan** et vérifier que les doublons s’affichent.

---

## 6. Vérifications rapides

- Pas de crash au démarrage (conteneur reste “Up”) même sans PLEX_HOST/PLEX_TOKEN au lancement.
- Page d’accueil et Settings accessibles.
- Après config (token + serveur + save) : scan possible, liste de doublons cohérente.

Si tu reviens à ton ancienne config plus tard, relance simplement en montant l’ancien dossier config à la place de `PMDA_fresh`.
