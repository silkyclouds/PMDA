# Plan : Frontend React + séparation backend + test local Unraid

## Vue d’ensemble
Intégrer l’UI React (pmda-dashboard) sous `frontend/`, retirer l’UI inline du backend, et documenter le workflow de test sur le serveur Unraid (build beta, déploiement WebUI, accès Plex/config).

---

## 1. Intégration frontend React

- Cloner **https://github.com/silkyclouds/pmda-dashboard.git** dans **PMDA/frontend**.
- Mettre à jour **.cursorrules** pour refléter les deux repos : PMDA principal et `silkyclouds/pmda-dashboard`. Pour toute modif UI/UX, faire les changements dans `frontend/`, commit et push vers pmda-dashboard pour que Lovable reste à jour.

## 2. Nettoyage backend (pmda.py)

- Retirer toute l’UI inline : la constante `HTML` (l.3018–3833), la route `GET /`, et tout usage de `render_template_string`.
- Convertir `/api/edition_details` pour qu’il retourne du JSON structuré (ex. `tracks` + `info`) au lieu de HTML.
- Activer CORS si nécessaire pour que le frontend (dev ou build servi) puisse appeler l’API.

## 3. Test local sur Unraid (192.168.3.2)

- **Build beta** : construire l’image multi-plateforme et pousser vers Docker Hub :
  - `docker buildx build --platform linux/amd64,linux/arm64 -t meaning/pmda:beta --push .`
- **Déploiement WebUI** : sur le serveur (SSH `root@192.168.3.2`), lancer ou redémarrer le conteneur **PMDA_WEBUI** pour utiliser `meaning/pmda:beta`. La WebUI est accessible sur **http://192.168.3.2:5005**.
- **Accès Plex et config** : sur ce serveur, les données Plex (token, URL, section IDs) et toute la config PMDA sont accessibles en lecture (variables d’environnement du conteneur, fichiers dans `/mnt/cache/appdata/PMDA`, etc.) pour les tests, la doc ou le diagnostic. L’assistant peut s’appuyer sur ces infos lors des validations ou du débogage.

---

## Référence .cursorrules

Le fichier **.cursorrules** (local, non versionné) inclut désormais :
- Le **workflow de test** : build beta → push → lancer/redémarrer la WebUI sur 192.168.3.2 (conteneur PMDA_WEBUI).
- La précision que l’assistant **a accès** aux secrets/config Plex et à la config PMDA sur le serveur pour les tests.
