# Unraid bloqué sur « Mounting disks… »

**Symptôme :** Après un Start array, l’interface reste sur « Array Starting - Mounting disks… » indéfiniment.

---

## Cause identifiée (serveur MURRAY, 2026-01-25)

- **Disque en cause :** **disk2** → `/dev/md2p1` (XFS).  
  Modèle : WDC WD140EDFZ-11A0VA0, serial 9MGH0SWK (`rdevName.2=sdx`).

- **Comportement :**
  - Au montage de disk2, le noyau affiche : `XFS (md2p1): SB validate failed with error -5`.
  - `xfs_repair -n /dev/md2p1` échoue avec : **« superblock read failed … Input/output error »**.
  - La lecture du superblock XFS renvoie des erreurs I/O au niveau bloc → le disque (ou le chemin vers lui) est en défaut, pas seulement une corruption logique.

- **Conséquence :** emhttp monte les disques array dans l’ordre ; dès qu’il tente de monter disk2, le montage échoue (et peut crasher le processus `mount`). La séquence s’arrête, **fsState** reste en « Starting », **fsProgress** en « Mounting disks… », et `/mnt/user` n’est jamais créé.

- **État observé :**
  - **mdState=STARTED** (array RAID démarré).
  - Seul **/mnt/cache** (pool Cache, nvme) est monté (éventuellement après montage manuel).
  - **/mnt/disk1**, **/mnt/disk2**, etc. sont des répertoires vides (pas de montage XFS).
  - **/mnt/user** n’existe pas (shfs ne démarre qu’une fois tous les disques array montés).

---

## Que faire

### 1. Arrêter l’array pour débloquer l’interface

Tant que emhttp est bloqué sur « Mounting disks… », le bouton **Stop** de l’interface peut ne pas aboutir (emhttp ne traite pas la requête).

- **Option A – Redémarrage propre (recommandé)**  
  - Faire un **Reboot** ou **Shutdown** depuis l’interface Unraid (ou en CLI : `reboot`).  
  - Après le redémarrage, **ne pas** lancer le Start array tout de suite.

- **Option B – Arrêt array sans reboot**  
  - Si vous avez accès à l’interface (même avec la barre « Mounting disks… »), essayez quand même **Array Operation → Stop**.  
  - Si ça ne répond pas, passer par un reboot (option A).

### 2. Désactiver disk2 pour pouvoir redémarrer l’array

Une fois l’array **stopped** ( après reboot ou stop réussi) :

1. Aller dans **Main → Array Operation** (ou **Array Devices**).
2. Identifier **disk2** (WDC WD140EDFZ, 9MGH0SWK).
3. Désactiver ce disque :
   - Soit via l’interface (bouton/option pour marquer le disque comme « disabled » / « missing » selon votre version).
   - Soit en **retirant physiquement** le disque : au prochain Start, Unraid le verra comme « missing » et proposera de démarrer avec disque manquant (contenu émulé depuis la parité).
4. Démarrer l’array : **Start**.  
   Unraid ne montera plus le disque physique défaillant ; le slot disk2 sera servi en **émulation** (reconstruit à partir de la parité). Vous récupérez ainsi **/mnt/user** et la fin du blocage « Mounting disks… ».

### 3. Remplacer disk2 à terme

- Une fois l’array démarré avec disk2 en émulé, prévoir le **remplacement** du disque défaillant et une **reconstruction** du slot (procédure standard Unraid : assigner le nouveau disque au slot disk2, puis rebuild).
- Faire un **SMART** sur l’ancien disque (WDC 9MGH0SWK) pour confirmer l’état :
  ```bash
  smartctl -H /dev/sdx
  smartctl -a /dev/sdx
  ```
  (Adapter `sdx` au nom réel du disque disk2 sur votre système.)

### 4. Pool Cache (nvme) et « wrong or no file system »

Si au boot vous aviez aussi une erreur du type **« cache: mount error: wrong or no file system »** pour le pool **Cache** (nvme) :

- Le pool **Cache** (nvme) est en BTRFS ; une fois monté manuellement (`mount /dev/nvme0n1p1 /mnt/cache`), il est utilisable.
- Pour que Unraid le monte automatiquement au prochain démarrage, vérifier que le type de FS du pool Cache est bien **btrfs** dans la config (Main → Pool). Si le type est incorrect, le corriger afin d’éviter le même blocage au prochain boot.

---

## Résumé

| Problème | Action |
|----------|--------|
| Blocage « Mounting disks… » | Reboot ou Stop array si possible. |
| Cause | disk2 (md2p1) en erreur I/O, superblock XFS illisible. |
| Déblocage | Désactiver / retirer disk2, puis Start array (disk2 émulé). |
| Suite | Remplacer le disque et lancer un rebuild du slot disk2. |

Ce document peut être ajouté au wiki ou à la doc projet si besoin.
