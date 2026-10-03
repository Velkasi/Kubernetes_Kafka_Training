# Guide pas à pas — POC Kafka sur Minikube

Ce guide te fait construire le pipeline **à la main**, une brique à la fois. À chaque étape :

- **Objectif** : ce que tu construis et pourquoi.
- **À faire** : les commandes à taper.
- **Ce qui se passe** : ce que Kubernetes ou Kafka fait en coulisse.
- **Vérifier** : comment savoir que ça marche.
- **Pour aller plus loin** : une petite manipulation ou une question pour ancrer la notion.

Ne saute pas les vérifications : c'est là que tu apprends le plus. Le script `scripts/01-start.sh`
fait les étapes 1 à 7 d'un coup — garde-le pour plus tard, quand tu voudras reconstruire le labo
rapidement.

```
 Postgres simu-a ──JDBC source──┐
                                ├──► topic lab.simulation.measures (Avro) ──► JDBC sink ──► Postgres central
 Postgres simu-b ──JDBC source──┘          │
                                     Schema Registry
 Kafka Connect (2 pods)  ← connecteurs déclarés par kafkactl → Ns4Kafka ← droits du groupe GitLab
```

**Deux « namespaces » et un « groupe GitLab », comme au travail :**

| Objet | Nom dans le labo | Rôle | En prod |
|---|---|---|---|
| Namespace **Kubernetes** | `kafka-lab` | regroupe les pods, services, volumes | celui où tourne ton cluster Connect |
| Namespace **Ns4Kafka** | `lab` | possède le préfixe `lab.` (topics, connecteurs, groupes) | le tien, créé par l'équipe Kafka |
| Groupe **GitLab** | `kafka-lab/simulation` | ses membres ont le droit d'agir sur le namespace `lab` | le groupe GitLab de ton équipe |
| Groupe GitLab admin | `kafka-lab/kafka-admins` | administrateurs Ns4Kafka | l'équipe Kafka |

Dans le labo, GitLab est **simulé** par Ns4Kafka (utilisateurs locaux, voir étape 7). L'annexe B
explique comment brancher un **vrai** groupe gitlab.com si tu veux aller jusqu'au bout.

---

## Étape 0 — Préparer ton PC

**Objectif.** Installer les outils et démarrer un cluster Kubernetes local.

**À faire.**

1. Installer :
   - **Minikube** et **kubectl** (sous Windows : `winget install Kubernetes.minikube Kubernetes.kubectl`)
   - **kafkactl 1.22.0** : binaire de ta plateforme sur https://github.com/michelin/kafkactl/releases/tag/v1.22.0
     (sous Windows : `kafkactl-1.22.0-windows-amd64.exe`, renommé en `kafkactl.exe`). Mets-le dans
     ton `PATH` et vérifie : `kafkactl --version`.
     > Pourquoi 1.22.0 et pas 1.20.0 : les commandes `kafkactl connector stop`, `list-offsets` et
     > `reset-offsets` utilisées aux étapes 9 et 13 n'existent qu'à partir de kafkactl 1.22.0
     > (et Ns4Kafka 1.22, déployé à l'étape 7). Vérifie la version installée au travail
     > (`kafkactl --version` affiche aussi la version du serveur Ns4Kafka).
   - **Un shell bash** : sous Windows, Git Bash ou WSL. Toutes les commandes du guide sont en bash.
2. Sous Git Bash uniquement, une fois par terminal :
   ```bash
   export MSYS_NO_PATHCONV=1
   ```
   Sinon Git Bash transforme `/opt/kafka/...` en `C:/Program Files/Git/opt/kafka/...` et les
   `kubectl exec` échouent de façon incompréhensible.
3. Récupérer le labo dans `D:\07_kafka-lab` et démarrer le cluster :
   ```bash
   git clone <url-de-ce-depot> /d/07_kafka-lab   # ou copie le dossier à cet endroit
   cd /d/07_kafka-lab                            # toutes les commandes partent de ce dossier
   git config core.autocrlf false                # garde les fins de ligne LF (voir .gitattributes)
   minikube start --cpus 4 --memory 8192 --disk-size 20g
   ```

**Ce qui se passe.** Minikube crée une machine virtuelle (ou un conteneur Docker) qui fait tourner un
cluster Kubernetes complet à un seul nœud. `kubectl` est configuré automatiquement pour lui parler.

**Vérifier.**
```bash
kubectl get nodes                 # 1 nœud, STATUS = Ready
kubectl get storageclass          # "standard (default)" : c'est lui qui créera les volumes
```

---

## Étape 1 — Namespace Kubernetes et Secret

**Objectif.** Isoler tout le labo dans un namespace Kubernetes, et y stocker les mots de passe Postgres.

**À faire.**
```bash
kubectl apply -f k8s/00-namespace.yml
kubectl config set-context --current --namespace=kafka-lab
```
La 2e commande évite de taper `-n kafka-lab` partout. Le reste du guide suppose que tu l'as faite.

**Ce qui se passe.** Un *namespace Kubernetes* est un espace de noms pour les objets du cluster.
Ne le confonds pas avec le *namespace Ns4Kafka* de l'étape 7 : même mot, deux choses différentes.

**Vérifier.**
```bash
kubectl get secret pg-credentials -o yaml
echo bGFiLXBhc3N3b3Jk | base64 -d       # remplace par la valeur de POSTGRES_PASSWORD affichée
```

**Pour aller plus loin.** Tu viens de voir qu'un Secret est seulement encodé en base64, pas
chiffré : quiconque peut lire le Secret lit le mot de passe. C'est pour ça qu'en prod les mots de
passe des connecteurs passent par `kafkactl connect-cluster vault` (chiffrement AES256).

---

## Étape 2 — Le broker Kafka

**Objectif.** Démarrer un broker Kafka. En prod il existe déjà ; ici tu le crées pour comprendre ce
que les autres briques attendent de lui.

**À faire.** Ouvre d'abord `k8s/01-kafka.yml` et lis-le, puis :
```bash
kubectl apply -f k8s/01-kafka.yml
kubectl get pods -w                       # Ctrl+C quand kafka-0 est 1/1 Running
```

**Ce qui se passe.**
- C'est un **StatefulSet**, pas un Deployment : le pod a un nom stable (`kafka-0`) et son propre
  volume persistant (PVC). Si le pod redémarre, il retrouve ses données.
- Le **Service headless** (`clusterIP: None`) donne au broker le nom DNS `kafka` dans le cluster.
- **KRaft** : Kafka gère lui-même ses métadonnées, sans Zookeeper. Ce broker est à la fois
  *broker* et *controller*.
- `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092` : c'est l'adresse que le broker **annonce**
  aux clients. Un client se connecte d'abord, puis reconnecte sur l'adresse annoncée. Si elle est
  fausse, la connexion initiale marche mais tout le reste échoue — erreur classique.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` : comme en prod, aucun topic n'apparaît « tout seul ».
  Tes topics passeront par kafkactl.
- `enableServiceLinks: false` : sinon Kubernetes injecte une variable `KAFKA_PORT` dans le pod,
  que l'image Kafka prend pour un paramètre de config et le broker plante.

**Vérifier.**
```bash
kubectl logs kafka-0 | grep -i "started"                  # "Kafka Server started"
kubectl get pvc                                           # data-kafka-0 : Bound
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
La liste est vide : c'est normal, aucun topic n'existe encore.

**Pour aller plus loin.** Supprime le pod (`kubectl delete pod kafka-0`) et regarde le StatefulSet
le recréer avec le même nom et le même volume (`kubectl get pods -w`, puis `kubectl get pvc`).

---

## Étape 3 — Schema Registry

**Objectif.** Démarrer le registre qui stocke les schémas Avro.

**À faire.**
```bash
kubectl apply -f k8s/02-schema-registry.yml
kubectl rollout status deploy/schema-registry
```

**Ce qui se passe.** Schema Registry stocke lui-même ses schémas **dans Kafka**, dans un topic
interne `_schemas`. Chaque message Avro ne contient qu'un identifiant de schéma (5 octets) ; le
consommateur va chercher le schéma complet dans le registre. C'est ce qui rend Avro compact, et ce
qui permet de **contrôler** les évolutions du format.

**Vérifier.** Dans un **second terminal** (laisse-le tourner) :
```bash
kubectl port-forward svc/schema-registry 8081:8081
```
Puis dans le premier :
```bash
curl http://localhost:8081/subjects        # []  -> aucun schéma pour l'instant
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Le topic `_schemas` est apparu. Tu peux ensuite fermer ce port-forward.

---

## Étape 4 — Les trois bases Postgres

**Objectif.** Créer deux bases de simulation **différentes** et la base centrale.

**À faire.** Lis d'abord les ConfigMaps en haut de `k8s/03-...` et `k8s/04-...` : compare les deux
tables. Puis :
```bash
kubectl apply -f k8s/03-postgres-simu-a.yml -f k8s/04-postgres-simu-b.yml -f k8s/05-postgres-central.yml
kubectl get pods -w
```

**Ce qui se passe.** Le script SQL du ConfigMap est monté dans `/docker-entrypoint-initdb.d` :
l'image Postgres l'exécute **uniquement au premier démarrage**, quand le volume est vide. Si tu
modifies le ConfigMap après coup, il ne sera pas rejoué tant que le PVC existe.

**Vérifier.**
```bash
kubectl exec -it simu-a-0 -- psql -U lab -d simu -c '\d mesures'
kubectl exec -it simu-b-0 -- psql -U lab -d simu -c '\d sim_results'
kubectl exec -it central-0 -- psql -U lab -d central -c '\d simulation_measures'
```

**Pour aller plus loin.** Note les différences entre A et B : noms (`machine` / `equipment_code`,
`updated_at` / `last_update`), types (`VARCHAR` / `INTEGER`, `NUMERIC` / `REAL`). C'est exactement
le problème que la normalisation devra résoudre : à l'arrivée, une seule structure.

---

## Étape 5 — Le cluster Kafka Connect

**Objectif.** Construire l'image Connect avec le plugin JDBC et la déployer en 2 exemplaires.

**À faire.**
```bash
cat docker/connect/Dockerfile
minikube image build -t connect-lab:1.0 docker/connect     # quelques minutes
minikube image ls | grep connect-lab
kubectl apply -f k8s/06-connect.yml
kubectl get pods -l app=connect -w                         # ~1 min avant 1/1 Ready
```

**Ce qui se passe.**
- `minikube image build` construit l'image **directement dans Minikube**. Le manifest dit
  `imagePullPolicy: Never` : Kubernetes ne cherche pas l'image sur Internet. Si tu oublies le build,
  tu auras `ErrImageNeverPull`.
- Les 2 pods ont le même `CONNECT_GROUP_ID` : ils forment **un seul cluster Connect**. Ils se
  partagent les connecteurs et stockent leur état dans 3 topics Kafka internes (configs, offsets,
  status). C'est ce qui permet à un pod de reprendre le travail d'un autre.
- Connect n'a pas de base de données à lui : **tout son état est dans Kafka**.
- `CONNECT_REST_ADVERTISED_HOST_NAME` = l'IP du pod : chaque worker doit pouvoir joindre l'autre
  (une requête REST reçue par le mauvais pod est transférée au leader).

**Vérifier.** Second terminal : `kubectl port-forward svc/connect 8083:8083`, puis :
```bash
curl -s http://localhost:8083/connector-plugins | tr ',' '\n' | grep -i jdbc
curl -s http://localhost:8083/connectors          # []
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Tu dois voir `JdbcSourceConnector`, `JdbcSinkConnector`, et les topics `lab-connect-*`.

---

## Étape 6 — AKHQ et le générateur de données

**Objectif.** Une interface web pour voir ce qui circule, et des données qui arrivent en continu.

**À faire.**
```bash
kubectl apply -f k8s/07-akhq.yml -f k8s/08-generator.yml
kubectl rollout status deploy/akhq
kubectl port-forward svc/akhq 8090:8080          # dans un terminal dédié, à laisser ouvert
```
Ouvre http://localhost:8090.

**Vérifier.**
```bash
kubectl logs deploy/generator --tail=5           # pas d'erreur
kubectl exec simu-a-0 -- psql -U lab -d simu -tAc "SELECT count(*) FROM mesures"
```
Relance la 2e commande 10 secondes plus tard : le compteur a augmenté. Le générateur insère une
ligne toutes les 2 s dans chaque base, et toutes les 10 itérations il **modifie** une ligne existante
de A (pour vérifier plus tard que les mises à jour remontent aussi).

---

## Étape 7 — Ns4Kafka, groupe GitLab et kafkactl

**Objectif.** Déployer l'API Ns4Kafka, puis reproduire **exactement** la situation du travail :
1. l'**équipe Kafka** (admin) crée ton namespace `lab` et le rattache à ton **groupe GitLab** ;
2. **toi**, membre de ce groupe, tu travailles dans ce namespace avec kafkactl.

À partir d'ici, **tout passe par kafkactl**, exactement comme au travail.

### 7.1 Déployer Ns4Kafka

```bash
kubectl apply -f k8s/09-ns4kafka.yml
kubectl rollout status deploy/ns4kafka
kubectl logs deploy/ns4kafka | grep -i "startup completed"   # pas d'exception avant
kubectl port-forward svc/ns4kafka 8080:8080                  # terminal dédié, à laisser ouvert
```

**Comment GitLab est simulé.** Lis le haut de `k8s/09-ns4kafka.yml`. En prod, kafkactl envoie ton
**token GitLab** à Ns4Kafka, qui demande à GitLab « qui est-ce et de quels groupes est-il
membre ? ». Ici, Ns4Kafka connaît deux tokens en dur, chacun membre d'un groupe GitLab fictif :

| Token kafkactl | Groupe GitLab | Qui |
|---|---|---|
| `kafka-admin-token` | `kafka-lab/kafka-admins` | l'équipe Kafka (`admin-group` de Ns4Kafka) |
| `simulation-dev-token` | `kafka-lab/simulation` | toi et ton équipe projet |

Pour Ns4Kafka, le résultat est identique : un utilisateur + une liste de groupes. Les RoleBindings
fonctionnent donc exactement comme en prod.

### 7.2 Côté équipe Kafka : créer le namespace

Dans ton terminal principal :
```bash
export KAFKACTL_CONFIG="$PWD/kafkactl/config.yml"
cd kafkactl                                       # kafkactl se lance depuis ce dossier
cat config.yml                                    # 2 contextes : lab-admin et lab
kafkactl config use-context lab-admin
kafkactl auth renew                               # se connecte avec le token admin
kafkactl auth info                                # "Admin gitlab authenticated."
kafkactl apply -f admin/namespace-lab.yml
kafkactl get namespace
```

**Ce qui se passe.** Lis `admin/namespace-lab.yml`, c'est le plus important de l'étape :
- **Namespace** : rattaché au cluster Kafka `local`, autorisé à utiliser le cluster Connect
  `connect-lab`, avec des **validateurs** qui imposent des règles à tes topics et connecteurs.
- **RoleBinding** : **qui** a le droit d'agir sur ce namespace → le groupe GitLab
  `kafka-lab/simulation`, avec la liste des ressources et des verbes autorisés.
- **AccessControlEntry OWNER** : le namespace **possède** tout ce qui commence par `lab.` (topics,
  connecteurs, consumer groups) et `connect-lab.` (consumer groups des sinks). Tout topic ou
  connecteur que tu créeras devra commencer par ce préfixe.

### 7.3 Côté toi : travailler dans le namespace

```bash
kafkactl config use-context lab                   # change de token = change d'utilisateur
kafkactl auth renew
kafkactl auth info                                # tes droits : namespace lab, verbes, ressources
kafkactl get all                                  # tes ACL : préfixes lab. et connect-lab.
kafkactl get acl
```
En prod, c'est par là que tu commences : `auth info` et `get acl` te montrent ton namespace, ton
préfixe et tes droits.

> `kafkactl config use-context` réécrit les 3 dernières lignes de `config.yml` : c'est normal.

**Vérifier — en provoquant des refus.**
```bash
# 1) Un topic hors de ton préfixe
cat > /tmp/test-topic.yml <<'EOF'
apiVersion: v1
kind: Topic
metadata: { name: autre.test }
spec: { replicationFactor: 1, partitions: 1, configs: { min.insync.replicas: '1', cleanup.policy: delete } }
EOF
kafkactl apply -f /tmp/test-topic.yml --dry-run      # refusé : namespace is not owner of the resource

# 2) Dans ton préfixe, mais 10 partitions
sed -e 's/autre.test/lab.test/' -e 's/partitions: 1/partitions: 10/' /tmp/test-topic.yml > /tmp/t2.yml
kafkactl apply -f /tmp/t2.yml --dry-run              # refusé : value must be no more than 6

# 3) Une ressource d'admin avec ton compte
kafkactl apply -f admin/namespace-lab.yml --dry-run  # refusé : Namespace, RoleBinding inconnus pour toi
```
Lis bien les messages d'erreur : ce sont les mêmes que tu verras en prod.

**Pour aller plus loin.** Dans `admin/namespace-lab.yml`, remplace `kafka-lab/simulation` par
`kafka-lab/autre-equipe` dans le premier RoleBinding, réapplique-le en `lab-admin`, puis repasse en
`lab` : `kafkactl get topic` répond `The server does not have resource type topic`. Ton token n'a
pas changé, mais ton groupe GitLab n'a plus aucun droit : kafkactl ne te propose que les ressources
que tes RoleBindings autorisent. Remets la bonne valeur et réapplique.

---

## Étape 8 — Le topic

**Objectif.** Créer le topic normalisé via kafkactl.

**À faire.**
```bash
cat resources/01-topic.yml
kafkactl apply -f resources/01-topic.yml --dry-run
kafkactl apply -f resources/01-topic.yml
kafkactl get topic
```

**Ce qui se passe.** kafkactl envoie le YAML à Ns4Kafka. Ns4Kafka vérifie le préfixe et les
validateurs, l'enregistre, puis le crée réellement sur le broker (sa boucle de synchronisation
tourne toutes les quelques secondes).

**Vérifier.** Dans AKHQ, onglet *Topics* : `lab.simulation.measures`, 3 partitions, 0 message.

**Pour aller plus loin.** Passe `partitions` à 2 dans le fichier puis lance `kafkactl diff -f
resources/01-topic.yml` et `kafkactl apply`. Ns4Kafka refuse (`value is immutable`) : le nombre de
partitions ne se change pas. Remets 3. En prod, choisis-le bien dès le départ.

---

## Étape 9 — Première source : base A

**Objectif.** Faire remonter la base A dans le topic, en Avro.

**À faire.** Lis `resources/03-src-simu-a.yml` en entier, puis :
```bash
kafkactl apply -f resources/03-src-simu-a.yml --dry-run
kafkactl apply -f resources/03-src-simu-a.yml
kafkactl get connector                                     # attendre RUNNING
```

**Ce qui se passe — lis cette partie lentement.**
- **`mode: timestamp+incrementing`** : à chaque interrogation (`poll.interval.ms`, 5 s), le
  connecteur ne lit que les lignes dont `updated_at` a avancé, ou à `updated_at` égal, dont
  `measure_id` est plus grand. Il retient la dernière position dans ses **offsets** (stockés dans
  Kafka). C'est pourquoi chaque table source doit avoir une colonne date de mise à jour fiable et un
  identifiant croissant.
- **La requête est enveloppée** dans `SELECT * FROM (...) t` : le connecteur y ajoute lui-même son
  `WHERE updated_at > ? ...`. Sans l'enveloppe, son WHERE porterait sur les noms de colonnes
  d'origine et la requête échouerait.
- **Chaque colonne est castée** : la normalisation (noms et types communs, `source_id` littéral) se
  fait ici. Le `CAST` systématique a un second rôle, expliqué à l'étape 12.
- **Les transformations (SMT)** :
  - `createKey` + `extractKey` : la clé du message devient `source_id`. Kafka envoie tous les
    messages d'une même clé dans la même partition → l'ordre est garanti par source.
  - `setName` : donne un nom stable au record Avro (`com.lab.simulation.SimulationMeasure`).
- **`auto.register.schemas: 'true'`** pour l'instant : le convertisseur Avro enregistre lui-même le
  schéma qu'il génère. On le verrouillera à l'étape 11.
- Ns4Kafka a **validé** le connecteur avant de l'envoyer à Connect : préfixe `lab.`, cluster
  `connect-lab` autorisé, classe JDBC autorisée, convertisseurs présents (validateur du namespace).

**Vérifier.**
```bash
kafkactl connector list-offsets lab.src-simu-a      # position lue dans la base A
kafkactl get schema                                 # lab.simulation.measures-value est apparu
```
Dans AKHQ, ouvre le topic → *Data* : les messages sont décodés, clé `SIMU_A`, valeur avec tes
8 champs normalisés.

---

## Étape 10 — Le sink vers la base centrale

**Objectif.** Déverser le topic dans `simulation_measures`.

**À faire.**
```bash
cat resources/04-sink-central.yml
kafkactl apply -f resources/04-sink-central.yml
kafkactl get connector
```
Puis :
```bash
../scripts/status.sh     # pods, connecteurs, volumes source / centrale
```

**Ce qui se passe.**
- Le sink est un **consumer** : il fait partie d'un *consumer group* nommé `connect-lab.sink-central`
  et commit sa position dans Kafka. Ce nom commence par `connect-lab.` : c'est pour ça que
  l'équipe Kafka a donné à ton namespace l'ACL GROUP `connect-lab.` (étape 7).
- `tasks.max: 3` = 3 partitions : chaque tâche lit une partition en parallèle.
- **`insert.mode: upsert`** sur la clé `(source_id, measure_id)` : si un message est relu (après un
  crash par exemple), la ligne est mise à jour au lieu d'être dupliquée. C'est ce qui rend le
  pipeline **idempotent**.

**Vérifier.**
```bash
kafkactl group list                    # le groupe du sink, avec son lag
kubectl exec central-0 -- psql -U lab -d central -c \
  "SELECT source_id, count(*), max(measured_at) FROM simulation_measures GROUP BY 1"
```
Relance la requête : le compteur SIMU_A suit celui de la base A.

**Pour aller plus loin.** Dans AKHQ, onglet *Consumer Groups* : regarde le *lag* par partition. Un
lag qui reste proche de 0 = le sink suit le rythme.

---

## Étape 11 — Figer le contrat Avro

**Objectif.** Transformer le schéma généré par la source A en **contrat** que toutes les sources
devront respecter.

**Pourquoi ce n'est pas fait au début.** Le connecteur JDBC génère son schéma à partir des métadonnées
SQL (types, colonnes nullables…). Un `.avsc` écrit à la main ne correspond presque jamais à
l'octet près à ce qu'il produit, et le connecteur échouerait. On laisse donc la première source
enregistrer le schéma, on le vérifie, puis on le fige.

**À faire.**
1. Regarder le schéma :
   ```bash
   kafkactl get schema lab.simulation.measures-value -o yaml
   ```
   Vérifie les champs, leurs types, et le nom `com.lab.simulation.SimulationMeasure`.
2. Copier la valeur de `spec.schema` (le JSON) dans `schemas/simulation-measure.avsc`. Plus simple,
   via le registre (port-forward de l'étape 3 relancé) :
   ```bash
   curl -s http://localhost:8081/subjects/lab.simulation.measures-value/versions/latest/schema \
     > schemas/simulation-measure.avsc
   ```
3. Déclarer ce schéma comme ressource gérée, et fixer la règle d'évolution :
   ```bash
   kafkactl apply -f resources/02-schema.yml          # "unchanged" : pas de nouvelle version
   kafkactl subject-config update lab.simulation.measures-value --compatibility backward
   kafkactl get schema                                # CONFIG = BACKWARD
   ```
4. Interdire aux sources d'enregistrer un autre schéma. Dans `resources/03-src-simu-a.yml` **et**
   `resources/05-src-simu-b.yml`, change :
   ```yaml
   value.converter.auto.register.schemas: 'false'
   value.converter.use.latest.version: 'true'
   ```
   Puis :
   ```bash
   kafkactl diff -f resources/03-src-simu-a.yml       # ne montre que ces 2 lignes
   kafkactl apply -f resources/03-src-simu-a.yml
   ```

**Ce qui se passe.** Désormais, chaque source sérialise avec la dernière version enregistrée. Si
ce qu'elle produit ne correspond pas, elle **échoue** au lieu de créer silencieusement une version
2 du schéma. `BACKWARD` signifie : une nouvelle version doit pouvoir lire les anciennes données,
donc tout champ ajouté doit avoir une valeur par défaut.

**Vérifier.** `kafkactl get connector` : `lab.src-simu-a` toujours RUNNING, et les données
continuent d'arriver dans la base centrale.

Ce fichier `schemas/simulation-measure.avsc` est celui que tu reprendras en prod.

---

## Étape 12 — Deuxième source : base B

**Objectif.** Le vrai test de la normalisation : une base structurée autrement doit produire
**exactement** le même schéma.

**À faire.**
```bash
grep -A14 "query:" resources/05-src-simu-b.yml     # compare avec la requête de A
kafkactl apply -f resources/05-src-simu-b.yml
kafkactl get connector
```

**Vérifier.**
```bash
curl -s http://localhost:8081/subjects/lab.simulation.measures-value/versions    # toujours [1]
kubectl exec central-0 -- psql -U lab -d central -c \
  "SELECT source_id, count(*) FROM simulation_measures GROUP BY 1"
```
Une seule version de schéma, et SIMU_B arrive dans la même table : **la normalisation est validée**.

**Ce qui se passe — le rôle du `CAST` systématique.** Une colonne `NOT NULL` lue telle quelle donne un
champ Avro obligatoire ; une expression calculée donne un champ optionnel (`["null", ...]`). Si A
lisait `machine` tel quel et B calculait `'EQ-' || equipment_code`, les deux schémas différeraient
et B serait rejetée. En castant **toutes** les colonnes partout, les métadonnées sont identiques
d'une base à l'autre, donc le schéma aussi.

> **Subtilité (vérifiée avec le driver PostgreSQL).** Un `CAST` vers **le même type** est supprimé
> par Postgres : `CAST(updated_at AS TIMESTAMP)` sur une colonne déjà `TIMESTAMP NOT NULL` reste
> NOT NULL, donc champ Avro **obligatoire**. Ici c'est sans conséquence : `measured_at` et
> `updated_at` sont `TIMESTAMP NOT NULL` dans A **et** dans B, donc obligatoires des deux côtés.
> En prod, si une source a une colonne nullable et l'autre non (ou un type différent), le schéma
> divergera : compare toujours le schéma de la 2e source avant de figer le contrat
> (`kafkactl get schema ... -o yaml`).

**Pour aller plus loin — casse-le exprès.** Dans `05-src-simu-b.yml`, remplace
`CAST('EQ-' || equipment_code AS VARCHAR(50)) AS equipment` par `equipment_code AS equipment`
(un `INTEGER` au lieu d'un texte), puis `kafkactl apply`. Attends 10 s :
```bash
kafkactl get connector
curl -s http://localhost:8083/connectors/lab.src-simu-b/status | tr ',' '\n' | grep -i -m3 "trace\|state"
```
La tâche est en FAILED avec une erreur de schéma incompatible : le contrat a joué son rôle. Remets
le `CAST`, réapplique, puis `kafkactl connector restart lab.src-simu-b`.

> Pourquoi pas simplement retirer le `CAST` de `measured_value` (`REAL`) ? Parce qu'un `float` Avro
> est lisible comme `double` : Schema Registry le juge **compatible** et rien ne casse. Un entier à
> la place d'un texte, lui, ne l'est pas.

---

## Étape 13 — Tests de robustesse

Ces quatre tests sont ceux qui convaincront ton équipe. Fais-les à la main d'abord ;
`scripts/tests.sh` les automatise ensuite.

### 13.1 Un pod Connect meurt

```bash
curl -s http://localhost:8083/connectors?expand=status | tr ',' '\n' | grep worker_id
kubectl delete pod <un-des-pods-connect>
kubectl get pods -l app=connect -w
```
Relance la commande `curl` : les tâches du pod supprimé ont été **réassignées** à l'autre pod
(`worker_id` a changé), puis rééquilibrées quand le nouveau pod est prêt. Vérifie les compteurs :
pas de trou. Le port-forward vers `svc/connect` peut se couper si son pod meurt : relance-le.

### 13.2 La base centrale tombe

```bash
kubectl scale statefulset/central --replicas=0
kafkactl group list            # le lag du sink augmente : les messages s'accumulent dans Kafka
# attendre 1 à 2 minutes
kubectl scale statefulset/central --replicas=1
kafkactl get connector         # si le sink est FAILED : kafkactl connector restart lab.sink-central
kafkactl group list            # le lag redescend à ~0
```
C'est l'intérêt de Kafka : la cible peut tomber sans perte, le topic sert de tampon.

### 13.3 Rejouer tout l'historique

```bash
kafkactl connector stop lab.sink-central
kubectl exec central-0 -- psql -U lab -d central -c "TRUNCATE simulation_measures"
kafkactl connector reset-offsets lab.sink-central
kafkactl connector resume lab.sink-central
```
La table se reconstruit à partir du topic, dans la limite de la rétention (7 jours ici).
`reset-offsets` exige un connecteur à l'état **STOPPED** (pas seulement PAUSED).

### 13.4 Pas de doublon, pas de perte, mises à jour propagées

```bash
../scripts/tests.sh check
```
0 doublon, autant de lignes dans la centrale que dans chaque source, et les lignes de A modifiées
par le générateur ont la même valeur dans la centrale.

---

## Étape 14 — Bilan et passage en prod

**Ce que tu as validé :** normalisation multi-sources, contrat Avro, haute dispo de Connect,
résilience côté cible, idempotence, rejeu — le tout avec les droits d'un groupe GitLab sur un
namespace Ns4Kafka, comme au travail.

**Ce qui change en prod :**

| Labo | Prod |
|---|---|
| Tu crées le namespace (contexte `lab-admin`) | Il existe : `kafkactl auth info`, `kafkactl get acl` |
| Groupe GitLab simulé `kafka-lab/simulation` | Le vrai groupe GitLab de ton équipe (RoleBinding fait par l'équipe Kafka) |
| Token `simulation-dev-token` | Ton Personal Access Token GitLab (dans ta config kafkactl, jamais dans Git) |
| Préfixe `lab.` | Le préfixe dont ton namespace est OWNER |
| Broker, Registry en clair | SASL/TLS : ajouter `producer.override.sasl.jaas.config` (source) et `consumer.override.sasl.jaas.config` (sink) |
| `replicationFactor: 1` | 3 en général (le validateur te le dira) |
| Mot de passe en clair | `kafkactl connect-cluster vault <cluster> '<mdp>'` → `${aes256:...}` |
| `connect-lab` (Minikube) | Un cluster Connect de ton namespace, ou le tien déployé sur K8s + ressource `ConnectCluster` |
| Namespace Kubernetes `kafka-lab` | Le namespace K8s de ton équipe (si vous déployez votre propre Connect) |
| Contexte kafkactl `lab` | Contexte `dev` puis `prod`, token GitLab |
| `schemas/simulation-measure.avsc` | Le même fichier, renommé selon ton préfixe |

**Checklist avant la prod :**
- [ ] Ton compte est bien membre du groupe GitLab lié à ton namespace (`kafkactl auth info`)
- [ ] Version de kafkactl compatible avec le Ns4Kafka de l'entreprise (`kafkactl --version`)
- [ ] Flux réseau ouverts entre Connect et chaque base source / la base centrale
- [ ] Comptes en lecture seule sur les bases sources
- [ ] Colonne date de mise à jour + identifiant croissant confirmés dans chaque table source
- [ ] Nullabilité et types identiques entre sources (voir la subtilité de l'étape 12)
- [ ] Quota de connecteurs du namespace suffisant (`kafkactl get quota` si exposé)
- [ ] Classe JDBC autorisée par le validateur du namespace

---

## Annexe A — Dépannage

| Symptôme | Cause probable | Que faire |
|---|---|---|
| `ErrImageNeverPull` sur connect | Image non construite dans Minikube | `minikube image build -t connect-lab:1.0 docker/connect` |
| schema-registry en `CrashLoopBackOff` | Kafka pas encore prêt | Attendre, ou `kubectl rollout restart deploy/schema-registry` |
| `kubectl exec ... /opt/...` : fichier introuvable (Windows) | Conversion de chemin Git Bash | `export MSYS_NO_PATHCONV=1` |
| `$'\r': command not found` dans un script ou le générateur | Fichiers convertis en CRLF par Git | `git config core.autocrlf false`, puis `git rm --cached -r . && git reset --hard` |
| kafkactl : `Connection refused` | Port-forward ns4kafka coupé | Relancer `kubectl port-forward svc/ns4kafka 8080:8080` |
| kafkactl : `No JWT found` / `401` | Pas encore authentifié, ou token inconnu | `kafkactl auth renew` ; vérifier `user-token` dans `config.yml` |
| kafkactl : `The server does not have resource type ...` | Ton groupe GitLab n'a pas de RoleBinding donnant cette ressource | `kafkactl auth info` ; bon contexte (`lab` / `lab-admin`) ? |
| kafkactl : `Unmatched argument ... stop` / `list-offsets` | kafkactl < 1.22.0 | Installer kafkactl 1.22.0 |
| kafkactl : `schemaFile` introuvable | kafkactl lancé hors du dossier `kafkactl/` | `cd kafkactl` |
| Connecteur refusé : `failed to find any class ... JdbcSourceConnector` | Plugin JDBC absent de l'image Connect | Reconstruire l'image (étape 5), vérifier `/connector-plugins` |
| Connecteur `FAILED` | Voir la trace | `curl -s localhost:8083/connectors/<nom>/status` |
| Pods en `Pending` | Mémoire Minikube insuffisante | `kubectl describe pod <pod>` ; relancer Minikube avec plus de RAM |
| Build de l'image en échec | Proxy d'entreprise | `minikube start --docker-env HTTPS_PROXY=...` |

**Commandes utiles.**
```bash
kubectl get pods                      kubectl describe pod <pod>      kubectl logs -f <pod>
kubectl get events --sort-by=.lastTimestamp
kafkactl get all                      kafkactl get connector          kafkactl diff -f <fichier>
kafkactl connector restart|pause|resume|stop <nom>
kafkactl delete connector <nom>       kafkactl delete topic <nom>
```

**Tout recommencer.** `scripts/99-clean.sh` supprime le namespace Kubernetes et ses volumes.
`minikube delete` efface complètement le cluster.

---

## Annexe B — Brancher un vrai groupe GitLab (optionnel)

Pour aller jusqu'au bout de « identique à la prod » : Ns4Kafka interroge **gitlab.com** avec ton
token, et c'est ton **vrai** groupe GitLab qui reçoit les droits.

1. **Sur gitlab.com** (compte gratuit) :
   - crée un groupe, par exemple `mon-pseudo-kafka-lab`, et dedans un sous-groupe `simulation`.
     Le chemin complet `mon-pseudo-kafka-lab/simulation` est celui qui apparaît dans l'URL ;
   - crée un **Personal Access Token** (*Preferences → Access Tokens*) avec le scope **`read_api`**
     (Ns4Kafka appelle `/api/v4/user` et `/api/v4/groups`). Ne le committe jamais.
2. **Activer GitLab dans Ns4Kafka** : dans `k8s/09-ns4kafka.yml`, passe
   `MICRONAUT_SECURITY_GITLAB_ENABLED` à `"true"`, puis :
   ```bash
   kubectl apply -f k8s/09-ns4kafka.yml
   kubectl rollout status deploy/ns4kafka          # puis relance le port-forward 8080
   ```
   Les utilisateurs locaux restent actifs : le contexte `lab-admin` fonctionne toujours.
   Minikube doit pouvoir joindre `https://gitlab.com` (proxy d'entreprise : voir annexe A).
3. **Côté équipe Kafka** : lie ton vrai groupe au namespace `lab`. Dans
   `admin/rolebinding-gitlab.yml`, remplace `MON-GROUPE/simulation` par ton chemin, puis :
   ```bash
   kafkactl config use-context lab-admin
   kafkactl apply -f admin/rolebinding-gitlab.yml
   ```
4. **Côté toi** : une config kafkactl **personnelle**, ignorée par Git (`.gitignore`) :
   ```bash
   cp config.yml config.local.yml
   # dans config.local.yml, ajoute un contexte :
   #   - name: "lab-gitlab"
   #     context:
   #       api: "http://localhost:8080"
   #       user-token: "<ton token gitlab.com>"
   #       namespace: "lab"
   export KAFKACTL_CONFIG="$PWD/config.local.yml"
   kafkactl config use-context lab-gitlab
   kafkactl auth renew
   kafkactl auth info          # ton vrai pseudo GitLab, et les droits de ton vrai groupe
   kafkactl get topic
   ```
5. **Vérifier que c'est bien le groupe qui compte** : sur gitlab.com, quitte le sous-groupe
   `simulation` (ou crée un second compte non membre), puis `kafkactl auth renew` et
   `kafkactl get topic` → refusé (`The server does not have resource type topic`). C'est exactement ce qui se passera au travail si tu changes
   d'équipe.

Tu peux aussi faire de `admin-group` (dans `k8s/09-ns4kafka.yml`) un vrai groupe gitlab.com : ses
membres deviennent administrateurs Ns4Kafka.
