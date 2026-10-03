# Labo Minikube — pipeline Kafka simulation → base centrale (Avro)

Labo d'apprentissage qui reproduit l'architecture cible sur ton PC, **avec kafkactl, Ns4Kafka et
des groupes GitLab** comme en prod : un namespace Kubernetes `kafka-lab`, un namespace Ns4Kafka
`lab`, et un groupe GitLab `kafka-lab/simulation` qui a les droits sur ce namespace.

👉 **Commence par [GUIDE.md](GUIDE.md)** : il te fait tout construire à la main, étape par étape,
avec ce qui se passe en coulisse et comment vérifier chaque brique.

## Contenu

```
k8s/                    manifests Kubernetes, un fichier par brique, numérotés dans l'ordre
  00 namespace + secret     01 Kafka (KRaft)        02 Schema Registry
  03-05 Postgres simu-a, simu-b, central            06 Kafka Connect (2 pods)
  07 AKHQ (interface web)   08 générateur de données 09 Ns4Kafka (+ groupes GitLab simulés)
docker/connect/         image Kafka Connect + plugin JDBC
kafkactl/
  config.yml            contextes kafkactl "lab-admin" (équipe Kafka) et "lab" (toi)
  admin/                ressources admin (namespace, rolebinding GitLab, ACL) — en prod, déjà faites pour toi
  resources/            topic, schéma, connecteurs : ce que tu appliqueras aussi en prod
  schemas/              le contrat Avro, rempli à l'étape 11 du guide
scripts/
  01-start.sh           déploie toute l'infra d'un coup (une fois le guide fait une première fois)
  status.sh             pods, connecteurs, volumes source / centrale
  tests.sh              tests de robustesse automatisés
  99-clean.sh           supprime le labo
```

## Identités du labo

| Contexte kafkactl | Token | Groupe GitLab (simulé) | Rôle |
|---|---|---|---|
| `lab-admin` | `kafka-admin-token` | `kafka-lab/kafka-admins` | équipe Kafka : crée namespaces, RoleBindings, ACL |
| `lab` | `simulation-dev-token` | `kafka-lab/simulation` | toi : topics, schémas, connecteurs du namespace `lab` |

GitLab est simulé par Ns4Kafka (utilisateurs locaux). Pour utiliser un vrai groupe gitlab.com,
voir l'annexe B du guide.

## Versions

| Brique | Image / version |
|---|---|
| Kafka (KRaft) | `apache/kafka:3.9.1` |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.10` |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.9.10` + `kafka-connect-jdbc 10.8.4` |
| Ns4Kafka | `michelin/ns4kafka:1.22.5` |
| kafkactl | **1.22.0** (nécessaire pour `connector stop / list-offsets / reset-offsets`) |
| AKHQ | `tchiotludo/akhq:0.26.0` |
| PostgreSQL | `postgres:16-alpine` |

## Ressources nécessaires

Minikube avec 4 CPU et 8 Go de RAM, kubectl, kafkactl 1.22.0, un shell bash (Git Bash ou WSL sous
Windows). Accès Internet pour Docker Hub et Confluent Hub.

Sous Windows, clone le dépôt dans `D:\07_kafka-lab` avec `core.autocrlf=false` : les scripts et
ConfigMaps doivent garder des fins de ligne LF (voir `.gitattributes`).
