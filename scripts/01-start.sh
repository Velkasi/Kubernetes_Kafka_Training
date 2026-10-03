#!/usr/bin/env bash
# Deploie tout le labo d'un coup (etapes 1 a 7 du guide).
# A utiliser APRES avoir fait le guide une premiere fois a la main.
#   ./scripts/01-start.sh              # build de l'image Connect dans Minikube inclus
#   SKIP_BUILD=1 ./scripts/01-start.sh # image connect-lab:1.0 deja presente
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash : ne pas convertir /opt/... en C:/Program Files/Git/opt/...

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=kafka-lab
cd "$ROOT"

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

step "1. Namespace Kubernetes + Secret"
kubectl apply -f k8s/00-namespace.yml
kubectl config set-context --current --namespace="$NS" >/dev/null

step "2. Broker Kafka"
kubectl apply -f k8s/01-kafka.yml
kubectl -n "$NS" rollout status statefulset/kafka --timeout=300s

step "3. Schema Registry"
kubectl apply -f k8s/02-schema-registry.yml

step "4. Bases Postgres"
kubectl apply -f k8s/03-postgres-simu-a.yml -f k8s/04-postgres-simu-b.yml -f k8s/05-postgres-central.yml

step "5. Kafka Connect"
if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  minikube image build -t connect-lab:1.0 docker/connect
fi
kubectl apply -f k8s/06-connect.yml

step "6. AKHQ + generateur"
kubectl apply -f k8s/07-akhq.yml -f k8s/08-generator.yml

step "7. Ns4Kafka"
kubectl apply -f k8s/09-ns4kafka.yml

step "Attente que tout soit pret"
for s in statefulset/simu-a statefulset/simu-b statefulset/central deployment/schema-registry \
         deployment/connect deployment/akhq deployment/generator deployment/ns4kafka; do
  kubectl -n "$NS" rollout status "$s" --timeout=420s
done

step "7b. Namespace Ns4Kafka 'lab' (contexte admin, comme l'equipe Kafka en prod)"
if command -v kafkactl >/dev/null 2>&1; then
  kubectl -n "$NS" port-forward svc/ns4kafka 8080:8080 >/dev/null 2>&1 &
  PF=$!
  trap 'kill $PF 2>/dev/null || true' EXIT
  sleep 4
  export KAFKACTL_CONFIG="$ROOT/kafkactl/config.yml"
  cd "$ROOT/kafkactl"
  kafkactl config use-context lab-admin
  # Ns4Kafka peut mettre quelques secondes a accepter les requetes apres son demarrage.
  for i in 1 2 3 4 5 6; do
    kafkactl apply -f admin/namespace-lab.yml && break
    echo "Ns4Kafka pas encore pret, nouvel essai dans 10 s..."; sleep 10
  done
  kafkactl config use-context lab
else
  echo "kafkactl introuvable dans le PATH : applique kafkactl/admin/namespace-lab.yml a la main (etape 7)."
fi

step "Termine"
cat <<'EOF'
Terminaux a ouvrir (a laisser tourner) :
  kubectl port-forward svc/ns4kafka 8080:8080        # kafkactl
  kubectl port-forward svc/akhq 8090:8080            # http://localhost:8090
  kubectl port-forward svc/schema-registry 8081:8081 # etape 11
  kubectl port-forward svc/connect 8083:8083         # API REST Connect
Puis :
  export KAFKACTL_CONFIG="$PWD/kafkactl/config.yml" && cd kafkactl
  kafkactl config use-context lab && kafkactl get all
EOF
