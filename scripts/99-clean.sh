#!/usr/bin/env bash
# Supprime le labo : le namespace Kubernetes, et avec lui tous les pods, services et volumes (PVC).
# Le cluster Minikube reste en place (minikube delete pour tout effacer).
set -euo pipefail
read -r -p "Supprimer le namespace kafka-lab et TOUTES ses donnees ? [o/N] " ok
[[ "$ok" == "o" || "$ok" == "O" ]] || { echo "Annule."; exit 0; }
kubectl delete namespace kafka-lab --wait=true
rm -f "$HOME/.kafkactl/jwt"   # jeton kafkactl en cache, devenu invalide
echo "Labo supprime. L'image connect-lab:1.0 reste dans Minikube (minikube image rm connect-lab:1.0)."
