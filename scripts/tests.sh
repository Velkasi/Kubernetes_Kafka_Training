#!/usr/bin/env bash
# Tests de robustesse automatises (etape 13 du guide).
#   ./scripts/tests.sh check      # 0 doublon, 0 perte, mises a jour de A propagees
#   ./scripts/tests.sh failover   # 13.1 un pod Connect meurt
#   ./scripts/tests.sh central    # 13.2 la base centrale tombe
#   ./scripts/tests.sh replay     # 13.3 rejouer tout l'historique
#   ./scripts/tests.sh all
# Prerequis pour central/replay : port-forward ns4kafka actif (kafkactl), contexte "lab".
set -uo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=kafka-lab
export KAFKACTL_CONFIG="${KAFKACTL_CONFIG:-$ROOT/kafkactl/config.yml}"

k() { kubectl -n "$NS" "$@"; }
sql() { k exec "$1" -- psql -U lab -d "$2" -tAc "$3"; }
kctl() { (cd "$ROOT/kafkactl" && kafkactl "$@"); }
title() { printf '\n\033[1;34m=== %s\033[0m\n' "$*"; }
ok() { printf '  \033[32mOK\033[0m  %s\n' "$*"; }
ko() { printf '  \033[31mKO\033[0m  %s\n' "$*"; FAILED=1; }
FAILED=0

wait_sync() {   # attend que le lag du sink redescende
  echo "  attente de la synchronisation (${1:-60} s max)..."
  for _ in $(seq 1 "$(( ${1:-60} / 5 ))"); do
    sleep 5
    lag=$(k exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
          --describe --group connect-lab.sink-central 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')
    [[ "$lag" -le 5 ]] && { echo "  lag = $lag"; return 0; }
  done
  echo "  lag encore a $lag"
}

check() {
  title "Coherence sources / centrale"
  # Doublons : impossibles par construction (cle primaire + upsert), on le verifie quand meme.
  d=$(sql central-0 central "SELECT count(*) FROM (SELECT 1 FROM simulation_measures GROUP BY source_id, measure_id HAVING count(*) > 1) x")
  [[ "$d" == "0" ]] && ok "0 doublon" || ko "$d doublons"

  # Pertes : on compare les lignes plus vieilles que 30 s (les plus recentes peuvent etre en transit).
  for src in "SIMU_A simu-a-0 mesures measure_id mesure_le" "SIMU_B simu-b-0 sim_results result_id measured_at"; do
    read -r id pod table col ts <<<"$src"
    max=$(sql "$pod" simu "SELECT coalesce(max($col),0) FROM $table WHERE $ts < now() - interval '30 seconds'")
    n_src=$(sql "$pod" simu "SELECT count(*) FROM $table WHERE $col <= $max")
    n_cen=$(sql central-0 central "SELECT count(*) FROM simulation_measures WHERE source_id='$id' AND measure_id <= $max")
    [[ "$n_src" == "$n_cen" ]] && ok "$id : $n_src lignes dans la source = $n_cen dans la centrale" \
                               || ko "$id : $n_src lignes dans la source, $n_cen dans la centrale"
  done

  # Mises a jour : les lignes de A modifiees par le generateur ont la meme valeur dans la centrale.
  values=$(sql simu-a-0 simu "SELECT string_agg('(' || measure_id || ',' || valeur || ')', ',')
                              FROM mesures WHERE updated_at > mesure_le + interval '1 second'
                                             AND updated_at < now() - interval '30 seconds'")
  if [[ -z "$values" ]]; then
    echo "  (aucune ligne de A modifiee depuis plus de 30 s : relance plus tard)"
  else
    n=$(grep -o '(' <<<"$values" | wc -l | tr -d ' ')
    diff=$(sql central-0 central "SELECT count(*) FROM (VALUES $values) v(id, val)
                                  LEFT JOIN simulation_measures c ON c.source_id = 'SIMU_A' AND c.measure_id = v.id
                                  WHERE c.value IS DISTINCT FROM v.val::double precision")
    [[ "$diff" == "0" ]] && ok "$n lignes modifiees dans A : toutes a jour dans la centrale" \
                         || ko "$diff lignes modifiees sur $n ne sont pas a jour dans la centrale"
  fi
}

failover() {
  title "13.1 Un pod Connect meurt"
  pod=$(k get pods -l app=connect -o jsonpath='{.items[0].metadata.name}')
  echo "  suppression de $pod"
  k delete pod "$pod" --wait=false
  sleep 20
  k rollout status deployment/connect --timeout=180s
  pod=$(k get pods -l app=connect -o jsonpath='{.items[0].metadata.name}')
  states=$(k exec "$pod" -- curl -s 'http://localhost:8083/connectors?expand=status')
  grep -q '"state":"FAILED"' <<<"$states" && ko "une tache est FAILED apres le basculement" || ok "toutes les taches tournent"
  wait_sync 90
  check
}

central() {
  title "13.2 La base centrale tombe"
  k scale statefulset/central --replicas=0
  echo "  centrale arretee pour 60 s : les messages s'accumulent dans le topic"
  sleep 60
  k exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group connect-lab.sink-central 2>/dev/null | awk 'NR>1 {s+=$6} END {print "  lag pendant la panne : " s}'
  k scale statefulset/central --replicas=1
  k rollout status statefulset/central --timeout=180s
  sleep 10
  if kctl get connector lab.sink-central -o yaml 2>/dev/null | grep -q FAILED; then
    echo "  sink FAILED : redemarrage"; kctl connector restart lab.sink-central
  fi
  wait_sync 120
  check
}

replay() {
  title "13.3 Rejouer tout l'historique"
  kctl connector stop lab.sink-central
  sleep 5
  sql central-0 central "TRUNCATE simulation_measures"
  echo "  table centrale videe : $(sql central-0 central 'SELECT count(*) FROM simulation_measures') lignes"
  kctl connector reset-offsets lab.sink-central
  kctl connector resume lab.sink-central
  wait_sync 180
  check
}

case "${1:-check}" in
  check) check ;;
  failover) failover ;;
  central) central ;;
  replay) replay ;;
  all) failover; central; replay ;;
  *) echo "usage : $0 [check|failover|central|replay|all]"; exit 2 ;;
esac
exit $FAILED
