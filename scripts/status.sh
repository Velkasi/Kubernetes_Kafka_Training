#!/usr/bin/env bash
# Etat du labo : pods, connecteurs (API REST Connect), volumes source / centrale.
set -uo pipefail
export MSYS_NO_PATHCONV=1
NS=kafka-lab
k() { kubectl -n "$NS" "$@"; }
sql() { k exec "$1" -- psql -U lab -d "$2" -tAc "$3" 2>/dev/null || echo "?"; }

echo "=== Pods"
k get pods -o wide --no-headers | awk '{printf "  %-34s %-6s %-18s %s\n", $1, $2, $3, $4" restarts"}'

echo
echo "=== Connecteurs (Kafka Connect)"
CONNECT_POD=$(k get pods -l app=connect -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "$CONNECT_POD" ]]; then
  k exec "$CONNECT_POD" -- curl -s 'http://localhost:8083/connectors?expand=status' \
    | tr '{' '\n' | grep -o '"name":"[^"]*","connector":{"state":"[A-Z]*","worker_id":"[^"]*"' \
    | sed -E 's/"name":"([^"]*)","connector":\{"state":"([A-Z]*)","worker_id":"([^"]*)"/  \1  \2  (worker \3)/' \
    || true
  k exec "$CONNECT_POD" -- curl -s 'http://localhost:8083/connectors?expand=status' \
    | grep -q '"state":"FAILED"' && echo "  /!\\ au moins une tache FAILED : curl .../connectors/<nom>/status"
else
  echo "  aucun pod connect"
fi

echo
echo "=== Volumes"
A=$(sql simu-a-0 simu "SELECT count(*) FROM mesures")
B=$(sql simu-b-0 simu "SELECT count(*) FROM sim_results")
echo "  source simu-a (mesures)       : $A"
echo "  source simu-b (sim_results)   : $B"
echo "  centrale simulation_measures  :"
sql central-0 central "SELECT '    ' || source_id || ' : ' || count(*) || '  (derniere mesure ' || max(measured_at) || ')'
                       FROM simulation_measures GROUP BY source_id ORDER BY 1"
