#!/usr/bin/env bash
# Fault-tolerance test. The cluster must already be running.
#   1. A background writer sends a stream of writes (unique key per write) to any node.
#   2. Repeatedly: wait for a stable leader, kill it, measure re-election time, restart it.
#   3. Wait for all nodes to converge, then verify identical committed logs and no lost writes.
# Usage: bash failover-test.sh [runs]     (default 5)

RUNS=${1:-5}
STOP=logs/writer.stop
ACKED=logs/acked.txt
rm -f "$STOP" "$ACKED" logs/nodeF.log
touch "$ACKED"

now()   { date +%s%3N; }
field() { grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2 | tr -d '"'; }

wait_for_leader() {   # prints the leader's number; waits up to 15 s
  local t l
  t=$(now)
  while [ $(( $(now) - t )) -lt 15000 ]; do
    if l=$(bash find-leader.sh); then echo "$l"; return 0; fi
    sleep 0.2
  done
  return 1
}

# ---------- background writer ----------
writer() {
  local i=0 n=1 code
  while [ ! -f "$STOP" ]; do
    i=$((i+1))
    while [ ! -f "$STOP" ]; do
      code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 -X PUT "localhost:808$n/config/w$i" \
             -H "Content-Type: text/plain" -d "$i")
      if [ "$code" = "200" ]; then echo "$i" >> "$ACKED"; break; fi
      n=$(( n % 5 + 1 ))
      sleep 0.05
    done
  done
}

L=$(wait_for_leader) || { echo "No leader found. Start the cluster first: bash run-cluster.sh"; exit 1; }
startTerm=$(curl -s localhost:808$L/status | field term)
writer &
WPID=$!
echo "writer started (term $startTerm)"
sleep 2

times=()
for run in $(seq 1 "$RUNS"); do
  L=$(wait_for_leader) || { echo "run $run: no leader for 15 s, skipping"; continue; }
  oldTerm=$(curl -s localhost:808$L/status | field term)
  bash kill-node.sh "$L" > /dev/null
  t0=$(now)

  new=""
  while [ -z "$new" ] && [ $(( $(now) - t0 )) -lt 10000 ]; do
    for n in 1 2 3 4 5; do
      [ "$n" = "$L" ] && continue
      s=$(curl -s --max-time 1 localhost:808$n/status)
      if echo "$s" | grep -q '"role":"LEADER"'; then
        new=$n; term=$(echo "$s" | field term); since=$(echo "$s" | field leaderSinceMs)
        break
      fi
    done
  done

  if [ -z "$new" ]; then
    echo "run $run: no new leader within 10 s!"
  elif [ "$since" -lt "$t0" ]; then
    echo "run $run: node$new was already leader before the kill, skipped"
  else
    ms=$(( since - t0 ))
    times+=("$ms")
    echo "run $run: killed node$L (term $oldTerm) -> node$new elected in term $term after ${ms} ms"
  fi

  sleep 1
  bash start-node.sh "$L" > /dev/null
  sleep 15
done

touch "$STOP"
wait "$WPID"
echo "writer stopped: $(wc -l < "$ACKED") writes acknowledged"

# ---------- re-election stats ----------
if [ ${#times[@]} -gt 0 ]; then
  sorted=$(printf "%s\n" "${times[@]}" | sort -n)
  cnt=${#times[@]}
  echo "--- re-election time (ms): $(echo $sorted)"
  echo "    min $(echo "$sorted" | head -1)   median $(echo "$sorted" | sed -n "$(( (cnt + 1) / 2 ))p")   p90 $(echo "$sorted" | sed -n "$(( (cnt * 9 + 9) / 10 ))p")   max $(echo "$sorted" | tail -1)"
fi

# ---------- wait for convergence, then compare committed logs ----------
echo "--- waiting for all 5 nodes to converge (up to 30 s)"
for try in $(seq 1 30); do
  for n in 1 2 3 4 5; do
    echo "node$n: $(curl -s --max-time 2 localhost:808$n/admin/log-hash)"
  done > logs/hashes.txt
  present=$(grep -c sha256 logs/hashes.txt)
  h=$(grep -o '"sha256":"[0-9a-f]*"' logs/hashes.txt | sort -u | wc -l)
  c=$(grep -o '"commitIndex":[0-9]*' logs/hashes.txt | sort -u | wc -l)
  if [ "$present" = "5" ] && [ "$h" = "1" ] && [ "$c" = "1" ]; then break; fi
  sleep 1
done
cat logs/hashes.txt
if [ "$present" = "5" ] && [ "$h" = "1" ] && [ "$c" = "1" ]; then
  echo "RESULT: committed logs IDENTICAL on all 5 nodes"
else
  echo "RESULT: committed logs DIFFER"
fi

# ---------- no lost writes ----------
L=$(wait_for_leader)
curl -s localhost:808$L/config | grep -o '"w[0-9]*"' | tr -d '"w' | sort > logs/stored.txt
sort "$ACKED" > logs/acked_sorted.txt
lost=$(comm -23 logs/acked_sorted.txt logs/stored.txt | wc -l)
echo "RESULT: $lost of $(wc -l < "$ACKED") acknowledged writes lost"

# ---------- election stability ----------
endTerm=$(curl -s localhost:808$L/status | field term)
echo "RESULT: terms used: $(( endTerm - startTerm )) for $RUNS leader kills (ideal: about $RUNS)"
echo "        pre-votes started: $(cat logs/node[1-5].log | grep -c 'starting pre-vote')," \
     "real elections: $(cat logs/node[1-5].log | grep -c '> CANDIDATE')"