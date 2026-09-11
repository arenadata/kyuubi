#!/usr/bin/env bash
#
# Proves a Kerberized client reaches the gateway, and that the ticket is what
# decides where the session goes.
#
# Two callers whose clusters differ: alice belongs to trino-analytics, carol to
# trino-etl. One session could have landed anywhere; two that land differently
# show the identity being used.
set -euo pipefail

NS="${NS:-gateway}"
HERE="$(cd "$(dirname "$0")" && pwd)"
USERS=(alice carol)

# One Secret with a keytab per caller. Fetched rather than seeded, because the
# keys are the realm's and only it knows them.
ARGS=()
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
for u in "${USERS[@]}"; do
  PRINCIPAL="$u" SECRET="krb-client-$u" "$HERE/kerberos-keytab.sh" >/dev/null
  kubectl -n "$NS" get secret "krb-client-$u" -o jsonpath='{.data.gateway\.keytab}' \
    | base64 -d > "$TMP/$u.keytab"
  kubectl -n "$NS" delete secret "krb-client-$u" >/dev/null
  ARGS+=(--from-file="$u.keytab=$TMP/$u.keytab")
done
kubectl -n "$NS" create secret generic krb-client-keytab "${ARGS[@]}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl -n "$NS" delete job krb-client krb-client-carol --ignore-not-found >/dev/null
kubectl apply -f "$HERE/kerberos-test.yaml" >/dev/null

for j in krb-client krb-client-carol; do
  kubectl -n "$NS" wait --for=condition=complete --timeout=120s "job/$j" >/dev/null 2>&1 || true
  printf '%-18s %s\n' "$j" "$(kubectl -n "$NS" logs "job/$j" 2>/dev/null | grep -E '^OK|^REFUSED' | head -1)"
done

echo
echo "where the gateway sent them:"
kubectl -n "$NS" logs deploy/kyuubi-routing-gateway --tail=200 2>/dev/null \
  | grep -oE "Opening trino session for [a-z]+ on [^ ]+" | tail -4
