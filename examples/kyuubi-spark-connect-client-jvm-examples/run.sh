#!/usr/bin/env bash
#
# Usage:
#   mnv clean package
#   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=KERBEROS ./run.sh
#   KYUUBI_URL=sc://host:10199/;use_ssl=true KYUUBI_AUTH=LDAP KYUUBI_USERNAME=john KYUUBI_PASSWORD=secret ./run.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

exec java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar "$SCRIPT_DIR/target/kyuubi-spark-connect-client-jvm-examples-1.0-SNAPSHOT.jar" "$@"
