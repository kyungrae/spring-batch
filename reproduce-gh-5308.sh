#!/usr/bin/env bash
#
# Reproduce the JobOperator stop/restart optimistic-locking race (gh-5308) per RDBMS vendor.
#
# It runs both JobOperatorFunctionalTests and GracefulShutdownFunctionalTests N times against
# each configured vendor and records pass/fail. The race is timing- and vendor-sensitive:
#   - HSQLDB (in-memory, the default) almost never fails -> this is why CI does not catch it.
#   - MySQL (REPEATABLE READ) fails frequently.
#   - PostgreSQL (READ COMMITTED) fails occasionally.
#
# The sample datasource is made switchable per vendor through -Dbatch.* system properties
# (see spring-batch-samples/src/main/resources/data-source-context.xml on this branch).
# GracefulShutdownFunctionalTests is also vendor-switchable via the same properties.
#
# Prerequisites: a running MySQL and/or PostgreSQL reachable with the settings below, e.g.:
#   docker run -d --name mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -p 3306:3306 mysql:8.0
#   docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=spring_batch \
#       -p 5432:5432 postgres:16
# (MySQL must contain a `spring_batch` schema; the metadata tables are created by the test.)
#
# Usage:
#   ./reproduce-gh-5308.sh            # 30 runs per cell (default)
#   RUNS=100 ./reproduce-gh-5308.sh   # 100 runs per cell
#
# To compare before/after a fix, run twice and move the output directory between runs:
#   RUNS=100 ./reproduce-gh-5308.sh && mv "${TMPDIR:-/tmp}/gh-5308" "${TMPDIR:-/tmp}/gh-5308-baseline"
#   # apply the fix, then run again to get "${TMPDIR:-/tmp}/gh-5308" with the post-fix results.
#
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUNS="${RUNS:-30}"
LOG_DIR="${TMPDIR:-/tmp}/gh-5308"
RESULTS="${LOG_DIR}/results.tsv"
mkdir -p "$LOG_DIR"
echo -e "test\tvendor\trun\trc" > "$RESULTS"

TESTS=(JobOperatorFunctionalTests GracefulShutdownFunctionalTests)

run_cell() {
	local test="$1" vendor="$2"; shift 2
	echo "===== ${test} on ${vendor}: ${RUNS} runs ====="
	local pass=0 fail=0 i
	for i in $(seq 1 "$RUNS"); do
		if (cd "$ROOT" && ./mvnw -q -pl spring-batch-samples test -Dtest="$test" \
				-Dsurefire.failIfNoSpecifiedTests=false "$@") \
				>"$LOG_DIR/${test}-${vendor}-${i}.log" 2>&1; then
			pass=$((pass + 1))
			echo -e "${test}\t${vendor}\t${i}\t0" >> "$RESULTS"
			gzip -f "$LOG_DIR/${test}-${vendor}-${i}.log"
		else
			fail=$((fail + 1))
			echo -e "${test}\t${vendor}\t${i}\t1" >> "$RESULTS"
		fi
		if [ $((i % 20)) -eq 0 ]; then
			echo "  progress: ${i}/${RUNS} pass=${pass} fail=${fail}"
		fi
	done
	echo "  ${test}/${vendor}: PASS=${pass} FAIL=${fail}"
}

for test in "${TESTS[@]}"; do
	# HSQLDB (in-memory, default) -- rarely fails.
	run_cell "$test" hsqldb -Dbatch.database=hsqldb

	# MySQL -- adjust host/credentials to your environment.
	run_cell "$test" mysql \
		-Dbatch.database=mysql -Dbatch.jdbc.driver=com.mysql.cj.jdbc.Driver \
		-Dbatch.jdbc.url=jdbc:mysql://localhost:3306/spring_batch \
		-Dbatch.jdbc.username=root -Dbatch.jdbc.password=

	# PostgreSQL -- adjust host/credentials to your environment.
	run_cell "$test" postgresql \
		-Dbatch.database=postgresql -Dbatch.jdbc.driver=org.postgresql.Driver \
		-Dbatch.jdbc.url=jdbc:postgresql://localhost:5432/spring_batch \
		-Dbatch.jdbc.username=postgres -Dbatch.jdbc.password=postgres
done

echo "==== Summary ===="
echo "Per-run logs and results.tsv in ${LOG_DIR}"
awk -F'\t' 'NR>1 {pass[$1"\t"$2]+= ($4==0); total[$1"\t"$2]++}
END {
	printf "%-35s %-10s %-10s %-10s\n", "TEST", "VENDOR", "PASS", "FAIL"
	for (k in total) {
		split(k, a, "\t"); f = total[k] - pass[k];
		printf "%-35s %-10s %-10d %-10d\n", a[1], a[2], pass[k], f
	}
}' "$RESULTS" | sort
