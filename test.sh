#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
cd "$SCRIPT_DIR"

export SECRETS_DIR="${SCRIPT_DIR}/config/ci"
export UNIVERSAL_CONFIG_DIR="${SCRIPT_DIR}/config/ci"

COMPOSE="docker compose \
  -f docker/docker-compose.base.yml \
  -f docker/docker-compose.test.yml"

# ensure clean docker compose state
$COMPOSE down --remove-orphans || true

LOGS_PID=
cleanup() {
	if [ -n "$LOGS_PID" ]; then
		kill -9 "$LOGS_PID" 2>/dev/null || true
	fi
	$COMPOSE down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Log dirs are created group-writable (setgid 2775) so the containers'
# non-root user (primary group GID 2523) can write to them even though the
# dirs are owned by root. /var/log/Wywy-Website/cache is bind-mounted as the
# sync container's log dir (base.yml:57) AND as the create_tables container's
# log dir (base.yml:28 maps it to /var/log/Wywy-Website/create_tables).
DATA_DIRS=(
	/var/log/Wywy-Website/cache
	/var/lib/Wywy-Website/cache
)

for dir in "${DATA_DIRS[@]}"; do
	mkdir -p "$dir" 2>/dev/null || sudo mkdir -p "$dir"
	chown 0:2523 "$dir" 2>/dev/null || sudo chown 0:2523 "$dir"
	chmod 2775 "$dir" 2>/dev/null || sudo chmod 2775 "$dir"
done

# Seed the cache file
printf '{}' >/var/lib/Wywy-Website/cache/cache.json 2>/dev/null ||
	sudo sh -c "printf '{}' > /var/lib/Wywy-Website/cache/cache.json"
chown 0:2523 /var/lib/Wywy-Website/cache/cache.json 2>/dev/null ||
	sudo chown 0:2523 /var/lib/Wywy-Website/cache/cache.json
chmod 664 /var/lib/Wywy-Website/cache/cache.json 2>/dev/null ||
	sudo chmod 664 /var/lib/Wywy-Website/cache/cache.json

# Start all services and wait for health checks.
# --build avoids create_tables old schema issues
# --wait-timeout bounds the first (uncapped) run; tune to ~2x the measured
# green-run duration after the first measurement.
$COMPOSE up --detach --build --wait --wait-timeout 1800 2>&1 || {
	rc=$?
	echo ""
	echo "============================================================"
	$COMPOSE logs --no-color 2>&1 || true
	echo "============================================================"
	exit $rc
}

# Stream container logs in background for real-time CI visibility.
$COMPOSE logs -f &
LOGS_PID=$!

# Capture the test container ID. Use ps -aq: plain -q hides exited containers,
# so if the suite finishes before ps runs, test_cid would be empty and the
# default test_exit=1 would produce a false red.
test_cid=$($COMPOSE ps -aq test || true)
test_exit=1
# docker wait prints the container exit code to stdout and itself exits 0, so
# capture it directly (the `&& test_exit=$?` form always yields 0 — a false
# green).
[ -n "$test_cid" ] && test_exit=$(docker wait "$test_cid")

# Aggregate exit code.
exit_code=0
[ "$test_exit" -ne 0 ] && exit_code=$test_exit

# Stop background log stream before teardown. The process can get stuck in a
# blocking Docker API read that SIGTERM doesn't interrupt — escalate to
# SIGKILL after a short grace period.
kill "$LOGS_PID" 2>/dev/null || true
for _ in 1 2 3; do
	kill -0 "$LOGS_PID" 2>/dev/null || break
	sleep 1
done
kill -9 "$LOGS_PID" 2>/dev/null || true
wait "$LOGS_PID" 2>/dev/null || true

$COMPOSE down --remove-orphans

exit $exit_code
