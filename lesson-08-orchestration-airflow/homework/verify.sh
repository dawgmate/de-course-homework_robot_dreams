#!/usr/bin/env bash
# Наскрізна перевірка DAG: стек Airflow піднімається → DAG парситься без помилок →
# має потрібні задачі → `airflow dags test` проганяє його за один день → дані в DuckDB →
# повторний прогін НЕ дублює дані (ідемпотентність). Зелений verify.sh = ДЗ зараховано.
# Запускайте з кореня цієї директорії.
set -uo pipefail

EXPECTED_ROWS=172340
EXPECTED_TYPES=5
TEST_DS="2024-01-14"
SCHED="airflow-scheduler"

cd "$(dirname "$0")" || exit 1
echo "==> Перевіряю DAG у: $(pwd)"

cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
fail() { echo "FAIL ❌  $1"; cleanup; exit 1; }
trap cleanup EXIT

[[ -f .env ]] || cp .env.example .env

duck() {  # $1 = SQL → друкує один скаляр (SQL передаємо через env, щоб не воювати з лапками)
    docker compose exec -T -e SQL="$1" "$SCHED" python -c '
import duckdb, os
con = duckdb.connect("/opt/airflow/data/github_analytics.duckdb", read_only=True)
print(con.execute(os.environ["SQL"]).fetchone()[0])
' 2>/dev/null | tr -d '[:space:]'
}

docker compose config >/dev/null 2>&1 || fail "docker-compose.yml невалідний"

echo "==> Піднімаю стек (build + Postgres + init + scheduler + webserver)..."
cleanup
docker compose up -d --build || fail "docker compose up не вдався"

echo "==> Чекаю, поки scheduler розпарсить DAG..."
parsed=0
for _ in $(seq 1 60); do
    if docker compose exec -T "$SCHED" airflow dags list 2>/dev/null | grep -q github_archive_daily; then
        parsed=1; break
    fi
    sleep 4
done
[[ "$parsed" == "1" ]] || fail "DAG 'github_archive_daily' не з'явився (scheduler не стартував або import error)"

echo "==> Перевіряю, що немає import errors..."
errs=$(docker compose exec -T "$SCHED" airflow dags list-import-errors 2>/dev/null)
echo "$errs" | grep -q "No data found" || { echo "$errs"; fail "DAG має import errors"; }

echo "==> Перевіряю склад задач..."
tasklist=$(docker compose exec -T "$SCHED" airflow tasks list github_archive_daily 2>/dev/null)
for t in check_availability download_archive validate_file load_to_duckdb notify_completion; do
    echo "$tasklist" | tr -d '\r' | grep -qx "$t" || fail "у DAG немає задачі '$t'"
done

echo "==> Чистий DuckDB і перший прогін: airflow dags test ... $TEST_DS"
docker compose exec -T "$SCHED" bash -lc 'rm -f /opt/airflow/data/*.duckdb' || true
docker compose exec -T "$SCHED" airflow dags test github_archive_daily "$TEST_DS" >/tmp/l08_run1.log 2>&1 \
    || { tail -25 /tmp/l08_run1.log; fail "перший 'airflow dags test' завершився помилкою"; }

rows1=$(duck "SELECT count(*) FROM raw.github_events_raw WHERE event_date = DATE '$TEST_DS'")
types=$(duck "SELECT count(DISTINCT event_type) FROM raw.github_events_raw WHERE event_date = DATE '$TEST_DS'")
echo "==> Після прогону 1: rows=${rows1:-?}, distinct event_type=${types:-?}"
[[ "$rows1" == "$EXPECTED_ROWS" ]]  || fail "очікував $EXPECTED_ROWS рядків, маю ${rows1:-?} (checkpoint у SPEC.md)"
[[ "$types" == "$EXPECTED_TYPES" ]] || fail "очікував $EXPECTED_TYPES типів подій, маю ${types:-?}"

echo "==> Повторний прогін того самого дня (перевірка ідемпотентності)..."
docker compose exec -T "$SCHED" airflow dags test github_archive_daily "$TEST_DS" >/tmp/l08_run2.log 2>&1 \
    || { tail -25 /tmp/l08_run2.log; fail "другий 'airflow dags test' завершився помилкою"; }
rows2=$(duck "SELECT count(*) FROM raw.github_events_raw WHERE event_date = DATE '$TEST_DS'")
echo "==> Після прогону 2: rows=${rows2:-?}"
[[ "$rows2" == "$EXPECTED_ROWS" ]] || fail "НЕ ідемпотентно: після повторного прогону маю ${rows2:-?}, а не $EXPECTED_ROWS"

echo "PASS ✅  DAG піднявся, відпрацював і завантажив $rows1 подій; повторний прогін не дублює дані."
exit 0
