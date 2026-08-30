#!/usr/bin/env bash
# Наскрізна перевірка стека: build → up → ingestor завершується → дані в Postgres.
# Це і є "тест" цього ДЗ — він дивиться на РЕЗУЛЬТАТ (рядки в таблиці), а не на ваш
# Dockerfile. Зелений verify.sh = ДЗ зараховано. Запускайте з кореня цієї директорії.
set -uo pipefail

EXPECTED_ROWS=211466
EXPECTED_TYPES=5

cd "$(dirname "$0")" || exit 1
echo "==> Перевіряю стек у: $(pwd)"

# що б не сталося — наприкінці прибираємо контейнери і volumes
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
fail() { echo "FAIL ❌  $1"; cleanup; exit 1; }
trap cleanup EXIT

# docker compose читає .env (а не .env.example) — створимо за потреби
[[ -f .env ]] || cp .env.example .env 2>/dev/null || fail "немає ні .env, ні .env.example"

# валідність compose-файлу (порожній/зламаний → зрозуміла помилка, а не сире сміття)
docker compose config >/dev/null 2>&1 || fail "docker-compose.yml порожній або невалідний — реалізуйте сервіси (SPEC.md, завдання 3–4)"

# чистий старт (прибираємо старі volumes, щоб TRUNCATE/COPY рахувались з нуля)
docker compose down -v >/dev/null 2>&1 || true

echo "==> docker compose up --build (Postgres healthcheck → ingestor)"
docker compose up -d --build || fail "docker compose up не вдався — перевірте Dockerfile і compose"

echo "==> Чекаю завершення ingestor..."
docker compose wait ingestor
code=$?
if [[ "$code" != "0" ]]; then
    echo "--- останні рядки логів ingestor ---"
    docker compose logs ingestor 2>/dev/null | tail -30
    fail "ingestor завершився з кодом $code"
fi

PSQL=(docker compose exec -T postgres psql -U "${POSTGRES_USER:-gh}" -d "${POSTGRES_DB:-gharchive}" -tAc)
rows=$("${PSQL[@]}" "SELECT count(*) FROM github_events" 2>/dev/null | tr -d '[:space:]')
types=$("${PSQL[@]}" "SELECT count(DISTINCT event_type) FROM github_events" 2>/dev/null | tr -d '[:space:]')

echo "==> github_events: rows=${rows:-?}, distinct event_type=${types:-?}"
[[ "$rows" == "$EXPECTED_ROWS" ]]   || fail "очікував $EXPECTED_ROWS рядків, маю ${rows:-?} (checkpoint у SPEC.md)"
[[ "$types" == "$EXPECTED_TYPES" ]] || fail "очікував $EXPECTED_TYPES типів подій, маю ${types:-?}"

echo "PASS ✅  Стек піднявся, ingestor завантажив $rows рядків у Postgres."
exit 0
