# Домашнє завдання — Заняття 03: SQL для Data Engineering

> Це **основний** варіант (з dbt). Якщо на занятті не вистачило часу на dbt, є полегшена
> версія тих самих завдань без dbt — лише SQL-запити у DuckDB: [`../homework-queries/`](../homework-queries/).
> Здавати треба **один** із двох варіантів.

## Що робимо

Будуємо **один dbt-проєкт** на DuckDB поверх даних **GitHub Archive** (тих самих, що й у ДЗ
заняття 02, але вже у вигляді партиційованого Parquet). Вам дано набір джерел із
документацією — ваша задача «зрозуміти» їх і зібрати аналітичний шар: сім моделей, від
чистого `stg_events` до оптимізованого звіту з partition pruning.

Це не «напишіть 5 окремих запитів». Це проєкт: моделі пов'язані через `ref()`, покриті
тестами, і будуються одним `dbt build`.

- **Специфікація кожної моделі:** [`MODELS.md`](MODELS.md) — головний документ, читайте його.
- **Стартовий проєкт (ваш код):** [`dbt_github_hw/`](dbt_github_hw/)
- **Еталон (для самоперевірки після здачі):** [`solution/dbt_github_hw/`](solution/dbt_github_hw/)

## Датасет (уже в репозиторії)

`../data/` (спільна для `homework/` і `solution/`, на рівні кореня заняття) містить
детермінований зріз реальних подій GitHub — нічого завантажувати не треба:

| Файл | Опис |
|---|---|
| `../data/events/event_date=*/` | GitHub events, 1–14 січня 2024 (по одній годині на день), партиційовано по `event_date` (14 партицій) |
| `dbt_github_hw/seeds/event_categories.csv` | Довідник `event_type` → `category` (`code`/`issues`/`social`), `is_write` |
| `dbt_github_hw/seeds/calendar.csv` | Календар на ті 14 днів (`day`, `is_weekend`, `iso_week`) |

Сирі дані **навмисно «брудні»** (усі 15 типів подій, боти, порожні push-и) — їх чистить
`stg_events`.

## Як запустити

```bash
# 1. Залежності (один раз — ця директорія має власне pyproject.toml/uv.lock)
cd lesson-03-sql/homework
uv sync

# 2. Перейти у стартовий проєкт
cd dbt_github_hw

# 3. Завантажити seeds (довідник + календар) у DuckDB
uv run dbt seed --profiles-dir .

# 4. Зібрати моделі + прогнати тести
uv run dbt build --profiles-dir .
```

`dbt build` = `run` (будує моделі) + `test` (прогоняє перевірки). Це і є ваш self-check.

## Як підходити

1. **Читайте `MODELS.md` зверху вниз.** Моделі залежать одна від одної: спочатку
   `stg_events`, далі решта.
2. **Реалізуйте по одній моделі за раз** і одразу перевіряйте лише її:
   ```bash
   uv run dbt build --profiles-dir . --select stg_events
   uv run dbt build --profiles-dir . --select repo_top_events
   ```
3. **Звіряйтеся з checkpoint** у `MODELS.md` (очікувана кількість рядків). Заглушка повертає
   0 рядків, тому тести проходять «вхолосту» — поки число рядків не збіглося з checkpoint,
   модель ще не готова.
4. **Task 7** — окремий жанр: не пишіть з нуля, а перепишіть готовий `report_category_week_naive`.
   Обов'язково перевірте `EXPLAIN ANALYZE` (інструкція в `MODELS.md`): має бути 7 партицій
   замість 14.

## Тести (самоперевірка)

Кожна модель має тест. Частина — стандартні dbt-тести у `models/schema.yml`
(`not_null`, `unique`, `accepted_values`), частина — singular-тести у `tests/`:

| Тест | Що перевіряє |
|---|---|
| `assert_stg_events_clean` | у `stg_events` не лишилось брудних рядків (Task 1) |
| `assert_running_events_monotonic` | накопичувальна кількість подій не спадає (Task 3) |
| `assert_starred_repos_without_push_correct` | жоден репозиторій зі списку не має push (Task 5) |
| `assert_report_matches_naive` | оптимізований звіт = naive порядково (Task 7) |

Готове ДЗ — це **зелений `dbt build`** (усі моделі + усі тести) **і** кількість рядків,
що збігається з checkpoint у `MODELS.md`.

## Що здавати

Pull request зі змістом директорії `dbt_github_hw/`:
- 7 реалізованих моделей у `models/` (+ незмінений `report_category_week_naive.sql`);
- лог успішного `uv run dbt build` (можна як текст у описі PR).

## Оцінювання — 15 балів

Бали за модель нараховуються, якщо вона збирається, проходить свої тести і збігається з
еталоном `solution/` за результатом.
