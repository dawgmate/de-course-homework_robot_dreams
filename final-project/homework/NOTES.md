# NOTES.md

## 1. Рішення й компроміси

- **Ідемпотентність Bronze.** Consumer спочатку групує повідомлення за Kafka partition,
  записує всі файли у `*.ndjson.tmp`, виконує `fsync`, атомарно перейменовує їх через
  `os.replace` і лише після цього синхронно комітить офсети. Bronze зберігає відносний
  `_source_file`, пропускає вже завантажені файли, а перед JDBC-записом робить `coalesce(1)`,
  тому весь append проходить однією транзакцією: збій залишає або весь батч, або нуль рядків.

- **Інкремент Silver.** Межа інкременту — `_ingested_at`, бо це час появи події у pipeline;
  `occurred_at` може бути старим у late event. Фільтр за `occurred_at` назавжди пропустив би
  запізнілі `payment_captured`, `ride_started` або інші події, що змінюють уже побудовану
  поїздку.

- **`silver.rides` і `fact_ride`.** Спочатку знаходжу `ride_id`, яких торкнулися нові події,
  а потім перебудовую ці поїздки з усієї історії `silver.events`. `fact_ride` так само бере
  змінені рядки `silver.rides` і замінює їх через `delete+insert`, тому accumulating snapshot
  коректно доповнюється пізніми віхами.

- **`agg_zone_hourly`.** Інкремент визначає зачеплені `requested_hour` і повністю перераховує
  кожну таку годину з актуального `fact_ride`; `requested_hour` використано як ключ заміни.
  Це важливо, коли пізній `ride_started` змінює pickup zone: перераховуються і стара, і нова
  комбінації зони в межах години, тому застарілий bucket не залишається.

- **`dim_zone` — `table`, `dim_driver` — `incremental`.** Зони — малий статичний seed, тому
  повна перебудова проста й передбачувана. Для водіїв знаходжу зачеплені ключі, але останній
  рейтинг і атрибути обираю з усієї історії за `occurred_at`; якби дивився лише новий батч,
  late event зі старішим event time міг би затерти актуальний рейтинг.

- **`--indirect-selection cautious`.** Цей режим не запускає тест, якщо не вибрані всі його
  parent-моделі. Без нього Silver-крок міг би спробувати виконати міжшаровий тест до побудови
  Gold; такі звірки винесені в selector `reconcile` і запускаються після обох шарів.

- **DAG без `data_interval_start/end`.** Джерело має processing-time landing-файли та late
  events, які не збігаються з event-time вікном розкладу. Стан зберігається у `_source_file` і
  high watermark таблиць, тому retry та повторний DAG-run безпечні, а дані не губляться через
  межі п'ятихвилинного інтервалу.

- **Обсяг у 100 разів більший.** Першими проблемами стануть один JDBC writer у Bronze,
  повноісторичні перерахунки в Postgres і локальні Spark/Airflow ресурси. Я переніс би landing
  в object storage, Bronze — у партиціонований lakehouse-формат із транзакціями, Spark — у
  кластер, а Gold — у колонковий DWH; Kafka отримала б більше partitions, контроль lag і
  масштабовану consumer group. Для rides/driver залишив би touched-key підхід, але матеріалізував
  би change sets і додав індекси/партиціонування за ingestion time та бізнес-ключами.

## 2. Журнал використання ШІ

### Випадок 1: атомарний запис landing-файлів

- **Що запитали:** реалізувати Kafka consumer з at-least-once доставкою й атомарним NDJSON sink.
- **Що отримали:** групування за partition, сортування за offset, тимчасові файли, `fsync`,
  `os.replace`, а commit офсетів — після `write_batch`.
- **Що було не так:** у першому варіанті temp-файл додавався до списку cleanup лише після
  завершення запису. Виняток усередині `write()` міг лишити `.tmp` на диску.
- **Чим виявили:** читання сценарію `test_failure_leaves_no_partial_file`; після виправлення
  `./verify.sh ingest` показав `14 passed`, Kafka e2e — `3 passed`, Bronze — `8 passed`.
- **Що зробили:** почав відстежувати temp-файл до відкриття/запису, а в `except` видаляю всі
  staged temp-файли; commit залишив строго після успішного запису.

### Випадок 2: інкремент hourly-вітрини

- **Що запитали:** побудувати incremental `agg_zone_hourly`, стійкий до зміни pickup zone.
- **Що отримали:** спочатку розглядався звичайний composite unique key
  `(requested_hour, pickup_zone_key)`.
- **Що було не так:** такий delete+insert оновив би новий bucket, але міг не видалити старий,
  якщо після переміщення поїздки він став порожнім.
- **Чим виявили:** зіставлення з питанням у SPEC про зміну зони та тестом
  `assert_agg_matches_fact`; `./verify.sh transform` перевірив late events, чотири батчі,
  повторний запуск і `incremental == full-refresh` — `6 passed`.
- **Що зробили:** ключем заміни зробив `requested_hour`, а результатом incremental-запуску —
  повний перерахунок усіх zone buckets у зачеплених годинах.

### Випадок 3: перевірка dbt-моделей, а не лише правдоподібного SQL

- **Що запитали:** реалізувати Silver/Gold та два власні singular-тести.
- **Що отримали:** touched-key моделі, full-history rebuild сутностей, UTC-макроси і два
  міжшарові тести з тегом `reconcile`.
- **Що було не так:** одного успішного `dbt build` недостатньо — тест може не запускатися через
  `--indirect-selection cautious`, а запит тесту може завжди повертати нуль рядків.
- **Чим виявили:** `tests/test_dbt_selection.py` дав `1 passed`; transform-перевірка —
  `6 passed`. Додатково навмисні зміни `latest_rating = -1` і неправильний `driver_key` дали
  відповідно `FAIL 1 assert_driver_latest_state` та `FAIL 1 assert_fact_keys_follow_silver`.
- **Що зробили:** обидва тести позначив `tags=['reconcile']`, виконав негативні mutation runs,
  після кожного відновив Gold через `--full-refresh` (`PASS=29`).

### Випадок 4: Airflow-перевірка

- **Що запитали:** зібрати DAG `bronze_spark → bronze_contract → silver → gold → reconcile`.
- **Що отримали:** DAG із розкладом `*/5 * * * *`, `catchup=False`, одним active run,
  retries/timeouts і dbt selectors.
- **Що було не так:** `./verify.sh orchestrate` не дійшов до імпорту DAG: Docker BuildKit
  впав під час resolve/build базового Airflow image. Це інфраструктурна, але все ще незакрита
  перевірка.
- **Чим виявили:** сім setup errors у `tests/test_orchestrate.py`, усі на кроці
  `docker compose ... up --build`, до запуску жодного DAG-тесту.
- **Що зробили:** код DAG лишив без непідтверджених змін; наступний крок — успішно зібрати
  `airflow-init` із повним Docker log і повторити `./verify.sh orchestrate`.

## 3. Ваші dbt-тести

- **Файл:** `dbt_rides/tests/assert_driver_latest_state.sql`
- **Що перевіряє:** кожен водій у `dim_driver` має vehicle/medallion/rating із найновішої
  `ride_accepted` за `occurred_at`, незалежно від ingestion order.
- **Як переконались, що він ловить помилку:** у локальному warehouse виконав
  `update gold.dim_driver set latest_rating = -1` для одного відомого водія. Команда
  `./dbt.sh test --select assert_driver_latest_state` повернула `FAIL 1` і exit code 1;
  `dbt build --selector gold --full-refresh` відновив таблицю.

- **Файл:** `dbt_rides/tests/assert_fact_keys_follow_silver.sql`
- **Що перевіряє:** zone/driver keys у `fact_ride` відповідають natural keys із
  `silver.rides`, а unknown members використовуються лише для відсутніх значень.
- **Як переконались, що він ловить помилку:** для однієї поїздки з відомим водієм замінив
  `gold.fact_ride.driver_key` на `'unknown'`. Тест повернув `FAIL 1` і exit code 1;
  після цього Gold відновлено через `--full-refresh`, усі 29 Gold build/tests пройшли.

## 4. Що зробили б далі

Насамперед завершив би `./verify.sh orchestrate`: окремо зібрав би Airflow image з
`docker compose --progress plain --profile airflow build airflow-init`, усунув проблему pull/resolve
і повторив три послідовні DAG runs. Далі додав би метрики Kafka lag, freshness/SLA alerting,
dead-letter потік для невалідних подій, observability для кількості оброблених файлів/рядків та
автоматичні retention/compaction політики для landing і Bronze.
