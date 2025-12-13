# Контракти Redis-каналів (FXCM Connector)

Цей документ — **джерело правди** по форматах JSON-повідомлень, які публікує конектор у Redis PubSub.

- TypedDict-референс для основних payload-ів: [fxcm_schema.py](../fxcm_schema.py)
- Реальна публікація: [connector.py](../connector.py)
- Валідація OHLCV (інгістор): [fxcm_ingestor.py](../fxcm_ingestor.py)
- Tick→bucket семантика та додаткові OHLCV поля: [docs/TIK_bucket.md](TIK_bucket.md)

## 0. Загальні правила

- Кожне повідомлення — **один JSON-об’єкт** (не масив).
- Кодування: UTF‑8.
- Серіалізація: `json.dumps(..., separators=(",", ":"))` (мінімум пробілів).
- Споживачі **мають ігнорувати невідомі поля** (forward compatibility). Конектор може додавати нові ключі без зміни базового контракту.

## 1. Канал `fxcm:ohlcv`

**Призначення:** основні OHLCV бари (історичний poll або tick-агрегація).

**Producer:** `connector.publish_ohlcv_to_redis()`

**Consumer (валидація):** `fxcm_ingestor.FXCMIngestor.validate_payload()`

### 1.1 Повідомлення (root)

Тип: JSON object.

Обов’язкові поля:

- `symbol` *(string)* — нормалізований символ AiOne_t: прибираємо `/`, робимо `upper` (наприклад, `"XAU/USD" → "XAUUSD"`).
- `tf` *(string)* — таймфрейм (як у публікації): типово `"m1"`, `"m5"` або нормалізовані `"1m"`, `"5m"` залежно від конфігурації/виклику.
- `bars` *(array[object])* — масив барів, **відсортований за `open_time` строго зростаюче**.

Опційні поля:

- `source` *(string)* — джерело даних (наприклад, `"stream"` або `"tick_agg"`).
- `sig` *(string)* — HMAC-підпис (якщо увімкнено `FXCM_HMAC_SECRET`).

Важливо про HMAC:

- Підпис рахується **лише** по базовому payload: `{ "symbol", "tf", "bars" }`.
- Якщо `source` присутній у root, він **не** входить у HMAC (це очікувана поведінка поточної реалізації).

### 1.2 Один бар у `bars[]`

Обов’язкові поля (базовий контракт):

- `open_time` *(int)* — UNIX timestamp у **мілісекундах** (UTC).
- `close_time` *(int)* — UNIX timestamp у **мілісекундах** (UTC). Валідація: `close_time >= open_time`.
- `open`, `high`, `low`, `close` *(float)*.
- `volume` *(float)*.

Опційні поля (розширення, можуть бути відсутні):

- `complete` *(bool)* — для tick-агрегації: `true` лише коли бар закритий. Якщо `false`, інгістор **ігнорує** такий бар.
- `synthetic` *(bool)* — `true`, якщо бар згенеровано як synthetic-gap заповнення.
- `source` *(string)* — джерело конкретного бару (може дублювати root `source`).
- `tf` *(string)* — таймфрейм бару (якщо потрібно відправляти мульти-TF в одному батчі; зараз використовується опційно).
- Мікроструктурні метрики (якщо присутні у DataFrame):
  - `tick_count` *(int)*
  - `bar_range` *(float)*
  - `body_size` *(float)*
  - `upper_wick` *(float)*
  - `lower_wick` *(float)*
  - `avg_spread` *(float)*
  - `max_spread` *(float)*

## 2. Канал `fxcm:price_tik`

**Призначення:** регулярні снепшоти bid/ask/mid по кожному символу.

**Producer:** `connector.PriceSnapshotWorker` (кожен символ публікується окремим повідомленням).

### 2.1 Повідомлення

Обов’язкові поля:

- `symbol` *(string)* — нормалізований символ (`EURUSD`, `XAUUSD`, ...).
- `bid`, `ask`, `mid` *(float)*.
- `tick_ts` *(float)* — UNIX timestamp (секунди) часу **останнього тика** по цьому символу.
- `snap_ts` *(float)* — UNIX timestamp (секунди) часу **формування снепшота**.

## 3. Канал `fxcm:market_status`

**Призначення:** стан ринку `open/closed` + календарний зріз.

**Producer:** `_publish_market_status()`

### 3.1 Повідомлення

Обов’язкові поля:

- `type` *(string)* — завжди `"market_status"`.
- `state` *(string)* — `"open"` або `"closed"`.
- `ts` *(string)* — ISO‑8601 UTC без мікросекунд.
- `session` *(object)* — календарний контекст (див. 3.2).

Поля лише для `state="closed"` (якщо `next_open` відомий):

- `next_open_utc` *(string)* — ISO‑8601 UTC.
- `next_open_ms` *(int)* — UNIX ms.
- `next_open_in_seconds` *(float)* — скільки секунд до відкриття.

### 3.2 Блок `session`

Контракт відповідає `SessionContextPayload` у [fxcm_schema.py](../fxcm_schema.py).

Типи/поля (частина опційна):

- `tag` *(string)* — тег сесії (або `AUTO`-резолв).
- `timezone` *(string)* — таймзона сесії/календаря.
- `weekly_open`, `weekly_close` *(string, optional)*.
- `daily_breaks` *(array, optional)* — список перерв (формат як у `calendar_snapshot()`).
- `holidays` *(array[string], optional)*.
- `next_open_utc` *(string)*
- `next_open_ms` *(int)*
- `next_open_seconds` *(float)*
- `session_open_utc`, `session_close_utc` *(string, optional)* — якщо сесія резолвиться.
- `stats` *(object, optional)* — агрегована статистика по сесіях/символах (див. SessionStatsTracker у `connector.py`).

## 4. Канал `fxcm:heartbeat`

**Призначення:** технічний стан процесу + розширений контекст для діагностики.

**Producer:** `_publish_heartbeat()` або AsyncSupervisor.

### 4.1 Root поля

- `type` *(string)* — завжди `"heartbeat"`.
- `state` *(string)* — поточний стан циклу: типово `"stream"`, `"idle"`, `"warmup"`, `"warmup_cache"`, `"error"`.
- `ts` *(string)* — ISO‑8601 UTC без мікросекунд.

Опційні поля:

- `last_bar_close_ms` *(int)* — останній відомий close_time (ms).
- `next_open_utc` *(string)* — ISO‑8601 UTC (коли ринок закритий).
- `sleep_seconds` *(float)* — планована пауза до наступного циклу.
- `context` *(object)* — детальний контекст (див. 4.2).

### 4.2 `context` (детальна діагностика)

`context` залежить від режиму `mode`:

- `mode="stream"` — цикл активний.
- `mode="idle"` — календар/ринок закритий або тимчасово недоступний.
- `mode="warmup_sample"` — POC warmup.

Поля, які зараз формує [stream_fx_data](../connector.py) (частина опційна):

- `channel` *(string|null)* — куди публікується heartbeat.
- `mode` *(string)* — `stream|idle|warmup_sample`.
- `calendar_open` *(bool)*
- `ticks_alive` *(bool)*
- `effective_market_open` *(bool)*
- `redis_connected` *(bool)*
- `redis_required` *(bool)*
- `redis_channel` *(string)* — зараз `"fxcm:ohlcv"`.
- `poll_seconds` *(int)*
- `lookback_minutes` *(int)*
- `publish_interval_seconds` *(int)*
- `cycle_seconds` *(float)* — тривалість циклу.
- `stream_targets` *(array[object])* — `[{"symbol": "XAU/USD", "tf": "m1", "staleness_seconds"?: float}, ...]`.
- `published_bars` *(int)* — скільки барів видано за цикл.
- `cache_enabled` *(bool)*
- `lag_seconds` *(float, optional)* — лаг до `last_bar_close_ms`.

Поля, специфічні для idle:

- `idle_reason` *(string)* — причина паузи (поточні значення: `calendar_closed`, `ticks_stopped_after_calendar_close`, `fxcm_backoff`, `fxcm_temporarily_unavailable`, ...).
- `next_open_seconds` *(float)* — таймер до наступного відкриття.

Додаткові блоки (включаються за наявності відповідних підсистем):

- `session` *(object)* — як у `fxcm:market_status` (SessionContextPayload).
- `price_stream` *(object)* — метадані `PriceSnapshotWorker.snapshot_metadata()` (канал, інтервал, `tick_silence_seconds`, per-symbol state, ...).
- `tick_cadence` *(object)* — snapshot `TickCadenceController`.
- `history` *(object)* — snapshot `HistoryQuota`.
- `diag.backoff` *(object)* — backoff-snapshots для FXCM/Redis.
- `supervisor` / `async_supervisor` *(object)* — діагностика AsyncStreamSupervisor (черги, задачі, лічильники).
- `history_backoff_seconds`, `redis_backoff_seconds` *(float, optional)* — залишок паузи.

## 5. Канал `fxcm:status`

**Призначення:** публічний агрегований статус для сторонніх систем.

**Producer:** `_publish_public_status()` (генерується з heartbeat/market_status).

Докладніше: [docs/fxcm_status.md](fxcm_status.md)

### 5.1 Повідомлення

- `ts` *(float)* — UNIX timestamp (секунди).
- `process` *(string)* — `stream|idle|sleep|error`.
- `market` *(string)* — `open|closed|unknown`.
- `price` *(string)* — `ok|stale|down`.
- `ohlcv` *(string)* — `ok|delayed|down`.
- `note` *(string)* — короткий підсумок для людини.
- `session` *(object, optional)* — “людський” зріз сесії:
  - `name` *(string)*
  - `tag` *(string)*
  - `state` *(string)* — `open|preopen|closed|unknown`
  - `current_open_utc`, `current_close_utc`, `next_open_utc` *(string, optional)*
  - `seconds_to_close`, `seconds_to_next_open` *(float, optional)*
  - `state_detail` *(string, optional)* — з’являється лише коли `state="closed"`: `intrabreak|overnight|weekend`.
