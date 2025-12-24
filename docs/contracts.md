# Контракти Redis-каналів (FXCM Connector)

Цей документ — **джерело правди** по форматах JSON-повідомлень, які публікує конектор у Redis PubSub.

> Примітка про ізоляцію середовищ: у прикладах нижче використовується канонічний префікс `fxcm:*`, але в локальній розробці канали можуть бути префіксовані (наприклад, `fxcm_local:*`) через `FXCM_CHANNEL_PREFIX` / `FXCM_*_CHANNEL`. Формат payload-ів при цьому **не змінюється**.

- TypedDict-референс для основних payload-ів: [fxcm_schema.py](../fxcm_schema.py)
- Реальна публікація: [connector.py](../connector.py)
- Валідація OHLCV (інгістор): [fxcm_ingestor.py](../fxcm_ingestor.py)
- Tick→bucket семантика та додаткові OHLCV поля: [docs/TIK_bucket.md](TIK_bucket.md)

## 0. Загальні правила

- Кожне повідомлення — **один JSON-об’єкт** (не масив).
- Кодування: UTF‑8.
- Серіалізація: `json.dumps(..., separators=(",", ":"))` (мінімум пробілів).
- Споживачі **мають ігнорувати невідомі поля** (forward compatibility).
- Продьюсер (конектор) **не має** публікувати “випадкові” нові поля: фактичний список ключів фіксується у [fxcm_schema.py](../fxcm_schema.py) і перевіряється runtime-валідацією перед publish.

## 1. Канал `fxcm:ohlcv`

**Призначення:** основні OHLCV бари (історичний poll або tick-агрегація).

**Producer:** `connector.publish_ohlcv_to_redis()`

**Consumer (валидація):** `fxcm_ingestor.FXCMIngestor.validate_payload()`

### 1.1 Повідомлення (root)

Тип: JSON object.

Обов’язкові поля:

- `symbol` *(string)* — нормалізований символ AiOne_t: прибираємо `/`, робимо `upper` (наприклад, `"XAU/USD" → "XAUUSD"`).
- `tf` *(string)*  таймфрейм (як у публікації): типово нормалізовані `"1m"`, `"5m"`, ...
- `bars` *(array[object])*  масив барів, **відсортований за `open_time` строго зростаюче**.

Опційні поля:

- `source` *(string)*  джерело даних (наприклад, `"stream"` або `"tick_agg"`).
- `sig` *(string)*  HMAC-підпис (якщо увімкнено `FXCM_HMAC_SECRET`).

Фактичні значення `source`, які наразі генерує код конектора:

- `"stream"` — регулярний history-poller у live-циклі.
- `"tick_agg"` — tick→bucket агрегація у 1m/5m (коли `tick_aggregation_enabled=true`).
- `"history_s3"` — публікація warmup-слайсу з кешу у відповідь на SMC-команду `fxcm_warmup` (для не-tick TF).

Важливо про HMAC:

- Підпис рахується **лише** по базовому payload: `{ "symbol", "tf", "bars" }`.
- Якщо `source` присутній у root, він **не** входить у HMAC (це очікувана поведінка поточної реалізації).

### 1.2 Один бар у `bars[]`

Обов’язкові поля (базовий контракт):

- `open_time` *(int)*  UNIX timestamp у **мілісекундах** (UTC).
- `close_time` *(int)*  UNIX timestamp у **мілісекундах** (UTC). Валідація: `close_time >= open_time`.
- `open`, `high`, `low`, `close` *(float)*.
- `volume` *(float)*.

Опційні поля (розширення, можуть бути відсутні):

- `complete` *(bool)*  для tick-агрегації: `true` лише коли бар закритий. Якщо `false`, інгістор **ігнорує** такий бар.

Примітка (live-оновлення):

- У tick-агрегації конектор може публікувати **live-бар** з `complete=false` під час формування хвилини.
- Частота live-оновлень тротлиться на стороні конектора (типово ~0.25s, тобто 200–500 мс діапазон).
- На закритті хвилини обовʼязково публікується фінальний бар з `complete=true`.
- `synthetic` *(bool)*  `true`, якщо бар згенеровано як synthetic-gap заповнення.
- `source` *(string)*  джерело конкретного бару (може дублювати root `source`).
- `tf` *(string)*  таймфрейм бару (якщо потрібно відправляти мульти-TF в одному батчі; зараз використовується опційно).
- Мікроструктурні метрики (якщо присутні у DataFrame):
  - `tick_count` *(int)*
  - `bar_range` *(float)*
  - `body_size` *(float)*
  - `upper_wick` *(float)*
  - `lower_wick` *(float)*
  - `avg_spread` *(float)*
  - `max_spread` *(float)*

### 1.3 JSON-приклади `fxcm:ohlcv`

Мінімальний приклад (stream / без додаткових полів у барі):

```json
{
  "symbol": "XAUUSD",
  "tf": "1m",
  "source": "stream",
  "bars": [
    {
      "open_time": 1765557600000,
      "close_time": 1765557659999,
      "open": 4278.0,
      "high": 4284.0,
      "low": 4277.0,
      "close": 4281.0,
      "volume": 123.0
    }
  ]
}
```

Розширений приклад (tick_agg / з `complete`, `synthetic`, quality-метриками та опційним HMAC):

```json
{
  "symbol": "XAUUSD",
  "tf": "1m",
  "source": "tick_agg",
  "sig": "<hex>",
  "bars": [
    {
      "open_time": 1765557600000,
      "close_time": 1765557659999,
      "open": 4278.0,
      "high": 4284.0,
      "low": 4277.0,
      "close": 4281.0,
      "volume": 123.0,

      "complete": true,
      "synthetic": false,
      "source": "tick_agg",
      "tf": "1m",

      "tick_count": 37,
      "bar_range": 7.0,
      "body_size": 3.0,
      "upper_wick": 1.0,
      "lower_wick": 3.0,
      "avg_spread": 0.05,
      "max_spread": 0.12
    }
  ]
}
```

### 1.4 Snapshot контракту (дозволені ключі)

Це “короткий чек-лист” для code review/інтеграцій: якщо в продьюсері/консьюмері зʼявляється новий ключ — його треба **явно** додати у `fxcm_schema.py` та оновити цей список.

Дозволені ключі root-повідомлення `fxcm:ohlcv`:

```text
required: symbol, tf, bars
optional: source, sig
```

Дозволені ключі для одного бару у `bars[]`:

```text
required: open_time, close_time, open, high, low, close, volume
optional: complete, synthetic, source, tf, tick_count, bar_range, body_size, upper_wick, lower_wick, avg_spread, max_spread
```

## 2. Канал `fxcm:price_tik`

**Призначення:** регулярні снепшоти bid/ask/mid по кожному символу.

**Producer:** `connector.PriceSnapshotWorker` (кожен символ публікується окремим повідомленням).

### 2.1 Повідомлення

Обов’язкові поля:

- `symbol` *(string)*  нормалізований символ (`EURUSD`, `XAUUSD`, ...).
- `bid`, `ask`, `mid` *(float)*.
- `tick_ts` *(float)*  UNIX timestamp (секунди) часу **останнього тика** по цьому символу.
- `snap_ts` *(float)*  UNIX timestamp (секунди) часу **формування снепшота**.

### 2.2 JSON-приклад `fxcm:price_tik`

```json
{
  "symbol": "XAUUSD",
  "bid": 2045.1,
  "ask": 2045.3,
  "mid": 2045.2,
  "tick_ts": 1701600000.0,
  "snap_ts": 1701600003.0
}
```

### 2.3 Snapshot контракту (дозволені ключі)

Дозволені ключі повідомлення `fxcm:price_tik`:

```text
required: symbol, bid, ask, mid, tick_ts, snap_ts
optional: (немає)
```

## 3. Канал `fxcm:market_status`

**Призначення:** стан ринку `open/closed` + календарний зріз.

**Producer:** `_publish_market_status()`

### 3.1 Повідомлення

Обов’язкові поля:

- `type` *(string)*  завжди `"market_status"`.
- `state` *(string)*  `"open"` або `"closed"`.
- `ts` *(string)*  ISO8601 UTC без мікросекунд.
- `session` *(object)*  календарний контекст (див. 3.2).

Поля лише для `state="closed"` (якщо `next_open` відомий):

- `next_open_utc` *(string)*  ISO8601 UTC.
- `next_open_ms` *(int)*  UNIX ms.
- `next_open_in_seconds` *(float)*  скільки секунд до відкриття.

### 3.2 Блок `session`

Контракт відповідає `SessionContextPayload` у [fxcm_schema.py](../fxcm_schema.py).

Типи/поля (частина опційна):

- `tag` *(string)*  тег сесії (або `AUTO`-резолв).
- `timezone` *(string)*  таймзона сесії/календаря.
- `weekly_open`, `weekly_close` *(string, optional)*.
- `daily_breaks` *(array, optional)*  список перерв (формат як у `calendar_snapshot()`).
- `holidays` *(array[string], optional)*.
- `next_open_utc` *(string)*
- `next_open_ms` *(int)*
- `next_open_seconds` *(float)*
- `session_open_utc`, `session_close_utc` *(string, optional)*  якщо сесія резолвиться.
- `stats` *(object, optional)*  агрегована статистика по сесіях/символах (див. SessionStatsTracker у `connector.py`).

## 4. Канал `fxcm:heartbeat`

**Призначення:** технічний стан процесу + розширений контекст для діагностики.

**Producer:** `_publish_heartbeat()` або AsyncSupervisor.

### 4.1 Root поля

- `type` *(string)*  завжди `"heartbeat"`.
- `state` *(string)*  поточний стан циклу: типово `"stream"`, `"idle"`, `"warmup"`, `"warmup_cache"`, `"error"`.
- `ts` *(string)*  ISO8601 UTC без мікросекунд.

Опційні поля:

- `last_bar_close_ms` *(int)*  останній відомий close_time (ms).
- `next_open_utc` *(string)*  ISO8601 UTC (коли ринок закритий).
- `sleep_seconds` *(float)*  планована пауза до наступного циклу.
- `context` *(object)*  детальний контекст (див. 4.2).

### 4.2 `context` (детальна діагностика)

`context` залежить від режиму `mode`:

- `mode="stream"`  цикл активний.
- `mode="idle"`  календар/ринок закритий або тимчасово недоступний.
- `mode="warmup_sample"`  POC warmup.

Поля, які зараз формує [stream_fx_data](../connector.py) (частина опційна):

- `channel` *(string|null)*  куди публікується heartbeat.
- `mode` *(string)*  `stream|idle|warmup_sample`.
- `calendar_open` *(bool)*
- `ticks_alive` *(bool)*
- `effective_market_open` *(bool)*
- `redis_connected` *(bool)*
- `redis_required` *(bool)*
- `redis_channel` *(string)*  зараз `"fxcm:ohlcv"`.
- `poll_seconds` *(int)*
- `lookback_minutes` *(int)*
- `publish_interval_seconds` *(int)*
- `cycle_seconds` *(float)*  тривалість циклу.
- `stream_targets` *(array[object])*  `[{"symbol": "XAU/USD", "tf": "m1", "staleness_seconds"?: float}, ...]`.
- `published_bars` *(int)*  скільки барів видано за цикл.
- `cache_enabled` *(bool)*
- `lag_seconds` *(float, optional)*  лаг до `last_bar_close_ms`.

Поля, специфічні для idle:

- `idle_reason` *(string)*  причина паузи (поточні значення: `calendar_closed`, `ticks_stopped_after_calendar_close`, `fxcm_backoff`, `fxcm_temporarily_unavailable`, ...).
- `next_open_seconds` *(float)*  таймер до наступного відкриття.

Додаткові блоки (включаються за наявності відповідних підсистем):

- `session` *(object)*  як у `fxcm:market_status` (SessionContextPayload).
- `price_stream` *(object)*  метадані `PriceSnapshotWorker.snapshot_metadata()` (канал, інтервал, `tick_silence_seconds`, per-symbol state, ...).
- `tick_cadence` *(object)*  snapshot `TickCadenceController`.
- `history` *(object)*  snapshot `HistoryQuota`.
- `diag.backoff` *(object)*  backoff-snapshots для FXCM/Redis.
- `supervisor` / `async_supervisor` *(object)*  діагностика AsyncStreamSupervisor (черги, задачі, лічильники).
- `history_backoff_seconds`, `redis_backoff_seconds` *(float, optional)*  залишок паузи.

## 5. Канал `fxcm:status`

**Призначення:** публічний агрегований статус для сторонніх систем.

**Producer:** `_publish_public_status()` (генерується з heartbeat/market_status).

Докладніше: [docs/fxcm_status.md](fxcm_status.md)

### 5.1 Повідомлення

- `ts` *(float)*  UNIX timestamp (секунди).
- `process` *(string)*  `stream|idle|sleep|error`.
- `market` *(string)*  `open|closed|unknown`.
- `price` *(string)*  `ok|stale|down`.
- `ohlcv` *(string)*  `ok|delayed|down`.
- `note` *(string)*  короткий підсумок для людини.
- `session` *(object, optional)*  людський зріз сесії:
  - `name` *(string)*
  - `tag` *(string)*
  - `state` *(string)*  `open|preopen|closed|unknown`
  - `current_open_utc`, `current_close_utc`, `next_open_utc` *(string, optional)*
  - `seconds_to_close`, `seconds_to_next_open` *(float, optional)*
  - `state_detail` *(string, optional)* — з’являється лише коли `state="closed"`: `intrabreak|overnight|weekend`.

## 6. Канал `fxcm:commands`

**Призначення:** керуючий канал від SMC (S3) для примусового warmup/backfill, коли SMC бачить, що дані «порожні» або застарілі.

**Producer:** SMC/зовнішня оркестрація.

**Consumer:** `connector.FxcmCommandWorker`.

### 6.1 Повідомлення

Тип: JSON object.

Обов’язкові поля:

- `type` *(string)*  одне з:
  - `"fxcm_warmup"`  прогріти кеш історії та (для не-tick TF) опублікувати warmup-слайс у `fxcm:ohlcv`.
  - `"fxcm_backfill"`  добрати свіжу історію й опублікувати в `fxcm:ohlcv`.
  - `"fxcm_set_universe"`  задати **ефективний** список `(symbol, tf)` для live-стріму (dynamic universe v1).
- `symbol` *(string)*  символ у форматі `XAUUSD` або `XAU/USD` (конектор приймає обидва).
- `tf` *(string)*  таймфрейм у форматі `1m`/`5m`/`1h` або FXCM-форматі `m1`/`m5`/`m60`.

Опційні поля:

- `min_history_bars` *(int)*  для `fxcm_warmup`: скільки барів брати з кешу для публікації (якщо 0  береться дефолт warmup з кеш-менеджера).
- `lookback_minutes` *(int)*  для `fxcm_backfill`: скільки хвилин назад спробувати добрати (якщо 0  дефолт 60).

Для `type="fxcm_set_universe"`:

- `targets` *(array[object])*  список таргетів, які SMC хоче зробити активними. Кожен елемент:
  - `symbol` *(string)*  `XAUUSD` або `XAU/USD`.
  - `tf` *(string)*  `1m/5m/15m/1h/...` або `m1/m5/m15/m60/...`.
- `reason` *(string, optional)*  довільна причина (наприклад, `"smc_focus"`).
- `expires_at_ms` *(int, optional)*  опційний TTL від SMC (у v1 конектор може ігнорувати).

### 6.2 Семантика виконання (важливі інваріанти)

- Якщо `stream_targets` задані, команди для нецільових `(symbol, tf)` **ігноруються**.
- Є rate-limit: не частіше ніж раз на ~2 секунди для ключа `(symbol, tf, type)`.

Dynamic universe v1 (`fxcm_set_universe`):

- Команда застосовується **лише** якщо `dynamic_universe_enabled=true` (інакше — ігнорується з INFO-логом).
- `targets` приводяться до FXCM-форматів (`XAU/USD`, `m1`/`m5`/`m15`/`m60`/`h4`/`d1`).
- Конектор відфільтровує `targets` як перетин із `base_targets` (тобто з дозволеним списком `stream.config`).
- Після фільтрації застосовується ліміт `dynamic_universe_max_targets`.
- Після оновлення universe конектор у наступному циклі оновлює активні символи **без перезапуску процесу**:
  - `PriceSnapshotWorker.set_symbols()`
  - `TickOhlcvWorker.set_symbols()`
  - refresh FXCM OfferTable subscription.

Інваріант 1m/5m (tick-агрегація):

- Якщо увімкнено tick-агрегацію і `tf` належить до {`1m`, `5m`} (або відповідні `m1/m5`), то:
  - `fxcm_warmup` **оновлює лише кеш** і **не публікує** history у `fxcm:ohlcv`.
  - `fxcm_backfill` **ігнорується**.

Для не-tick TF (наприклад `15m`, `1h`, `4h`, `1d`):

- `fxcm_warmup`: після `ensure_ready()` бере слайс із кешу і публікує в `fxcm:ohlcv` з `source="history_s3"`.
- `fxcm_backfill`: викликає `_fetch_and_publish_recent(..., allow_calendar_closed=True)` і публікує у `fxcm:ohlcv`.

Примітка: повідомлення `fxcm:commands` **не мають HMAC-підпису**. HMAC застосовується до `fxcm:ohlcv` (якщо увімкнено секрет).
