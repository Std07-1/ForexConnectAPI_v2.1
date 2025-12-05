# План переходу на двопотокову async-архітектуру

## 1. Цілі та обмеження

- **Залишити стабільний OHLCV-стрім 1m/5m.** Поточний `_fetch_and_publish_recent` залишається джерелом правди для Redis `fxcm:ohlcv`.
- **Додати живі снепшоти цін раз на 3–5 с.** Створюємо новий канал `fxcm:price_tik`, який публікує останній bid/ask/mid по символах.
- **Не множити `get_history`.** FXCM не любить надмірні запити, тому всі виклики виконуються в одному потоці з throttle та backoff.
- **Чіткий контроль циклів.** Виносимо блокуючі виклики в FXCM-thread, а Redis/метрики/heartbeat — у asyncio event loop із дрібними тасками.
- **Прозора діагностика.** Heartbeat повинен відображати нові стани: `STREAM`, `IDLE`, `DISCONNECTED`, `RECOVERING`.

## 2. Архітектура v2: FXCM-thread + asyncio-loop

### 2.1 Потоки та їх обов'язки

| Потік | Обов'язки |
| --- | --- |
| **FXCM thread** | `login/logout`, усі `get_history`, підписка на quotes/OfferTable, ведення лічильника `MAX_HISTORY_CALLS_PER_MIN`, запис подій у черги. |
| **Asyncio loop thread** | Три основні таски: `history_consumer` (публікація OHLCV), `tick_consumer` (снепшоти кожні 3–5 с), `heartbeat_task` (діагностика, Prometheus, Redis health). Додатково обробляє retry/backoff логіку та логування. |

FXCM-thread не торкається Redis напряму. Всі дані проходять через thread-safe черги.

### 2.2 Канали з'єднання потоків

| Черга | Тип | Виробник → Споживач | Вміст |
| --- | --- | --- | --- |
| `price_queue` | `asyncio.Queue[PriceTick]` або `janus.Queue` | FXCM quotes → `tick_consumer` | Кортеж `(symbol, bid, ask, mid, tick_ts)`. |
| `ohlcv_queue` | `asyncio.Queue[OhlcvBatch]` | FXCM history poller → `history_consumer` | Набори нових барів + метадані (`tf`, `window`). |
| `diag_queue` (опц.) | `asyncio.Queue[DiagEvent]` | FXCM thread → heartbeat | Стани login/errors для оновлення `RECOVERING`/`DISCONNECTED`. |

### 2.3 Діагностичні стани

| Стан | Коли виставляємо |
| --- | --- |
| `STREAM` | Успішно отримуємо нові бари та снепшоти (обидві черги поповнюються). |
| `IDLE` | Ринок закритий або `next_open_seconds > 0`, немає нових tick’ів > N секунд. |
| `DISCONNECTED` | Остання спроба `login` або `get_history` завершилася помилкою, FXCM недоступний. |
| `RECOVERING` | Вдалося перелогінитися, але ще не прийшло жодного нового бару/тику після відновлення. |

Heartbeat/diagnostics повинні логувати поточний стан і timestamp останньої зміни.

## 3. Етапи міграції

### Stage 1 — Tick stream у поточній синхронній архітектурі

#### Що реалізовано

1. **Підписка на FXCM OfferTable.** Використовуємо `forexconnect.Common.subscribe_table_updates`, що будує коректний `O2GTableListenerImpl`. Це усуває попередню проблему `None.subscribe_update(...)` (null-pointer у fxcorepy) і гарантує, що OFFERS-таблиця активна.
2. **PriceSnapshotWorker** живе в окремому потоці, приймає тики через `enqueue_tick` і кожні `price_snap_interval_seconds` (3 с за замовчуванням) публікує останні bid/ask/mid у Redis `fxcm:price_tik`. Формат payload сталий:

    ```json
    {"symbol":"XAUUSD","bid":4209.62,"ask":4210.02,"mid":4209.82,"tick_ts":1764866660.0,"snap_ts":1764866661.0}
    ```

    `tick_ts` — час тика, `snap_ts` — час формування снепшоту; у канал потрапляє максимум один запис на символ за цикл, щоб не перевантажувати Redis.
3. **Heartbeat контекст.** Поле `price_stream` містить повну телеметрію воркера: `state` (`ok/waiting/stale/stopped`), `tick_silence_seconds`, `symbols_state` (per-symbol bid/ask/mid/tick_age), чергу, канал і останні `snap_ts`/`tick_ts`. Також верхній рівень heartbeat отримує `tick_silence_seconds`, тож приймачі бачать глобальну паузу.
4. **Debug viewer (режим PRICE).** В `tools/debug_viewer.py` додано режим `5`, який рендерить панель Live FXCM price, читаючи `price_stream` із heartbeat. Це дозволяє очима бачити середину по XAUUSD/EURUSD та вік останніх тиків.
5. **Guardʼи та діагностика.** Якщо `fxcorepy` або `Common` недоступні, конектор логує зрозуміле повідомлення і вимикає tick stream без крешів. Відписка теж захищена від відсутності `Common.unsubscribe_table_updates`.

#### Як правильно працювати зі стрімом

1. **Перед стартом** переконайся, що `fxcorepy` та офіційний ForexConnect SDK встановлені, а `ForexConnect` працює в режимі table manager (стандартний контекст `with ForexConnect() as fx`).
2. **Запуск конектора** (`python connector.py`) підніме `PriceSnapshotWorker` автоматично, якщо в конфізі `price_stream.enabled = true`. У логах має зʼявитися `FXCM OfferTable підписка активована...`.
3. **Моніторинг:**
    - У heartbeat шукай `context.price_stream.state` і `tick_silence_seconds`. Значення < 10 с означає, що FXCM шле живі тики; > 30 с → потрібно перевірити OfferTable або ринок.
    - У debug viewer натисни `5`, щоб відкрити Live FXCM price: там видно bid/ask/mid та `tick_age` по кожному символу. Панель також підсвічує `waiting/stale`, якщо тики зникли.
4. **Acceptance критерії:** `symbol` у форматі OHLCV (наприклад, `XAUUSD`), у payload присутні `bid/ask/mid`, `tick_ts ≠ snap_ts`, cadence ~3 с без лавини повідомлень. Поточна реалізація відповідає цим вимогам (перевірено на live FXCM).
5. **Відлагодження:** Якщо `fxcm:price_tik` мовчить, перевір логи конектора: повідомлення `forexconnect.Common недоступний` або `OFFERS table ...` вказують на проблеми з SDK. Для глибшого аналізу можна тимчасово збільшити логування `_handle_row` і відстежити, чи приходять події `INSERT/UPDATE`.

### Stage 2 — Перехід на asyncio таски

1. Запустити окремий event loop (`asyncio.run(main())`) або в окремому потоці.
2. Обгорнути поточний history-цикл у продюсер, який кладе бари в `ohlcv_queue` (через `call_soon_threadsafe`).
3. Реалізувати `history_consumer`, `tick_consumer`, `heartbeat_task` як async-функції з власними sleep/backoff.
4. Redis-операції перевести на `aioredis` або `asyncio.to_thread(publish)`.

#### Реалізація станом на 04.12.2025

- Додано `AsyncStreamSupervisor`, який запускає окремий event loop у тлі та має черги `ohlcv`, `heartbeat`, `market_status`, `price_snapshots` з backpressure.
- FXCM-потік тепер може передавати події через sink-и (`ohlcv_sink`, `heartbeat_sink`, `market_status_sink`, `PriceSnapshotWorker.snapshot_callback`). Якщо в `runtime_settings.stream.async_supervisor` виставити `true`, `main()` стартує supervisor і маршрутизує всі публікації через нього.
- Supervisor виконує `publish_ohlcv_to_redis`, `_publish_heartbeat`, `_publish_market_status` та `_publish_price_snapshot` в executor-і, повторно використовує `PublishDataGate`, а при чергах out-of-capacity піднімає `SinkBackpressure`, що повертається у `_fetch_and_publish_recent` та блокує зсув `last_open_time`.
- PriceSnapshotWorker тепер працює у «push-only» режимі, коли задано callback: воркер лише агрегує тики, а публікація відбувається в async-шарі, тож Redis I/O винесено з FXCM-thread.
- Підписка на FXCM OfferTable автоматично оновлюється після кожного reconnection: supervisor через `fx_session_observer` закриває старий listener і створює новий, тому tick-стрім не «сліпне» після відновлення сесії.

#### Acceptance checklist (04.12.2025)

- **Паритет із legacy-потоком.**
  - `fxcm:ohlcv` оновлюється з тим самим лагом, що й до переходу (1–5 с після закриття бару).
  - `fxcm:heartbeat` / `fxcm:market_status` коректно перемикаються між `stream/idle/closed`, не залипають після пауз.
  - `fxcm:price_tik` дотримується `price_snap_interval_seconds`, а viewer показує актуальний `tick_silence_seconds`.
- **Стабільність supervisor-а.**
  - Після однієї торгової сесії `total_enqueued == total_processed`, `total_dropped = 0`, `queue_depth_total ≈ 0`.
  - `backpressure_events = 0` (допускаються поодинокі й зрозумілі випадки).
  - Усі таски знаходяться в стані `running`, `idle_seconds` відображає реальні паузи, без помилок у логах.
- **Деградація та shutdown.**
  - Короткі збої Redis/FXCM приводять лише до спроб reconnect; supervisor і FX-потік не падають, черги не ростуть безконтрольно.
  - `FXCMOfferSubscription` закривається під час shutdown і автоматично пересоздається після reconnection, tick-стрім не втрачається.
  - Команда зупинки (`Ctrl+C` або `python -m connector` exit) завершує supervisor без traceback’ів.

> Висновок: Stage 2 виконано; асинхронний шар стабільний і може вважатися основою для подальшої оптимізації.

### Stage 3 — Оптимізація `get_history`, backoff та tick-driven керування

**Мета Stage 3:** тримати latency < 200 мс/бар без глушіння стріму та UI. Будь-який контроль навантаження має бути неблокуючим: замість пауз у FXCM-thread ми даємо сигнал через телеметрію, щоб оператор бачив, що бюджет вичерпано, а воркер продовжує обслуговувати heartbeat. Кожен підетап завершується acceptance checklist і чіткими сценаріями застосування.

**Принципи Stage 3:**

- Throttle тільки через неблокуючі rate-лімітери (signalling-only), щоб heartbeat і снепшоти не мовчали.
- Будь-яка деградація відображається у heartbeat/context/debug viewer через додаткові поля, а не через тишу в каналах.
- Tick-активність визначає cadence history-полера, але рішення мають бути прозорими та відрендереними в UI.

#### 3.1 Неблокуючий throttle `get_history` та бюджет запитів

**Ціль.** Захистити FXCM API від бурстів `get_history`, не стопорячи цикл та не вимикаючи heartbeat. Ми збираємо дані про бюджет і виносимо попередження в телеметрію, щоб оператор міг втрутитися.

**Сценарії:**

- High-volatility (NFP): history-полер може спробувати зробити 40+ викликів/хвилину. Маємо зменшити cadence, але FXCM-thread продовжує штовхати heartbeat і тики.
- Тривала нічна сесія: бюджет не використовується, throttle пасивний, метрики показують запас.

**Механіка:**

- Конфіг `runtime_settings.json` піднято до `history.max_calls_per_min=120`, `history.max_calls_per_hour=2400`, `min_interval_by_tf={"1m":2.5,"5m":7.0}`. Окремо визначено `history.priority_targets` (XAUUSD/EURUSD для обох таймфреймів) та резерви `priority_reserve_per_min=1`, `priority_reserve_per_hour=12`.
- Порогові значення для `min_interval`, WARN і резервів задаються окремо через `stream.history_load_thresholds` (`min_interval`, `warn`, `reserve`, `critical`). За замовчуванням: 0.45 / 0.7 / 0.7 / 0.9, але їх можна змінити без редагування коду.
- `HistoryQuota` підтримує ковзні вікна, мінімальні інтервали по таймфрейму й «золоту» чергу: поки резерв не виконано, non-priority виклики блокуються з причиною `priority_reserve`, але цикл продовжує heartbeat/тики.
- Активація `min_interval`/`priority_reserve` прив'язана до `load_ratio = calls/limit` та конфігурованих порогів: нижче `history_load_thresholds.min_interval` — усі виклики проходять, між `min_interval` та `reserve` — `min_interval` діє лише на non-priority, вище `reserve` — додаються резерви та скорочення non-priority, а `critical` стримує WARN.
- Конфіг прив'язаний до конкретних ключів у `config/runtime_settings.json → stream`:
  - `history_priority_targets` — список `SYMBOL:tf` (наприклад, `"XAU/USD:m1"`). Підтримуються ті ж форматери, що й у `stream.config`; парситься через `_parse_priority_targets_setting` у `config.py` і передається в `FXCMConfig.history_priority_targets`.
  - `history_priority_reserve_per_min` / `_per_hour` — числовий резерв викликів, який завжди залишається для пріоритетів, коли load >70 %.
    - `history_min_interval_seconds_m1` / `_m5` задають базовий `min_interval_by_tf`; його застосування залежить від load. Якщо потрібно інший таймфрейм, можна додати новий ключ і відразу отримати підтримку в `HistoryQuota` (використовує `_tf_key`).
    - `history_load_thresholds` — обʼєкт із порогами `min_interval/warn/reserve/critical`. Їх змінюємо, коли потрібно раніше/пізніше вмикати `min_interval`, попередження та CRITICAL.
    - `history_max_calls_per_min` / `_per_hour` задають глобальні вікна. Важливо, що всі похідні розрахунки (`load`, пороги з `history_load_thresholds`) беруть саме ці значення, тому при зміні лімітів переглядаємо і пороги.

> **Останні зміни (2025‑12‑05).** Пороги `history_load_thresholds` тепер задаються виключно через runtime-конфіг; значення автоматично обмежуються діапазоном `[0, 1]`. Рекомендовано дотримуватися порядку `min_interval ≤ warn ≤ reserve ≤ critical ≤ 1`, щоби state-машина `HistoryQuota` працювала прогнозовано й `throttle_state` синхронізувався з фактами.

- Алгоритм `HistoryQuota.allow_call` працює так:
    1. Обчислюємо `load_ratio = max(calls_60s/limit_60s, calls_3600s/limit_3600s)`.
    2. Якщо load < `history_load_thresholds.min_interval` — працюємо в «зеленій» зоні: `min_interval` та резерви вимкнено, усі пари рівні; відмова можлива лише при реальному перевищенні `minute_quota/hour_quota`.
    3. Якщо `min_interval` ≤ load < `reserve` — `min_interval` застосовується лише до non-priority. Пріоритетні символи (з `history_priority_targets`) ігнорують локальні обмеження, але всі продовжують рахуватися у глобальну квоту.
    4. Якщо load ≥ `reserve` — додається `priority_reserve`: non-priority можуть відхилятися з причиною `priority_reserve`, щоб зберегти ресурс для пріоритетів, доки ті не досягнуть свого резерву. Паралельно `min_interval` продовжує діяти на non-priority.
    5. При load ≥ `critical` та реальній `minute_quota`/`hour_quota` відмові state переходить у `critical` і зʼявляються WARN-логи.
    6. Всі deny-рішення реєструються через `register_skip`, і heartbeat/viewer отримують `skipped_polls` із причиною (`min_interval`, `priority_reserve`, `minute_quota`, `hour_quota`).
- Supervisor і стрім логує `History quota guard triggered … reason=… wait=…`, щоб бачити реальні причини пауз та швидко змінювати налаштування.

**Телеметрія:**

- `heartbeat.context.history` тепер містить `calls_60s`, `calls_3600s`, `throttle_state`, `next_slot_seconds`, `priority` (targets, reserve, priority calls) та `last_denied_reason`.
- Debug viewer (режим 6) показує таблицю «History budget» з резервом, останньою причиною відмови та топ-`skipped_polls` для швидкого дебагу.
- Логи `FXCM history throttle` рейт-лімітяться (12 с на пару) й переходять у WARN тільки для `minute_quota`/`hour_quota`; `min_interval` видно як INFO та не впливає на `state`.

**Acceptance:**

- Stage відпрацьований, якщо після 10 хвилин живого ринку (20+ інструментів) `calls_60s <= 120`, `priority.calls_60s` не опускається до нуля, а Heartbeat продовжує виходити щосекунди.
- Логи містять `FXCM history throttle (priority_reserve)` тільки коли намагаємося витиснути все з non-priority; промахи за годинним бюджетом видно через `reason=minute_quota|hour_quota`.
- Автотести: `tests/test_stream.py::HistoryQuotaTest::test_sliding_windows_limit_calls` та `tests/test_stream.py::HistoryQuotaTest::test_priority_reserve_blocks_non_priority` підтверджують ковзні вікна й роботу резерву.
- WARN/CRITICAL у Heartbeat з'являються лише при фактичних quota-deny; `min_interval` залишається у `throttle_state=ok`, що можна перевірити через `tests/test_stream.py::HistoryQuotaTest::test_min_interval_and_skipped_snapshot`.

> **Статус Stage 3.1:** вимоги прийнято, throttling повністю конфігурується через `runtime_settings.json`. Далі переходимо до Stage 3.2 (backoff FXCM/Redis) — див. наступний розділ.

#### 3.2 Backoff при збоях FXCM/Redis

**Ціль.** Відреагувати на збої без лавини WARN/ERROR: ми сповільнюємося лише там, де є помилка, а UI бачить причину через metadata.

**Основні сценарії:**

- FXCM тимчасово повертає `NetworkError`: history-полер пропускає один цикл, backoff зростає до 180 с, але heartbeat і тики не замовкають.
- Redis канал недоступний або publish зависає: supervisor не робить блокуючий sleep, а сигналізує про паузу, накопичує події й автоматично відновлює публікацію.

**Архітектура backoff.**

1. Вводимо `BackoffState` та `FxcmBackoffController` у `connector.py`. Контролер зберігає `active`, `sleep_seconds`, `last_error_type`, `fail_count`, `last_error_ts`, підраховує експоненційну паузу `sleep = min(max_s, current * multiplier)` і додає `± jitter`.
2. `fail(err_ctx)` повертає рекомендовану затримку й фіксує `last_error_type` (`fxcm_history`, `redis`).
3. `success()` скидає стан до базових значень.
4. Є два екземпляри: `fxcm_history_backoff` (працює у FXCM-thread) та `redis_backoff` (у supervisor).

**Неблокуюча інтеграція.**

- Для history полера підтримуємо `next_history_allowed_ts`. Коли `fail()` повертає паузу, оновлюємо цей timestamp і просто пропускаємо `_fetch_and_publish_recent`, поки час не мине. Heartbeat/price-stream продовжують роботу без `time.sleep` у головному циклі.
- Для supervisor усе публікування Redis проходить через єдиний helper. При `redis.ConnectionError` викликаємо `redis_backoff.fail("redis")`, виставляємо `redis_paused_until`, повертаємо керований статус (наприклад, `False`), але не блокуємо event loop. Після першої успішної публікації — `success()`.

**Телеметрія та UI.**

- Heartbeat `context.diag.backoff` міститиме зріз по кожному контролеру:

    ```json
    "diag": {
        "backoff": {
            "fxcm_history": {
                "active": true,
                "sleep_seconds": 45.0,
                "last_error_type": "NetworkError",
                "fail_count": 3,
                "last_error_ts": "2025-12-05T12:00:01Z"
            },
            "redis": {
                "active": false,
                "sleep_seconds": 5.0,
                "last_error_type": null,
                "fail_count": 0,
                "last_error_ts": null
            }
        }
    }
    ```

- Debug viewer (режим 5/6, опційно новий режим 7) показує банер: «FXCM BACKOFF 45 s (NetworkError ×3)», «REDIS BACKOFF 15 s (ConnectionError)».
- Prometheus: нові метрики `fxcm_backoff_events_total{type}` та `fxcm_backoff_sleep_seconds` (гистограма/summary) для обох контролерів.

**Конфігурація.**

- `runtime_settings.stream.history_backoff` і `runtime_settings.stream.redis_backoff` містять `base_seconds`, `max_seconds`, `multiplier`, `jitter` (дефолти: 5/180/2.0/0.15 для FXCM, 5/60/2.0/0.15 для Redis).
- `config.py` додає відповідні dataclass-поля, `FXCMConfig` зберігає параметри, `tests/test_config.py` перевіряє дефолт і override.

**Тестування.**

1. Юніт-тести `test_backoff_state_toggle`, `test_backoff_caps_at_max` покривають контролер.
2. Інтеграційний тест history: мок `_fetch_and_publish_recent` кидає `IOError`, перевіряємо, що стрім не падає, а `diag.backoff.fxcm_history.active` стає `True` і sleep не перевищує `max`.
3. Інтеграційний тест Redis: змокати supervisor publish для підняття `ConnectionError`, переконатися, що процес не завершується і `redis_backoff` сигналізує у heartbeat.
4. Прогін `pytest tests/test_stream.py tests/test_config.py` + ручний live-тест із інʼєкцією помилки для валідації телеметрії.

**Acceptance.**

- При інʼєкції `IOError` backoff зростає ≤ `max_seconds`, `diag.backoff` відбиває активний стан, після відновлення повертається до `inactive` протягом ≤ 1 циклу.
- Redis outage не зупиняє supervisor, publish-операції автоматично повторюються після паузи, `redis_backoff` видно у heartbeat та viewer.

#### 3.3 Tick-driven state та адаптивна частота опитування

**Ціль.** Використовувати тики як ground truth для прийняття рішень щодо cadence `get_history`, роблячи ці рішення прозорими для UI.

**Сценарії:**

- Відкритий ринок + silence > 12 с: історичний полер переходить у `lag` та збільшує `poll_seconds`, поки не повернеться tick-активність.
- Закритий ринок або свято: `calendar_overrides` підказує, що можна перевести обидва таймфрейми у «sleep mode» (20+ с), не витрачаючи бюджет запитів.

**Механіка:**

- `TickHealthMonitor` читає `tick_silence_seconds`, `symbols_state`, календар. Він виставляє `stream_state.prices = ok|lag|stale`, `stream_state.history = active|paused`.
- `poll_seconds_m1`/`poll_seconds_m5` стають динамічними (мінімум 2/3 с, максимум 20 с). Зміни логуються (`INFO TickHealth: m1 poll 3.0 -> 6.0 (reason=idling)`).
- Рішення про паузу застосовуються через supervisor (наприклад, `history_scheduler.update_cadence()`), а не через sleep у FXCM-thread.

**Телеметрія:**

- Heartbeat `price_stream.adaptive_poll` з `current_m1`, `current_m5`, `target_reason`, `tick_silence_seconds`.
- Viewer режим 5 відображає «Adaptive cadence»; QA може побачити причину зміни cadence без вилазок у логи.

**Acceptance:**

- Реальний відкритий ринок (silence <5 с) → `prices=ok`, cadence повертається до мінімуму за ≤2 цикли.
- Симульоване закриття (silence >60 с + календар «closed») → `prices=stale`, cadence ≥20 с, `history` позначено як `paused`.
- Тести: `tests/test_stream.py::test_tick_health_drives_state`, `tests/test_stream.py::test_adaptive_poll_boundaries`.

> Після завершення Stage 3 ми отримуємо саморегульовану систему: FXCM не перевантажується, деградації видно в viewer, а cadence коригується динамічно без глушіння стріму.

## 4. Технічний план

### 4.1 Структури даних

- `PriceTick = Tuple[str, float, float, float, float]`
- `price_queue: asyncio.Queue[PriceTick]` — використовуємо `Janus`, щоб отримати `.sync_q` у FXCM-thread.
- `OhlcvBatch = Tuple[str, str, List[Dict[str, Any]], float]` — symbol, tf, список барів, timestamp вибірки.
- `ohlcv_queue: asyncio.Queue[OhlcvBatch]` — аналогічно через `Janus`.

### 4.2 Redis-канали

- `fxcm:ohlcv` — без змін, публікуємо нові бари.
- `fxcm:price_tik` — новий канал для снепшотів; формат як у Stage 1.
- `fxcm:heartbeat` / `fxcm:market_status` — ті самі, але з додатковими полями про tick-стрім (останній `price_snap_ts`, `tick_silence_seconds`).

### 4.3 Псевдокод `tick_consumer`

```python
async def tick_consumer(price_queue, redis_client, interval_s: float = 3.0) -> None:
    last_by_symbol: dict[str, PriceTick] = {}
    while True:
        try:
            while True:
                symbol, bid, ask, mid, tick_ts = price_queue.get_nowait()
                last_by_symbol[symbol] = (symbol, bid, ask, mid, tick_ts)
        except asyncio.QueueEmpty:
            pass
        if last_by_symbol:
            snap_ts = time.time()
            payloads = [
                {
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "tick_ts": tick_ts,
                    "snap_ts": snap_ts,
                }
                for symbol, (_, bid, ask, mid, tick_ts) in last_by_symbol.items()
            ]
            await publish_price_snapshots(redis_client, payloads)
        await asyncio.sleep(interval_s)
```

### 4.4 Псевдокод `history_consumer`

```python
async def history_consumer(ohlcv_queue, redis_client, gate) -> None:
    while True:
        symbol, tf, bars, polled_at = await ohlcv_queue.get()
        new_bars = gate.filter(symbol, tf, bars)
        if new_bars:
            await publish_ohlcv_async(redis_client, symbol, tf, new_bars)
            update_metrics(symbol, tf, new_bars)
```

### 4.5 Throttle та backoff

- `MAX_HISTORY_CALLS_PER_MIN = 30` (налаштовується). FXCM-thread веде лічильник у ковзному вікні; при перевищенні — `sleep(throttle_delay)` і WARN у heartbeat.
- `backoff_s = min(backoff_s * 2, 300)` при помилках FXCM. Після успішного циклу — `backoff_s = 5`.
- Окремий лічильник для tick-помилок; при зникненні quotes > 60 с — переводити стан у `RECOVERING`.

### 4.6 Вимоги до снепшотів

- Інтервал публікації: 3 с за замовчуванням, допускається 5 с (конфіг `price_snap_interval_seconds`).
- У payload обов'язково `tick_ts` (час останнього bid/ask) й `snap_ts` (час формування пакета).
- Для кожного символу публікується лише останній стан, щоб не роздувати канал.
- `publish_price_snapshots` має бути ідпотентним і захищеним від відсутніх символів (ігнорує порожні payloads).

---

Цей документ є джерелом правди: під час реалізації кожен крок має відсилатися до відповідного розділу (`Stage 1/2/3`, стани діагностики, черги). Будь-які відхилення потрібно спочатку задокументувати тут, а вже потім змінювати код.
