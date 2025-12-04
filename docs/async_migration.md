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

### Stage 3 — Оптимізація `get_history` та tick-driven логіка

Stage 3 зосереджується на обмеженні навантаження на FXCM та кращому виявленні деградацій, спираючись на інфраструктуру Stage 1/2.

#### 3.1. Throttle `get_history`

- Конфіг `MAX_HISTORY_CALLS_PER_MIN` (наприклад, 30) у runtime_settings.
- FXCM-цикл веде ковзний лічильник викликів `get_history` за 60 секунд.
- При наближенні до стелі:
  - збільшуємо паузу між опитуваннями або пропускаємо цикл;
  - логгуємо WARN + метрику `fxcm_history_throttled_total`;
  - додаємо прапор у heartbeat (`diag.throttled = true`).

#### 3.2. Backoff при помилках FXCM/Redis

- Ввести `backoff_seconds`, який стартує з `poll_seconds` і збільшується експоненційно при помилках (`min=5`, `max=300`).
- Розширити FXCM diagnostics / supervisor панель полями `backoff_seconds` та `last_fxcm_error`.
- Показувати статус `fxcm_backoff_active` у viewer; скидати до базового значення після успішного циклу.

#### 3.3. Tick-driven state та frequency tuning

- Використовувати `tick_silence_seconds` як ключовий сигнал:
  - якщо ринок відкритий, а `tick_silence_seconds > T_idle`, показувати стан `recovering`/`lag` замість «stream ok»;
  - дублювати значення у heartbeat / supervisor (і, за потреби, у Prometheus).
- Поступово зменшувати частоту `get_history` (спочатку для `m5`, потім для `m1`) базуючись на tick-стрімі, щоб не втрачати синхронізацію з ринком.
- Протоколювати зміни частоти, щоб UI бачив поточні налаштування (`poll_seconds_m1`, `poll_seconds_m5`).

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
