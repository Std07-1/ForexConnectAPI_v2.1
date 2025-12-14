# Debug viewer (tools/debug_viewer.py)

FXCM debug viewer — це консольний live-dashboard, який читає heartbeat/market_status/OHLCV з Redis і допомагає оперативно
діагностувати збої стріму, лаги та календарні паузи. Він оновлює інцидент-стрічку, малює таймлайн станів та підсвічує алерти
відповідно до таймфреймів.

## Швидкий старт

```powershell
python tools/debug_viewer.py --redis-host 127.0.0.1 --redis-port 6379
```

- Канали Redis беруться з блоку `viewer.*` у `config/runtime_settings.json`; CLI-прапорці дозволяють тимчасово підмінити host/port.
- Отримані дані зберігаються лише в оперативному стані `ViewerState`, тож запуск безпечний для продакшн Redis.

### Гарячі клавіші

| Клавіша | Дія |
| --- | --- |
| `Q` / `Ctrl+C` | Вихід. |
| `P` або `Space` | Пауза/продовження автооновлення. |
| `C` | Очистити таймлайн. |
| `0` | Таймлайн (режим TIMELINE). |
| `1` | Зведення (SUMMARY). |
| `2` | Сесії (SESSION). |
| `3` | Алерти (ALERTS). |
| `4` | Потоки OHLCV (STREAMS). |
| `5` | Live FXCM price (PRICE). |
| `6` | Async supervisor (SUPERVISOR). |
| `7` | History/backoff (HISTORY). |
| `8` | S1 stats / gaps (STAT). |

Меню відображається внизу інтерфейсу (`MENU_TEXT`). Поточний режим також видно у блоку "Monitor mode" в summary.

## Панелі та режими

### 0. Timeline

- Сітка подій heartbeat/market-status (символи `S`, `I`, `O`, `C`).
- Показує вікно часу, максимальні/середні прогалини між подіями та легенду.

### 1. Summary

- **FXCM diagnostics:** все з `heartbeat.context` — режим, цикловий час, lag, Redis-стан, publish interval, цілі стріму та блок `price_stream` (канал `fxcm:price_tik`, інтервал, останній тик/снепшот, `tick_silence_seconds`).
- **Recent incidents:** останні 24 переходи станів (`fxcm_pause`, `redis_disconnect`, `ohlcv_msg_idle`, тощо) з позначками ACTIVE/CLEAR.

### 2. Session

- Поточні таймінги сесій, timezone, weekly open/close та статистика `stats.symbols` із heartbeat.
- Додатково показує діаграму діапазонів/бейслайнів для кожної сесії (`session_range_snapshot`).

### 3. Alerts / Issue counters

- `build_alerts_panel` виводить активні алерти з severity (danger/warning/info) та timestamp.
- `build_issue_panel` сумує накопичені лічильники по ISSUE_LABELS, включно з останнім часом і деталями.

### 4. Stream/OHLCV

- `build_stream_targets_panel`: стейлнесс/лаг по поточних стрім-таргетах, трендову спарк-лінію та час останнього оновлення.
- `build_ohlcv_panel`: розкладка по символах/TF із lag, msg age, кількістю барів у повідомленні та історією.

### 5. Live FXCM price

- Панель `Live FXCM price` підтягує `price_stream` із heartbeat: стан воркера (`state`), канал, інтервал, queue depth, останні `snap_ts`/`tick_ts` і глобальний `tick_silence_seconds`.
- Блок `Adaptive cadence` (Stage 3.3) витягує `context.tick_cadence`: показує поточний стан тиків (`prices`), стан history-полера (`history`), причину зміни cadence, множник, фактичний `tick_silence_seconds`, мінімальний `next wakeup` і таблицю cadence/next poll для кожного таймфрейму.
- Рядок `Viewer snapshots` рахує, скільки публікацій `fxcm:price_tik` встиг побачити сам viewer від моменту запуску. Це значення також підтягується як fallback для блоку «Спеціальні канали», тому `price` видно навіть без supervisor publish_counts.
- Таблиця нижче показує mid/bid/ask та `tick_age` для кожного символу (XAUUSD, EURUSD тощо) — це допомагає миттєво побачити, коли FXCM перестав присилати живі тики.
- Натисни `5`, щоб перемикатися до цього режиму або повернутися назад через меню внизу.

### 6. Async supervisor

- Новий режим для моніторингу `async_supervisor`: показує підсумковий стан (loop alive, uptime, backpressure, останній publish та помилку).
- Панель «Supervisor: метрики» агрегує counters виробника: total enqueued/processed/dropped, сумарний queue depth, загальну кількість publish-ів, активні таски, помилки та кількість backpressure-подій; кожен рядок має підказку з коротким описом метрики.
- Блок «Спеціальні канали» показує фіксовані sink-и (налаштовуються `viewer.supervisor_channels`) з власними підказками (`viewer.supervisor_channel_hints`), щоби контролювати критичні канали навіть коли вони не в топі.
- Таблиця «Supervisor: черги» відображає глибину кожної sink-черги, місткість, відсоток заповнення, вік останнього enqueue та лічильники processed/dropped.
- Таблиця «Supervisor: таски» показує стан кожного consumer-а (running/idle/error), кількість оброблених подій, помилки та час із моменту останньої активності.
- Якщо heartbeat ще не містить `context.supervisor`, режим підказує ввімкнути `stream.async_supervisor`, щоб діагностика зʼявилася.

### 7. History quota / Backoff

- Режим HISTORY (клавіша `7`, Stage 3.1/3.2) ділиться на дві панелі: «Backoff diag» та «History quota».
- «Backoff diag» читає `context.diag.backoff` із heartbeat і для кожного ключа (наприклад, `fxcm_history`, `redis_stream`) показує стан active/idle, `remaining_seconds`, `sleep_seconds`, лічильник збоїв і останню помилку з timestamp. Це прямий моніторинг Fxcm/Redis backoff-контролерів, тож можна побачити, хто саме притримує стрім.
- «History quota» відображає `context.history`: бюджети `calls_60s/3600s`, кольоровий `throttle_state`, `next_slot_seconds`, останню причину deny та резерви для `history_priority_targets`. Окрема таблиця показує список пріоритетних таргетів і лічильник `skipped_polls` по символах/TF, що допомагає перевірити fairness та резерв Stage 3.1.
- Якщо будь-який блок ще не прийшов у heartbeat, панель показує жовтий хінт із часом останнього оновлення, щоби швидко діагностувати відсутність телеметрії.

### 8. S1 stats / gaps (STAT)

- Режим для валідації поведінки S1 (коли календар може бути закритий, але тики ще «живі»).
- Панель `S1 stats` читає ключові поля з `heartbeat.context`: `calendar_open`, `ticks_alive`, `effective_market_open`, `idle_reason`, `tick_silence_seconds`, а також `history_state` і `tick_cadence` (якщо вони присутні).
- Панель `Top 10 largest gaps` аналізує прогалини між OHLCV-чанками за `open_time/close_time` та показує найбільші розриви (оцінка `missing_bars`/`missing_min`).
- Колонка `sleep_model` класифікує gap як `sleep` або `awake` за порогом `viewer.sleep_model_gap_threshold_minutes`.
- `synth_min` показує, скільки хвилин у межах gap було заповнено synthetic-барами (рахується по `bar.synthetic=true`). Якщо продьюсер не надсилає `synthetic`, значення буде 0.
- Під `Top 10 largest gaps` є компактна панель `S2/S3 статистика`: вона агрегує команди з каналу `fxcm:commands` (warmup/backfill/set_universe) і підтягує короткий зріз `history/backoff/cadence` з heartbeat.

#### Увімкнення tick aggregation (Phase B)

- У файлі `config/runtime_settings.json` → `stream.*`:
  - `tick_aggregation_enabled: true` — вмикає tick→OHLCV агрегацію (джерело `source="tick_agg"`).
  - `tick_aggregation_max_synth_gap_minutes` — максимальний розрив (хв), який дозволено «добивати» synthetic-барами.
- Коли `tick_aggregation_enabled=true`, конектор не публікує FXCM history OHLCV для `1m/5m`, щоб не змішувати два джерела в одному каналі `fxcm:ohlcv`.
- У поточній реалізації конектор публікує у `fxcm:ohlcv` лише `complete=true` бари; live (`complete=false`) не транслюється.

## Інциденти та алерти

Viewer відстежує кілька ключових ситуацій:

- `fxcm_pause` / `calendar_pause` — стан `idle` із відповідними причинами.
- `redis_disconnect` — `context.redis_connected` впав, коли Redis обовʼязковий.
- `lag_spike` — `context.lag_seconds` перевищує `lag_spike_threshold`.
- `ohlcv_msg_idle`, `ohlcv_lag`, `ohlcv_empty` — похідні від `state.ohlcv_targets` та віку останніх повідомлень.

Кожен інцидент потрапляє у `incident_feed` (макс. 24 події), а активні стани дублюються у `alerts`:

- Severity кольорів контролює `ALERT_SEVERITY_COLORS` (danger → червоний, warning → жовтий, info → блакитний).
- Redis health (ping/info) перевіряється інтервалом `redis_health_interval` та виводиться окремою панеллю в режимах ALERTS/STREAMS.

## Конфігурація viewer.*

Файл `config/runtime_settings.json` містить всі ключі для viewer. Найважливіші:

| Ключ | Опис |
| --- | --- |
| `heartbeat_channel`, `market_status_channel`, `ohlcv_channel` | Назви Redis-каналів для підписки. Порожній `ohlcv_channel` вимикає каналу. |
| `commands_channel` | (optional) Канал команд (за замовчуванням `fxcm:commands`). Порожній рядок вимикає підписку. |
| `redis_health_interval` | Як часто викликати `INFO`/`PING` для панелі Redis (секунди). |
| `lag_spike_threshold` | Межа (сек) для інциденту `lag_spike`. |
| `heartbeat_alert_seconds` | Поріг віку heartbeat для алерта `heartbeat_stale`. |
| `ohlcv_msg_idle_warn_seconds` / `ohlcv_msg_idle_error_seconds` | Базові пороги для алерта "немає нових OHLCV". |
| `ohlcv_lag_warn_seconds` / `ohlcv_lag_error_seconds` | Пороги лагу між останнім close та поточним часом. |
| `tf_1m_idle_warn_seconds`, `tf_1m_idle_error_seconds` | Спеціальні пороги для конкретного TF (доступні також для `tf_5m`, і можна додавати власні). |
| `timeline_matrix_rows`, `timeline_max_columns`, `timeline_history_max`, `timeline_focus_minutes` | Контролюють розмірність таймлайну. |
| `supervisor_channels` | Масив назв sink-каналів, які треба завжди показувати в панелі Supervisor Metrics (наприклад `["ohlcv", "heartbeat", "price"]`). |
| `supervisor_channel_hints` | (optional) словник `"channel": "опис"`, яким можна перевизначити підказки для спеціальних каналів. |
| `sleep_model_gap_threshold_minutes` | Поріг (хв) для класифікації OHLCV gap як `sleep`/`awake` у режимі STAT. |

фрагмент:

```json
  "viewer": {
    "heartbeat_channel": "fxcm:heartbeat",
    "market_status_channel": "fxcm:market_status",
    "ohlcv_channel": "fxcm:ohlcv",
    "commands_channel": "fxcm:commands",
    "redis_health_interval": 8,
    "lag_spike_threshold": 180,
    "heartbeat_alert_seconds": 45,
    "ohlcv_msg_idle_warn_seconds": 90,
    "ohlcv_msg_idle_error_seconds": 180,
    "ohlcv_lag_warn_seconds": 45,
    "ohlcv_lag_error_seconds": 120,
    "sleep_model_gap_threshold_minutes": 60,
    "timeline_matrix_rows": 10,
    "timeline_max_columns": 120,
    "timeline_history_max": 2400,
    "timeline_focus_minutes": 30,
    "supervisor_channels": ["ohlcv", "heartbeat", "price"],
    "supervisor_channel_hints": {
      "price": "fxcm:price_tik pipeline"
    }
  }
```

## Публічний статус-канал `fxcm:status`

Конектор відтепер транслює окремий канал `fxcm:status` з агрегованим станом (`process`, `market`, `price`, `ohlcv`, `session`, `note`). Він призначений для сторонніх таблиць/дашбордів і дублюється кожного разу, коли оновлюється heartbeat або market_status. Viewer НЕ підписується на цей канал, аби не дублювати інформацію: усі діагностичні панелі й далі читають сирі `heartbeat`, `market_status`, `ohlcv`. Якщо потрібен публічний зріз, клієнт може просто показати:

- `process`: stream/idle/sleep/error;
- `market`: open/closed/unknown;
- `price`: ok/stale/down (по снепшотам `fxcm:price_tik`);
- `ohlcv`: ok/delayed/down (за лагом барів);
- `session`: людське ім'я + таймери `seconds_to_close`/`seconds_to_next_open`;
- `note`: короткий текст (`ok`, `idle: calendar_closed`, `backoff FXCM 30s`, ...).

Канал можна перейменувати через `FXCM_STATUS_CHANNEL` або `stream.status_channel`, але він завжди залишається «публічним» SPI для консюмерів, тоді як всі інші канали (`fxcm:heartbeat`, `fxcm:market_status`, `fxcm:ohlcv`) залишаються внутрішніми для AiOne/SMC та viewer.

### TF-специфічні пороги

Функція `resolve_idle_thresholds` шукає ключ `tf_<label>_idle_warn_seconds` / `_error_seconds`. Якщо їх немає, порогові
значення обчислюються пропорційно довжині таймфрейму (`1.5x` та `3x` від тривалості свічки), але не нижче глобальних базових.
Це дозволяє налаштовувати режим, коли наприклад `m5` може простоювати кілька хвилин без алертів, а `m1` — значно чутливіше.

## Prometheus-мітрики viewer

Якщо `prometheus_client` доступний, viewer оновлює додаткові метрики:

- `ai_one_fxcm_ohlcv_lag_seconds{symbol,tf}` — lag між останньою свічкою та *now*.
- `ai_one_fxcm_ohlcv_msg_age_seconds{symbol,tf}` — вік останнього OHLCV payload.
- `ai_one_fxcm_ohlcv_gaps_total{symbol,tf}` — скільки разів з'являвся idle/lag для комбінації symbol/tf.

Ці метрики віддзеркалюють локальне бачення delay з боку споживача, тож допомагають розрізнити проблеми каналу vs продьюсера.

## Типові сценарії використання

1. **Латентність під час торгів:** увімкни режим STREAMS (`4`) і відсортуй по стовпцю "Lag (s)" — побачиш, які символи відстають.
2. **Календарна пауза:** у summary побачиш `Idle reason = calendar_closed`, а інцидентна панель покаже `calendar_pause`.
3. **Проблеми з Redis:** алерт `redis_disconnect` спливе, Redis панель підсвітиться червоним, а issue counter наросте.
4. **Діагностика сесій:** режим SESSION покаже, чи не просів діапазон `LDN_METALS` порівняно з baseline (баровий індикатор).

## Тести

- Юніт-тести для viewer логіки: `pytest tests/test_debug_viewer.py`.
- Покривають інцидентний фід, TF-aware пороги та резолвінг налаштувань.

## Додаткові нотатки

- Viewer не вимагає ForexConnect SDK; достатньо Redis із heartbeat та OHLCV повідомленнями.
- Логування Rich/Console вже налаштовано; для headless середовищ можна проганяти у tmux/screen з тим самим CLI.
- Якщо потрібно бачити лише телеметрію, можна запустити viewer на DR-сервері з read-only доступом до Redis.
