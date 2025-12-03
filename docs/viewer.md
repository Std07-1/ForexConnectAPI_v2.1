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

Меню відображається внизу інтерфейсу (`MENU_TEXT`). Поточний режим також видно у блоку "Monitor mode" в summary.

## Панелі та режими

### 0. Timeline

- Сітка подій heartbeat/market-status (символи `S`, `I`, `O`, `C`).
- Показує вікно часу, максимальні/середні прогалини між подіями та легенду.

### 1. Summary

- **FXCM diagnostics:** все з `heartbeat.context` — режим, цикловий час, lag, Redis-стан, publish interval, цілі стріму.
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
| `redis_health_interval` | Як часто викликати `INFO`/`PING` для панелі Redis (секунди). |
| `lag_spike_threshold` | Межа (сек) для інциденту `lag_spike`. |
| `heartbeat_alert_seconds` | Поріг віку heartbeat для алерта `heartbeat_stale`. |
| `ohlcv_msg_idle_warn_seconds` / `ohlcv_msg_idle_error_seconds` | Базові пороги для алерта "немає нових OHLCV". |
| `ohlcv_lag_warn_seconds` / `ohlcv_lag_error_seconds` | Пороги лагу між останнім close та поточним часом. |
| `tf_1m_idle_warn_seconds`, `tf_1m_idle_error_seconds` | Спеціальні пороги для конкретного TF (доступні також для `tf_5m`, і можна додавати власні). |
| `timeline_matrix_rows`, `timeline_max_columns`, `timeline_history_max`, `timeline_focus_minutes` | Контролюють розмірність таймлайну. |

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
