# FXCM Connector

> Легковаговий стрімер OHLCV барів із FXCM (ForexConnect API) до Redis з валідацією HMAC, файловим кешем та продакшн-орієнтованим моніторингом.

**Версія релізу:** береться з файлу `VERSION` (виводиться на старті як `FXCM_CONNECTOR_VERSION`).

- Redis-повідомлення `fxcm:ohlcv` підписуються HMAC і перевіряються `fxcm_ingestor` перед записом у UnifiedStore.
- Idle-heartbeat підтягує останній close з кешу, тож UI бачить лаг і `next_open` навіть коли ринок спить.
- Файловий кеш має graceful fallback у read-only режим, піднімаючи `fxcm_connector_errors_total{type="cache_io"}` при будь-якому FS-проколі.

Документ описує повний життєвий цикл конектора: установку, конфіг, деплой, моніторинг і розробку.

---

## Що нового (Stage 3 · грудень 2025)

Stage 3 закрито повністю: усі три підпроекти пройшли рев'ю, мають тести й описані контракти.

- **Stage 3.1 — HistoryQuota / неблокуючий throttle.** Квоти на `get_history` тепер запобігають бурстам без зупинки стріму: ковзні вікна, `history_priority_targets`, мінімальні інтервали по TF та резерв пріоритетів конфігуруються у `runtime_settings.json`, а телеметрія (`context.history`, viewer HIST-панель) показує `throttle_state`, причини deny й `skipped_polls`.
- **Stage 3.2 — Backoff для FXCM / Redis.** FxcmBackoffController і Redis-backoff сповільнюють лише проблемні підсистеми: history-полер чекає без блокування heartbeat, supervisor паузить publish-и при помилках Redis, а стан видно у `diag.backoff` і viewer Backoff-панелі. Тести покривають state/ceil та інтеграцію в стрім.
- **Stage 3.3 — Tick-driven cadence + auto-sleep.** TickCadenceController підлаштовує `poll_seconds` і `sleep_seconds` під реальну тикову активність: режими live/idle, множники та причини (`reason`) передаються в heartbeat (`context.tick_cadence`) і виводяться у viewer (Adaptive cadence у PRICE/SUPR). HistoryQuota працює поверх адаптивного cadence, а fairness-планувальник гарантує, що жоден TF чи символ не голодує навіть під високим навантаженням, тож latency <200 мс/бар зберігається без ручних втручань.
- **SMC-команди — `fxcm:commands` (warmup/backfill).** Конектор підписується на канал команд і виконує `fxcm_warmup`/`fxcm_backfill` за політикою SMC; інваріант збережено: **1m/5m live OHLCV — лише з tick_agg**, history-публікація для `1m/5m` не вмикається.
- **Dynamic stream universe v1.** Конектор може працювати або зі статичним `stream.config`, або в режимі “SMC-driven universe”, коли SMC надсилає `fxcm_set_universe` і задає ефективний піднабір `(symbol, tf)` для live-стріму.

## 1. Призначення

Конектор логіниться в FXCM через ForexConnect, витягує OHLCV-бари, нормалізує їх до AiOne_t-схеми та транслює у Redis:

> У документації нижче використовується продакшн-префікс каналів `fxcm:*`. Для ізоляції локальної розробки можна змінити префікс через `FXCM_CHANNEL_PREFIX` (наприклад, `fxcm_local:*`).

- `fxcm:ohlcv` — основні дані (із опційним HMAC-підписом `sig`).
- `fxcm:market_status` — події `open/closed` з `next_open_utc`, `next_open_ms`, `next_open_in_seconds` та блоком `session` (таймзона, години роботи, перерви).
- `fxcm:heartbeat` (налаштовується) — технічний стан процесу з розширеним `context` (канал, Redis-стан, лаг, стрім-таргети, причина паузи, тривалість циклу тощо).
- `fxcm:price_tik` — снепшоти останнього bid/ask/mid по кожному символу зі штампами `tick_ts` (останній тик) та `snap_ts` (час формування пакета).
- `fxcm:status` — публічний агрегований стан для зовнішніх систем (process / market / price / ohlcv / session / note), щоб консюмери бачили готовність конектора без знання heartbeat.
- `fxcm:commands` — **канал керування (SMC)**: конектор лише **споживає** команди `fxcm_warmup`/`fxcm_backfill` (див. `docs/contracts.md`).
  Також підтримується команда `fxcm_set_universe` (dynamic universe v1), яка задає ефективний список таргетів для live-стріму.

  Формат повідомлення:

  ```json
  {"type":"fxcm_warmup","symbol":"XAUUSD","tf":"1m","min_history_bars":1000}
  ```

Локальний файловий кеш (`cache/` або зовнішня директорія) мінімізує холодний старт, а Prometheus-метрики дають спостережність.

### 1.1 Контракти каналів (зведено)

| Канал | Producer | Частота | Основні поля |
| --- | --- | --- | --- |
| `fxcm:ohlcv` | `connector.publish_ohlcv_to_redis` | За кожен цикл per `stream.config` | `symbol`, `tf`, `bars` (масив `open_time/close_time/open/high/low/close/volume` у мс, float), опційно `source` (`stream`/`tick_agg`/`history_s3`) і `sig` (HMAC). |
| `fxcm:market_status` | `_publish_market_status` | При зміні стану або раз на ≤30 c | `type="market_status"`, `state=open/closed`, `ts`, `next_open_{utc,ms,in_seconds}`, `session` (тег, таймзона, вікна, статистика). |
| `fxcm:heartbeat` | `_publish_heartbeat` / AsyncSupervisor | Кожен цикл + idle/ warmup | `type="heartbeat"`, `state`, `last_bar_close_ms`, `sleep_seconds`, `context` (Redis, `stream_targets`, `lag_seconds`, `context.history`, `context.tick_cadence`, `context.diag.backoff`, `session`). |
| `fxcm:price_tik` | `PriceSnapshotWorker` | Кожні `stream.price_snap_interval_seconds` сек | `symbol`, `bid`, `ask`, `mid`, `tick_ts`, `snap_ts`; канал вказаний у `stream.price_snap_channel`. |
| `fxcm:status` | `_publish_public_status` | Разом із heartbeat/market_status (~5–10 с) | `ts`, `process`, `market`, `price`, `ohlcv`, `session`, `note`; призначений для зовнішніх споживачів (див. §1.2). |
| `fxcm:commands` | SMC (producer) → `FxcmCommandWorker` (consumer) | За потреби | `type` = `fxcm_warmup` / `fxcm_backfill` / `fxcm_set_universe`. Для `1m/5m` (tick_agg) — warmup лише кеш, backfill ігнорується. |

> Детальні JSON-приклади для heartbeat/market-status див. §9.1, а інгістор очікує рівно ці ключі під час HMAC-перевірки.

> **Повний опис всіх контрактів/TypedDict та каналів:** `docs/contracts.md`.

Додатково: перед публікацією конектор робить runtime-валідацію схеми (див. `validate_*_payload_contract` у `fxcm_schema.py`), щоб під час оновлень не можна було «випадково» змінити формат повідомлень без оновлення контрактів і тестів.

### 1.1.1 Швидка інтеграція для UDS/SMC (ohlcv / tik / status)

Найчастіший сценарій для UDS/SMC — підписатися на 3 канали й застосувати однакові правила фрешності/ідемпотентності:

| Канал | TypedDict (контракт) | Для чого в UDS/SMC | Критичні правила споживання |
| --- | --- | --- | --- |
| `fxcm:ohlcv` | `OhlcvPayload` / `OhlcvBarPayload` | OHLCV для стратегій/індикаторів, запис у сховище, синхронізація з баровим тактом | Ідемпотентність: ключ бару — `(symbol, tf, open_time)`; часові поля **в мс**; якщо `complete=false` — бар **не фінальний** (можна пропускати, так робить інгістор); `sig` (якщо є) перевіряється HMAC над базовим payload `{symbol, tf, bars}`. |
| `fxcm:price_tik` | `PriceTickSnapPayload` | Жива ціна (mid/bid/ask), “останній тик” для UI/алертів, швидкі перевірки spread | Фрешність: використовуй `tick_ts` як “час останнього тика” (event-time), а `snap_ts` як “коли сформовано/опубліковано” (publish-time); допускай пропуски (це снепшоти, не повний tick-stream). |
| `fxcm:status` | `PublicStatusPayload` | Єдина точка truth про готовність конектора: чи ринок open, чи ціна жива, чи OHLCV ок | Не плутай `session.state` і `market`: `market=open/closed/unknown` — агрегований стан; `session.state_detail` з’являється **лише** коли `session.state=closed` і має `intrabreak`/`overnight`/`weekend`; для “можна торгувати?” в першу чергу орієнтуйся на `market` + `price` + `ohlcv`. |

Мінімальна логіка “OK to use data” для UDS/SMC зазвичай виглядає так: `status.market == "open"` і `status.price == "ok"` і `status.ohlcv == "ok"`. Деталі полів та всі опційні ключі — у `docs/contracts.md`.

### 1.2 Публічний статус-канал `fxcm:status`

Для зовнішніх систем тепер достатньо трьох підписок: `fxcm:ohlcv` (бари), `fxcm:price_tik` (жива ціна) і `fxcm:status` (агрегований стан). Канал `fxcm:status` оновлюється разом із heartbeat/market_status приблизно щоп'ять секунд і містить лише «людські» поля, без внутрішніх діагностичних дрібниць:

```json
{
  "ts": 1764867000.0,
  "process": "stream",
  "market": "open",
  "price": "ok",
  "ohlcv": "ok",
  "session": {
    "name": "Tokyo",
    "tag": "TOKYO",
    "state": "open",
    "current_open_utc": "2025-12-05T00:00:00Z",
    "current_close_utc": "2025-12-05T09:00:00Z",
    "next_open_utc": "2025-12-05T09:00:00Z",
    "seconds_to_close": 5400,
    "seconds_to_next_open": 5400
  },
  "note": "ok"
}
```

- `process`: `stream` (активний стрім), `idle` (ринок тихий), `sleep` (авто-сон поза сесією), `error` (критична помилка).
- `market`: `open`, `closed`, `unknown` (якщо ще не отримали FXCM статус).
- `price`: `ok`, `stale` (тиків давно не було), `down` (price feed відсутній).
- `ohlcv`: `ok`, `delayed` (лаг >60 с), `down` (барів немає або процес у паузі).
- `session`: структурований зріз активної сесії (людська назва, тег, відкриття/закриття, таймери до подій). `state_detail` з’являється лише коли `session.state=closed` і має значення `intrabreak`/`overnight`/`weekend`.
- `note`: короткий текст для людей: `ok`, `idle: calendar_closed`, `backoff FXCM 30s`, `price stale` тощо.

Канал можна перевизначити через `FXCM_STATUS_CHANNEL` або `stream.status_channel` у `runtime_settings.json`; дефолт — `fxcm:status`. Усі інші розширені поля (`heartbeat.context`, `diag.backoff`, `history`) залишаються внутрішнім контрактом AiOne/SMC і не потрібні стороннім споживачам.

> **Обов'язкове ознайомлення:** перед підпискою сторонніх систем прочитай `docs/fxcm_status.md`. Файл містить повний опис полів, допустимі стани та приклади споживачів, тож інтеграція без нього вважається порушенням контракту каналу `fxcm:status`.

## 2. Можливості

- Warmup історії + стрім `m1/m5/...` таймфреймів у Redis.
- Торговий календар з урахуванням 24/5, денних перерв і свят (`sessions.py`).
- Автоматичне визначення активної сесії (Tokyo/London/New York) + статистика range/avg по кожній сесії у heartbeat/market-status.
- Прометеус-метрики: опубліковані бари, лаг, стейлнесс, статус ринку, помилки.
- HMAC-підпис/верифікація (спільний `fxcm_security.compute_payload_hmac`).
- Graceful cache fallback: при IO-помилці кеш стає read-only, але стрім триває.
- Конфіг через `.env` + `config/runtime_settings.json`; без прив’язки до CLI аргументів.
- Розширюване тестове покриття (pytest + unittest) з фейковим FXCM/Redis.

## 3. Системні вимоги

- **Python 3.7.x** — бойові середовища (сумісність із ForexConnect SDK).
- **Python 3.10+** — лише локальні тести без реального SDK.
- **ForexConnect SDK** (Windows FXCM дистрибутив із `pyfxconnect`).
- **Redis 6.x+** (локально або remote, підтримується AUTH/ACL).
- **Windows Server / Linux** — перевірено на Windows (prod) + WSL для девелопменту.
- **Prometheus** (опційно) для збору метрик через `start_http_server`.
- **backports.zoneinfo + tzdata** — встановлюються автоматично через `requirements.txt` і потрібні Python 3.7 для коректної роботи торгового календаря.

## 4. Архітектура (високий рівень)

```text
ForexConnect (FXCM SDK)
  └─ connector.py (головний потік)
      ├─ history poller → OHLCV batches (source="stream")
      ├─ OfferTable ticks → PriceSnapshotWorker → price snapshots (fxcm:price_tik)
      ├─ (опц.) TickOhlcvWorker: tick→bucket→1m (+5m з 1m) → OHLCV (source="tick_agg")
      ├─ (опц.) FxcmCommandWorker: subscribe `fxcm:commands` → warmup/backfill тригери (з інваріантом 1m/5m)
      ├─ Prometheus exporter (/metrics)
      └─ Publish layer:
          ├─ sync publish (напряму в Redis)
          └─ або AsyncStreamSupervisor (окремий asyncio loop + черги/backpressure)

fxcm_ingestor → Redis subscriber → (опц.) HMAC verify → UnifiedStore
tools/debug_viewer.py → Redis subscriber → Live UI/діагностика
```

- `connector.py` — CLI + основний цикл (`fetch_history_sample`, `stream_fx_data`).
- `cache_utils.py` — CSV/META серіалізація та merge.
- `sessions.py` — календар, вікна запитів, функції `is_trading_time`, `next_trading_open`.
- `fxcm_ingestor.py` — окремий сервіс, що перевіряє схему та HMAC (описано нижче).

## 5. Установка та перший запуск

1. **Склонуй репозиторій та підготуй .env:**

    ```powershell
    git clone <repo>
    cd fxcm_connector
    copy .env.template .env.local  # локальний профіль (ізольовані канали)
    copy .env.template .env.prod   # прод-профіль (сумісний з fxcm:*)
    ```

    Потім:

    - у `.env.local` вистав `FXCM_CHANNEL_PREFIX=fxcm_local` (або інший префікс) і підстав FXCM креденшали / Redis
    - у `.env.prod` залиш `FXCM_CHANNEL_PREFIX=fxcm` і підстав прод значення
    - створи `.env` як диспетчер (1–2 рядки):

      ```dotenv
      AI_ONE_ENV_FILE=.env.local
      FXCM_CONNECTOR_VERSION_OVERRIDE=3.3
      ```

    > **Зворотна сумісність:** якщо не хочеш профілі, можеш як і раніше покласти всі змінні напряму в `.env` і не задавати `AI_ONE_ENV_FILE`.

1. **Створи Python 3.7 venv і постав залежності:**

    ```powershell
    py -3.7 -m venv .venv_fxcm37
    .\.venv_fxcm37\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    ```

    > `requirements.txt` вже містить `backports.zoneinfo` та `tzdata`, тож після інсталяції не буде помилок `ModuleNotFoundError: zoneinfo` навіть у старих середовищах.

1. **Переконайся, що ForexConnect SDK доступний** (DLL/pyfxconnect у `PYTHONPATH`).

1. **POC / warmup-запуск:**

    ```powershell
    python -m connector  # прогріє кеш і може відправити warmup-пакет
    ```

## 6. Конфігурація

### 6.1 Змінні середовища

| Назва | Значення за замовчуванням | Опис |
| --- | --- | --- |
| `AI_ONE_ENV_FILE` | – | Опційний “диспетчер” профілю: якщо задано, конектор завантажує `.env` і додатково файл профілю (наприклад, `.env.local` / `.env.prod`). |
| `FXCM_USERNAME` / `FXCM_PASSWORD` | – | Обов’язкові креденшали FXCM. |
| `FXCM_CONNECTION` | `Demo` | Demo/Real. |
| `FXCM_HOST_URL` | `http://www.fxcorporate.com/Hosts.jsp` | Endpoint для SDK. |
| `FXCM_REDIS_HOST` / `FXCM_REDIS_PORT` | `127.0.0.1` / `6379` | Redis для стріму. |
| `FXCM_REDIS_REQUIRED` | `1` | Якщо `1` — Redis обов'язковий; `0` → file-only режим (лише кеш). |
| `FXCM_REDIS_PASSWORD` | – | Опційний пароль (ACL). |
| `FXCM_CACHE_ENABLED` | `1` | Керує HistoryCache. |
| `FXCM_METRICS_ENABLED` | `1` | Вмикає Prometheus-server. |
| `FXCM_METRICS_PORT` | `9200` | Порт `/metrics`. |
| `FXCM_CHANNEL_PREFIX` | `fxcm` | Префікс Redis Pub/Sub каналів конектора (ізоляція локальне/прод). Якщо не задані явні канали нижче, вони будуються як `${FXCM_CHANNEL_PREFIX}:<suffix>`. |
| `FXCM_OHLCV_CHANNEL` | `${FXCM_CHANNEL_PREFIX}:ohlcv` | Канал для OHLCV payload-ів (override для `stream.ohlcv_channel`). |
| `FXCM_PRICE_SNAPSHOT_CHANNEL` | `${FXCM_CHANNEL_PREFIX}:price_tik` | Канал для price snapshots (override для `stream.price_snap_channel`). |
| `FXCM_HEARTBEAT_CHANNEL` | `${FXCM_CHANNEL_PREFIX}:heartbeat` | Куди шлеться heartbeat. |
| `FXCM_STATUS_CHANNEL` | `${FXCM_CHANNEL_PREFIX}:status` | Канал публічного статусу (process/market/price/ohlcv/session). |
| `FXCM_COMMANDS_CHANNEL` | `${FXCM_CHANNEL_PREFIX}:commands` | Канал команд від SMC (S3): `fxcm_warmup`/`fxcm_backfill`. |
| `FXCM_DYNAMIC_UNIVERSE_ENABLED` | `false` | Якщо `true`, ефективні `stream_targets` беруться з `fxcm_set_universe` (якщо SMC вже задав), інакше з `dynamic_universe_default_targets`. |
| `FXCM_DYNAMIC_UNIVERSE_MAX_TARGETS` | `20` | Верхня межа кількості активних таргетів у dynamic режимі. |
| `FXCM_BACKFILL_MIN_MINUTES` | `10` | Мінімальне значення `lookback_minutes` для `fxcm_backfill` (clamp). |
| `FXCM_BACKFILL_MAX_MINUTES` | `360` | Максимальне значення `lookback_minutes` для `fxcm_backfill` (clamp). |
| `FXCM_SESSION_TAG` | `AUTO` | Якщо `AUTO` — тег визначається за `session_windows`; можна вказати фіксований (наприклад, `LDN_METALS`). |
| `FXCM_HMAC_SECRET` | – | Секрет для `sig`. Якщо задано — `fxcm_ingestor` вимагає підпис. |
| `FXCM_HMAC_ALGO` | `sha256` | Алгоритм (`sha256`, `sha512`, ...). |

Окремий набір ENV для інгістора описано нижче.

#### 6.1.1 FXCM ingestor (whitelist/ліміти)

Інгістор підтягує whitelist-налаштування та ліміт розміру payload виключно з ENV/`runtime_settings.json`, тому їх варто явним чином задекларувати:

| Назва | Дефолт / джерело | Опис |
| --- | --- | --- |
| `FXCM_INGEST_ALLOWED_SYMBOLS` | Значення `stream.config` → `DEFAULT_ALLOWED_SYMBOLS` (`XAU/USD, EUR/USD`) | Кома-сепарований whitelist символів. Якщо ENV порожній, береться масив з runtime settings або дефолт. |
| `FXCM_INGEST_ALLOWED_TIMEFRAMES` | Значення `stream.config` → `DEFAULT_ALLOWED_TIMEFRAMES` (`1m,5m,15m,30m,1h,4h,1d`) | Перелік таймфреймів; нормалізуються в `1m/5m/.../1d`. |
| `FXCM_INGEST_MAX_BATCH` | `DEFAULT_MAX_BARS_PER_PAYLOAD = 5_000` | Жорсткий ліміт на кількість барів у payload. Все, що вище, відкидається одразу. |
| `FXCM_INGEST_HMAC_SECRET` | `FXCM_HMAC_SECRET` | Окремий секрет для підпису інгістора; якщо не задано, використовує той самий, що й конектор. |
| `FXCM_INGEST_HMAC_ALGO` | `FXCM_HMAC_ALGO` або `sha256` | Алгоритм перевірки підпису. |

Порядок резолву для whitelist-ів: спочатку ENV, далі `FXCM_STREAM_CONFIG` (якщо задано), потім `stream.config` у `runtime_settings.json`, і лише після цього — вбудований дефолт. `FXCM_INGEST_MAX_BATCH` завжди обрізається до мінімуму `1`, тож можна безпечно ставити значення з CLI/ENV.

> **Мультисесії:** `sessions.py` тепер використовує `zoneinfo`, тож календарні оверрайди можуть посилатися на `Europe/London`, `Asia/Tokyo` тощо. Для кожного потоку можна виставити власний тег (наприклад, `FXCM_SESSION_TAG=LDN_METALS`) або залишити `AUTO`, тоді тег вибирається відповідно до `session_windows`. Приклад для «недільного» старту через Токіо з паузою перед Лондоном:
>
> ```json
> {
>   "weekly_open_utc": "00:00@Asia/Tokyo",
>   "daily_breaks": [
>     "06:00-08:30@UTC",
>     "21:55-23:00@UTC"
>   ],
>   "session_windows": [
>     { "tag": "TOKYO_METALS", "start": "00:00", "end": "06:00", "tz": "UTC", "timezone": "Asia/Tokyo" },
>     { "tag": "LDN_METALS", "start": "08:30", "end": "14:30", "tz": "UTC", "timezone": "Europe/London" },
>     { "tag": "NY_METALS", "start": "14:30", "end": "21:55", "tz": "UTC", "timezone": "America/New_York" }
>   ]
> }
> ```
>
> Heartbeat/market-status `context.session` тепер містить актуальний тег, таймзону, `session_open_utc`/`session_close_utc` та секцію `stats` (range/avg) для кожної сесії за останні 24 години, тож UI може показувати реальний перехід «Tokyo → London → New York» без накладок.

> **Секрети:** `.env.template` — лише плейсхолдер. Використовуй секрет-стор (Azure Key Vault, GitHub Secrets тощо). Не коміть `.env`, `.env.local`, `.env.prod`.

> **Рекомендація:** зберігай у `.env` лише `AI_ONE_ENV_FILE` (і, за потреби, `FXCM_CONNECTOR_VERSION_OVERRIDE`). Усі креденшали/канали тримай у `.env.local/.env.prod`, щоб “перемикач” працював передбачувано.

### 6.2 Runtime JSON (`config/runtime_settings.json`)

```json
{
  "cache": {
    "dir": "cache",
    "max_bars": 3000,
    "warmup_bars": 1000
  },
  "stream": {
    "mode": 1,
    "async_supervisor": true,
    "poll_seconds": 5,
    "fetch_interval_seconds": 5,
    "publish_interval_seconds": 5,
    "lookback_minutes": 5,
    "config": "XAU/USD:m1,XAU/USD:m5, EUR/USD:m1,EUR/USD:m5",
    "price_snap_channel": "fxcm:price_tik",
    "price_snap_interval_seconds": 3,
    "status_channel": "fxcm:status",
    "commands_channel": "fxcm:commands",
    "dynamic_universe_enabled": false,
    "dynamic_universe_default_targets": "XAU/USD:m1,XAU/USD:m5",
    "dynamic_universe_max_targets": 20,
    "backfill_min_minutes": 10,
    "backfill_max_minutes": 360,
    "tick_aggregation_enabled": false,
    "tick_aggregation_max_synth_gap_minutes": 60,
    "history_max_calls_per_min": 360,
    "history_max_calls_per_hour": 7200,
    "history_min_interval_seconds_m1": 2,
    "history_min_interval_seconds_m5": 6,
    "history_priority_targets": [
      "XAU/USD:m1"
    ],
    "history_priority_reserve_per_min": 108,
    "history_priority_reserve_per_hour": 2160,
    "history_load_thresholds": {
      "min_interval": 0.7,
      "warn": 0.9,
      "reserve": 0.9,
      "critical": 0.95
    },
    "history_backoff": {
      "base_seconds": 6,
      "max_seconds": 120,
      "multiplier": 2,
      "jitter": 0.3
    },
    "redis_backoff": {
      "base_seconds": 2,
      "max_seconds": 45,
      "multiplier": 2,
      "jitter": 0.25
    }
  },
  "sample_request": {
    "symbol": "XAU/USD",
    "timeframe": "m1",
    "hours": 24
  },
  "viewer": {
    "heartbeat_channel": "fxcm:heartbeat",
    "market_status_channel": "fxcm:market_status",
    "ohlcv_channel": "fxcm:ohlcv",
    "redis_health_interval": 8,
    "lag_spike_threshold": 180,
    "heartbeat_alert_seconds": 45,
    "ohlcv_msg_idle_warn_seconds": 90,
    "ohlcv_msg_idle_error_seconds": 180,
    "ohlcv_lag_warn_seconds": 45,
    "ohlcv_lag_error_seconds": 120,
    "timeline_matrix_rows": 10,
    "timeline_max_columns": 120,
    "timeline_history_max": 2400,
    "timeline_focus_minutes": 30
  },
  "backoff": {
    "fxcm_login": {
      "base_delay": 2.0,
      "factor": 2.0,
      "max_delay": 60.0
    },
    "fxcm_stream": {
      "base_delay": 5.0,
      "factor": 2.0,
      "max_delay": 300.0
    },
    "redis_stream": {
      "base_delay": 1.0,
      "factor": 2.0,
      "max_delay": 60.0
    }
  }
}
```

- `stream.mode=0` → один warmup-прохід; `1` → нескінченний стрім.
- `stream.config` приймає рядок або масив `{ "symbol": ..., "tf": ... }`.
- `stream.price_snap_channel` задає Redis-канал для тик-снепшотів, а `stream.price_snap_interval_seconds` — інтервал між пакетами (3–5 с). Ці значення читає `PriceSnapshotWorker`, що агрегує OfferTable-тикі.
- `stream.tick_aggregation_enabled` вмикає tick→OHLCV агрегацію (джерело `source="tick_agg"`). Якщо `true`, FXCM history-полінг **не публікує** `1m/5m` у `fxcm:ohlcv`, щоб не змішувати два джерела.
- `stream.tick_aggregation_max_synth_gap_minutes` задає максимальний розрив (хв), який дозволено заповнювати synthetic-барами в tick-агрегації.
- `stream.tick_aggregation_live_publish_interval_seconds` задає тротлінг live-оновлень `complete=false` (типово `0.25`), щоб не публікувати кожен тик.

**Tick→bucket (див. docs/TIK_bucket.md):**

- Bucket-семантика: інтервали `[start_ms, end_ms)`, тик із `ts_ms == end_ms` відкриває наступний bucket.
- Публікуються лише `complete=true` бари; live (`complete=false`) не транслюється у `fxcm:ohlcv`.
- `5m` формується агрегуванням `complete=true` 1m-барів (1m — шар істини).
- Довгі gap’и не заповнюються нескінченними synthetic-барами: ліміт контролюється `tick_aggregation_max_synth_gap_minutes`.

- `stream.history_*` контролюють неблокуючий throttle `HistoryQuota` (Stage 3.1):

  - `history_max_calls_per_min` / `_per_hour` — глобальні бюджети FXCM-викликів.
  - `history_min_interval_seconds_<tf>` — локальні мінімальні інтервали по таймфреймах (наприклад, `m1`, `m5`).
  - `history_priority_targets` — символи/таймфрейми з пріоритетом (`SYMBOL:tf`).
  - `history_priority_reserve_per_min` / `_per_hour` — резерв викликів, що зберігається для пріоритетних пар.
  - `history_load_thresholds` — пороги вмикання `min_interval`, WARN-стану, резервів та CRITICAL (`min_interval`, `warn`, `reserve`, `critical` у діапазоні 0–1). **Оновлено 2025‑12‑05:** тепер усі пороги конфігуруються тут, без правок у коді.
  - `history_backoff` — параметри експоненційного backoff для викликів FXCM history (`base_seconds`, `max_seconds`, `multiplier`, `jitter`). Якщо FXCM повертає ретраябельну помилку, стрім робить паузу й пропускає лише history частину, але продовжує heartbeat/price feed. **Новинка Stage 3.2.**
  - `redis_backoff` — аналогічна схема пауз для Redis-публікацій (для supervisor та fallback sink-ів). Backoff активний для всіх каналів і прогортається автоматично після успішної публікації. **Новинка Stage 3.2.**
- `cache.dir` можна винести у `/var/lib/fxcm_connector/<env>_cache` для продакшну.
- `viewer.*` задає канали Redis та таймінги алертів для `tools/debug_viewer.py` (щоб не плодити ENV). Деталі: `docs/viewer.md`.
- `backoff.*` контролюють експоненційні паузи для FXCM/Redis.

#### 6.2.1 Останні зміни Stage 3.1 (2025‑12‑05)

- `history_load_thresholds` тепер джерело правди для порогів завантаження (`min_interval`, `warn`, `reserve`, `critical`).
- Значення автоматично обрізаються до `[0,1]`, тож став пороги у порядку зростання, щоб уникати неочікуваної поведінки (`min_interval ≤ warn ≤ reserve ≤ critical ≤ 1`).
- У heartbeat (`context.history`) відображаються `last_denied_reason`, `skipped_polls` і `throttle_state`, що допомагає тестувати різні конфіги в реальному часі.

#### 6.2.2 Stage 3.2 — Backoff FXCM / Redis

- `stream.history_backoff` та `stream.redis_backoff` — конфіг для `FxcmBackoffController` та supervisor-паблішера (експоненційні паузи з `base_seconds`, `max_seconds`, `multiplier`, `jitter`). При фейлі history `_fetch_and_publish_recent` просто пропускає ітерацію до `next_history_allowed_ts`, тож heartbeat/price_stream не зупиняються; при збоях Redis supervisor ставить `redis_paused_until`, але таски лишаються живими.
- Heartbeat `context.diag.backoff.<key>` показує `active`, `sleep_seconds`, `fail_count`, `last_error_type/ts`, а viewer (PRICE/HISTORY режими) рендерить Backoff-панель з тими самими даними. Це дає прозору діагностику для FXCM/Redis без копання в логи.
- Юніт- та інтеграційні тести (`tests/test_stream.py`, `tests/test_config.py`) накривають cap на `max_seconds`, reset після `success()`, а також перевіряють, що heartbeat не мовчить під час backoff.

#### 6.2.3 Stage 3.3 — Tick-driven cadence та auto-sleep

- `TickCadenceTuning` / `TickCadenceController` керують `poll_seconds` і `sleep_seconds` history-циклу на базі `tick_silence_seconds`, стану ринку й акумульованих лагів. Режими live/idle змінюють множники, а `next_wakeup_in_seconds()` повертає планований інтервал для кожного TF.
- Heartbeat містить блок `context.tick_cadence` (state, history_state, multiplier, reason, cadence/next_poll per TF), який читає viewer: у PRICE-режимі додано панель Adaptive cadence, а supervisor-режим дублює той самий зріз поруч із async-метриками. `price_stream` також відстежує `Viewer snapshots`, щоб бачити активність `fxcm:price_tik` навіть коли supervisor не звітує publish_counts.
- Автотести покривають контролер (подовження циклу при тиші, пришвидшення при live) і UI (viewer панелі), тож ми гарантуємо latency <200 мс/бар без ручного втручання.

## 7. Робочі режими

### 7.1 Warmup

- Використовує `HistoryCache.ensure_ready` для прогріву.
- Може публікувати warmup-пакет до Redis (контролюється `stream.mode`).
- Heartbeat-подія `state="warmup"`.

### 7.2 Стрім (реальний час)

- У циклі викликає `_fetch_and_publish_recent` (FXCM history), що поважає календар і lookback. Якщо увімкнена tick-агрегація (`stream.tick_aggregation_enabled=true`), history-публікація для `1m/5m` пропускається, щоб не змішувати джерела OHLCV.
- `PublishDataGate` відкидає дублікати та зберігає останні `open_time` / `close_time` для lag-метрик.
- Кожне видання барів → Prometheus `fxcm_ohlcv_bars_total` та оновлення лагів.
- Якщо FXCM повертає `PriceHistoryCommunicator is not ready` або торгове вікно закрите — піднімається `MarketTemporarilyClosed`, публікується `state=closed` і heartbeat `idle` з `next_open`.
- File-only fallback: встанови `FXCM_REDIS_REQUIRED=0`, щоб дозволити стрім працювати навіть без Redis (оновлюється лише кеш + лаг-метрики).

## 8. Runbook продакшн-деплою

1. **Файлова структура**

- Код: `/opt/fxcm_connector` (read-only для сервісного користувача `fxcm`).
- Секрети: `/etc/fxcm_connector/<env>/.env` + копія `runtime_settings.json`.
- Кеш: `/var/lib/fxcm_connector/<env>_cache` (групові права для читання/запису).

1. **Systemd unit** (`/etc/systemd/system/fxcm-connector.service`):

  ```ini
  [Unit]
  Description=FXCM Connector
  After=network-online.target redis.service

  [Service]
  User=fxcm
  WorkingDirectory=/opt/fxcm_connector
  EnvironmentFile=/etc/fxcm_connector/prod/.env
  ExecStart=/opt/fxcm_connector/.venv_fxcm37/bin/python -m connector
  Restart=on-failure
  RestartSec=5s
  TimeoutStopSec=30s

  [Install]
  WantedBy=multi-user.target
  ```

1. **Післярелізні перевірки**

- `journalctl -u fxcm-connector -f` → шукаємо `warmup_cache` → `stream`.
- `redis-cli SUBSCRIBE fxcm:heartbeat` → `state=stream|idle`, наявність `last_bar_close_ms`.
- `curl http://127.0.0.1:9200/metrics | grep fxcm_stream_lag_seconds` → <120 сек під час торгів.

1. **Оновлення без простою**

- Розгорни нову версію в `releases/<sha>`.
- Переключи symlink і зроби `systemctl restart fxcm-connector`.
- Оскільки кеш у `/var/lib/...`, warmup не перезаписується.

## 9. Моніторинг та алерти

- **Prometheus:**
  - `fxcm_ohlcv_bars_total{symbol,tf}` — публікації з конектора.
  - `fxcm_stream_lag_seconds{symbol,tf}` — різниця між `now` і `close_time`.
  - `fxcm_stream_staleness_seconds{symbol,tf}` — час із останнього бару.
  - `fxcm_stream_last_close_ms{symbol,tf}` — останній close у мс.
  - `fxcm_dropped_bars_total{symbol,tf}` — бари, відкинуті під час публікації/інгісту.
  - `fxcm_connector_errors_total{type}` — `fxcm`, `redis`, `cache_io`, `pricehistory_not_ready`.
  - `fxcm_pricehistory_not_ready_total` — скільки разів FXCM повернув «PriceHistoryCommunicator is not ready».
  - `fxcm_market_status` (0/1) та `fxcm_next_open_seconds` — статус ринку й відлік до наступного відкриття.
  - `fxcm_connector_heartbeat_timestamp` — UNIX-час останнього heartbeat.
  - `ai_one_fxcm_ohlcv_lag_seconds{symbol,tf}` — лаги, які бачить `tools/debug_viewer.py` під час читання Redis (див. `docs/viewer.md`).
  - `ai_one_fxcm_ohlcv_msg_age_seconds{symbol,tf}` — скільки триває тиша в каналі барів.
  - `ai_one_fxcm_ohlcv_gaps_total{symbol,tf}` — кількість інцидентів idle/lag із боку viewer.
  - `context.price_stream` у heartbeat відображає стан каналу `fxcm:price_tik`: канал, інтервал публікації, перелік активних символів та `tick_silence_seconds` (час від останнього тика).

  ### 9.1 Контракти heartbeat та market-status

  **Heartbeat (`fxcm:heartbeat`):**

  ```json
  {
    "type": "heartbeat",
    "state": "stream",
    "ts": "2025-11-30T22:28:52+00:00",
    "last_bar_close_ms": 1764541739999,
    "context": {
      "channel": "fxcm:heartbeat",
      "mode": "stream",
      "redis_connected": true,
      "redis_required": true,
      "redis_channel": "fxcm:ohlcv",
      "poll_seconds": 5,
      "lookback_minutes": 5,
      "publish_interval_seconds": 5,
      "cycle_seconds": 0.42,
      "lag_seconds": 1.1,
      "stream_targets": [
        { "symbol": "XAU/USD", "tf": "m1", "staleness_seconds": 1.1 }
      ],
      "published_bars": 2,
      "cache_enabled": true,
      "price_stream": {
        "channel": "fxcm:price_tik",
        "symbols": ["XAUUSD", "EURUSD"],
        "interval_seconds": 3.0,
        "last_snap_ts": 1764541732000.0,
        "last_tick_ts": 1764541731000.0,
        "tick_silence_seconds": 0.9
      },
      "session": {
        "tag": "LDN_METALS",
        "timezone": "Europe/London",
        "session_open_utc": "2025-11-30T08:30:00+00:00",
        "session_close_utc": "2025-11-30T14:30:00+00:00",
        "weekly_open": "00:00@Asia/Tokyo",
        "weekly_close": "21:55@America/New_York",
        "daily_breaks": [
          { "start": "06:00", "end": "08:30", "tz": "UTC" },
          { "start": "21:55", "end": "23:00", "tz": "UTC" }
        ],
        "next_open_utc": "2025-11-30T23:00:00+00:00",
        "next_open_ms": 1764543600000,
        "next_open_seconds": 1800.0,
        "stats": {
          "TOKYO_METALS": {
            "tag": "TOKYO_METALS",
            "timezone": "Asia/Tokyo",
            "session_open_utc": "2025-11-30T00:00:00+00:00",
            "session_close_utc": "2025-11-30T06:00:00+00:00",
            "symbols": [
              { "symbol": "XAUUSD", "tf": "1m", "bars": 360, "range": 4084.0, "avg": 4235.32 }
            ]
          },
          "LDN_METALS": {
            "tag": "LDN_METALS",
            "timezone": "Europe/London",
            "session_open_utc": "2025-11-30T08:30:00+00:00",
            "session_close_utc": "2025-11-30T14:30:00+00:00",
            "symbols": [
              { "symbol": "XAUUSD", "tf": "1m", "bars": 240, "range": 2383.0, "avg": 4251.0 }
            ]
          }
        }
      }
    }
  }
  ```

  **Market status (`fxcm:market_status`):**

  ```json
  {
    "type": "market_status",
    "state": "closed",
    "ts": "2025-11-30T22:29:00+00:00",
    "next_open_utc": "2025-11-30T23:00:00+00:00",
    "next_open_ms": 1764543600000,
    "next_open_in_seconds": 1800.0,
    "session": {
      "tag": "LDN_METALS",
      "timezone": "Europe/London",
      "session_open_utc": "2025-11-30T08:30:00+00:00",
      "session_close_utc": "2025-11-30T14:30:00+00:00",
      "weekly_open": "00:00@Asia/Tokyo",
      "weekly_close": "21:55@America/New_York",
      "daily_breaks": [
        { "start": "06:00", "end": "08:30", "tz": "UTC" },
        { "start": "21:55", "end": "23:00", "tz": "UTC" }
      ],
      "next_open_utc": "2025-11-30T23:00:00+00:00",
      "next_open_ms": 1764543600000,
      "next_open_seconds": 1800.0,
      "stats": {
        "TOKYO_METALS": {
          "tag": "TOKYO_METALS",
          "timezone": "Asia/Tokyo",
          "session_open_utc": "2025-11-30T00:00:00+00:00",
          "session_close_utc": "2025-11-30T06:00:00+00:00",
          "symbols": [
            { "symbol": "XAUUSD", "tf": "1m", "bars": 360, "range": 4084.0, "avg": 4235.32 }
          ]
        }
      }
    }
  }
  ```

  Контекст idle/warmup/warmup_cache додає `idle_reason`, `next_open_seconds`, `cache_source`, `published_bars` тощо; `lag_seconds` обчислюється навіть коли нових барів немає.
  `context.session` синхронізований із торговим календарем (`sessions.py`) і містить тег сесії, таймзону, години роботи, перерви, наступне відкриття/завершення поточної сесії та `stats` (range/avg/bars) для кожної сесії за останню добу.

- **Heartbeat payload:**

  ```json
  {
  "type": "heartbeat",
  "state": "warmup" | "warmup_cache" | "stream" | "idle",
  "ts": "2025-11-30T01:00:00+00:00",
  "last_bar_close_ms": 1746187200000,
  "next_open_utc": "2025-11-30T22:15:00+00:00",
  "sleep_seconds": 5
  }
  ```

  - `idle` heartbeat тепер завжди включає `last_bar_close_ms` (з кешу), тож UI може показувати лаг навіть під час пауз.
  - Якщо `next_open_utc` змінюється, конектор публікує і `market_status`.

- **Алерти** (рекомендації):
  1. `fxcm_stream_lag_seconds > 180` протягом 3 хвилин — перевірити FXCM/мережу.
  2. `fxcm_connector_errors_total{type="cache_io"}` зростає — диск/права.
  3. Відсутність heartbeat >30 секунд — рестарт процесу.

## 10. Безпека та HMAC

1. **Публікація**

- Якщо `FXCM_HMAC_SECRET` задано, `publish_ohlcv_to_redis` додає поле `sig`.
- Підпис обчислюється як `HMAC(secret, json.dumps(base_payload, sort_keys=True, separators=(",", ":")))`.

1. **Валідація**

- `fxcm_ingestor` (див. `tests/test_ingestor.py`) очікує `sig`, якщо конфіг має секрет.
- Невірний/відсутній підпис → `IngestValidationError` та миттєвий дроп payload.
- Після HMAC інгістор виконує повний чекліст:

  1. `symbol` та `tf` повинні входити до whitelists (`FXCM_INGEST_ALLOWED_SYMBOLS` / `FXCM_INGEST_ALLOWED_TIMEFRAMES`, див. §6.1.1).
  2. `len(bars) ≤ max_bars_per_payload` (дефолт `DEFAULT_MAX_BARS_PER_PAYLOAD = 5_000`).
  3. Кожен бар містить `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume` і всі значення можна привести до int/float.
  4. `close_time ≥ open_time`, `low ≤ high` — інакше payload вважається пошкодженим.
  5. `open_time` ≥ `MIN_ALLOWED_BAR_TIMESTAMP_MS` (UTC 2000-01-01) та ≤ `now + MAX_FUTURE_DRIFT_SECONDS` (дозволяємо дрейф до 86 400 с).
  6. `open_time` суворо зростає в межах одного payload, щоб уникати дубльованих/перемішаних свічок.

- Для синхронізації просто вистав `FXCM_INGEST_HMAC_SECRET=$FXCM_HMAC_SECRET` (алгоритм також можна успадкувати через `FXCM_INGEST_HMAC_ALGO` → `FXCM_HMAC_ALGO`).

1. **Практичні поради**

- Зберігай секрет лише у секрет-сторі, не в CI логах.
- Для rotation: розгорни новий секрет на інгісторі → онови конектор → перевір heartbeat.

## 11. Розробка та тестування

### 11.1 Запуск тестів

```powershell
python -m pytest tests/test_stream.py tests/test_ingestor.py -vv
python -m unittest tests.test_config
```

### 11.2 Статичні перевірки

```powershell
python -m pip install -r dev-requirements.txt
python -m mypy connector.py config.py sessions.py cache_utils.py fxcm_ingestor.py
ruff check .
```

Рекомендується додати ці команди в CI (GitHub Actions / Jenkins) перед merge.

### 11.3 Lint/format

- Можна додати pre-commit з `ruff`, `black`, `mypy`.
- Всі логи/рядки — українською (бачимо в `logging`), тож не локалізуємо.

## 12. Масштабування

### 12.1 Процесний шардинг

- Декілька воркерів із різними `stream.config` (наприклад, `worker-a` обслуговує XAU, `worker-b` — EUR/GBP).
- Кожен воркер має власний `cache.dir` для уникнення гонок.
- Оркестратор слідкує за heartbeat (`last_bar_close_ms`), робить рестарт лише потрібного воркера.

### 12.2 Thread-pool (POC only)

- ForexConnect не гарантує thread-safe поведінку, тож тредінг — останній варіант.
- Якщо використовуєш `ThreadPoolExecutor`, синхронізуй запити per-symbol і стеж за GIL.

## 13. Торговий календар

- Календар зберігається у `config/calendar_overrides.json`.
- Атрибути: `holidays`, `daily_breaks`, `weekly_open_utc`, `weekly_close_utc`.
- `session_windows` — необов'язковий масив `{tag,start,end,tz,timezone}` для автоперемикання heartbeat-тегів та підрахунку сесійних метрик.
- `daily_breaks` приймає рядки `HH:MM-HH:MM@TZ`, масиви `['HH:MM','HH:MM','America/New_York']` або об'єкти `{start,end,tz}`.
- `weekly_open_utc`/`weekly_close_utc` підтримують суфікс таймзони (`@America/New_York`), за замовчуванням UTC.
- Дефолт відповідає FXCM/TradingView для металів: неділя 18:00  п'ятниця 16:55 (New York) з щоденною паузою 17:00-18:00 (New York) та автоматичним DST.
- Усі середовища читають один файл, але deployment tooling може підміняти під конкретний контур.
- Під час старту логуються активні оверрайди.

## 14. Траблшутинг

| Симптом | Причина | Дія |
| --- | --- | --- |
| `ForexConnect SDK не встановлено` | Python ≠ 3.7 або немає DLL | Установи офіційний SDK, перевір `pyfxconnect` у venv. |
| `PriceHistoryCommunicator is not ready` | Ринок закритий/FXCM не готовий | Конектор сам перейде в `idle`, почекай `next_open`. |
| Немає даних у Redis | Redis недоступний або ACL | Перевір `run_redis_healthcheck`, логи about redis reconnect. |
| Dashboard показує UNKNOWN | Listener не отримав `fxcm:market_status` | Перепідпишись до старту сервісу або збережи останній статус у state store. |
| Кеш не пишеться | Диск read-only / немає quota | Подивися лог `Файловий кеш вимкнено через помилку IO`. Віднови диск, перезапусти сервіс. |

## 15. Подальший розвиток

- Heartbeat-вебхук для зовнішніх інцидент-ботів.
- Adaptive backoff залежно від FXCM latency.
- Поглиблені інтеграційні тести з реальним Redis (Docker Compose).

## Ліцензія

### Proprietary License. Будь-яке використання чи розповсюдження можливе лише за попередньою письмовою згодою власника (див. [LICENSE.md](LICENSE.md))

## Контакти

### Власник: Stanislav (Std07-1)

### Email: [Viktoriakievstd1@gmail.com](mailto:Viktoriakievstd1@gmail.com)

### GitHub: [Std07-1](https://github.com/Std07-1)

### Telegram: `@Std07_1`

### Оновлено: 13.12.2025
