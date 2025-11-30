# FXCM Connector

> Легковаговий стрімер OHLCV барів із FXCM (ForexConnect API) до Redis з валідацією HMAC, файловим кешем та продакшн-орієнтованим моніторингом.

**Версія 2.1 · 30 листопада 2025**

- Redis-повідомлення `fxcm:ohlcv` підписуються HMAC і перевіряються `fxcm_ingestor` перед записом у UnifiedStore.
- Idle-heartbeat підтягує останній close з кешу, тож UI бачить лаг і `next_open` навіть коли ринок спить.
- Файловий кеш має graceful fallback у read-only режим, піднімаючи `fxcm_connector_errors_total{type="cache_io"}` при будь-якому FS-проколі.

Документ описує повний життєвий цикл конектора: установку, конфіг, деплой, моніторинг і розробку.

---

## 1. Призначення

Конектор логіниться в FXCM через ForexConnect, витягує OHLCV-бари, нормалізує їх до AiOne_t-схеми та транслює у Redis:

- `fxcm:ohlcv` — основні дані (із опційним HMAC-підписом `sig`).
- `fxcm:market_status` — події `open/closed` з `next_open_utc`.
- `fxcm:heartbeat` (налаштовується) — технічний стан процесу.

Локальний файловий кеш (`cache/` або зовнішня директорія) мінімізує холодний старт, а Prometheus-метрики дають спостережність.

## 2. Можливості

- Warmup історії + стрім `m1/m5/...` таймфреймів у Redis.
- Торговий календар з урахуванням 24/5, денних перерв і свят (`sessions.py`).
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

## 4. Архітектура (високий рівень)

```
ForexConnect → connector.py ─┬─缓存 HistoryCache (CSV+META)
           ├─Redis publisher (fxcm:ohlcv, fxcm:market_status, heartbeat)
           ├─Prometheus exporter
           └─Optional HMAC signing

fxcm_ingestor → Redis subscriber → HMAC verify → UnifiedStore
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
 copy .env.template .env  # додай власні креденшали FXCM
 ```

2. **Створи Python 3.7 venv і постав залежності:**

 ```powershell
 py -3.7 -m venv .venv_fxcm37
 .\.venv_fxcm37\Scripts\Activate.ps1
 python -m pip install --upgrade pip
 python -m pip install -r requirements.txt
 ```

3. **Переконайся, що ForexConnect SDK доступний** (DLL/pyfxconnect у `PYTHONPATH`).

4. **POC / warmup-запуск:**

 ```powershell
 python connector.py  # прогріє кеш і може відправити warmup-пакет
 ```

## 6. Конфігурація

### 6.1 Змінні середовища

| Назва | Значення за замовчуванням | Опис |
| --- | --- | --- |
| `FXCM_USERNAME` / `FXCM_PASSWORD` | – | Обов’язкові креденшали FXCM. |
| `FXCM_CONNECTION` | `Demo` | Demo/Real. |
| `FXCM_HOST_URL` | `http://www.fxcorporate.com/Hosts.jsp` | Endpoint для SDK. |
| `FXCM_REDIS_HOST` / `FXCM_REDIS_PORT` | `127.0.0.1` / `6379` | Redis для стріму. |
| `FXCM_REDIS_PASSWORD` | – | Опційний пароль (ACL). |
| `FXCM_CACHE_ENABLED` | `1` | Керує HistoryCache. |
| `FXCM_METRICS_ENABLED` | `1` | Вмикає Prometheus-server. |
| `FXCM_METRICS_PORT` | `9200` | Порт `/metrics`. |
| `FXCM_HEARTBEAT_CHANNEL` | `fxcm:heartbeat` | Куди шлеться heartbeat. |
| `FXCM_HMAC_SECRET` | – | Секрет для `sig`. Якщо задано — `fxcm_ingestor` вимагає підпис. |
| `FXCM_HMAC_ALGO` | `sha256` | Алгоритм (`sha256`, `sha512`, ...). |
| `FXCM_INGEST_HMAC_SECRET` | – | (ingestor) переозначення секрету, fallback → `FXCM_HMAC_SECRET`. |

> **Секрети:** `.env.template` — лише плейсхолдер. Використовуй секрет-стор (Azure Key Vault, GitHub Secrets тощо). Не коміть `.env`.

### 6.2 Runtime JSON (`config/runtime_settings.json`)

```json
{
  "cache": { "dir": "cache", "max_bars": 3000, "warmup_bars": 1000 },
  "stream": {
  "mode": 1,
  "poll_seconds": 5,
  "fetch_interval_seconds": 5,
  "publish_interval_seconds": 5,
  "lookback_minutes": 5,
  "config": "XAU/USD:m1,XAU/USD:m5"
  },
  "sample_request": { "symbol": "EUR/USD", "timeframe": "m1", "hours": 24 },
  "backoff": {
  "fxcm_login": { "base_delay": 2, "factor": 2, "max_delay": 60 },
  "fxcm_stream": { "base_delay": 5, "factor": 2, "max_delay": 300 },
  "redis_stream": { "base_delay": 1, "factor": 2, "max_delay": 60 }
  }
}
```

- `stream.mode=0` → один warmup-прохід; `1` → нескінченний стрім.
- `stream.config` приймає рядок або масив `{ "symbol": ..., "tf": ... }`.
- `cache.dir` можна винести у `/var/lib/fxcm_connector/<env>_cache` для продакшну.
- `backoff.*` контролюють експоненційні паузи для FXCM/Redis.

## 7. Робочі режими

### 7.1 Warmup

- Використовує `HistoryCache.ensure_ready` для прогріву.
- Може публікувати warmup-пакет до Redis (контролюється `stream.mode`).
- Heartbeat-подія `state="warmup"`.

### 7.2 Стрім (реальний час)

- У циклі викликає `_fetch_and_publish_recent`, що поважає календар і lookback.
- `PublishDataGate` відкидає дублікати та зберігає останні `open_time` / `close_time` для lag-метрик.
- Кожне видання барів → Prometheus `fxcm_ohlcv_bars_total` та оновлення лагів.
- Якщо FXCM повертає `PriceHistoryCommunicator is not ready` або торгове вікно закрите — піднімається `MarketTemporarilyClosed`, публікується `state=closed` і heartbeat `idle` з `next_open`.

## 8. Runbook продакшн-деплою

1. **Файлова структура**

- Код: `/opt/fxcm_connector` (read-only для сервісного користувача `fxcm`).
- Секрети: `/etc/fxcm_connector/<env>/.env` + копія `runtime_settings.json`.
- Кеш: `/var/lib/fxcm_connector/<env>_cache` (групові права для читання/запису).

2. **Systemd unit** (`/etc/systemd/system/fxcm-connector.service`):

 ```ini
 [Unit]
 Description=FXCM Connector
 After=network-online.target redis.service

 [Service]
 User=fxcm
 WorkingDirectory=/opt/fxcm_connector
 EnvironmentFile=/etc/fxcm_connector/prod/.env
 ExecStart=/opt/fxcm_connector/.venv_fxcm37/bin/python connector.py
 Restart=on-failure
 RestartSec=5s
 TimeoutStopSec=30s

 [Install]
 WantedBy=multi-user.target
 ```

3. **Післярелізні перевірки**

- `journalctl -u fxcm-connector -f` → шукаємо `warmup_cache` → `stream`.
- `redis-cli SUBSCRIBE fxcm:heartbeat` → `state=stream|idle`, наявність `last_bar_close_ms`.
- `curl http://127.0.0.1:9200/metrics | grep fxcm_stream_lag_seconds` → <120 сек під час торгів.

4. **Оновлення без простою**

- Розгорни нову версію в `releases/<sha>`.
- Переключи symlink і зроби `systemctl restart fxcm-connector`.
- Оскільки кеш у `/var/lib/...`, warmup не перезаписується.

## 9. Моніторинг та алерти

- **Prometheus:**
  - `fxcm_ohlcv_bars_total{symbol,tf}` — публікації.
  - `fxcm_stream_lag_seconds{symbol,tf}` — різниця між `now` і `close_time`.
  - `fxcm_stream_staleness_seconds{symbol,tf}` — час із останнього бару.
  - `fxcm_connector_errors_total{type}` — `fxcm`, `redis`, `cache_io`, `pricehistory_not_ready`.
  - `fxcm_market_status` (0/1), `fxcm_next_open_seconds`.

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

2. **Валідація**

- `fxcm_ingestor` (див. `tests/test_ingestor.py`) очікує `sig`, якщо конфіг має секрет.
- Невірний/відсутній підпис → `IngestValidationError` та дроп payload.
- Для синхронізації просто вистав `FXCM_INGEST_HMAC_SECRET=$FXCM_HMAC_SECRET`.

3. **Практичні поради**

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

### Оновлено: 30.11.2025
