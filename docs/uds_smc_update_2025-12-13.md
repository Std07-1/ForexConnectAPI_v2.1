# Інтеграція FXCM → Redis для UDS/SMC (актуально станом на 2025-12-13)

Цей документ **самодостатній**: містить повний опис контрактів трьох ключових каналів (`fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`) та рекомендації, як UDS/SMC має їх споживати. Посилання на “канонічні контракти” свідомо не використовуються.

> Примітка про ізоляцію local/prod: назви каналів у цьому гайді наведені як `fxcm:*` (канонічні). У локальному профілі вони можуть бути `fxcm_local:*` (через `FXCM_CHANNEL_PREFIX` / `FXCM_*_CHANNEL`). Формат повідомлень не змінюється — змінюється лише назва каналу.

## 0) TL;DR (як не зламати інтеграцію)

1. **Тримайте 3 підписки:** `fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`.
2. **OHLCV**: ключ бару для дедуп — `(symbol, tf, open_time)`; `open_time/close_time` — **epoch milliseconds**; `complete=false` — **не фінальний бар** (рекомендовано пропускати в UDS).
3. **TIK**: `tick_ts` — event-time (час останнього тика), `snap_ts` — publish-time (час формування снепшота).
4. **STATUS**: “можна користуватись даними” ⇒ `market=open` і `price=ok` і `ohlcv=ok`. `session.state_detail` існує **лише** при `session.state=closed`.
5. Якщо `price=ok`, але `ohlcv=delayed/down` і `note=only_live_bars_no_complete` — це майже завжди означає: **є live complete=false, але немає фінальних complete=true** (UDS/SMC їх ігнорує) → див. §5.

## 1) Загальні правила

### 1.1 Формат повідомлень

- Кожне повідомлення — один JSON-об’єкт (UTF‑8), що публікується в Redis PubSub.
- Консьюмер має бути **forward-compatible**: незнайомі поля ігноруються; відсутні опційні поля мають мати дефолти.

### 1.2 Нормалізація `symbol`

- `symbol` у `fxcm:ohlcv` нормалізується до формату без `/` і в upper-case (приклад: `"XAU/USD"` → `"XAUUSD"`, `"EUR/USD"` → `"EURUSD"`).
- `symbol` у `fxcm:price_tik` передається як є зі снепшота ціни; в поточній реалізації він також очікується у цьому ж нормалізованому форматі.

### 1.3 Нормалізація `tf`

- `tf` (timeframe) — рядок на кшталт: `"1m"`, `"5m"`, `"15m"`, `"30m"`, `"1h"`, `"4h"`, `"1d"`.

## 2) Канал `fxcm:ohlcv` (OHLCV бари)

### 2.1 Призначення

Передає батч завершених/частково-формованих OHLCV-барів для одного `symbol` і одного `tf`.

### 2.2 JSON-приклад (мінімальний)

```json
{
   "symbol": "XAUUSD",
   "tf": "1m",
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

### 2.3 JSON-приклад (з опційними полями)

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

### 2.4 Поля верхнього рівня

- `symbol` (string, required): нормалізований символ (без `/`, upper-case).
- `tf` (string, required): нормалізований timeframe (`1m`, `5m`, ...).
- `bars` (array, required): список барів (див. нижче).
- `source` (string, optional): джерело батчу (наприклад, `"stream"`, `"tick_agg"`, `"history_s3"`).
- `sig` (string, optional): hex HMAC-підпис батчу (див. §2.6).

### 2.5 Поля бару

**Обов’язкові поля:**

- `open_time` (int, ms epoch): початок бару.
- `close_time` (int, ms epoch): кінець бару.
- `open`, `high`, `low`, `close` (float).
- `volume` (float).

**Опційні поля (forward-compatible):**

- `complete` (bool): якщо `false` — бар ще формується. Якщо поля немає, вважати `true`.
- `synthetic` (bool): якщо `true` — бар може бути “згенерованим” для заповнення пропусків. Якщо поля немає, вважати `false`.
- `source` (string): джерело конкретного бару (може відрізнятись від root `source`).
- `tf` (string): дублювання timeframe на рівні бару (якщо присутнє — не суперечить root).
- Мікроструктурні/quality-метрики (optional): `tick_count` (int); `bar_range`, `body_size`, `upper_wick`, `lower_wick`, `avg_spread`, `max_spread` (float).

### 2.6 HMAC-підпис (`sig`)

Якщо `sig` присутній, він обчислюється як HMAC над **базовим payload**:

```json
{ "symbol": "...", "tf": "...", "bars": [...] }
```

Правила детермінованої серіалізації (важливо для сумісної верифікації):

- JSON серіалізується з `sort_keys=true`, `separators=(",", ":")`, `ensure_ascii=false`.
- Байти для HMAC — UTF‑8.
- Підпис — `hexdigest()`.
- Алгоритм HMAC (`sha256`, `sha512`, ...) задається конфігом; дефолт — `sha256`.

Примітка: опційні поля на root рівні (`source`) або в барах не повинні ламати інтеграцію — консьюмер має бути готовий ігнорувати незнайомі ключі.

### 2.7 Рекомендації для UDS/SMC

- **Ідемпотентність / дедуп:** ключ бару — `(symbol, tf, open_time)`.
- **Сортування:** бари можуть приходити батчами; очікується монотонний порядок `open_time`, але не треба на цьому “падати” — краще відсортувати перед записом.
- **`complete=false`:** рекомендовано не писати в UDS як “фінальний бар”. Якщо потрібен live-bar для UI — тримайте окремим шаром/кешем.
- **Live `complete=false` може повторюватися** для тієї самої хвилини (той самий `(symbol, tf, open_time)`): це нормальна поведінка для UI/діагностики.
- **Фінальні `complete=true` бари мають з’являтися регулярно**: для `1m` — приблизно раз на хвилину, для `5m` — раз на 5 хв.
- **`synthetic=true`:** трактуйте як валідний OHLCV (для безгеповості), але не робіть з нього “сигнал якості” без додаткової логіки.

## 3) Канал `fxcm:price_tik` (снепшоти ціни)

### 3.1 Призначення

Передає поточний снепшот ціни по кожному символу (bid/ask/mid) з двома таймстемпами: коли був останній тик (`tick_ts`) та коли сформовано снепшот (`snap_ts`).

### 3.2 JSON-приклад

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

### 3.3 Поля

- `symbol` (string, required)
- `bid`, `ask`, `mid` (float, required)
- `tick_ts` (float seconds epoch, required): event-time останнього відомого тика.
- `snap_ts` (float seconds epoch, required): publish-time снепшота.

### 3.4 Рекомендації для UDS/SMC

- Фрешність price оцінюйте по `tick_ts` (а не по `snap_ts`).
- Це снепшоти, а не “повний потік тиков”: допускайте пропуски, дублікати й нерівномірні інтервали.

## 4) Канал `fxcm:status` (публічний агрегований статус)

### 4.1 Призначення

Дозволяє зовнішнім системам (UDS/SMC) прийняти рішення “можна працювати / не можна працювати” без читання внутрішнього heartbeat.

### 4.2 JSON-приклад

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

### 4.3 Поля верхнього рівня

- `ts` (float seconds epoch, required): час формування статусу.
- `process` (string, required): `stream` / `idle` / `sleep` / `error`.
- `market` (string, required): `open` / `closed` / `unknown`.
- `price` (string, required): `ok` / `stale` / `down`.
- `ohlcv` (string, required): `ok` / `delayed` / `down`.
- `session` (object або null, optional): зріз поточної сесії (див. нижче).
- `note` (string, required): коротка людиночитна причина/коментар.

### 4.4 Поля `session`

- `name` (string, required): людиночитна назва.
- `tag` (string, required): стабільний тег.
- `state` (string, required): `open` або `closed`.
- `state_detail` (string, optional): з’являється **лише** коли `state=closed`; значення: `intrabreak` / `overnight` / `weekend`.
- `current_open_utc` / `current_close_utc` (string ISO8601, required коли `state=open`).
- `next_open_utc` (string ISO8601, required): наступний open.
- `seconds_to_close` / `seconds_to_next_open` (int, optional): таймери до подій.

### 4.5 Рекомендації для UDS/SMC

- Мінімальний gate для використання даних: `market == "open"` і `price == "ok"` і `ohlcv == "ok"`.
- Не робіть логіку, що “`state_detail` має бути завжди”: його може не бути взагалі.

## 5) UPDATE: Швидка локалізація “OHLCV DOWN / система висить” (суто для конектора)

Це розділ для оператора/інженера: що перевірити першими 5 хв, щоб знайти root-cause.

### 5.1 Найчастіший корінь проблеми

UDS/SMC (інгістор) **ігнорує `complete=false`** і пише тільки `complete=true`.
Тому якщо у стрімі є лише live-оновлення, UDS не рухається → `stale_tail` → “OHLCV down”.

Ознаки:

- `fxcm:price_tik` оновлюється
- `fxcm:ohlcv` часто містить `"complete":false`
- `fxcm:status`: `price=ok`, `ohlcv=delayed/down`, `note=only_live_bars_no_complete`

### 5.2 Перевірка №1 — чи реально виходять `complete=true`

Підпишіться на OHLCV і подивіться 1–2 хв:

```powershell
redis-cli SUBSCRIBE fxcm:ohlcv
```

Очікування:

- `complete:true` для `1m` — кожну хвилину
- `complete:true` для `5m` — кожні 5 хв

Швидкий grep-контроль (лог у файл):

```powershell
redis-cli SUBSCRIBE fxcm:ohlcv > ohlcv.log
findstr /C:"\"complete\":true" ohlcv.log
findstr /C:"\"complete\":false" ohlcv.log
```

Якщо бачите багато `complete:false`, але майже немає `complete:true` — це корінь проблеми.

### 5.3 Перевірка №2 — чи тики реально “живі”, а не тільки снепшоти

```powershell
redis-cli SUBSCRIBE fxcm:price_tik
```

Якщо `price_tik` є, але `fxcm:ohlcv` з `tick_agg` дає 0/мало апдейтів — значить tick-agg worker або підписка OfferTable/Universe не працюють як треба.

### 5.4 Перевірка №3 — `fxcm:status` і `fxcm:heartbeat` (причина “ohlcv down”)

```powershell
redis-cli SUBSCRIBE fxcm:status
redis-cli SUBSCRIBE fxcm:heartbeat
```

У `fxcm:status` дивіться:

- `price=ok/stale/down`
- `ohlcv=ok/delayed/down`
- `note=...`

Корисні інтерпретації:

- `price=ok`, але `ohlcv=down` → майже завжди означає: **немає complete=true барів** (live `complete=false` інгістор не рахує).
- `note=only_live_bars_no_complete` → тики живі, але чекаємо “закриття хвилини” (див. №4–№5).

У `fxcm:heartbeat.context` дивіться:

- `stream_targets[*].staleness_seconds` — скільки часу з останнього complete=true
- `stream_targets[*].live_age_seconds` або `ohlcv_live_age_seconds` — скільки часу з останнього live complete=false
- `universe.active_symbols_count` / `universe.last_apply_ts` — чи застосувався universe

### 5.5 Перевірка №4 — Prometheus (дешево, але дуже показово)

```powershell
curl -s http://127.0.0.1:9200/metrics | findstr fxcm_tick_agg
curl -s http://127.0.0.1:9200/metrics | findstr fxcm_ohlcv_
curl -s http://127.0.0.1:9200/metrics | findstr fxcm_universe_
```

Ключове:

- `fxcm_tick_agg_ticks_total{symbol="XAUUSD"}` росте?
- `fxcm_tick_agg_bars_total{symbol="XAUUSD",tf="1m",...}` росте? (це **complete-бари**)

Якщо ticks ростуть, а complete bars — ні, проблема саме у “закритті хвилини”/flush.

### 5.6 Перевірка №5 — швидкий smoke по командах S3 (universe)

```powershell
redis-cli PUBLISH fxcm:commands "{\"type\":\"fxcm_set_universe\",\"targets\":[{\"symbol\":\"XAU/USD\",\"tf\":\"m1\"},{\"symbol\":\"XAU/USD\",\"tf\":\"m5\"}]}"
```

Потім дивіться `heartbeat.context.stream_targets` і фактичні публікації `fxcm:ohlcv`.

### 5.7 Практичний висновок “куди дивитися першими”

1) Чи виходять `complete:true` бари для `1m` регулярно? Якщо ні — root-cause у закритті/flush або в тиках.
2) Чи після `fxcm_set_universe` реально оновилась OfferTable subscription і symbols у tick-agg?
3) Чи статус `ohlcv down` не ставиться помилково, коли є live-бари (`note=only_live_bars_no_complete`).

## 6) UPDATE: Bootstrap історії для першого старту SMC (щоб SMC не падав “без барів”)

Проблема: SMC/інгістор може не стартувати або “зависати”, якщо на старті немає мінімального обсягу історії (часто **≈300 барів**, у вашому кейсі може знадобитися **≈1600 `1m`** + відповідний обсяг `5m`).

Важливо: коли `tick_aggregation_enabled=true`, конектор **не публікує** FXCM history для `1m/5m` у стрім-циклі (щоб не змішувати джерела). Тому для першого запуску потрібен **bootstrap** — одноразове наповнення локального кешу, а потім стандартний старт конектора.

### 6.1 Налаштування warmup-обсягу

У файлі [config/runtime_settings.json](config/runtime_settings.json) виставіть:

- `cache.warmup_bars: 300` (мінімальний запуск) або `1600` (якщо потрібно більше для 1m/5m логіки)
- `cache.max_bars`: достатньо великий ліміт (наприклад, `5000+`), щоб кеш не обрізався

### 6.2 Одноразове наповнення кешу (до старту SMC)

Запустіть утиліту bootstrap (приклад для XAU/USD):

```powershell
C:/Aione_projects/fxcm_connector/.venv_fxcm37/Scripts/python.exe -m tools.cache_bootstrap --symbol XAU/USD --timeframes m1 m5 --min-bars 1600
```

Для “мінімального старту” замініть `1600` на `300`.

`--min-bars` корисний для автоматизації: якщо FXCM/кеш не набрав потрібний обсяг (наприклад, через вихідні/закритий ринок), утиліта завершується з кодом `2`.

### 6.3 Далі — звичайний запуск конектора

Після того як кеш наповнений, запускайте конектор у stream-mode як завжди:

- на старті він опублікує warmup із локального кешу (це і є “перші N барів” для SMC),
- далі “чиста” історія буде накопичуватись у реальному часі через tick aggregation без потреби зупиняти систему.
