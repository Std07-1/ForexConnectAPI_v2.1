# Інтеграція FXCM → Redis для UDS/SMC (актуально станом на 2025-12-13)

Цей документ **самодостатній**: містить повний опис контрактів трьох ключових каналів (`fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`) та рекомендації, як UDS/SMC має їх споживати. Посилання на “канонічні контракти” свідомо не використовуються.

## 0) TL;DR (як не зламати інтеграцію)

1. **Тримайте 3 підписки:** `fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`.
2. **OHLCV**: ключ бару для дедуп — `(symbol, tf, open_time)`; `open_time/close_time` — **epoch milliseconds**; `complete=false` — **не фінальний бар** (рекомендовано пропускати в UDS).
3. **TIK**: `tick_ts` — event-time (час останнього тика), `snap_ts` — publish-time (час формування снепшота).
4. **STATUS**: “можна користуватись даними” ⇒ `market=open` і `price=ok` і `ohlcv=ok`. `session.state_detail` існує **лише** при `session.state=closed`.

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
- `source` (string, optional): джерело батчу (наприклад, `"stream"`, `"tick_agg"`).
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
