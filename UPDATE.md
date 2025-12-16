# UPDATE

## v2.2 (2025-12-13)

- Додано повний, точний опис контрактів Redis-каналів `fxcm:*` (OHLCV / price_tik / market_status / heartbeat / status) з правилами семантики полів, required/optional ключами та нюансами HMAC.
- Розширено типізацію контрактів у схемі (TypedDict) для всіх Redis payload-ів, щоб продюсер/консьюмери мали єдине джерело правди по структурам повідомлень.
- Додано практичний самодостатній гайд для інтеграції UDS/SMC (без зовнішніх посилань): правила дедуп по `(symbol, tf, open_time)`, трактування `complete=false`, розділення `tick_ts` vs `snap_ts`, мінімальний gate по `fxcm:status`.
- Уточнено та синхронізовано документацію публічного статусу: `state_detail` існує лише для `session.state=closed` (`intrabreak`/`overnight`/`weekend`).
- Оновлено README: додано коротку “шпаргалку” для UDS/SMC по каналах `fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`.

## v2.3 (2025-12-15)

- Додано bootstrap-режим для підготовки історії перед першим стартом SMC: утиліта `tools/cache_bootstrap.py` отримала `--min-bars` (fail-fast, exit code 2 при нестачі барів).
- Виправлено запуск `tools/cache_bootstrap.py` напряму (без `-m`): корінь репозиторію додається в `sys.path` до імпортів локальних модулів.
- Оновлено інструкцію в `docs/uds_smc_update_2025-12-13.md` для bootstrap історії (рекомендований запуск через `-m tools.cache_bootstrap`).
- Піднято дефолт `cache.warmup_bars` у `config/runtime_settings.json` до 1600 для сценарію старту SMC з достатньою історією по XAU 1m.
- Додано захист у `FXCMOfferSubscription`: якщо час з OfferTable приходить як epoch-milliseconds, він нормалізується до epoch-seconds. Це зменшує ризик стану `only_live_bars_no_complete`, коли tick-agg не може закрити бари через “майбутні” timestamp-и.
- Додано захист у `PublishDataGate` від “отруєного” cutoff у майбутньому: якщо `last_open`/`last_close` виявляються занадто попереду поточного часу, gate скидається і не блокує публікацію `complete=true` барів.
- Tick-agg: додано кламп для випадків, коли `tick_ts` приходить у майбутньому (більше ніж на 5с від `time.time()`), щоб `clock-flush` міг закривати `complete=true` бари вчасно.
- Heartbeat: додано `context.tick_agg` (queue depth, skew `tick_ts`, лічильники клампів, `last_complete_close_ms`) для швидкої діагностики `only_live_bars_no_complete`.
- Heartbeat/status: `lag_seconds` та `last_bar_close_ms` тепер синхронізуються з `PublishDataGate` (тобто з реально опублікованими `complete=true` барами, включно з tick-agg), щоб статус не показував фальшивий `DELAYED` при асинхронних публікаціях.
- History→OHLCV нормалізація: додано фільтр по `close_time` (майбутнє) — поточні незакриті бари з FXCM history відкидаються, щоб не спамити WARNING у `PublishDataGate` під час warmup/cache.
- Tick-agg: `volume` у live-превʼю (`complete=false`) рахується як tick volume (1.0 за кожен прийнятий тик), а закриті бари (`complete=true`) з tick-agg не публікуються назовні. Коли tick-agg внутрішньо закриває бар, стрім форсує FXCM history poll для відповідного `(symbol, tf)` (без очікування `publish_interval_seconds`), щоб якнайшвидше опублікувати complete=true свічку з «реальним» `Volume`.
- Tick-agg: додано калібрування preview-обсягу під FXCM history (`volume ≈ tick_count * k`, де `k` оцінюється по зіставлених барах з однаковим `open_time`). У heartbeat `context.tick_agg.volume_calibration` видно поточний `k`, кількість семплів та останню пару (history_volume vs tick_count).
- Debug viewer: додано режим `[9] VCAL`, який показує `heartbeat.context.tick_agg.volume_calibration` у вигляді таблиці (k, samples, остання пара history_volume/tick_count/ratio), а також короткі індикатори heartbeat/tick-agg (ts/age, tick age, clock flush age), щоб було видно чи UI реально оновлюється.
- Логи history→OHLCV: зменшено шум — відкидання поточного незакритого бару (close_time у майбутньому) тепер логиться як `DEBUG` з рейт-лімітом, а `WARNING` лишається лише для справжніх аномалій `open_time`.

## v2.4 (2025-12-16)

- `fxcm:status`: у блоці `session` додано `symbols` з per-(Symbol, TF) статистикою `High/Low/Avg` (те саме, що показує debug viewer у режимі Session), щоб консьюмери могли читати ці значення напряму зі статус-каналу.
- `fxcm_schema.py`: оновлено TypedDict-контракти для `fxcm:status.session.symbols` та додано runtime-валідацію `validate_status_payload_contract` + контрактний тест.
