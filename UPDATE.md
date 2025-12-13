# UPDATE

## v2.2 (2025-12-13)

- Додано повний, точний опис контрактів Redis-каналів `fxcm:*` (OHLCV / price_tik / market_status / heartbeat / status) з правилами семантики полів, required/optional ключами та нюансами HMAC.
- Розширено типізацію контрактів у схемі (TypedDict) для всіх Redis payload-ів, щоб продюсер/консьюмери мали єдине джерело правди по структурам повідомлень.
- Додано практичний самодостатній гайд для інтеграції UDS/SMC (без зовнішніх посилань): правила дедуп по `(symbol, tf, open_time)`, трактування `complete=false`, розділення `tick_ts` vs `snap_ts`, мінімальний gate по `fxcm:status`.
- Уточнено та синхронізовано документацію публічного статусу: `state_detail` існує лише для `session.state=closed` (`intrabreak`/`overnight`/`weekend`).
- Оновлено README: додано коротку “шпаргалку” для UDS/SMC по каналах `fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`.
