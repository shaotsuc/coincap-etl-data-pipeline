{{
  config(
    schema='dm',
    materialized='table',
    )
}}

SELECT
    asset_symbol,
    asset_full_name,
    supply_amt,
    market_cap_USD,
    trading_volume_24h_USD,
    price_USD,
    change_percent_24h,
    VWAP_24h
FROM {{ ref('stg_asset') }}