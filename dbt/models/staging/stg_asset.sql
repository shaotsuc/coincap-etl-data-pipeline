
{{
    config(
        schema='stg',
        materialized='table',
    )
}}


SELECT 
    id as asset_id,
    rank::INT as asset_rank,
    symbol as asset_symbol,
    name as asset_full_name,
    supply::FLOAT as supply_amt,
    COALESCE("maxSupply"::FLOAT, 0) as total_issued_asset_amt,
    COALESCE("marketCapUsd"::FLOAT, 0) as market_cap_USD, -- supply x price
    COALESCE("volumeUsd24Hr"::FLOAT, 0) as trading_volume_24h_USD,
    "priceUsd"::FLOAT as price_USD, -- volume-weighted price based on real-time market data, translated to USD
    "changePercent24Hr"::FLOAT as change_percent_24h,
    COALESCE("vwap24Hr"::FLOAT, 0) as VWAP_24h -- volume weighted avgerage price
FROM {{ ref( 'src_asset' ) }}

