
{{
    config(
        schema='src',
        materialized='view',
    )
}}


SELECT * 
FROM {{ source('coincap_raw','raw_asset' ) }}

