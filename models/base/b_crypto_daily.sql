-- models/base/base_crypto_daily.sql

{{ config(
    materialized='incremental',
    unique_key=['id', 'snapshot_date'],
    incremental_strategy='merge'
) }}

WITH final AS (
    SELECT
        id,
        symbol,
        name,
        DATE_TRUNC('day', last_updated) AS snapshot_date,
        AVG(current_price) AS avg_price_day,
        MAX(market_cap) AS max_market_cap_day,
        SUM(total_volume) AS total_volume_day,
        AVG(roi_percentage) AS avg_roi_percentage_day
    FROM {{ ref('st_crypto_model') }}
    {% if is_incremental() %}
      -- only pull rows after the max snapshot_date in target
      WHERE DATE_TRUNC('day', last_updated) > (
          SELECT COALESCE(MAX(snapshot_date), '1900-01-01')
          FROM {{ this }}
      )
    {% endif %}
    GROUP BY id, symbol, name, DATE_TRUNC('day', last_updated)
)

SELECT *
FROM final
