-- models/base/base_crypto_market_metrics.sql

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
        market_cap,
        fully_diluted_valuation,
        circulating_supply,
        total_supply,
        CASE 
            WHEN fully_diluted_valuation IS NULL OR fully_diluted_valuation = 0 THEN NULL
            ELSE market_cap / fully_diluted_valuation * 100
        END AS market_cap_to_fdv_pct,
        CASE
            WHEN total_supply IS NULL OR total_supply = 0 THEN NULL
            ELSE circulating_supply / total_supply * 100
        END AS circulating_supply_pct,
        roi_currency,
        roi_percentage,
        roi_times,
        last_updated
    FROM {{ ref('st_crypto_model') }}
)

SELECT *
FROM final

{% if is_incremental() %}
  -- Only include new or updated records
  WHERE DATE_TRUNC('day', last_updated) > (
      SELECT COALESCE(MAX(snapshot_date), '1900-01-01')
      FROM {{ this }}
  )
{% endif %}
