-- creates materialized views on top of streaming tables. CDC table cannot be a temporary view because CDC needs persistent state.
-- Alternative way - use separate schema in silver layer to create straming tables from operations and build materialized views on top of streaming tables in gold layer.
SELECT
    ACCOUNT_ID,
    EMAIL_ADDRESS,
    EMAIL_STATUS,
    PREFERRED_EMAIL_FLAG,
    UPDATED_AT,
    processed_time
FROM __silver__.customer_email_contact

-- If we don't want to use create_auto_cdc_flow which creates streaming tables.

-- WITH ranked AS (
--     SELECT
--         *,
--         ROW_NUMBER() OVER (
--             PARTITION BY ACCOUNT_ID
--             ORDER BY UPDATED_AT DESC
--         ) AS rn
--     FROM __silver__.customer_email_contact
-- )
-- SELECT *
-- FROM ranked
-- WHERE rn = 1