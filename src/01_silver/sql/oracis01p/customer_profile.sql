SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    ORG_NAME,
    CUSTOMER_TYPE,
    current_timestamp() AS processed_time
FROM __bronze__.customer_profile