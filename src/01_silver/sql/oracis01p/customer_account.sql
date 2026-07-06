SELECT
    ACCOUNT_ID,
    CUSTOMER_ID,
    ACCOUNT_STATUS,
    SERVICE_STATE,
    OPEN_DATE,
    current_timestamp() AS processed_time
FROM __bronze__.customer_account