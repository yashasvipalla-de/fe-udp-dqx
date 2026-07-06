SELECT
    ACCOUNT_ID,
    EMAIL_ADDRESS,
    EMAIL_STATUS,
    PREFERRED_EMAIL_FLAG,
    UPDATED_AT,
    current_timestamp() AS processed_time
FROM __bronze__.customer_email_contact