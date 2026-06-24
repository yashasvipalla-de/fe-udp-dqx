SELECT
    VKONT,
    SMTP_ADDR,
    current_timestamp() AS processed_time
FROM __bronze__.sapeast_ZCC_ACCNT_EMAIL