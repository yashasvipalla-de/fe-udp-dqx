SELECT
    VKONT,
    SMTP_ADDR,
    current_timestamp() AS processed_time
FROM __bronze__.sapwest_ZCC_ACCNT_EMAIL