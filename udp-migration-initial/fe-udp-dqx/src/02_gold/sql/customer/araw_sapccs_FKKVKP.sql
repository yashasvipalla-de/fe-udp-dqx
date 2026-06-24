SELECT
    'C1' AS SOURCE_SYSTEM,
    VKONT,
    GPART,
    processed_time
FROM __silver__.sapeast_FKKVKP

UNION ALL

SELECT
    'C2' AS SOURCE_SYSTEM,
    VKONT,
    GPART,
    processed_time
FROM __silver__.sapwest_FKKVKP