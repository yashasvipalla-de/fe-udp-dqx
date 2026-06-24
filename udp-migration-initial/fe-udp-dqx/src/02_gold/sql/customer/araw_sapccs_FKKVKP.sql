SELECT
    'C1' AS SOURCE_SYSTEM,
    *
FROM ss_silver.sapeast_FKKVKP

UNION ALL

SELECT
    'C2' AS SOURCE_SYSTEM,
    *
FROM ss_silver.sapwest_FKKVKP