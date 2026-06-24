SELECT
  user_id                            AS user_key,
  email                              AS email_address,
  age                                AS user_age,
  CAST(processed_time AS DATE)       AS processed_date
FROM
  __silver__.users
