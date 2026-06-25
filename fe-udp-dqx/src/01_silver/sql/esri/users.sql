SELECT 
  user_id,
  email,
  age,
  current_timestamp() as processed_time
FROM 
  __bronze__.users
