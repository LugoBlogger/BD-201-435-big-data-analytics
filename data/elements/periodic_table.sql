SELECT "Period", COUNT(*) 
  FROM periodic_table
  WHERE "Phase" = 'liq'
  GROUP BY "Period";