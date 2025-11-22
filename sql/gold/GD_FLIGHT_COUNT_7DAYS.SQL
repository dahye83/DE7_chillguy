CREATE OR REPLACE TABLE team_7_chill.GOLD.GD_FLIGHT_COUNT_7DAYS as
SELECT '인천' as DEPARTURE
     , NATION_NM_KOR
     , DESTINATION
     , COUNT(*) as FLIGHT_CNT
     , 'DESTINATION' AS LEVEL
FROM team_7_chill.silver.sl_airport_info_7days
GROUP BY 2, 3

UNION ALL

SELECT '인천' as DEPARTURE
     , NATION_NM_KOR
     , NULL AS DESTINATION
     , COUNT(*) as FLIGHT_CNT
     , 'NATION' AS LEVEL
FROM team_7_chill.silver.sl_airport_info_7days
GROUP BY 2;