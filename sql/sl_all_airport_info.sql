CREATE OR REPLACE TABLE SILVER.SL_ALL_AIRPORT_INFO AS
WITH yearly AS (
    SELECT
        /* FLIGHT_DATE: NUMBER → DATE 변환 */
        TO_DATE(TO_VARCHAR(s.FLIGHT_DATE), 'YYYYMMDD') AS DEPARTURE_DATE,
        s.SCHEDULED_TIME AS DEPARTURE_TM,
        s.AIRLINE,
        s.FLIGHT_NUMBER AS FLIGHT_CD,
        s.DESTINATION,

        /* 세계 공항 정보 매핑 */
        w.IATA_CODE AS IATA_CD,
        w.AIRPORT_NAME_ENG,
        w.AIRPORT_NAME_KOR,
        w.NATION_NAME_ENG,
        w.NATION_NAME_KOR,
        w.CITY_ENG,
        w.CITY_KOR,
        w.LATITUDE,
        w.LONGITUDE
    FROM BRONZE.BR_AIRPORT_SCHEDULE_STATIC_YEARLY s
    LEFT JOIN BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC w
        ON UPPER(s.DESTINATION) = UPPER(w.CITY_KOR)
),
week7 AS (
    SELECT
        s.DATE AS DEPARTURE_DATE,
        s.TIME AS DEPARTURE_TM,
        s.AIRLINE,
        s.FLIGHT AS FLIGHT_CD,
        s.AIRPORT AS DESTINATION,
        s.IATA_CODE AS IATA_CD,
        
        /* 세계 공항 정보 매핑 */
        w.AIRPORT_NAME_ENG,
        w.AIRPORT_NAME_KOR,
        w.NATION_NAME_ENG,
        w.NATION_NAME_KOR,
        w.CITY_ENG,
        w.CITY_KOR,
        w.LATITUDE,
        w.LONGITUDE
    FROM BRONZE.BR_AIRPORT_SCHEDULE_7_DAILY s
    LEFT JOIN BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC w
        ON UPPER(s.IATA_CODE) = UPPER(w.IATA_CODE)
),

/*중복도 포함된 "브론즈 통합 원본" */
all_flights AS (
    SELECT * FROM yearly
    UNION ALL
    SELECT * FROM week7
),

dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            /*중복 기준-> 5개 컬럼으로 판단*/
            PARTITION BY 
                DEPARTURE_DATE, --날짜--
                DEPARTURE_TM,   --출발 시간--
                AIRLINE,        --항공사--
                FLIGHT_CD,      --운항편--
                IATA_CD         --IATA 코드--
            ORDER BY DEPARTURE_DATE
        ) AS rn
    FROM all_flights
)

/*중복된 동일 비행기 기록 중 가장 처음 1개만 남기고 나머지 삭제, rn 컬럼 뺴고*/
SELECT 
    DEPARTURE_DATE,
    DEPARTURE_TM,
    AIRLINE,
    FLIGHT_CD,
    DESTINATION,
    IATA_CD,
    AIRPORT_NAME_ENG,
    AIRPORT_NAME_KOR,
    NATION_NAME_ENG,
    NATION_NAME_KOR,
    CITY_ENG,
    CITY_KOR,
    LATITUDE,
    LONGITUDE,
FROM dedup
WHERE rn = 1; --중복된 동일 비행기 기록 중 가장 처음 1개만 남기고 나머지 삭제--
