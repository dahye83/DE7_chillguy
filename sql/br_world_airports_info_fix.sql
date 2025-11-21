-- BROZE.BR_WORLD_AIRPORTS_INFO_STATIC 업데이트 및 추가 사항 --
/* 기존의 FRU -> BSZ 변경(최신반영)*/
UPDATE BR_WORLD_AIRPORTS_INFO_STATIC
SET IATA_CODE = 'BSZ'
WHERE IATA_CODE = 'FRU'

/* 화롄(HUN) 공항 추가 */
INSERT INTO BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC
VALUES ('HUN', 'RCYU', '화롄 공항', 'Hualien Airport', '아시아태평양',
        '대만', 'Taiwan', '화롄', 'Hualien', 24.0231, 121.6170);

/* 케언스(CNS) 공항 추가 */
INSERT INTO BRONZE.BR_WORLD_AIRPORTS_INFO_STATIC
VALUES ('CNS', 'YBCS', '케언스 공항', 'Cairns Airport', '오세아니아',
        '호주', 'Australia', '케언스', 'Cairns', -16.8858, 145.7553);